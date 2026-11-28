package zmux

import (
	"bytes"
	"context"
	"errors"
	"log/slog"

	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"time"

	"github.com/zmuxio/zmux-go/internal/core"
	"github.com/zmuxio/zmux-go/internal/poolhost"
	"github.com/zmuxio/zmux-go/shaping"
)

// closeCountConn counts the closes of a carrier.
type closeCountConn struct {
	net.Conn
	closes atomic.Int32
}

func (c *closeCountConn) Close() error {
	c.closes.Add(1)
	return c.Conn.Close()
}

// A carrier handed to zmux is closed by it, exactly once, whatever the
// failure: an invalid configuration, a missing secret source, a server
// without a Listener, a secure first pipe with a PipeAuth source.
func TestOwnershipOnFailure(t *testing.T) {
	psk := bytes.Repeat([]byte{1}, 32)
	for _, c := range []struct {
		start func(ctx context.Context, nc net.Conn) error
		name  string
	}{
		{name: "invalid config", start: func(ctx context.Context, nc net.Conn) error {
			_, err := Client(ctx, nc, &Config{JoinTimeout: -1})
			return err
		}},
		{name: "multipath without a source", start: func(ctx context.Context, nc net.Conn) error {
			_, err := Client(ctx, nc, &Config{SessionPolicy: SessionPolicy{Limits: CapacityLimits{SteadyAttachments: 2}}})
			return err
		}},
		{name: "resume without a source", start: func(ctx context.Context, nc net.Conn) error {
			_, err := Client(ctx, nc, &Config{ResumeTimeout: time.Second})
			return err
		}},
		{name: "EndToEnd only", start: func(ctx context.Context, nc net.Conn) error {
			_, err := Client(ctx, nc, &Config{SessionPolicy: SessionPolicy{Limits: CapacityLimits{SteadyAttachments: 2}}, PipeAuth: PipeAuth{EndToEnd: true}})
			return err
		}},
		{name: "Server with SteadyAttachments", start: func(ctx context.Context, nc net.Conn) error {
			_, err := Server(ctx, nc, &Config{SessionPolicy: SessionPolicy{Limits: CapacityLimits{SteadyAttachments: 2}}, PipeAuth: PipeAuth{PSK: psk}})
			return err
		}},
		{name: "Server with ResumeTimeout", start: func(ctx context.Context, nc net.Conn) error {
			_, err := Server(ctx, nc, &Config{ResumeTimeout: time.Second, PipeAuth: PipeAuth{PSK: psk}})
			return err
		}},
		{name: "RoleAuto with SteadyAttachments", start: func(ctx context.Context, nc net.Conn) error {
			_, err := Start(ctx, NetPipe(nc), RoleAuto, &Config{SessionPolicy: SessionPolicy{Limits: CapacityLimits{SteadyAttachments: 2}}, PipeAuth: PipeAuth{PSK: psk}})
			return err
		}},
		{name: "secure first pipe with a PSK", start: func(ctx context.Context, nc net.Conn) error {
			_, err := Start(ctx, secureTestPipe(nc), RoleClient, &Config{SessionPolicy: SessionPolicy{Limits: CapacityLimits{SteadyAttachments: 2}}, PipeAuth: PipeAuth{PSK: psk}})
			return err
		}},
		{name: "secure first pipe with a deferred PSK", start: func(ctx context.Context, nc net.Conn) error {
			_, err := Start(ctx, secureTestPipe(nc), RoleClient, &Config{SessionPolicy: SessionPolicy{Limits: CapacityLimits{SteadyAttachments: 2}}, PipeAuth: PipeAuth{DeferredPSK: true}})
			return err
		}},
		{name: "secure first pipe with the exporter", start: func(ctx context.Context, nc net.Conn) error {
			_, err := Start(ctx, secureTestPipe(nc), RoleClient, &Config{SessionPolicy: SessionPolicy{Limits: CapacityLimits{SteadyAttachments: 2}}, PipeAuth: PipeAuth{Exporter: true}})
			return err
		}},
	} {
		a, b := net.Pipe()
		cc := &closeCountConn{Conn: a}
		err := c.start(context.Background(), cc)
		_ = b.Close()
		if !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("%s: %v, want ErrInvalidConfig", c.name, err)
		}
		if n := cc.closes.Load(); n != 1 {
			t.Errorf("%s: carrier closed %d times, want once", c.name, n)
		}
	}
}

// secureTestConn declares a secure first pipe: a record mode and an
// exporter, as a secure-carrier transform's halves do.
type secureTestConn struct{ net.Conn }

func (secureTestConn) RecordMode() RecordMode { return RecordSealed }
func (secureTestConn) ExportKeyingMaterial(_ string, _ []byte, n int) ([]byte, error) {
	return make([]byte, n), nil
}

func secureTestPipe(nc net.Conn) PipeIO {
	s := secureTestConn{nc}
	return PipeIO{R: s, W: s, Framing: FramingRecords}
}

// On a secure first pipe, PipeAuth sets no source and EndToEnd is
// accepted: multipath runs on the secure carrier's secret.
func TestSecureFirstPipeEndToEnd(t *testing.T) {
	for _, auth := range []PipeAuth{{}, {EndToEnd: true}} {
		cfg := &Config{SessionPolicy: SessionPolicy{Limits: CapacityLimits{SteadyAttachments: 2}}, PipeAuth: auth}
		cc, err := cfg.coreConfig(core.RoleInitiator, core.RoleInitiator)
		if err != nil {
			t.Fatal(err)
		}
		a, b := net.Pipe()
		p := secureTestPipe(a)
		c, err := setupCore(t.Context(), p, cfg, cc, nil, nil, nil, nil)
		if errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("%+v: %v", auth, err)
		}
		_ = b.Close()
		if c != nil {
			_ = c.Close()
			_ = c.Wait()
		} else {
			ClosePipeIO(p)
		}
	}
}

// NewListener validates the whole configuration and returns the error.
func TestNewListenerInvalid(t *testing.T) {
	for _, c := range []struct {
		name string
		cfg  ListenerConfig
	}{
		{name: "long InstanceID", cfg: ListenerConfig{InstanceID: make([]byte, MaxInstanceIDLen+1)}},
		{name: "negative count", cfg: ListenerConfig{Backlog: -1}},
		{name: "invalid session Config", cfg: ListenerConfig{Config: &Config{JoinTimeout: -1}}},
		{name: "invalid Admission", cfg: ListenerConfig{Admission: &Admission{}}},
		{name: "invalid WrapPipe", cfg: ListenerConfig{WrapPipe: invalidWrapper{}}},
	} {
		l, err := NewListener(&c.cfg)
		if l != nil || !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("%s: %v", c.name, err)
		}
	}
}

type invalidWrapper struct{}

func (invalidWrapper) WrapPipe(_ context.Context, p PipeIO, _ PipeHint) (PipeIO, error) {
	return p, nil
}
func (invalidWrapper) Validate() error { return ErrInvalidConfig }

// logRecorder is a slog.Handler that keeps the messages.
type logRecorder struct {
	msgs  []string
	attrs []string // the values of the "callback" attribute
	mu    sync.Mutex
}

func (r *logRecorder) Enabled(_ context.Context, _ slog.Level) bool { return true }
func (r *logRecorder) Handle(_ context.Context, rec slog.Record) error {
	r.mu.Lock()
	r.msgs = append(r.msgs, rec.Message)
	rec.Attrs(func(a slog.Attr) bool {
		if a.Key == "callback" {
			r.attrs = append(r.attrs, a.Value.String())
		}
		return true
	})
	r.mu.Unlock()
	return nil
}
func (r *logRecorder) WithAttrs(_ []slog.Attr) slog.Handler { return r }
func (r *logRecorder) WithGroup(_ string) slog.Handler      { return r }

// ErrJoinPending says what is missing.
func TestJoinPendingText(t *testing.T) {
	for _, c := range []struct {
		want string
		jp   core.JoinPendingError
	}{
		{jp: core.JoinPendingError{Declare: true}, want: "DeclareEndToEnd"},
		{jp: core.JoinPendingError{Secret: true}, want: "SetJoinSecret"},
		{jp: core.JoinPendingError{Declare: true, Secret: true}, want: "DeclareEndToEnd) and the deferred PSK"},
	} {
		err := apiErr(&c.jp)
		if !errors.Is(err, ErrJoinPending) || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%+v: %v", c.jp, err)
		}
	}
}

// rejectPolicy sets a decision and rejects the look.

// Contained panics of host callbacks reach Config.Logger.
func TestLoggerContainedPanics(t *testing.T) {
	rec := &logRecorder{}
	l := slog.New(rec)
	_, _ = (&ShapingPolicy{Budget: func() shaping.Budget { panic("budget") }}).resolve(l)
	poolhost.ReportRejected(l, func(error) { panic("rejected") }, ErrInvalidConfig)
	for _, w := range []string{"ShapingPolicy.Budget", "DecisionRejected"} {
		found := false
		rec.mu.Lock()
		for _, a := range rec.attrs {
			found = found || a == w
		}
		rec.mu.Unlock()
		if !found {
			t.Errorf("%s not logged: %q", w, rec.attrs)
		}
	}
}
