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
	"testing/synctest"
	"time"

	"github.com/djylb/timeshim"

	"github.com/zmuxio/zmux-go/internal/core"
	"github.com/zmuxio/zmux-go/shaping"
)

// Values out of range are configuration errors, never corrected.
func TestConfigBounds(t *testing.T) {
	psk := PipeAuth{PSK: bytes.Repeat([]byte{1}, 32)}
	for _, c := range []struct {
		name string
		cfg  Config
		role core.Role
		ok   bool
	}{
		{"MaxSendBuffer below SendBuffer", Config{SendBuffer: 8 << 20, MaxSendBuffer: 4 << 20}, core.RoleInitiator, false},
		{"MaxSendBuffer below the default SendBuffer", Config{MaxSendBuffer: 1 << 20}, core.RoleInitiator, false},
		{"MaxSendBuffer at SendBuffer", Config{SendBuffer: 4 << 20, MaxSendBuffer: 4 << 20}, core.RoleInitiator, true},
		{"ReplayWindow below MaxPipes+H", Config{MaxPipes: 4, ReplayWindow: 11, PipeAuth: psk}, core.RoleInitiator, false},
		{"ReplayWindow at MaxPipes+H", Config{MaxPipes: 4, ReplayWindow: 12, PipeAuth: psk}, core.RoleInitiator, true},
		{"negative ResumeTimeout", Config{ResumeTimeout: -1}, core.RoleInitiator, false},
		{"negative JoinTimeout", Config{JoinTimeout: -1}, core.RoleInitiator, false},
		{"negative JoinEarlyHello", Config{JoinEarlyHello: -1}, core.RoleInitiator, false},
		{"JoinEarlyHello with NoJoinEarlyHello", Config{JoinEarlyHello: 1024, NoJoinEarlyHello: true}, core.RoleInitiator, false},
		{"NoJoinEarlyHello", Config{NoJoinEarlyHello: true}, core.RoleInitiator, true},
		{"negative RedialMinBackoff", Config{RedialMinBackoff: -1}, core.RoleInitiator, false},
		{"RedialMinBackoff above the default maximum", Config{RedialMinBackoff: 2 * time.Minute}, core.RoleInitiator, false},
		{"RedialMinBackoff above RedialMaxBackoff", Config{RedialMinBackoff: 5 * time.Second, RedialMaxBackoff: 2 * time.Second}, core.RoleInitiator, false},
		{"RedialMaxBackoff below the default minimum", Config{RedialMaxBackoff: 500 * time.Millisecond}, core.RoleInitiator, false},
		{"redial backoff bounds", Config{RedialMinBackoff: 2 * time.Second, RedialMaxBackoff: 2 * time.Second}, core.RoleInitiator, true},
		{"ZmuxPeer with RoleAuto", Config{EarlyData: EarlyData{ZmuxPeer: true}}, core.RoleAuto, false},
		{"ZmuxPeer as a client", Config{EarlyData: EarlyData{ZmuxPeer: true}}, core.RoleInitiator, true},
	} {
		_, err := c.cfg.coreConfig(c.role, c.role)
		if c.ok && err != nil || !c.ok && !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("%s: %v", c.name, err)
		}
	}
	// A fraction of a millisecond of the grace period is rounded up (the
	// unit of the parameter), not an error.
	cc, err := (&Config{ResumeTimeout: 1500 * time.Microsecond, PipeAuth: psk}).coreConfig(core.RoleInitiator, core.RoleInitiator)
	if err != nil {
		t.Fatalf("ResumeTimeout 1.5 ms: %v", err)
	}
	if cc.Local.ResumeGrace != 2 {
		t.Errorf("ResumeTimeout 1.5 ms: grace %d", cc.Local.ResumeGrace)
	}
}

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
		name  string
		start func(ctx context.Context, nc net.Conn) error
	}{
		{"invalid config", func(ctx context.Context, nc net.Conn) error {
			_, err := Client(ctx, nc, &Config{JoinTimeout: -1})
			return err
		}},
		{"multipath without a source", func(ctx context.Context, nc net.Conn) error {
			_, err := Client(ctx, nc, &Config{MaxPipes: 2})
			return err
		}},
		{"resume without a source", func(ctx context.Context, nc net.Conn) error {
			_, err := Client(ctx, nc, &Config{ResumeTimeout: time.Second})
			return err
		}},
		{"EndToEnd only", func(ctx context.Context, nc net.Conn) error {
			_, err := Client(ctx, nc, &Config{MaxPipes: 2, PipeAuth: PipeAuth{EndToEnd: true}})
			return err
		}},
		{"Server with MaxPipes", func(ctx context.Context, nc net.Conn) error {
			_, err := Server(ctx, nc, &Config{MaxPipes: 2, PipeAuth: PipeAuth{PSK: psk}})
			return err
		}},
		{"Server with ResumeTimeout", func(ctx context.Context, nc net.Conn) error {
			_, err := Server(ctx, nc, &Config{ResumeTimeout: time.Second, PipeAuth: PipeAuth{PSK: psk}})
			return err
		}},
		{"RoleAuto with MaxPipes", func(ctx context.Context, nc net.Conn) error {
			_, err := Start(ctx, NetPipe(nc), RoleAuto, &Config{MaxPipes: 2, PipeAuth: PipeAuth{PSK: psk}})
			return err
		}},
		{"secure first pipe with a PSK", func(ctx context.Context, nc net.Conn) error {
			_, err := Start(ctx, secureTestPipe(nc), RoleClient, &Config{MaxPipes: 2, PipeAuth: PipeAuth{PSK: psk}})
			return err
		}},
		{"secure first pipe with a deferred PSK", func(ctx context.Context, nc net.Conn) error {
			_, err := Start(ctx, secureTestPipe(nc), RoleClient, &Config{MaxPipes: 2, PipeAuth: PipeAuth{DeferredPSK: true}})
			return err
		}},
		{"secure first pipe with the exporter", func(ctx context.Context, nc net.Conn) error {
			_, err := Start(ctx, secureTestPipe(nc), RoleClient, &Config{MaxPipes: 2, PipeAuth: PipeAuth{Exporter: true}})
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

func (secureTestConn) RecordMode() RecordMode { return Sealed }
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
		cfg := &Config{MaxPipes: 2, PipeAuth: auth}
		cc, err := cfg.coreConfig(core.RoleInitiator, core.RoleInitiator)
		if err != nil {
			t.Fatal(err)
		}
		a, b := net.Pipe()
		p := secureTestPipe(a)
		c, err := setupCore(t.Context(), p, cfg, cc, nil, nil)
		if errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("%+v: %v", auth, err)
		}
		_ = b.Close()
		if c != nil {
			_ = c.Close()
			_ = c.Wait()
		} else {
			closePipeIO(p)
		}
	}
}

// NewListener validates the whole configuration and returns the error.
func TestNewListenerInvalid(t *testing.T) {
	for _, c := range []struct {
		name string
		cfg  ListenerConfig
	}{
		{"long InstanceID", ListenerConfig{InstanceID: make([]byte, MaxInstanceIDLen+1)}},
		{"negative count", ListenerConfig{Backlog: -1}},
		{"negative per-session count", ListenerConfig{MaxPendingPerSession: -1}},
		{"invalid session Config", ListenerConfig{Config: &Config{JoinTimeout: -1}}},
		{"invalid Admission", ListenerConfig{Admission: &Admission{}}},
		{"invalid WrapPipe", ListenerConfig{WrapPipe: invalidWrapper{}}},
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
	mu    sync.Mutex
	msgs  []string
	attrs []string // the values of the "callback" attribute
}

func (r *logRecorder) Enabled(context.Context, slog.Level) bool { return true }
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
func (r *logRecorder) WithAttrs([]slog.Attr) slog.Handler { return r }
func (r *logRecorder) WithGroup(string) slog.Handler      { return r }

func (r *logRecorder) has(sub string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, m := range r.msgs {
		if strings.Contains(m, sub) {
			return true
		}
	}
	return false
}

// A suspension of a session that can rejoin, with neither a Redial hook
// nor a running pool, is reported as EventResumeUnattended right after
// EventSessionSuspended and logged; removing the last pipe with a stream
// open is logged too.
func TestResumeUnattended(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rec := &logRecorder{}
		ccfg := mpConfig()
		ccfg.ResumeTimeout = 5 * time.Second
		ccfg.Logger = slog.New(rec)
		scfg := mpConfig()
		scfg.ResumeTimeout = 5 * time.Second
		e := newMemMPEnv(t, ccfg, scfg)
		st, err := e.client.OpenStreamSync(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		p0, _ := e.client.Pipe(0)
		if err := p0.Close(); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		var kinds []EventKind
		for len(kinds) < 2 {
			ev, err := e.client.NextEvent(ctx)
			if err != nil {
				t.Fatalf("events %v: %v", kinds, err)
			}
			if ev.Kind == EventSessionSuspended || ev.Kind == EventResumeUnattended {
				kinds = append(kinds, ev.Kind)
			}
		}
		if kinds[0] != EventSessionSuspended || kinds[1] != EventResumeUnattended {
			t.Fatalf("events %v", kinds)
		}
		if !e.client.ConnectionState().Suspended {
			t.Fatal("ConnectionState not suspended")
		}
		synctest.Wait()
		if !rec.has("without a redial source") || !rec.has("removed the last pipe") {
			t.Fatalf("log: %q", rec.msgs)
		}
		_ = st.Close()
	})
}

// A session with a running pool is attended: no EventResumeUnattended.
func TestResumeAttendedByPool(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ccfg := mpConfig()
		ccfg.ResumeTimeout = 5 * time.Second
		scfg := mpConfig()
		scfg.ResumeTimeout = 5 * time.Second
		e := newMemMPEnv(t, ccfg, scfg)
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan struct{})
		go func() { _ = e.client.MaintainPipes(ctx, PipePool{Dial: e.pipeDialer()}); close(done) }()
		defer func() { cancel(); <-done }()
		synctest.Wait()
		e.client.lock()
		attended := !e.client.unattendedLocked()
		e.client.unlock()
		if !attended {
			t.Fatal("a session with a running pool reported unattended")
		}
	})
}

// ErrJoinPending says what is missing.
func TestJoinPendingText(t *testing.T) {
	for _, c := range []struct {
		jp   core.JoinPendingError
		want string
	}{
		{core.JoinPendingError{Declare: true}, "DeclareEndToEnd"},
		{core.JoinPendingError{Secret: true}, "SetJoinSecret"},
		{core.JoinPendingError{Declare: true, Secret: true}, "DeclareEndToEnd) and the deferred PSK"},
	} {
		err := apiErr(&c.jp)
		if !errors.Is(err, ErrJoinPending) || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%+v: %v", c.jp, err)
		}
	}
}

// The Redial backoff starts at RedialMinBackoff, doubles up to
// RedialMaxBackoff, and counts from the start of the previous call.
func TestRedialBackoffSequence(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var mu sync.Mutex
		var calls []time.Time
		cfg := mpConfig()
		cfg.Clock = timeshim.Real
		cfg.ResumeTimeout = 10 * time.Minute
		cfg.RedialMinBackoff, cfg.RedialMaxBackoff = time.Second, 8*time.Second
		cfg.Redial = func(context.Context, Direction) (PipeIO, error) {
			mu.Lock()
			calls = append(calls, time.Now())
			mu.Unlock()
			return PipeIO{}, errors.New("down")
		}
		scfg := mpConfig()
		scfg.ResumeTimeout = 10 * time.Minute
		e := newMemMPEnv(t, cfg, scfg)
		p0, _ := e.client.Pipe(0)
		_ = p0.Close()
		time.Sleep(40 * time.Second)
		mu.Lock()
		defer mu.Unlock()
		if len(calls) < 6 {
			t.Fatalf("%d calls", len(calls))
		}
		// The waits are 1, 2, 4, 8, 8 s, each reduced by up to 0.2.
		want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 8 * time.Second}
		for i, w := range want {
			d := calls[i+1].Sub(calls[i])
			if d > w || d < w*8/10 {
				t.Errorf("wait %d: %v, want %v reduced by up to 0.2", i, d, w)
			}
		}
	})
}

// Contained panics of host callbacks reach Config.Logger.
func TestLoggerContainedPanics(t *testing.T) {
	rec := &logRecorder{}
	l := slog.New(rec)
	_ = askBudget(l, func() shaping.Budget { panic("budget") })
	var d PoolDecision
	_ = decide(l, policyFunc(func(*PoolView, *PoolDecision) { panic("decide") }), &PoolView{}, &d)
	reportRejected(l, func(error) { panic("rejected") }, ErrInvalidConfig)
	for _, w := range []string{"Config.ShapingBudget", "PoolPolicy.Decide", "DecisionRejected"} {
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
