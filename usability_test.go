package zmux

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"math"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/djylb/timeshim"

	"github.com/zmuxio/zmux-go/internal/core"
	"github.com/zmuxio/zmux-go/internal/poolhost"
	"github.com/zmuxio/zmux-go/shaping"
)

// Values out of range are configuration errors, never corrected.
func TestConfigBounds(t *testing.T) {
	psk := PipeAuth{PSK: bytes.Repeat([]byte{1}, 32)}
	for _, c := range []struct {
		cfg  Config
		name string
		role core.Role
		ok   bool
	}{
		{name: "SendBufferMax below SendBuffer", cfg: Config{SendBuffer: 8 << 20, SendBufferMax: 4 << 20}, role: core.RoleInitiator, ok: false},
		{name: "SendBufferMax below the default SendBuffer", cfg: Config{SendBufferMax: 1 << 20}, role: core.RoleInitiator, ok: false},
		{name: "SendBufferMax at SendBuffer", cfg: Config{SendBuffer: 4 << 20, SendBufferMax: 4 << 20}, role: core.RoleInitiator, ok: true},
		{name: "ReplayWindow below MaxPipes+H", cfg: Config{MaxPipes: 4, ReplayWindow: 11, PipeAuth: psk}, role: core.RoleInitiator, ok: false},
		{name: "ReplayWindow at MaxPipes+H", cfg: Config{MaxPipes: 4, ReplayWindow: 12, PipeAuth: psk}, role: core.RoleInitiator, ok: true},
		{name: "positive submillisecond ResumeTimeout", cfg: Config{ResumeTimeout: time.Millisecond - 1, PipeAuth: psk}, role: core.RoleInitiator, ok: false},
		{name: "negative SpreadChunk", cfg: Config{SchedulerParams: SchedulerParams{SpreadChunk: -1}}, role: core.RoleInitiator, ok: false},
		{name: "oversized SpreadChunk", cfg: Config{SchedulerParams: SchedulerParams{SpreadChunk: math.MaxInt32 + 1}}, role: core.RoleInitiator, ok: false},
		{name: "negative ResumeTimeout", cfg: Config{ResumeTimeout: -1}, role: core.RoleInitiator, ok: false},
		{name: "negative JoinTimeout", cfg: Config{JoinTimeout: -1}, role: core.RoleInitiator, ok: false},
		{name: "negative JoinEarlyHello", cfg: Config{JoinEarlyHello: -1}, role: core.RoleInitiator, ok: true},
		{name: "negative RedialMinBackoff", cfg: Config{RedialBackoff: Backoff{Min: -1}}, role: core.RoleInitiator, ok: false},
		{name: "RedialMinBackoff above the default maximum", cfg: Config{RedialBackoff: Backoff{Min: 2 * time.Minute}}, role: core.RoleInitiator, ok: false},
		{name: "RedialMinBackoff above RedialMaxBackoff", cfg: Config{RedialBackoff: Backoff{Min: 5 * time.Second, Max: 2 * time.Second}}, role: core.RoleInitiator, ok: false},
		{name: "RedialMaxBackoff below the default minimum", cfg: Config{RedialBackoff: Backoff{Max: 500 * time.Millisecond}}, role: core.RoleInitiator, ok: false},
		{name: "redial backoff bounds", cfg: Config{RedialBackoff: Backoff{Min: 2 * time.Second, Max: 2 * time.Second}}, role: core.RoleInitiator, ok: true},
		{name: "ZmuxPeer with RoleAuto", cfg: Config{EarlyData: EarlyData{ZmuxPeer: true}}, role: core.RoleAuto, ok: false},
		{name: "ZmuxPeer as a client", cfg: Config{EarlyData: EarlyData{ZmuxPeer: true}}, role: core.RoleInitiator, ok: true},
	} {
		_, err := c.cfg.coreConfig(c.role, c.role)
		if c.ok && err != nil || !c.ok && !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("%s: %v", c.name, err)
		}
	}
	for _, tc := range []struct {
		duration time.Duration
		wire     uint64
	}{
		{time.Millisecond, 1},
		{1500 * time.Microsecond, 2},
		{math.MaxInt64, 9_223_372_036_855},
	} {
		cfg := Config{MaxIdleTimeout: tc.duration, ResumeTimeout: tc.duration, PipeAuth: psk}
		cc, err := cfg.coreConfig(core.RoleInitiator, core.RoleInitiator)
		if err != nil {
			t.Fatalf("timeout %v: %v", tc.duration, err)
		}
		if cc.Local.MaxIdleTimeout != tc.wire || cc.Local.ResumeGrace != tc.wire ||
			cc.IdleTimeout != int64(tc.duration) || cc.Resume != int64(tc.duration) {
			t.Errorf("timeout %v: idle %d/%d, resume %d/%d, error %v",
				tc.duration, cc.Local.MaxIdleTimeout, cc.IdleTimeout, cc.Local.ResumeGrace, cc.Resume, err)
		}
	}
	for _, chunk := range []int64{0, 1, math.MaxInt32} {
		cfg := Config{SchedulerParams: SchedulerParams{SpreadChunk: chunk}}
		cc, err := cfg.coreConfig(core.RoleInitiator, core.RoleInitiator)
		if err != nil {
			t.Fatalf("SpreadChunk %d: %v", chunk, err)
		}
		want := int(chunk)
		if chunk == 0 {
			want = core.DefaultConfig().SpreadChunk
		}
		if cc.SpreadChunk != want {
			t.Errorf("SpreadChunk %d: got %d, %v, want %d", chunk, cc.SpreadChunk, err, want)
		}
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
		start func(ctx context.Context, nc net.Conn) error
		name  string
	}{
		{name: "invalid config", start: func(ctx context.Context, nc net.Conn) error {
			_, err := Client(ctx, nc, &Config{JoinTimeout: -1})
			return err
		}},
		{name: "multipath without a source", start: func(ctx context.Context, nc net.Conn) error {
			_, err := Client(ctx, nc, &Config{MaxPipes: 2})
			return err
		}},
		{name: "resume without a source", start: func(ctx context.Context, nc net.Conn) error {
			_, err := Client(ctx, nc, &Config{ResumeTimeout: time.Second})
			return err
		}},
		{name: "EndToEnd only", start: func(ctx context.Context, nc net.Conn) error {
			_, err := Client(ctx, nc, &Config{MaxPipes: 2, PipeAuth: PipeAuth{EndToEnd: true}})
			return err
		}},
		{name: "Server with MaxPipes", start: func(ctx context.Context, nc net.Conn) error {
			_, err := Server(ctx, nc, &Config{MaxPipes: 2, PipeAuth: PipeAuth{PSK: psk}})
			return err
		}},
		{name: "Server with ResumeTimeout", start: func(ctx context.Context, nc net.Conn) error {
			_, err := Server(ctx, nc, &Config{ResumeTimeout: time.Second, PipeAuth: PipeAuth{PSK: psk}})
			return err
		}},
		{name: "RoleAuto with MaxPipes", start: func(ctx context.Context, nc net.Conn) error {
			_, err := Start(ctx, NetPipe(nc), RoleAuto, &Config{MaxPipes: 2, PipeAuth: PipeAuth{PSK: psk}})
			return err
		}},
		{name: "secure first pipe with a PSK", start: func(ctx context.Context, nc net.Conn) error {
			_, err := Start(ctx, secureTestPipe(nc), RoleClient, &Config{MaxPipes: 2, PipeAuth: PipeAuth{PSK: psk}})
			return err
		}},
		{name: "secure first pipe with a deferred PSK", start: func(ctx context.Context, nc net.Conn) error {
			_, err := Start(ctx, secureTestPipe(nc), RoleClient, &Config{MaxPipes: 2, PipeAuth: PipeAuth{DeferredPSK: true}})
			return err
		}},
		{name: "secure first pipe with the exporter", start: func(ctx context.Context, nc net.Conn) error {
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
		cfg := &Config{MaxPipes: 2, PipeAuth: auth}
		cc, err := cfg.coreConfig(core.RoleInitiator, core.RoleInitiator)
		if err != nil {
			t.Fatal(err)
		}
		a, b := net.Pipe()
		p := secureTestPipe(a)
		c, err := setupCore(t.Context(), p, cfg, cc, nil, nil, nil)
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
		cfg  ListenerConfig
		name string
	}{
		{name: "long InstanceID", cfg: ListenerConfig{InstanceID: make([]byte, MaxInstanceIDLen+1)}},
		{name: "negative count", cfg: ListenerConfig{Backlog: -1}},
		{name: "negative per-session count", cfg: ListenerConfig{MaxPendingPerSession: -1}},
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

// The Redial backoff starts at RedialBackoff.Min, doubles up to
// RedialBackoff.Max, and counts from the start of the previous call.
func TestRedialBackoffSequence(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var mu sync.Mutex
		var calls []time.Time
		cfg := mpConfig()
		cfg.Clock = timeshim.Real
		cfg.ResumeTimeout = 10 * time.Minute
		cfg.RedialBackoff = Backoff{Min: time.Second, Max: 8 * time.Second}
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

// TestPoolDecisionRejectedLogged: a policy that panics or decides out of
// range is rejected with an error, reset to the zero decision on a panic,
// and the error goes to DecisionRejected, or to the logger without one.
func TestPoolDecisionRejectedLogged(t *testing.T) {
	var d PoolDecision
	err := decide(policyFunc(func(_ *PoolView, x *PoolDecision) { x.Count = 3; panic("decide") }), &PoolView{}, &d)
	if err == nil || !strings.Contains(err.Error(), "PoolPolicy.Decide") || d.Count != 0 {
		t.Fatalf("panic: %v, %+v", err, d)
	}
	errNo := errors.New("no")
	err = decide(rejectPolicy{errNo}, &PoolView{}, &d)
	if !errors.Is(err, errNo) || d.Count != 0 {
		t.Fatalf("returned error: %v, %+v", err, d)
	}
	err = decide(policyFunc(func(_ *PoolView, x *PoolDecision) { x.Recheck = -1 }), &PoolView{}, &d)
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("out of range: %v", err)
	}
	rec := &logRecorder{}
	var got error
	poolhost.ReportRejected(slog.New(rec), func(e error) { got = e }, err)
	if !errors.Is(got, err) || len(rec.msgs) != 0 {
		t.Fatalf("callback %v, logged %q", got, rec.msgs)
	}
	poolhost.ReportRejected(slog.New(rec), nil, err)
	if !rec.has("rejected") {
		t.Fatalf("not logged: %q", rec.msgs)
	}
	poolhost.ReportRejected(nil, nil, err) // no logger: nothing
}

// rejectPolicy sets a decision and rejects the look.
type rejectPolicy struct{ err error }

func (p rejectPolicy) Decide(_ *PoolView, d *PoolDecision) error { d.Count = 3; return p.err }

// Contained panics of host callbacks reach Config.Logger.
func TestLoggerContainedPanics(t *testing.T) {
	rec := &logRecorder{}
	l := slog.New(rec)
	_ = askBudget(l, func() shaping.Budget { panic("budget") })
	poolhost.ReportRejected(l, func(error) { panic("rejected") }, ErrInvalidConfig)
	for _, w := range []string{"Config.ShapingBudget", "DecisionRejected"} {
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
