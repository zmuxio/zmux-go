package zmux

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"log/slog"
	"net/netip"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/djylb/timeshim"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
	"github.com/zmuxio/zmux-go/shaping"
)

func TestSessionConfigSharesListenerCopy(t *testing.T) {
	base := &Config{MaxIncomingStreams: 8}
	lst := mustNewListener(&ListenerConfig{Config: base})
	defer func() { _ = lst.Close() }()
	base.MaxIncomingStreams = 16
	for range 2 {
		cfg, err := lst.sessionConfig(t.Context(), PipeIO{}, nil, netip.AddrPort{})
		if err != nil {
			t.Fatal(err)
		}
		if cfg != lst.cfg.Config || cfg.MaxIncomingStreams != 8 {
			t.Fatal("session did not get the listener's private Config")
		}
	}
}

// One listener, two kinds of session chosen per carrier: PipeAuth.Exporter
// where TLS ends here, PipeAuth.DeferredPSK otherwise, each with its own
// ShapingLimits; each joins with its own secret.
func TestSessionConfigMixed(t *testing.T) {
	leakcheck.Check(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	scfg, ccfg := selfSignedTLS(t)
	tlsLim, plainLim := shaping.Limits{Wire: &shaping.Account{Ceiling: 1, Initial: 1}}, shaping.Limits{Wire: &shaping.Account{Ceiling: 2, Initial: 2}}
	lst := mustNewListener(&ListenerConfig{Config: mpConfig(), SessionConfig: func(_ context.Context, info *CarrierInfo, _ any) (*Config, error) {
		if info.TLS != nil {
			c := exporterConfig()
			c.ShapingLimits = tlsLim
			return c, nil
		}
		c := deferredConfig()
		c.ShapingLimits = plainLim
		return c, nil
	}})
	defer func() { _ = lst.Close() }()
	tlsPair := func() *tls.Conn {
		a, b := tcpPair(t)
		go func() { _ = lst.HandlePipe(ctx, NetPipe(tls.Server(a, scfg)), PipeHint{}) }()
		return tls.Client(b, ccfg)
	}
	plainPair := func() PipeIO {
		a, b := tcpPair(t)
		go func() { _ = lst.HandlePipe(ctx, NetPipe(a), PipeHint{}) }()
		return NetPipe(b)
	}
	ce, err := Start(ctx, NetPipe(tlsPair()), RoleClient, exporterConfig())
	if err != nil {
		t.Fatal(err)
	}
	se, err := lst.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ce.Close(); _ = se.Close(); waitDone(t, ce); waitDone(t, se) }()
	echoOnce(t, ctx, ce, se)
	cd, err := Start(ctx, plainPair(), RoleClient, deferredConfig())
	if err != nil {
		t.Fatal(err)
	}
	sd, err := lst.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cd.Close(); _ = sd.Close(); waitDone(t, cd); waitDone(t, sd) }()
	echoOnce(t, ctx, cd, sd)
	if !reflect.DeepEqual(se.ShapingLimits(), tlsLim) || !reflect.DeepEqual(sd.ShapingLimits(), plainLim) {
		t.Fatalf("limits %+v, %+v", se.ShapingLimits(), sd.ShapingLimits())
	}
	if se.ConnectionState().JoinPending || !sd.ConnectionState().JoinPending {
		t.Fatal("PipeAuth not per session")
	}
	if err := se.SetJoinSecret(joinSecret); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("SetJoinSecret on the exporter session: %v", err)
	}
	if _, err := ce.AddPipe(ctx, NetPipe(tlsPair()), PipeOptions{}); err != nil {
		t.Fatalf("exporter join: %v", err)
	}
	mustSet(t, cd, joinSecret)
	mustSet(t, sd, joinSecret)
	if _, err := cd.AddPipe(ctx, plainPair(), PipeOptions{}); err != nil {
		t.Fatalf("deferred join: %v", err)
	}
	echoOnce(t, ctx, ce, se)
	echoOnce(t, ctx, cd, sd)
}

// nil selects Config; an error, a panic and a Config that breaks the
// listener's constraints or is invalid reject the carrier; an error and a
// panic reach the listener's Logger.
func TestSessionConfigOutcomes(t *testing.T) {
	leakcheck.Check(t)
	other := timeshim.Or(nil)
	for _, x := range []struct {
		f    func() (*Config, error)
		name string
		ok   bool
	}{
		{name: "nil", f: func() (*Config, error) { return nil, nil }, ok: true}, //nolint:nilnil // the case under test: no config and no error
		{name: "error", f: func() (*Config, error) { return nil, errors.New("no") }, ok: false},
		{name: "panic", f: func() (*Config, error) { panic("no") }, ok: false},
		{name: "clock", f: func() (*Config, error) { c := mpConfig(); c.ClockDomain = NewClockDomain(other); return c, nil }, ok: false},
		{name: "invalid", f: func() (*Config, error) {
			c := mpConfig()
			c.PipeAuth = PipeAuth{DeferredPSK: true, PSK: joinSecret}
			return c, nil
		}, ok: false},
	} {
		t.Run(x.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var log bytes.Buffer
			lst := mustNewListener(&ListenerConfig{Config: mpConfig(), Logger: slog.New(slog.NewTextHandler(&log, nil)), SessionConfig: func(context.Context, *CarrierInfo, any) (*Config, error) {
				return x.f()
			}})
			defer func() { _ = lst.Close() }()
			a, b := tcpPair(t)
			done := make(chan error, 1)
			go func() { done <- lst.HandlePipe(ctx, NetPipe(a), PipeHint{}) }()
			c, err := Start(ctx, NetPipe(b), RoleClient, mpConfig())
			if err == nil {
				defer func() { _ = c.Close(); waitDone(t, c) }()
			} else if x.ok {
				t.Fatal(err)
			}
			if x.ok {
				s, err := lst.Accept(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = s.Close(); waitDone(t, s) }()
				echoOnce(t, ctx, c, s)
				return
			}
			if err := <-done; err == nil {
				t.Fatal("carrier accepted")
			}
			if st := lst.Stats(); st.Rejected != 1 || st.Sessions != 0 {
				t.Fatalf("stats %+v", st)
			}
			logged := strings.Contains(log.String(), "ListenerConfig.SessionConfig")
			if want := x.name == "error" || x.name == "panic"; logged != want {
				t.Fatalf("logged %v: %q", logged, log.String())
			}
		})
	}
}
