package zmuxws

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/zmuxhttp"
	"github.com/zmuxio/zmux-go/zmuxtest"
)

// TestDialModeProfiles serves every HTTP carrier kind on one target
// (zmuxhttp.Mux) and dials it in auto mode, WebSocket preferred,
// directly and through each proxy profile: the most preferred kind that
// the path passes is chosen.
func TestDialModeProfiles(t *testing.T) {
	psk := bytes.Repeat([]byte{3}, 32)
	l := mustNewListener(&zmux.ListenerConfig{Config: mpConfig()})
	o := &zmuxhttp.Options{Next: next}
	stream, err := zmuxhttp.NewHandler(l, o)
	if err != nil {
		t.Fatal(err)
	}
	ex, err := zmuxhttp.NewExchangeHandler(l, &zmuxhttp.ExchangeOptions{Options: *o, HoldTime: time.Second}, zmuxhttp.ExchangeConfig{PSK: psk})
	if err != nil {
		t.Fatal(err)
	}
	ws, err := NewHandler(l, o)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(&zmuxhttp.Mux{Options: o, Stream: stream, Exchange: ex, WebSocket: ws})
	ctx, cancel := context.WithCancel(context.Background())
	accepted := make(chan *zmux.Conn, 8)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			s, err := l.Accept(ctx)
			if err != nil {
				return
			}
			accepted <- s
		}
	}()
	defer func() {
		cancel()
		<-done
		srv.Close()
		_ = ex.Close()
		_ = l.Close()
	}()
	tr := &http.Transport{MaxIdleConnsPerHost: 16}
	defer tr.CloseIdleConnections()
	hc := &http.Client{Transport: tr}
	for _, c := range []struct {
		name    string
		profile *zmuxtest.Profile
		want    zmux.CarrierKind
		tries   int
	}{
		{"direct", nil, zmux.CarrierWebSocket, 1},
		{"websocket front", new(zmuxtest.WebSocketFront()), zmux.CarrierWebSocket, 1},
		{"nginx defaults", new(zmuxtest.NginxDefaults()), zmux.CarrierExchange, 3},
		{"buffering cdn", new(zmuxtest.BufferingCDN(3 * time.Second)), zmux.CarrierExchange, 3},
	} {
		t.Run(c.name, func(t *testing.T) {
			u := srv.URL
			if c.profile != nil {
				px := zmuxtest.New(srv.URL, *c.profile)
				defer px.Close()
				u = px.URL
			}
			cands := []zmux.Candidate{
				Candidate("ws"+strings.TrimPrefix(u, "http")+"/zmux", nil),
				zmuxhttp.Streaming{Client: hc, URL: u + "/zmux"}.Candidate(),
				zmuxhttp.Exchange{Client: hc, URL: u + "/zmux", Config: zmuxhttp.ExchangeConfig{PSK: psk}}.Candidate(),
			}
			dctx, dcancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer dcancel()
			cfg := mpConfig()
			cfg.HandshakeTimeout = 3 * time.Second
			conn, err := zmux.DialChoice(dctx, zmux.AutoChoice(cands...), cfg,
				&zmux.CarrierChoicePolicy{TimerMin: 400 * time.Millisecond, TimerMax: time.Second, Join: zmux.SwitchOff})
			if err != nil {
				t.Fatal(err)
			}
			st := conn.ConnectionStats()
			if st.Carrier != c.want || len(st.CarrierCandidates) != c.tries {
				t.Fatalf("chose %v after %+v", st.Carrier, st.CarrierCandidates)
			}
			if c.name == "nginx defaults" && st.CarrierCandidates[0].Outcome != zmux.CandidateRefused {
				t.Fatalf("a WebSocket upgrade through the proxy: %v", st.CarrierCandidates[0].Outcome)
			}
			s := <-accepted
			echo(t, dctx, conn, s, 64<<10)
			closeBoth(conn, s)
		})
	}

	// Explicit WebSocket through a proxy without WebSocket: refused, and no
	// other kind is tried.
	px := zmuxtest.New(srv.URL, zmuxtest.NginxDefaults())
	defer px.Close()
	_, err = zmux.DialChoice(context.Background(), zmux.ExplicitChoice(Candidate("ws"+strings.TrimPrefix(px.URL, "http")+"/zmux", nil)), mpConfig(), nil)
	var me *zmux.CarrierChoiceError
	if !errors.As(err, &me) || len(me.Results) != 1 || me.Results[0].Outcome != zmux.CandidateRefused {
		t.Fatalf("explicit WebSocket: %v", err)
	}
}
