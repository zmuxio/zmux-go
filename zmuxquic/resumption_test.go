package zmuxquic

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/qlog"
	"github.com/quic-go/quic-go/qlogwriter"
	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/admit"
	"github.com/zmuxio/zmux-go/internal/prefixtest"
)

type observedTokenStore struct {
	quic.TokenStore
	ready chan struct{}
	once  sync.Once
	used  atomic.Bool
}

func (s *observedTokenStore) Put(key string, token *quic.ClientToken) {
	s.TokenStore.Put(key, token)
	s.once.Do(func() { close(s.ready) })
}

func (s *observedTokenStore) Pop(key string) *quic.ClientToken {
	token := s.TokenStore.Pop(key)
	if token != nil {
		s.used.Store(true)
	}
	return token
}

type retryTrace struct{ count atomic.Int32 }

func (t *retryTrace) AddProducer() qlogwriter.Recorder { return t }
func (t *retryTrace) Close() error                     { return nil }

func (t *retryTrace) RecordEvent(event qlogwriter.Event) {
	if packet, ok := event.(qlog.PacketReceived); ok && packet.Header.PacketType == qlog.PacketTypeRetry {
		t.count.Add(1)
	}
}

func TestQUICVersionBoundResumption(t *testing.T) {
	for _, tt := range []struct {
		name     string
		from, to quic.Version
		shrink   bool
	}{
		{name: "v1-to-v1", from: quic.Version1, to: quic.Version1},
		{name: "v1-to-v2", from: quic.Version1, to: quic.Version2},
		{name: "v2-to-v1", from: quic.Version2, to: quic.Version1},
		{name: "smaller-stream-limit", from: quic.Version1, to: quic.Version1, shrink: true},
	} {
		t.Run("ticket/"+tt.name, func(t *testing.T) {
			testVersionBoundResumption(t, tt.from, tt.to, false, tt.shrink)
		})
		if !tt.shrink {
			t.Run("token/"+tt.name, func(t *testing.T) {
				testVersionBoundResumption(t, tt.from, tt.to, true, false)
			})
		}
	}
}

func testVersionBoundResumption(t *testing.T, from, to quic.Version, tokens, shrink bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	serverTLS, clientTLS := tlsConfigs(t)
	addr := net.IPv4(127, 0, 0, 1)
	packet, err := net.ListenUDP("udp4", &net.UDPAddr{IP: addr})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = packet.Close() }()
	// Observe accepted Retry packets independently of TLS resumption.
	retries := &retryTrace{}
	transport := &quic.Transport{Conn: packet}
	defer func() { _ = transport.Close() }()
	tokenStore := &observedTokenStore{TokenStore: quic.NewLRUTokenStore(1, 1), ready: make(chan struct{})}
	if tokens {
		transport.VerifySourceAddress = func(net.Addr) bool { return true }
		clientTLS.ClientSessionCache = nil
	}
	serverQUIC := QUICConfig(&quic.Config{
		Allow0RTT: true, Versions: []quic.Version{quic.Version1, quic.Version2}, MaxIncomingStreams: 8,
	})
	var smaller atomic.Bool
	serverQUIC.GetConfigForClient = func(*quic.ClientInfo) (*quic.Config, error) {
		cfg := serverQUIC.Clone()
		cfg.GetConfigForClient = nil
		if smaller.Load() {
			cfg.MaxIncomingStreams = 4
		}
		return cfg, nil
	}
	listener, err := transport.ListenEarly(serverTLS, serverQUIC)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()

	clientQUIC := QUICConfig(&quic.Config{Versions: []quic.Version{from}})
	if tokens {
		clientQUIC.TokenStore = tokenStore
		clientQUIC.Tracer = func(context.Context, bool, quic.ConnectionID) qlogwriter.Trace { return retries }
	}
	warm, err := quic.DialAddr(ctx, listener.Addr().String(), clientTLS, clientQUIC)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = warm.CloseWithError(0, "") }()
	peer, err := listener.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = peer.CloseWithError(0, "") }()
	if tokens {
		select {
		case <-tokenStore.ready:
		case <-ctx.Done():
			t.Fatal("NEW_TOKEN not received")
		}
		if n := retries.count.Swap(0); n != 1 {
			t.Fatalf("cold handshake required %d Retries, want 1", n)
		}
	} else {
		waitTicket(t, ctx, clientTLS)
	}
	_ = warm.CloseWithError(0, "")
	_ = peer.CloseWithError(0, "")

	smaller.Store(shrink)
	clientQUIC = clientQUIC.Clone()
	clientQUIC.Versions = []quic.Version{to}
	qc, _, err := dialAddr(ctx, listener.Addr().String(), clientTLS, clientQUIC, true)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = qc.CloseWithError(0, "") }()
	remote, err := listener.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = remote.CloseWithError(0, "") }()
	wantEarly := !tokens && from == to && !shrink
	state := qc.ConnectionState()
	if state.Version != to || state.TLS.DidResume != !tokens || state.Used0RTT != wantEarly {
		t.Fatalf("reconnect version=%v resumed=%t early=%t, want version=%v resumed=%t early=%t",
			state.Version, state.TLS.DidResume, state.Used0RTT, to, !tokens, wantEarly)
	}
	if tokens {
		if !tokenStore.used.Load() {
			t.Fatal("client did not use the cached token")
		}
		var wantRetries int32
		if from != to {
			wantRetries = 1
		}
		if n := retries.count.Load(); n != wantRetries {
			t.Fatalf("reconnect required %d Retries, want %d", n, wantRetries)
		}
	}
	if wantEarly {
		// The admission policy for connections that accept 0-RTT is
		// covered by TestServeAdmissionRejectsEarlyData.
		return
	}
	if !tokens {
		if _, err := qc.OpenStreamSync(ctx); !errors.Is(err, quic.Err0RTTRejected) {
			t.Fatalf("old stream generation returned %v, want Err0RTTRejected", err)
		}
	}
	checkResumedAdmission(t, ctx, qc, remote)
}

func checkResumedAdmission(t *testing.T, ctx context.Context, client, server *quic.Conn) {
	t.Helper()
	auth, verifier, _ := prefixtest.New(t, admit.PrefixCarrierOptions{BindTLS: true}, zmux.VerdictClose)
	var decisions atomic.Uint32
	admission := verifier.Admission()
	admission.Decide = func(ctx context.Context, info zmux.CarrierInfo, prefix []byte) zmux.Decision {
		decisions.Add(1)
		return verifier.Decide(ctx, info, prefix)
	}
	lst := mustNewListener(&zmux.ListenerConfig{Config: mpConfig(), Admission: admission})
	defer func() { _ = lst.Close() }()
	service, err := AdoptConn(ctx, lst, server)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		service.Stop()
		_ = server.CloseWithError(0, "")
		<-service.done
	}()
	cfg := mpConfig()
	cfg.CarrierCredential = auth
	c, err := Client(ctx, client, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	s, err := lst.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	echo(t, c, s, 1024)
	if n := decisions.Load(); n != 1 {
		t.Fatalf("first stream reached admission %d times, want 1", n)
	}
	if client.Context().Err() != nil || server.Context().Err() != nil {
		t.Fatal("connection closed during 1-RTT fallback")
	}
}
