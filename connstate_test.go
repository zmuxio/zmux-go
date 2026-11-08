package zmux

import (
	"context"
	"crypto/tls"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

// ConnectionState reports the QMux version, the TLS state and ALPN of the
// bootstrap carrier, suspension, and the datagram limits of the session and
// of a stream.
func TestConnectionStateNegotiation(t *testing.T) {
	leakcheck.Check(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	scfg, ccfg := selfSignedTLS(t)
	scfg.NextProtos, ccfg.NextProtos = []string{DefaultALPN}, []string{DefaultALPN}
	a, b := tcpPair(t)
	cfg := &Config{EnableDatagrams: true}
	type res struct {
		c   *Conn
		err error
	}
	ch := make(chan res, 1)
	go func() { c, err := Server(ctx, tls.Server(a, scfg), cfg); ch <- res{c, err} }()
	cc, err := Client(ctx, tls.Client(b, ccfg), cfg)
	if err != nil {
		t.Fatal(err)
	}
	r := <-ch
	if r.err != nil {
		t.Fatal(r.err)
	}
	sc := r.c
	defer func() { _ = cc.Close(); _ = sc.Close(); waitDone(t, cc); waitDone(t, sc) }()
	echoOnce(t, ctx, cc, sc)
	st := cc.ConnectionState()
	if st.QMuxVersion != "draft-02" || st.TLS == nil || !st.TLS.HandshakeComplete || st.ApplicationProtocol != DefaultALPN || st.Suspended {
		t.Fatalf("state %+v", st)
	}
	if st.MaxDatagramPayload <= 0 {
		t.Fatalf("session datagram limit %d", st.MaxDatagramPayload)
	}
	s, err := cc.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	before := s.MaxDatagramPayload(0)
	if before <= 0 || before >= st.MaxDatagramPayload {
		t.Fatalf("stream datagram limit %d, session %d", before, st.MaxDatagramPayload)
	}
	if err := s.SendDatagram(0, make([]byte, before)); err != nil {
		t.Fatalf("a datagram of the reported size: %v", err)
	}
	if after := s.MaxDatagramPayload(0); after < before || after+1 != st.MaxDatagramPayload {
		t.Fatalf("stream datagram limit %d after the identifier, session %d", after, st.MaxDatagramPayload)
	}
	if s.MaxDatagramPayload(1<<20) >= s.MaxDatagramPayload(0) {
		t.Fatal("context identifier not counted")
	}
	// A plain carrier: no TLS, no application protocol.
	p, q := tcpPair(t)
	go func() { c, err := Server(ctx, p, nil); ch <- res{c, err} }()
	pc, err := Client(ctx, q, nil)
	if err != nil {
		t.Fatal(err)
	}
	r = <-ch
	if r.err != nil {
		t.Fatal(r.err)
	}
	defer func() { _ = pc.Close(); _ = r.c.Close(); waitDone(t, pc); waitDone(t, r.c) }()
	if st := pc.ConnectionState(); st.TLS != nil || st.ApplicationProtocol != "" || st.QMuxVersion != QMuxVersion || st.MaxDatagramPayload != 0 {
		t.Fatalf("plain state %+v", st)
	}
}
