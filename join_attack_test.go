package zmux

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"errors"
	"io"
	"math/big"
	"net"
	"net/netip"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/djylb/timeshim"

	"github.com/zmuxio/zmux-go/internal/join"
	"github.com/zmuxio/zmux-go/internal/wire"
	"github.com/zmuxio/zmux-go/varint"
)

// The tests in this file attack the Join handshake over secure
// carriers. The carriers are TLS: a third party holds a valid certificate for the
// origin's name, so that it can terminate a carrier, as a CDN or a mistaken CA can.

const atkName = "svc.example"

var atkPSK = bytes.Repeat([]byte{0x5a}, 32)

type atkPKI struct {
	pool        *x509.CertPool
	origin, bad tls.Certificate
}

func newAtkPKI(t *testing.T) *atkPKI {
	t.Helper()
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tm := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "root"}, IsCA: true,
		BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign, NotBefore: time.Now().Add(-time.Hour),
		NotAfter: time.Now().Add(time.Hour)}
	der, err := x509.CreateCertificate(rand.Reader, tm, tm, &k.PublicKey, k)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	leaf := func(serial int64) tls.Certificate {
		lk, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		lt := &x509.Certificate{SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: atkName},
			DNSNames: []string{atkName}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour)}
		d, err := x509.CreateCertificate(rand.Reader, lt, ca, &lk.PublicKey, k)
		if err != nil {
			t.Fatal(err)
		}
		return tls.Certificate{Certificate: [][]byte{d}, PrivateKey: lk}
	}
	pool := x509.NewCertPool()
	pool.AddCert(ca)
	return &atkPKI{pool: pool, origin: leaf(2), bad: leaf(3)}
}

// atkServe hands each accepted connection to h, which owns it.
func atkServe(t *testing.T, h func(net.Conn)) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	t.Cleanup(func() {
		_ = ln.Close()
		wg.Wait()
	})
	wg.Go(func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			wg.Go(func() { h(c) })
		}
	})
	return ln.Addr().String()
}

// atkRelay owns its accepted sockets until the relay returns or the test ends.
func atkRelay(t *testing.T, h func(net.Conn)) string {
	t.Helper()
	return atkServe(t, func(c net.Conn) {
		closeConn := sync.OnceFunc(func() { _ = c.Close() })
		stop := context.AfterFunc(t.Context(), closeConn)
		defer stop()
		defer closeConn()
		h(c)
	})
}

func atkConfig() *Config {
	return &Config{SessionPolicy: SessionPolicy{Limits: CapacityLimits{SteadyAttachments: 4}}, PipeAuth: PipeAuth{PSK: atkPSK, EndToEnd: true}, ResumeTimeout: 60 * time.Second,
		HandshakeTimeout: 30 * time.Second, SchedulerParams: SchedulerParams{Probation: -1},
		AllowRecordMode: func(RecordPolicyInput, RecordMode, any) bool { return true }}
}

type atkEnv struct {
	ctx   context.Context
	c, s  *Conn
	first net.Conn
	t     *testing.T
	pki   *atkPKI
	lst   *Listener
	tlsTo string // the origin over TLS
	addr  string // the origin over plain TCP
}

type atkOpts struct {
	client, server func(*Config)
	listener       func(*ListenerConfig)
}

func newAtkEnv(t *testing.T, o atkOpts) *atkEnv {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 120*time.Second)
	t.Cleanup(cancel)
	ccfg, scfg := atkConfig(), atkConfig()
	if o.client != nil {
		o.client(ccfg)
	}
	if o.server != nil {
		o.server(scfg)
	}
	lc := &ListenerConfig{Config: scfg}
	if o.listener != nil {
		o.listener(lc)
	}
	e := &atkEnv{t: t, ctx: ctx, pki: newAtkPKI(t), lst: mustNewListener(lc)}
	t.Cleanup(func() { _ = e.lst.Close() })
	e.addr = atkServe(t, func(c net.Conn) { _ = e.lst.HandlePipe(ctx, NetPipe(c), PipeHint{}) })
	e.tlsTo = atkServe(t, func(c net.Conn) {
		tc := tls.Server(c, &tls.Config{Certificates: []tls.Certificate{e.pki.origin}, MinVersion: tls.VersionTLS13})
		_ = e.lst.HandlePipe(ctx, NetPipe(secureTLS{tc}), PipeHint{})
	})
	nc, err := net.Dial("tcp", e.addr)
	if err != nil {
		t.Fatal(err)
	}
	e.first = nc
	if e.c, err = Start(ctx, NetPipe(nc), RoleClient, ccfg); err != nil {
		t.Fatal(err)
	}
	st, err := e.c.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = st.Write([]byte("hi")); err != nil {
		t.Fatal(err)
	}
	if e.s, err = e.lst.Accept(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.c.Close(); _ = e.s.Close() })
	ss, err := e.s.AcceptStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = io.ReadFull(ss, make([]byte, 2)); err != nil {
		t.Fatal(err)
	}
	_ = st.Close()
	_ = ss.Close()
	return e
}

// dialTLS opens a TLS carrier to addr, its handshake done.
func (e *atkEnv) dialTLS(addr string) secureTLS {
	e.t.Helper()
	raw, err := net.Dial("tcp", addr)
	if err != nil {
		e.t.Fatal(err)
	}
	e.t.Cleanup(func() { _ = raw.Close() })
	tc := tls.Client(raw, &tls.Config{RootCAs: e.pki.pool, ServerName: atkName, MinVersion: tls.VersionTLS13})
	if err := tc.HandshakeContext(e.ctx); err != nil {
		e.t.Fatal(err)
	}
	return secureTLS{tc}
}

func (e *atkEnv) dial(addr string) net.Conn {
	e.t.Helper()
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		e.t.Fatal(err)
	}
	e.t.Cleanup(func() { _ = nc.Close() })
	return nc
}

// suspend cuts the first pipe and waits until both ends have no pipe.
func (e *atkEnv) suspend() {
	e.t.Helper()
	_ = e.first.Close()
	for _, c := range []*Conn{e.c, e.s} {
		if c.ConnectionStats().PipesActive != 0 {
			waitTestEvent(e.t, e.ctx, c, "suspension", func(ev Event) bool {
				return ev.Kind == EventSessionSuspended
			})
		}
	}
}

func (e *atkEnv) alive(c *Conn) {
	e.t.Helper()
	if c.Context().Err() != nil {
		e.t.Fatalf("session ended: %v", c.Wait())
	}
}

// tap is what a third party read.
type tap struct {
	b  bytes.Buffer
	mu sync.Mutex
}

func (a *tap) add(p []byte) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.b.Write(p)
}

func (a *tap) bytes() []byte {
	a.mu.Lock()
	defer a.mu.Unlock()
	return bytes.Clone(a.b.Bytes())
}

// rogue terminates TLS with the third party's certificate, reads, and answers a whole
// Hello with answer. No origin stands behind it.
func (e *atkEnv) rogue(answer []byte) (string, *tap, <-chan struct{}) {
	a := &tap{}
	hello := make(chan struct{})
	return atkRelay(e.t, func(c net.Conn) {
		in := tls.Server(c, &tls.Config{Certificates: []tls.Certificate{e.pki.bad}, MinVersion: tls.VersionTLS13})
		b := make([]byte, 64<<10)
		first := true
		for {
			n, err := in.Read(b)
			if n > 0 {
				a.add(b[:n])
				if _, _, perr := join.ParseHelloExact(a.bytes(), join.Stream); first && perr == nil {
					first = false
					close(hello)
					if answer != nil {
						_, _ = in.Write(answer)
						_ = in.CloseWrite()
					}
				}
			}
			if err != nil {
				return
			}
		}
	}), a, hello
}

// capture runs a real join against a party that never answers, and returns
// its Hello and the abandoned pipe's handle.
func (e *atkEnv) capture(mode RecordMode) ([]byte, *Pipe) {
	e.t.Helper()
	received := make(chan []byte, 1)
	addr := atkRelay(e.t, func(c net.Conn) {
		var hello []byte
		buf := make([]byte, 64<<10)
		for len(hello) < MaxUntakenRead {
			n, err := c.Read(buf[:min(len(buf), MaxUntakenRead-len(hello))])
			hello = append(hello, buf[:n]...)
			if _, _, perr := join.ParseHelloExact(hello, join.Stream); perr == nil {
				received <- hello
				_, _ = io.Copy(io.Discard, c) // the joiner cancels after capture
				return
			}
			if err != nil {
				return
			}
		}
	})
	ctx, cancel := context.WithCancel(e.ctx)
	defer cancel()
	nc := e.dial(addr)
	done := make(chan error, 1)
	go func() {
		_, err := e.c.AddPipe(ctx, NetPipe(nc), PipeOptions{RecordMode: Set(mode)})
		done <- err
	}()
	var hello []byte
	select {
	case hello = <-received:
	case <-ctx.Done():
		e.t.Fatalf("waiting for Hello: %v", ctx.Err())
	}
	var attempted *Pipe
	for _, pipe := range e.c.Pipes() {
		if ps, ok := pipe.Stats(); ok && ps.Local && ps.Outbound == OutboundJoining {
			attempted = pipe
			break
		}
	}
	if attempted == nil {
		e.t.Fatal("no joining pipe for captured Hello")
	}
	cancel()
	if err := <-done; err == nil {
		e.t.Fatal("a join succeeded without an answer")
	}
	return hello, attempted
}

var atkGroup, _ = join.LookupGroup(join.GroupX25519)

// keyOf returns join_psk of the client's role, as both ends derive it.
func (e *atkEnv) keyOf() (su *join.Suite, psk, locator []byte) {
	e.t.Helper()
	e.c.lock()
	defer e.c.unlock()
	l := e.c.s.LocalParams()
	p, _ := e.c.s.PeerParams()
	su = join.Preferred([]uint64{join.SuiteAES})
	enc, err := join.SessionCrypto(l.SessionCrypto, p.SessionCrypto)
	if err != nil {
		e.t.Fatal(err)
	}
	secret := wire.SessionSecret(l.SessionNonce[:], p.SessionNonce[:], nil, atkPSK)
	if psk, err = join.PSK(su, secret, join.RoleInitiator, enc); err != nil {
		e.t.Fatal(err)
	}
	return su, psk, bytes.Clone(p.Locator)
}

// forge writes a Hello as the client's role holder would, for a pipe ID it chooses.
func (e *atkEnv) forge(id uint64, mode join.Mode, binding, batch []byte) ([]byte, *join.Joiner) {
	e.t.Helper()
	su, psk, loc := e.keyOf()
	j, err := join.NewJoiner(join.JoinerConfig{Suite: su, Group: atkGroup, Mode: mode, Framing: join.Stream, Locator: loc, PSK: psk, Binding: binding})
	if err != nil {
		e.t.Fatal(err)
	}
	h, err := j.WriteHello(nil, join.HelloPayload{Pipe: join.Pipe{ID: id, Direction: 2}, FirstBatch: batch})
	if err != nil {
		e.t.Fatal(err)
	}
	return h, j
}

// openHello opens a captured Hello as the acceptor does.
func (e *atkEnv) openHello(hello []byte) join.HelloPayload {
	e.t.Helper()
	h, _, err := join.ParseHelloExact(hello, join.Stream)
	if err != nil {
		e.t.Fatal(err)
	}
	_, psk, _ := e.keyOf()
	_, pl, err := join.Accept(h, join.Stream, psk, nil, func(uint64) bool { return true })
	if err != nil {
		e.t.Fatal(err)
	}
	return pl
}

// offer sends b on a new carrier to the plain origin and returns the carrier and
// whether a Reply began on it.
func (e *atkEnv) offer(b []byte) (net.Conn, bool) {
	e.t.Helper()
	nc := e.dial(e.addr)
	if _, err := nc.Write(b); err != nil {
		e.t.Fatal(err)
	}
	var one [1]byte
	_ = nc.SetReadDeadline(time.Now().Add(10 * time.Second))
	_, err := io.ReadFull(nc, one[:])
	_ = nc.SetReadDeadline(time.Time{})
	return nc, err == nil && one[0] == join.ReplyType
}

// rejoin joins the origin and checks that msg, written earlier, arrives whole.
func (e *atkEnv) rejoinDelivers(mode RecordMode, msg []byte) {
	e.t.Helper()
	if _, err := e.c.AddPipe(e.ctx, NetPipe(e.dialTLS(e.tlsTo)), PipeOptions{RecordMode: Set(mode)}); err != nil {
		e.t.Fatalf("join to the origin: %v", err)
	}
	ss, err := e.s.AcceptStream(e.ctx)
	if err != nil {
		e.t.Fatal(err)
	}
	got := make([]byte, len(msg))
	if _, err := io.ReadFull(ss, got); err != nil || !bytes.Equal(got, msg) {
		e.t.Fatalf("origin read %d bytes intact=%v, %v", len(got), bytes.Equal(got, msg), err)
	}
}

func atkSecret(tag string) []byte {
	return []byte("SECRET-BUSINESS-" + tag + "-" + strings.Repeat("x", 4096))
}

func framed(body []byte) []byte {
	return append(varint.AppendVarint(nil, uint64(len(body))), body...)
}

// ---- a third party that terminates the carrier ----

// A1 to A5: a third party reads one Hello and nothing else, whatever it answers. Its
// answer delivers nothing, confirms nothing and closes nothing: only the attachment
// fails, the session stays suspended and a real join delivers the data.
func TestJoinThirdParty(t *testing.T) {
	t.Parallel()
	fakeReply := make([]byte, 0, 1+32+2+16)
	fakeReply = append(fakeReply, join.ReplyType)
	fakeReply = append(fakeReply, make([]byte, 32)...)
	fakeReply = append(fakeReply, 0, 16)
	fakeReply = append(fakeReply, make([]byte, 16)...)
	for _, mode := range []RecordMode{RecordSealed, RecordBound, RecordTrusted} {
		for _, at := range []struct {
			answer func(id uint64) []byte
			name   string
		}{
			{name: "A1 silent", answer: nil},
			{name: "A2 forged acknowledgement", answer: func(uint64) []byte {
				return framed(wire.AppendAckEntry(wire.AppendPipeAckHeader(nil, 1), wire.AckEntry{PipeID: 2, Position: 1 << 16}))
			}},
			{name: "A3 forged stream data", answer: func(id uint64) []byte {
				return framed(append(wire.AppendStreamHeader(nil, id, 0, 6, true, true), "FORGED"...))
			}},
			{name: "A4 forged CONNECTION_CLOSE", answer: func(uint64) []byte {
				return framed(wire.AppendClose(nil, false, wire.ProtocolViolation, 0, nil))
			}},
			{name: "A5 forged Reply", answer: func(uint64) []byte { return fakeReply }},
			{name: "A5 record beyond max_record_size", answer: func(uint64) []byte {
				return append(varint.AppendVarint(nil, MaxRecordSize+1), 0xff)
			}},
			{name: "A5 length beyond any record", answer: func(uint64) []byte { return varint.AppendVarint(nil, 1<<40) }},
			{name: "A5 garbage", answer: func(uint64) []byte { return []byte{2, 0xff, 0xff} }},
		} {
			t.Run(mode.String()+"/"+at.name, func(t *testing.T) {
				t.Parallel()
				e := newAtkEnv(t, atkOpts{})
				st, err := e.c.OpenStreamSync(e.ctx)
				if err != nil {
					t.Fatal(err)
				}
				e.suspend()
				secret := atkSecret(mode.String())
				if _, err := st.Write(secret); err != nil {
					t.Fatal(err)
				}
				var answer []byte
				if at.answer != nil {
					answer = at.answer(uint64(st.StreamID()))
				}
				addr, a, hello := e.rogue(answer)
				ctx, cancel := context.WithCancel(e.ctx)
				defer cancel()
				carrier := e.dialTLS(addr)
				added := make(chan error, 1)
				go func() {
					_, err := e.c.AddPipe(ctx, NetPipe(carrier), PipeOptions{RecordMode: Set(mode)})
					added <- err
				}()
				waitTestSignal(t, hello, "Hello received by the third party")
				if answer == nil {
					cancel()
				}
				err = <-added
				if err == nil {
					t.Fatal("a third party completed the join")
				}
				up := a.bytes()
				if _, _, err := join.ParseHelloExact(up, join.Stream); err != nil {
					t.Fatalf("the third party read %d bytes beyond one Hello: %v", len(up), err)
				}
				if bytes.Contains(up, []byte("SECRET-BUSINESS")) {
					t.Fatal("plaintext in the Hello")
				}
				e.alive(e.c)
				if at.answer != nil && !errors.Is(err, context.Canceled) && e.c.ConnectionStats().JoinFailures == 0 {
					t.Fatalf("join failed with %v and no JoinFailures", err)
				}
				if n, _ := st.TryRead(make([]byte, 16)); n != 0 {
					t.Fatal("forged data reached the application")
				}
				e.rejoinDelivers(mode, secret)
				e.alive(e.c)
				runtime.KeepAlive(st)
			})
		}
	}
}

// relay carries plaintext between a carrier terminated by the third party and its own
// connection to the origin.
func (e *atkEnv) tlsRelay(a *tap) string {
	return atkRelay(e.t, func(c net.Conn) {
		in := tls.Server(c, &tls.Config{Certificates: []tls.Certificate{e.pki.bad}, MinVersion: tls.VersionTLS13})
		raw, err := net.Dial("tcp", e.tlsTo)
		if err != nil {
			return
		}
		out := tls.Client(raw, &tls.Config{RootCAs: e.pki.pool, ServerName: atkName, MinVersion: tls.VersionTLS13})
		defer func() { _ = out.Close(); _ = raw.Close() }()
		go func() { _, _ = io.Copy(in, out); _ = in.Close() }()
		b := make([]byte, 64<<10)
		for {
			n, err := in.Read(b)
			if n > 0 {
				a.add(b[:n])
				if _, werr := out.Write(b[:n]); werr != nil {
					return
				}
			}
			if err != nil {
				return
			}
		}
	})
}

// Bound: a third party that splices the carrier makes the origin refuse the Hello,
// because the two TLS connections have different exporters; a refused join is
// closed at once by default, not stalled.
func TestJoinBoundRelayRefused(t *testing.T) {
	t.Parallel()
	clock := NewClockDomain(timeshim.NewFake(time.Unix(1, 0)))
	setClock := func(c *Config) { c.ClockDomain = clock }
	e := newAtkEnv(t, atkOpts{client: setClock, server: setClock})
	st, err := e.c.OpenStreamSync(e.ctx)
	if err != nil {
		t.Fatal(err)
	}
	_ = e.first.Close()
	for _, c := range []*Conn{e.c, e.s} {
		for {
			event, err := c.NextEvent(e.ctx)
			if err != nil {
				t.Fatal(err)
			}
			if event.Kind == EventSessionSuspended {
				break
			}
		}
	}
	secret := atkSecret("bound-relay")
	if _, err := st.Write(secret); err != nil {
		t.Fatal(err)
	}
	a := &tap{}
	addr := e.tlsRelay(a)
	// Leave the protocol clock frozen. A stalled carrier could only be
	// released by the watchdog, while the default refusal closes it here.
	if _, err := e.c.AddPipe(e.ctx, NetPipe(e.dialTLS(addr)), PipeOptions{RecordMode: Set(RecordBound)}); err == nil {
		t.Fatal("Bound passed a third party")
	}
	if err := e.ctx.Err(); err != nil {
		t.Fatalf("waiting for the refused carrier to close: %v", err)
	}
	if got := e.s.ConnectionStats().JoinAuthFailures; got != 1 {
		t.Fatalf("origin recorded %d join authentication failures, want 1", got)
	}
	if _, _, err := join.ParseHelloExact(a.bytes(), join.Stream); err != nil {
		t.Fatalf("the third party read more than one Hello: %v", err)
	}
	if st := e.s.ConnectionStats(); st.PipesActive != 0 {
		t.Fatalf("origin active pipes %d", st.PipesActive)
	}
	e.rejoinDelivers(RecordBound, secret)
	runtime.KeepAlive(st)
}

// Sealed: a relay carries the join, reads ciphertext only, and a record it injects ends
// the pipe and not the session.
func TestJoinSealedRelay(t *testing.T) {
	t.Parallel()
	e := newAtkEnv(t, atkOpts{})
	secret := atkSecret("sealed-relay")
	outs := make(chan net.Conn, 1)
	a := &tap{}
	addr := atkRelay(t, func(in net.Conn) {
		out, err := net.Dial("tcp", e.addr)
		if err != nil {
			return
		}
		defer func() { _ = out.Close() }()
		outs <- out
		go func() { _, _ = io.Copy(in, out); _ = in.Close() }()
		b := make([]byte, 64<<10)
		for {
			n, err := in.Read(b)
			if n > 0 {
				a.add(b[:n])
				if _, werr := out.Write(b[:n]); werr != nil {
					return
				}
			}
			if err != nil {
				return
			}
		}
	})
	p, err := e.c.AddPipe(e.ctx, NetPipe(e.dial(addr)), PipeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	out := <-outs
	sp := waitTestAttachment(t, e.ctx, p, e.s)
	first, _ := e.c.Pipe(0)
	_ = first.Abort()
	st, err := e.c.OpenStreamSync(e.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Write(secret); err != nil {
		t.Fatal(err)
	}
	ss, err := e.s.AcceptStream(e.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(ss, make([]byte, len(secret))); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(a.bytes(), []byte("SECRET-BUSINESS")) {
		t.Fatal("the relay read a record")
	}
	if _, err := out.Write(append([]byte{0, 40}, make([]byte, 40)...)); err != nil {
		t.Fatal(err)
	}
	for ps, ok := sp.Stats(); ok && ps.Inbound == InboundOpen; ps, ok = sp.Stats() {
		waitTestEvent(t, e.ctx, e.s, "end of the injected record's pipe", func(ev Event) bool {
			return ev.PipeID == sp.ID() || ev.Kind == EventPipesRetired
		})
	}
	e.alive(e.s)
	e.alive(e.c)
	runtime.KeepAlive(st)
}

// ---- Secret Flags and zmux_session_crypto on the plaintext bootstrap ----

// paramTamper rewrites the transport parameters of the first write.
type paramTamper struct {
	net.Conn
	f    func(*wire.Params)
	pre  []byte
	done bool
}

func (c *paramTamper) Write(b []byte) (int, error) {
	if c.done {
		return c.Conn.Write(b)
	}
	c.pre = append(c.pre, b...)
	size, n := varint.ConsumeVarint(c.pre)
	if n <= 0 || n+int(size) > len(c.pre) {
		return len(b), nil
	}
	c.done = true
	rec := c.pre[n : n+int(size)]
	_, tn := varint.ConsumeVarint(rec)
	blen, bn := varint.ConsumeVarint(rec[tn:])
	var p wire.Params
	if werr := wire.DecodeParams(rec[tn+bn:tn+bn+int(blen)], &p); werr != nil {
		return 0, werr
	}
	c.f(&p)
	enc, _ := wire.AppendParams(nil, &p)
	nb := wire.AppendTransportParams(nil, enc)
	out := append(varint.AppendVarint(nil, uint64(len(nb))), nb...)
	out = append(out, c.pre[n+int(size):]...)
	if _, err := c.Conn.Write(out); err != nil {
		return 0, err
	}
	return len(b), nil
}

// startTampered starts a session whose client parameters a path party rewrites.
func startTampered(t *testing.T, cfg func() *Config, f func(*wire.Params)) (*Listener, *Conn, *Conn, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	lst := mustNewListener(&ListenerConfig{Config: cfg()})
	t.Cleanup(func() { _ = lst.Close() })
	ck, sk := kmPair("s0")
	go func() { _ = lst.HandlePipe(ctx, PipeIO{R: sk, W: sk}, PipeHint{}) }()
	tc := &tamperKM{Conn: ck, f: f, km: ck}
	c, err := Start(ctx, PipeIO{R: tc, W: tc}, RoleClient, cfg())
	if err != nil {
		return lst, nil, nil, err
	}
	t.Cleanup(func() { _ = c.Close() })
	// A session the tampering ended never reaches Accept: stop with it.
	actx, acancel := context.WithCancel(ctx)
	defer acancel()
	stopAfter := context.AfterFunc(c.Context(), acancel)
	defer stopAfter()
	s, err := lst.Accept(actx)
	if err != nil {
		return lst, c, nil, err
	}
	t.Cleanup(func() { _ = s.Close() })
	return lst, c, s, nil
}

type tamperKM struct {
	km *kmConn
	paramTamper
}

func (c *tamperKM) ExportKeyingMaterial(l string, ctx []byte, n int) ([]byte, error) {
	return c.km.ExportKeyingMaterial(l, ctx, n)
}

// Clearing one of two flags closes the session; it never takes the intersection, so
// that no end derives a secret without the PSK.
func TestSecretFlagsDowngradeCloses(t *testing.T) {
	cfg := func() *Config {
		c := exporterConfig()
		c.PipeAuth.PSK = atkPSK
		return c
	}
	var cleared atomic.Bool
	_, c, s, err := startTampered(t, cfg, func(p *wire.Params) {
		cleared.Store(p.SecretFlags&wire.SecretPSK != 0)
		p.SecretFlags &^= wire.SecretPSK
	})
	if s != nil {
		t.Fatal("the listener accepted a session with differing flags")
	}
	if c != nil {
		err = c.Wait()
	}
	if err == nil || !cleared.Load() {
		t.Fatalf("client's end: %v, flag cleared %v", err, cleared.Load())
	}
}

// Clearing all flags turns multipath off at the end that saw it: no join key exists and
// the other end's joins do not attach.
func TestSecretFlagsClearedDisableMultipath(t *testing.T) {
	cfg := func() *Config {
		c := mpConfig()
		c.HandshakeTimeout = 2 * time.Second
		return c
	}
	lst, c, s, err := startTampered(t, cfg, func(p *wire.Params) { p.SecretFlags = 0 })
	if err != nil {
		t.Fatal(err)
	}
	if s.ConnectionState().Multipath {
		t.Fatal("multipath without secret flags")
	}
	a, b := net.Pipe()
	defer func() { _ = a.Close(); _ = b.Close() }()
	if _, err := s.AddPipe(context.Background(), NetPipe(a), PipeOptions{}); err == nil {
		t.Fatal("a join without keys")
	}
	x, y := net.Pipe()
	defer func() { _ = x.Close(); _ = y.Close() }()
	jctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	refused := make(chan error, 1)
	go func() { refused <- lst.HandlePipe(jctx, NetPipe(y), PipeHint{}) }()
	if _, err := c.AddPipe(jctx, NetPipe(x), PipeOptions{}); err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("join to an end without multipath: %v", err)
	}
	if err := <-refused; err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("handle join without multipath: %v", err)
	}
}

// A zmux_session_crypto changed on the path makes the two encodings differ, and every
// join fails authentication.
func TestSessionCryptoTamperFailsJoins(t *testing.T) {
	lst, c, s, err := startTampered(t, mpConfig, func(p *wire.Params) {
		p.SessionCrypto = wire.SessionCrypto{Suites: []uint64{1, 7}, Groups: p.SessionCrypto.Groups}
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	a, b := tcpPair(t)
	go func() { _ = lst.HandlePipe(ctx, NetPipe(b), PipeHint{}) }()
	added := make(chan error, 1)
	go func() {
		_, err := c.AddPipe(ctx, NetPipe(a), PipeOptions{})
		added <- err
	}()
	// The joiner hears nothing from an acceptor that failed the Hello: it
	// is given up once the failure is counted.
	waitTestEvent(t, ctx, s, "join authentication failure", func(ev Event) bool { return ev.Kind == EventJoinAuthFailed })
	if n := s.ConnectionStats().JoinAuthFailures; n != 1 {
		t.Fatalf("%d authentication failures, want 1", n)
	}
	cancel()
	if err := <-added; err == nil {
		t.Fatal("a join succeeded with a tampered zmux_session_crypto")
	}
}

// ---- replay ----

// A replayed Hello moves no pipe, evicts nothing and ends no suspension.
func TestReplayMovesNothing(t *testing.T) {
	t.Parallel()
	for _, mode := range []RecordMode{RecordSealed, RecordTrusted} {
		t.Run(mode.String(), func(t *testing.T) {
			t.Parallel()
			e := newAtkEnv(t, atkOpts{})
			p, err := e.c.AddPipe(e.ctx, NetPipe(e.dial(e.addr)), PipeOptions{RecordMode: Set(mode)})
			if err != nil {
				t.Fatal(err)
			}
			waitTestAttachment(t, e.ctx, p, e.s)
			before := e.s.PipeStats()
			added := e.s.ConnectionStats().PipesAdded
			// A joiner that gave up abandons its ID over the live pipe: its Hello is dropped.
			hello, attempted := e.capture(mode)
			waitTestSignal(t, attempted.Done(), "abandoned pipe retirement")
			if _, replied := e.offer(hello); replied || e.s.ConnectionStats().JoinReplays != 1 {
				t.Fatalf("abandoned Hello: replied=%v, replays=%d", replied, e.s.ConnectionStats().JoinReplays)
			}
			// A Hello not seen yet gets a Reply, with no attachment state.
			fresh, _ := e.forge(20, join.Mode(mode), nil, nil)
			nc, replied := e.offer(fresh)
			if !replied {
				t.Fatal("no Reply to an authenticated Hello")
			}
			// Junk where the confirmation belongs fails this carrier only.
			_, _ = nc.Write(make([]byte, 64))
			_ = nc.SetReadDeadline(time.Now().Add(30 * time.Second))
			if _, err := io.Copy(io.Discard, nc); err != nil {
				t.Fatal(err)
			}
			after := e.s.PipeStats()
			if len(after) != len(before) || e.s.ConnectionStats().PipesAdded != added {
				t.Fatalf("pipes %d -> %d", len(before), len(after))
			}
			for i := range after {
				if after[i].PipeID != before[i].PipeID || after[i].FailureCount != 0 {
					t.Fatalf("a replay changed pipe %+v", after[i])
				}
			}
			ctx, cancel := context.WithTimeout(e.ctx, 60*time.Second)
			defer cancel()
			echoOnce(t, ctx, e.c, e.s)
		})
		t.Run(mode.String()+"/suspended", func(t *testing.T) {
			t.Parallel()
			e := newAtkEnv(t, atkOpts{})
			st, err := e.c.OpenStreamSync(e.ctx)
			if err != nil {
				t.Fatal(err)
			}
			e.suspend()
			if _, err := st.Write(make([]byte, 512)); err != nil {
				t.Fatal(err)
			}
			added := e.s.ConnectionStats().PipesAdded
			hello, _ := e.capture(mode)
			if _, replied := e.offer(hello); !replied {
				t.Fatal("no Reply")
			}
			if s := e.s.ConnectionStats(); s.PipesActive != 0 || s.PipesAdded != added {
				t.Fatalf("a replay ended the suspension: %+v", s)
			}
			p, err := e.c.AddPipe(e.ctx, NetPipe(e.dial(e.addr)), PipeOptions{RecordMode: Set(mode)})
			if err != nil {
				t.Fatal(err)
			}
			waitTestAttachment(t, e.ctx, p, e.s)
			if n := e.s.ConnectionStats().PipesActive; n != 1 {
				t.Fatalf("%d active pipes after resumption, want 1", n)
			}
			runtime.KeepAlive(st)
		})
	}
}

// A Bound Hello is bound to its carrier: on another one it does not open.
func TestReplayBoundOtherCarrierFails(t *testing.T) {
	t.Parallel()
	e := newAtkEnv(t, atkOpts{})
	carrier := e.dialTLS(e.tlsTo)
	ex, ok := Exporter(NetPipe(carrier))
	if !ok {
		t.Fatal("no exporter")
	}
	binding, err := ex.ExportKeyingMaterial(join.ExporterLabel, nil, join.BindingLen)
	if err != nil {
		t.Fatal(err)
	}
	hello, _ := e.forge(20, join.Bound, binding, nil)
	other := e.dialTLS(e.tlsTo)
	if _, err := other.Write(hello); err != nil {
		t.Fatal(err)
	}
	waitTestEvent(t, e.ctx, e.s, "join authentication failure", func(ev Event) bool { return ev.Kind == EventJoinAuthFailed })
	if n := e.s.ConnectionStats().JoinAuthFailures; n != 1 {
		t.Fatalf("%d authentication failures, want 1", n)
	}
	if n, err := other.Read(make([]byte, 1)); n != 0 || err == nil {
		t.Fatalf("answered on the other carrier: %d, %v", n, err)
	}
	// Control: on its own carrier the same Hello gets a Reply.
	if _, err := carrier.Write(hello); err != nil {
		t.Fatal(err)
	}
	var one [1]byte
	if _, err := io.ReadFull(carrier, one[:]); err != nil || one[0] != join.ReplyType {
		t.Fatalf("control: %x, %v", one, err)
	}
}

// A path party that forwards its copy first takes the Pipe ID: the original fails as
// lost, and nothing else happens.
func TestReplayTakesPipeID(t *testing.T) {
	t.Parallel()
	e := newAtkEnv(t, atkOpts{})
	addr := atkRelay(t, func(in net.Conn) {
		b := make([]byte, 64<<10)
		n, err := in.Read(b)
		if err != nil {
			return
		}
		copyCarrier, err := net.Dial("tcp", e.addr)
		if err != nil {
			return
		}
		defer func() { _ = copyCarrier.Close() }()
		_, _ = copyCarrier.Write(b[:n])
		var one [1]byte
		if _, err := io.ReadFull(copyCarrier, one[:]); err != nil {
			return
		}
		out, err := net.Dial("tcp", e.addr)
		if err != nil {
			return
		}
		defer func() { _ = out.Close() }()
		_, _ = out.Write(b[:n])
		_, _ = io.Copy(in, out)
	})
	if _, err := e.c.AddPipe(e.ctx, NetPipe(e.dial(addr)), PipeOptions{}); err == nil {
		t.Fatal("the original joined after its copy")
	}
	if n := e.s.ConnectionStats().JoinReplays; n != 1 {
		t.Fatalf("JoinReplays %d", n)
	}
	e.alive(e.c)
	e.alive(e.s)
	if _, err := e.c.AddPipe(e.ctx, NetPipe(e.dial(e.addr)), PipeOptions{}); err != nil {
		t.Fatalf("a later join: %v", err)
	}
}

// A first batch replayed many times reaches the application once.
func TestReplayFirstBatchDeduplicated(t *testing.T) {
	t.Parallel()
	e := newAtkEnv(t, atkOpts{})
	before := e.s.ConnectionStats().StreamsOpenedRemote
	st, err := e.c.OpenStreamSync(e.ctx)
	if err != nil {
		t.Fatal(err)
	}
	e.suspend()
	msg := make([]byte, 4096)
	_, _ = rand.Read(msg)
	if _, err := st.Write(msg); err != nil {
		t.Fatal(err)
	}
	hello, _ := e.capture(RecordSealed)
	if bytes.Contains(hello, msg[:64]) {
		t.Fatal("plaintext in the Hello")
	}
	var ss *Stream
	for range 5 {
		// Only the first copy of an ID gets as far as the batch; the bit stops the rest.
		_, _ = e.offer(hello)
	}
	if ss, err = e.s.AcceptStream(e.ctx); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(msg))
	if _, err := io.ReadFull(ss, got); err != nil || !bytes.Equal(got, msg) {
		t.Fatalf("batch: %v", err)
	}
	if _, err := e.c.AddPipe(e.ctx, NetPipe(e.dial(e.addr)), PipeOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Write([]byte("TAIL")); err != nil {
		t.Fatal(err)
	}
	tail := make([]byte, 4)
	if _, err := io.ReadFull(ss, tail); err != nil || string(tail) != "TAIL" {
		t.Fatalf("after the batch: %q, %v", tail, err)
	}
	if opened := e.s.ConnectionStats().StreamsOpenedRemote - before; opened != 1 {
		t.Fatalf("first batch opened %d streams, want 1", opened)
	}
	runtime.KeepAlive(st)
}

// ---- gating and liveness of a JOINING pipe ----

// A JOINING pipe has no SUSPECT and no stall: however long the Reply takes, within the
// handshake's bound, no liveness clock runs on it. The clocks start at ACTIVE.
func TestJoiningHasNoLiveness(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		e := newMemMPEnv(t, mpConfig(), mpConfig())
		live := PipeOptions{Liveness: Set(PipeLiveness{SuspectFloor: 50 * time.Millisecond, DeadTimeout: time.Second})}
		st, err := e.client.OpenStreamSync(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := st.Write(make([]byte, 1<<10)); err != nil {
			t.Fatal(err)
		}
		a, b := net.Pipe()
		defer func() { _ = a.Close(); _ = b.Close() }()
		type result struct {
			p   *Pipe
			err error
		}
		done := make(chan result, 1)
		go func() {
			p, err := e.client.AddPipe(t.Context(), NetPipe(a), live)
			done <- result{p, err}
		}()
		synctest.Wait() // Hello cannot be handed off until the peer reads.
		synctest.Sleep(1500 * time.Millisecond)
		synctest.Wait()
		for _, ps := range e.client.PipeStats() {
			if ps.PipeID != 0 && (ps.SuspectCount != 0 || ps.FailureCount != 0 || ps.Outbound == OutboundSuspect) {
				t.Fatalf("a JOINING pipe has liveness: %+v", ps)
			}
		}
		select {
		case r := <-done:
			t.Fatalf("the join ended before the peer read its Hello: %v", r.err)
		default:
		}
		go func() { _ = e.lst.HandlePipe(t.Context(), NetPipe(b), PipeHint{}) }()
		r := <-done
		if r.err != nil {
			t.Fatalf("the join: %v", r.err)
		}
		ps, ok := r.p.Stats()
		if !ok || ps.Outbound != OutboundActive || ps.FailureCount != 0 || ps.SuspectCount != 0 {
			t.Fatalf("a newly active pipe has a failure from before: %+v", ps)
		}
		runtime.KeepAlive(st)
	})
}

// ---- first batch ----

func TestFirstBatchSize(t *testing.T) {
	t.Parallel()
	const over = 256 // the Hello without its batch
	for _, x := range []struct {
		name          string
		cfg           int
		atLeast, most int
	}{
		{"default", 0, 4 << 10, 8192}, // the batch gives way to the Hello floor
		{"set", 1 << 10, 512, 1<<10 + over},
		{"off", -1, 0, over},
	} {
		t.Run(x.name, func(t *testing.T) {
			t.Parallel()
			e := newAtkEnv(t, atkOpts{client: func(c *Config) { c.JoinEarlyHello = x.cfg }})
			st, err := e.c.OpenStreamSync(e.ctx)
			if err != nil {
				t.Fatal(err)
			}
			e.suspend()
			if _, err := st.Write(make([]byte, 64<<10)); err != nil {
				t.Fatal(err)
			}
			hello, _ := e.capture(RecordSealed)
			if len(hello) > 8192 {
				t.Fatalf("Hello of %d bytes, over the handshake message floor", len(hello))
			}
			if len(hello) < x.atLeast || len(hello) > x.most {
				t.Fatalf("Hello of %d bytes, want %d to %d", len(hello), x.atLeast, x.most)
			}
			if x.name == "off" && len(e.openHello(hello).FirstBatch) != 0 {
				t.Fatal("a first batch while off")
			}
			runtime.KeepAlive(st)
		})
	}
}

// No batch carries a FIN, even of a stream already closed for writing.
func TestFirstBatchNoFIN(t *testing.T) {
	t.Parallel()
	e := newAtkEnv(t, atkOpts{})
	st, err := e.c.OpenStreamSync(e.ctx)
	if err != nil {
		t.Fatal(err)
	}
	e.suspend()
	if _, err := st.Write([]byte("whole")); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	hello, _ := e.capture(RecordSealed)
	batch := e.openHello(hello).FirstBatch
	if len(batch) == 0 {
		t.Fatal("no first batch")
	}
	var f wire.Frame
	for b := batch; len(b) > 0; {
		n, derr := wire.DecodeFrame(b, &f)
		if derr != nil {
			t.Fatal(derr)
		}
		if f.Kind != wire.KindStream || f.Fin {
			t.Fatalf("batch frame kind %v fin %v", f.Kind, f.Fin)
		}
		b = b[n:]
	}
	if _, replied := e.offer(hello); !replied {
		t.Fatal("no Reply")
	}
	ss, err := e.s.AcceptStream(e.ctx)
	if err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 5)
	if _, err := io.ReadFull(ss, got); err != nil {
		t.Fatal(err)
	}
	var extra [1]byte
	if n, err := ss.TryRead(extra[:]); n != 0 || !errors.Is(err, ErrWouldBlock) {
		t.Fatalf("after the first batch: %d, %v", n, err)
	}
	if _, err := e.c.AddPipe(e.ctx, NetPipe(e.dial(e.addr)), PipeOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := ss.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("after the real join: %v", err)
	}
}

// A frame in error in a batch is dropped and counted; it never closes the session.
func TestFirstBatchDroppedFrameCounted(t *testing.T) {
	t.Parallel()
	e := newAtkEnv(t, atkOpts{})
	for i, batch := range [][]byte{
		append(wire.AppendStreamHeader(nil, 4, 0, 1, true, true), 'x'),      // FIN
		wire.AppendMaxData(nil, 1<<40),                                      // not STREAM
		append(wire.AppendStreamHeader(nil, 8, 1<<60, 1, false, true), 'x'), // beyond flow control
		{0xff}, // undecodable
	} {
		before := e.s.ConnectionStats().JoinBatchDropped
		hello, _ := e.forge(uint64(20+2*i), join.Sealed, nil, batch)
		if _, replied := e.offer(hello); !replied {
			t.Fatalf("batch %d: no Reply", i)
		}
		if n := e.s.ConnectionStats().JoinBatchDropped; n == before {
			t.Fatalf("batch %d: not counted", i)
		}
		e.alive(e.s)
	}
}

// A reset's final size covers what first batches carried, even with nothing sent on a
// pipe: otherwise the receiver sees a final size below its data.
func TestFirstBatchResetFinalSize(t *testing.T) {
	t.Parallel()
	e := newAtkEnv(t, atkOpts{})
	st, err := e.c.OpenStreamSync(e.ctx)
	if err != nil {
		t.Fatal(err)
	}
	e.suspend()
	if _, err := st.Write(make([]byte, 2048)); err != nil {
		t.Fatal(err)
	}
	hello, _ := e.capture(RecordSealed)
	if _, replied := e.offer(hello); !replied {
		t.Fatal("no Reply")
	}
	ss, err := e.s.AcceptStream(e.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(ss, make([]byte, 2048)); err != nil {
		t.Fatal(err)
	}
	st.CancelWrite(9)
	if _, err := e.c.AddPipe(e.ctx, NetPipe(e.dial(e.addr)), PipeOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := ss.Read(make([]byte, 1)); err == nil || strings.Contains(err.Error(), "FINAL_SIZE") {
		t.Fatalf("read after the reset: %v", err)
	}
	e.alive(e.s)
	e.alive(e.c)
}

// ---- Unconfirmed carriers and the cap ----

// A new authenticated Hello evicts the oldest Unconfirmed carrier and is never refused
// for lack of room; a real join still passes. Config.SessionPolicy.Limits.PendingPeerJoins sets the cap (zero: 4).
func TestUnconfirmedEviction(t *testing.T) {
	for _, limit := range []int{0, 2} {
		t.Run(strconv.Itoa(limit), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				testUnconfirmedEviction(t, limit)
			})
		})
	}
}

func testUnconfirmedEviction(t *testing.T, limit int) {
	t.Helper()
	ccfg, scfg := atkConfig(), atkConfig()
	scfg.SessionPolicy.Limits.PendingPeerJoins = limit
	e := newMemMPEnv(t, ccfg, scfg)
	peer := &atkEnv{t: t, c: e.client}
	want := limit
	if want == 0 {
		want = 4
	}
	joins := make([]chan error, want+2)
	for i := range joins {
		a, b := net.Pipe()
		t.Cleanup(func() { _ = a.Close(); _ = b.Close() })
		joins[i] = make(chan error, 1)
		go func() { joins[i] <- e.lst.HandlePipe(t.Context(), NetPipe(b), PipeHint{}) }()
		hello, joiner := peer.forge(uint64(20+2*i), join.Sealed, nil, nil)
		if _, err := a.Write(hello); err != nil {
			t.Fatal(err)
		}
		r := unitReader{p: NetPipe(a)}
		if err := r.more(); err != nil {
			t.Fatal(err)
		}
		reply, _, err := joiner.ReadReply(r.buf)
		if err != nil || reply.Decision != join.DecisionAccept {
			t.Fatalf("Hello %d: reply %+v, %v", i, reply, err)
		}
	}
	synctest.Wait()
	for i, joined := range joins {
		select {
		case err := <-joined:
			if i >= 2 || err == nil {
				t.Fatalf("unexpected result for Join %d: %v", i, err)
			}
		default:
			if i < 2 {
				t.Fatalf("oldest Join %d was not evicted", i)
			}
		}
	}
	view, err := e.server.Capacity()
	if err != nil || view.PendingPeerJoins != want {
		t.Fatalf("pending Joins: %+v, %v", view, err)
	}
	if st := e.lst.Stats(); st.Refused != 2 || st.Rejected != 0 || st.Joins != 0 {
		t.Fatalf("stats after the evictions: %+v", st)
	}
	p, err := e.pipeDialer()(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.client.AddPipe(t.Context(), p, PipeOptions{}); err != nil {
		t.Fatalf("a real join among pending Joins: %v", err)
	}
}

// Two Joins can receive accepted Replies for one free slot. Their confirmations
// compete for it under the session owner.
func TestCapCheckedAtConfirmed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		scfg := mpConfig()
		scfg.SessionPolicy.Limits = CapacityLimits{SteadyAttachments: 2, JoinHeadroom: new(1)}
		e := newMemMPEnv(t, mpConfig(), scfg)
		p, err := e.pipeDialer()(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := e.client.AddPipe(t.Context(), p, PipeOptions{}); err != nil {
			t.Fatal(err)
		}
		release := make(chan struct{})
		pending := make([]*Pipe, 2)
		for i := range pending {
			a, b := net.Pipe()
			t.Cleanup(func() { _ = a.Close(); _ = b.Close() })
			go func() { _ = e.lst.HandlePipe(t.Context(), NetPipe(b), PipeHint{}) }()
			pending[i], err = e.client.AddPipe(t.Context(), NetPipe(holdLaterWrites(a, release)), PipeOptions{ReturnOnReply: true})
			if err != nil {
				t.Fatalf("a prediction refused: %v", err)
			}
		}
		before, err := e.server.Capacity()
		if err != nil || before.PeerAttachments != 2 || before.PendingPeerJoins != 2 {
			t.Fatalf("before confirmation: %+v, %v", before, err)
		}
		close(release)
		confirmed := 0
		for _, p := range pending {
			if err := p.WaitAttached(t.Context()); err == nil {
				confirmed++
			}
		}
		after, err := e.server.Capacity()
		if err != nil || confirmed != 1 || after.PeerAttachments != 3 || after.PendingPeerJoins != 0 {
			t.Fatalf("after confirmation, confirmed %d: %+v, %v", confirmed, after, err)
		}
	})
}

// ---- session protection ----

// An Unmanaged first pipe has no join keys until its host declares it end to end.
func TestDeclareEndToEndGates(t *testing.T) {
	t.Parallel()
	noDecl := func(c *Config) { c.PipeAuth.EndToEnd = false; c.HandshakeTimeout = 2 * time.Second }
	e := newAtkEnv(t, atkOpts{client: noDecl, server: noDecl})
	if !e.c.ConnectionState().JoinPending || !e.s.ConnectionState().JoinPending {
		t.Fatal("join keys without a declaration")
	}
	if _, err := e.c.AddPipe(e.ctx, NetPipe(e.dial(e.addr)), PipeOptions{}); !errors.Is(err, ErrJoinPending) {
		t.Fatalf("join before the declaration: %v", err)
	}
	// A join at an end that has not declared is held silently, then closed.
	e.c.DeclareEndToEnd()
	if e.c.ConnectionState().JoinPending {
		t.Fatal("pending after the declaration")
	}
	if _, err := e.c.AddPipe(e.ctx, NetPipe(e.dial(e.addr)), PipeOptions{}); err == nil {
		t.Fatal("the peer has not declared: a join passed")
	}
	if e.s.ConnectionStats().JoinPending == 0 {
		t.Fatal("the held join is not counted")
	}
	if n := e.s.ConnectionStats().JoinAuthFailures; n != 0 {
		t.Fatalf("a held join counted as %d auth failures", n)
	}
	e.s.DeclareEndToEnd()
	if _, err := e.c.AddPipe(e.ctx, NetPipe(e.dial(e.addr)), PipeOptions{}); err != nil {
		t.Fatalf("join after both declared: %v", err)
	}
}

// Unmanaged stays: the session's secret came from a pipe its host only declared.
func TestWeakestRecordModeUnmanaged(t *testing.T) {
	t.Parallel()
	e := newAtkEnv(t, atkOpts{})
	if m := e.c.WeakestRecordMode(); m != RecordUnmanaged {
		t.Fatalf("first pipe: %v", m)
	}
	p, err := e.c.AddPipe(e.ctx, NetPipe(e.dial(e.addr)), PipeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if st, _ := p.Stats(); st.RecordMode != RecordSealed {
		t.Fatalf("joined pipe %v", st.RecordMode)
	}
	waitTestAttachment(t, e.ctx, p, e.s)
	first, _ := e.c.Pipe(0)
	_ = first.Abort()
	if m := e.c.WeakestRecordMode(); m != RecordUnmanaged {
		t.Fatalf("after the first pipe: %v", m)
	}
}

// sealedFirst is a first pipe on the secure carrier.
type sealedFirst struct{ *kmConn }

func (sealedFirst) RecordMode() RecordMode { return RecordSealed }

// On the secure carrier the session's protection is its weakest pipe's.
func TestWeakestRecordModeSecure(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cfg := func() *Config {
		return &Config{SessionPolicy: SessionPolicy{Limits: CapacityLimits{SteadyAttachments: 4}}, SchedulerParams: SchedulerParams{Probation: -1},
			AllowRecordMode: func(RecordPolicyInput, RecordMode, any) bool { return true }}
	}
	lst := mustNewListener(&ListenerConfig{Config: cfg()})
	defer func() { _ = lst.Close() }()
	ck, sk := kmPair("s0")
	sf, cf := sealedFirst{sk}, sealedFirst{ck}
	go func() { _ = lst.HandlePipe(ctx, PipeIO{R: sf, W: sf}, PipeHint{}) }()
	c, err := Start(ctx, PipeIO{R: cf, W: cf}, RoleClient, cfg())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	s, err := lst.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	echoOnce(t, ctx, c, s)
	if m := c.WeakestRecordMode(); m != RecordSealed {
		t.Fatalf("first pipe: %v", m)
	}
	a, b := tcpPair(t)
	go func() { _ = lst.HandlePipe(ctx, NetPipe(b), PipeHint{}) }()
	p, err := c.AddPipe(ctx, NetPipe(a), PipeOptions{RecordMode: Set(RecordTrusted)})
	if err != nil {
		t.Fatal(err)
	}
	if m := c.WeakestRecordMode(); m != RecordTrusted {
		t.Fatalf("with a Trusted pipe: %v", m)
	}
	waitTestAttachment(t, ctx, p, s)
	if m := s.WeakestRecordMode(); m != RecordTrusted {
		t.Fatalf("acceptor with a Trusted pipe: %v", m)
	}
	if err := p.Abort(); err != nil {
		t.Fatal(err)
	}
	waitTestSignal(t, p.Done(), "Trusted retirement")
	if m := c.WeakestRecordMode(); m != RecordSealed {
		t.Fatalf("after Trusted retirement: %v", m)
	}
}

// The acceptor takes only Sealed unless it allows more, and a refused Mode leaves the
// session alone.
func TestAcceptorDefaultSealedOnly(t *testing.T) {
	t.Parallel()
	e := newAtkEnv(t, atkOpts{server: func(c *Config) { c.AllowRecordMode = nil }})
	if _, err := e.c.AddPipe(e.ctx, NetPipe(e.dial(e.addr)), PipeOptions{RecordMode: Set(RecordTrusted)}); err == nil {
		t.Fatal("Trusted at a default acceptor")
	}
	e.alive(e.s)
	if _, err := e.c.AddPipe(e.ctx, NetPipe(e.dial(e.addr)), PipeOptions{}); err != nil {
		t.Fatalf("Sealed: %v", err)
	}
}

// ---- the reject policy ----

func TestOnReject(t *testing.T) {
	t.Parallel()
	unknown := func() []byte {
		h := []byte{join.ProfileJoin, join.SuiteAES, byte(join.Sealed), 16}
		h = append(h, make([]byte, 16)...)
		h = append(h, make([]byte, 32)...)
		h = append(h, 0, 32)
		return append(h, make([]byte, 32)...)
	}()
	for _, x := range []struct {
		name  string
		act   RejectAction
		stall time.Duration
	}{{"close", RejectClose, 0}, {"stall", RejectStall, 300 * time.Millisecond}} {
		t.Run(x.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				got := make(chan RejectInfo, 1)
				cfg := atkConfig()
				cfg.ClockDomain = NewClockDomain(timeshim.Real)
				cfg.JoinTimeout = x.stall
				l := mustNewListener(&ListenerConfig{Config: cfg, OnReject: func(i RejectInfo) RejectAction {
					got <- i
					return x.act
				}})
				defer func() { _ = l.Close() }()
				checkRejectedJoinClose(t, l, unknown, x.stall)
				if i := <-got; i.Reason != "unknown_locator" || i.Profile != join.ProfileJoin {
					t.Fatalf("OnReject: %+v", i)
				}
			})
		})
	}
}

// A stalled join that names a session uses the listener's join deadline.
func TestJoinStallListenerTimeout(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		const stall = 300 * time.Millisecond
		got := make(chan string, 1)
		server := atkConfig()
		server.JoinTimeout = stall
		env := newMemMPEnvOn(t, atkConfig(), &ListenerConfig{Config: server,
			SessionConfig: func(context.Context, *CarrierInfo, any) (*Config, error) {
				c := server.Clone()
				c.JoinTimeout = time.Minute
				return c, nil
			},
			OnReject: func(i RejectInfo) RejectAction { got <- i.Reason; return RejectStall },
		})
		e := &atkEnv{t: t, c: env.client}
		hello, _ := e.forge(40, join.Sealed, nil, nil)
		hello[len(hello)-1] ^= 1 // fails authentication
		checkRejectedJoinClose(t, env.lst, hello, stall)
		if r := <-got; r != "auth" {
			t.Fatalf("reason %q", r)
		}
	})
}

// checkRejectedJoinClose runs in a synctest bubble on an in-memory carrier.
func checkRejectedJoinClose(t *testing.T, l *Listener, hello []byte, stall time.Duration) {
	t.Helper()
	a, b := net.Pipe()
	defer func() { _ = a.Close(); _ = b.Close() }()
	go func() { _ = l.HandlePipe(t.Context(), NetPipe(b), PipeHint{}) }()
	if _, err := a.Write(hello); err != nil {
		t.Fatal(err)
	}
	type readResult struct {
		err error
		n   int64
	}
	done := make(chan readResult, 1)
	go func() {
		n, err := io.Copy(io.Discard, a)
		done <- readResult{err, n}
	}()
	if stall > 0 {
		synctest.Sleep(stall - time.Nanosecond)
		synctest.Wait()
		select {
		case r := <-done:
			t.Fatalf("carrier ended before the stall deadline: %+v", r)
		default:
		}
		synctest.Sleep(time.Nanosecond)
	}
	synctest.Wait()
	select {
	case r := <-done:
		if r.err != nil || r.n != 0 {
			t.Fatalf("carrier ended with %d bytes, %v", r.n, r.err)
		}
	default:
		t.Fatal("carrier still open after rejection")
	}
}

// Under memory pressure a join of a session that is not suspended goes through the
// reject policy as memory_pressure.
func TestJoinMemoryPressureRejects(t *testing.T) {
	t.Parallel()
	got := make(chan string, 4)
	e := newAtkEnv(t, atkOpts{listener: func(l *ListenerConfig) {
		l.OnReject = func(i RejectInfo) RejectAction { got <- i.Reason; return RejectClose }
	}})
	full := e.lst.budget.Limit() - e.lst.budget.Used() - uint64(receiveFlightSize(nil))
	if !e.lst.budget.TryCharge(full) {
		t.Fatal("failed to fill the listener budget")
	}
	defer e.lst.budget.Release(full)
	hello, _ := e.forge(42, join.Sealed, nil, nil)
	if _, replied := e.offer(hello); replied {
		t.Fatal("a Reply under pressure")
	}
	select {
	case r := <-got:
		if r != "memory_pressure" {
			t.Fatalf("reason %q", r)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no OnReject")
	}
}

// ---- split carriers over HTTP ----

// ---- lanes ----

type laneConn struct {
	net.Conn
	id    any
	opens *atomic.Int64
}

func (c laneConn) CarrierConn() any { return c.id }

func (c laneConn) OpenLane(context.Context) (io.WriteCloser, error) {
	c.opens.Add(1)
	return nil, errors.New("no lane in this test")
}

// Sealed hides the carrier's lanes, on both ends: a joined pipe never opens one.
func TestSealedRefusesLanes(t *testing.T) {
	t.Parallel()
	var opens atomic.Int64
	e := newAtkEnv(t, atkOpts{client: func(c *Config) { c.MaxLanes = 4 }, server: func(c *Config) { c.MaxLanes = 4 }})
	carrier := laneConn{Conn: e.dial(e.addr), id: new(int), opens: &opens}
	p, err := e.c.AddPipe(e.ctx, PipeIO{R: carrier, W: carrier, Carrier: CarrierQUIC}, PipeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	first, _ := e.c.Pipe(0)
	waitTestAttachment(t, e.ctx, p, e.s)
	_ = first.Abort()
	ctx, cancel := context.WithTimeout(e.ctx, 60*time.Second)
	defer cancel()
	echoOnce(t, ctx, e.c, e.s)
	if n := opens.Load(); n != 0 {
		t.Fatalf("a Sealed pipe opened %d lanes", n)
	}
	if st, _ := p.Stats(); st.LanesOut > 1 {
		t.Fatalf("lanes %d", st.LanesOut)
	}
}

// ---- what a carrier's end means ----

// finRelay carries a Sealed join from the joiner to the acceptor unit by unit, and can
// drop whole records, then end the carrier at a record boundary.
type finRelay struct {
	drop atomic.Bool
}

func (r *finRelay) serve(in, out net.Conn) {
	var buf []byte
	b := make([]byte, 64<<10)
	skip := -1 // the clear part of the Hello, known once its Locator length has come
	dropped := 0
	for {
		n, err := in.Read(b)
		buf = append(buf, b[:n]...)
		if err != nil {
			return
		}
		if skip < 0 && len(buf) >= 4 {
			skip = 4 + int(buf[3]) + 32
			if len(buf) < skip {
				skip = -1
				continue
			}
			if _, err := out.Write(buf[:skip]); err != nil {
				return
			}
			buf = buf[skip:]
		}
		for skip >= 0 && len(buf) >= 2 {
			unit := 2 + int(binary.BigEndian.Uint16(buf))
			if len(buf) < unit {
				break
			}
			if r.drop.Load() {
				dropped += unit
				if dropped >= 2<<10 {
					_ = out.(*net.TCPConn).CloseWrite()
					_, _ = io.Copy(io.Discard, in)
					return
				}
			} else if _, err := out.Write(buf[:unit]); err != nil {
				return
			}
			buf = buf[unit:]
		}
	}
}

// A carrier that ends at a record boundary with no PIPE_ABANDON fails its pipe: the data
// it lost is sent again and no stream sees an end.
func TestSealedEndAtRecordBoundaryIsAFailure(t *testing.T) {
	t.Parallel()
	e := newAtkEnv(t, atkOpts{})
	r := &finRelay{}
	addr := atkRelay(t, func(in net.Conn) {
		out, err := net.Dial("tcp", e.addr)
		if err != nil {
			return
		}
		defer func() { _ = out.Close() }()
		go func() { _, _ = io.Copy(in, out); _ = in.Close() }()
		r.serve(in, out)
	})
	p, err := e.c.AddPipe(e.ctx, NetPipe(e.dial(addr)), PipeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	sp := waitTestAttachment(t, e.ctx, p, e.s)
	// Data must take the relayed pipe, whose round trip the default scheduler
	// may avoid as long as the direct pipe's is shorter.
	e.c.SetScheduler(SchedulerSpreadRoundRobin())
	st, err := e.c.OpenStreamSync(e.ctx)
	if err != nil {
		t.Fatal(err)
	}
	// No redundant copies: a copy acknowledged on the direct pipe would leave
	// the failed pipe nothing owed, and so nothing to reinject.
	st.SetRedundant(false)
	if _, err := st.Write([]byte{0}); err != nil {
		t.Fatal(err)
	}
	ss, err := e.s.AcceptStream(e.ctx)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.New()
	read := make(chan int64, 1)
	go func() {
		n, _ := io.Copy(sum, ss)
		read <- n
	}()
	r.drop.Store(true)
	chunk := bytes.Repeat([]byte{7}, 4<<10)
	want := sha256.New()
	_, _ = want.Write([]byte{0})
	total := int64(1)
	write := func() {
		t.Helper()
		if _, err := st.Write(chunk); err != nil {
			t.Fatal(err)
		}
		_, _ = want.Write(chunk)
		total += int64(len(chunk))
	}
	for range 16 {
		write()
	}
	waitTestSignal(t, sp.Done(), "acceptor's failed pipe")
	select {
	case n := <-read:
		t.Fatalf("the stream ended after %d bytes", n)
	default:
	}
	write()
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case n := <-read:
		if n != total || !bytes.Equal(sum.Sum(nil), want.Sum(nil)) {
			t.Fatalf("read %d of %d bytes, intact %v", n, total, bytes.Equal(sum.Sum(nil), want.Sum(nil)))
		}
	case <-e.ctx.Done():
		t.Fatal("the stream's data did not all arrive")
	}
	if n := e.c.ConnectionStats().BytesReinjected; n == 0 {
		t.Fatal("no data reinjected after the pipe failed")
	}
	e.alive(e.s)
	e.alive(e.c)
}

// A Hello in Bound on a carrier with no Binding is refused as auth and counted.
func TestBoundHelloWithoutBinding(t *testing.T) {
	t.Parallel()
	reason := make(chan string, 1)
	e := newAtkEnv(t, atkOpts{listener: func(l *ListenerConfig) {
		l.OnReject = func(i RejectInfo) RejectAction {
			reason <- i.Reason
			return RejectClose
		}
	}})
	hello, _ := e.forge(20, join.Bound, make([]byte, join.BindingLen), nil)
	if _, replied := e.offer(hello); replied {
		t.Fatal("a Reply on a carrier without a Binding")
	}
	select {
	case got := <-reason:
		if got != "auth" {
			t.Fatalf("refusal reason %q, want auth", got)
		}
	case <-e.ctx.Done():
		t.Fatal(e.ctx.Err())
	}
	if n := e.s.ConnectionStats().JoinAuthFailures; n != 1 {
		t.Fatalf("join authentication failures: %d", n)
	}
	e.alive(e.s)
}

// testSuiteOutsideSet is a Suite this process has but never offers, so it is
// outside every session's set.
const testSuiteOutsideSet = 63

func init() {
	join.Register(join.Suite{ID: testSuiteOutsideSet, AEAD: join.AESGCM{}, Hash: join.SHA256{}, NonceLen: 32})
}

// A Suite outside the implementation, or outside the found session's set, goes to
// the reject policy as unknown_suite, before any AEAD.
func TestJoinUnknownSuiteRejects(t *testing.T) {
	t.Parallel()
	for _, x := range []struct {
		name  string
		suite byte
	}{{"implementation", 62}, {"session", testSuiteOutsideSet}} {
		t.Run(x.name, func(t *testing.T) {
			t.Parallel()
			got := make(chan RejectInfo, 4)
			e := newAtkEnv(t, atkOpts{listener: func(l *ListenerConfig) {
				l.OnReject = func(i RejectInfo) RejectAction { got <- i; return RejectClose }
			}})
			_, psk, loc := e.keyOf()
			su, _ := join.LookupSuite(testSuiteOutsideSet)
			j, err := join.NewJoiner(join.JoinerConfig{Suite: su, Group: atkGroup, Mode: join.Sealed, Framing: join.Stream, Locator: loc, PSK: psk})
			if err != nil {
				t.Fatal(err)
			}
			hello, err := j.WriteHello(nil, join.HelloPayload{Pipe: join.Pipe{ID: 44, Direction: 2}})
			if err != nil {
				t.Fatal(err)
			}
			hello[1] = x.suite
			if _, replied := e.offer(hello); replied {
				t.Fatal("a Reply to an unusable Suite")
			}
			select {
			case i := <-got:
				if i.Reason != "unknown_suite" || i.Profile != join.ProfileJoin {
					t.Fatalf("OnReject %q profile %d", i.Reason, i.Profile)
				}
			case <-e.ctx.Done():
				t.Fatal("no OnReject")
			}
			if n := e.s.ConnectionStats().JoinAuthFailures; n != 0 {
				t.Fatalf("join authentication failures: %d", n)
			}
			e.alive(e.s)
		})
	}
}

// A carrier beyond the pending limit of its source, the first check of an
// accepted carrier, goes to the reject policy as pending_limit, then is
// closed at once: without a slot it cannot be stalled.
func TestPendingLimitRejects(t *testing.T) {
	t.Parallel()
	for _, x := range []struct {
		name string
		act  RejectAction
	}{{"close", RejectClose}, {"stall", RejectStall}} {
		t.Run(x.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				got := make(chan RejectInfo, 4)
				cfg := atkConfig()
				cfg.ClockDomain = NewClockDomain(timeshim.Real)
				lst := mustNewListener(&ListenerConfig{Config: cfg, MaxPendingPerSource: 2, FirstRecordTimeout: 30 * time.Second,
					OnReject: func(i RejectInfo) RejectAction { got <- i; return x.act }})
				defer func() { _ = lst.Close() }()
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				src := netip.MustParseAddrPort("192.0.2.1:1000")
				held := make([]net.Conn, 0, 2)
				defer func() {
					for _, a := range held {
						_ = a.Close()
					}
				}()
				for range 2 { // they hold the source's slots without a first record
					a, b := net.Pipe()
					held = append(held, a)
					go func() { _ = lst.HandlePipe(ctx, NetPipe(b), PipeHint{Source: src}) }()
				}
				synctest.Wait()
				lst.pmu.Lock()
				n := lst.pending.Held(lst.sourceKey(src))
				lst.pmu.Unlock()
				if n != 2 {
					t.Fatalf("held %d slots, want 2", n)
				}
				a, b := net.Pipe()
				defer func() { _ = a.Close() }()
				start := time.Now()
				if err := lst.HandlePipe(ctx, NetPipe(b), PipeHint{Source: src}); !errors.Is(err, errRefused) {
					t.Fatalf("HandlePipe: %v", err)
				}
				if d := time.Since(start); d != 0 {
					t.Fatalf("refusal took %v", d)
				}
				if _, err := a.Read(make([]byte, 1)); err == nil {
					t.Fatal("a byte on the refused carrier")
				}
				i := <-got
				if i.Reason != "pending_limit" || i.Profile != 0 || i.Src != src {
					t.Fatalf("OnReject %+v", i)
				}
				if lst.Stats().Refused != 1 {
					t.Fatalf("refused: %d", lst.Stats().Refused)
				}
			})
		})
	}
}
