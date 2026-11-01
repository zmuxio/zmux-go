package zmux

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

// TestHTTPExchangeVectors checks fixed vectors of the exchange carrier:
// the carrier key, both requests and both responses, built by
// the client's encoder and the server's response writer, and verified by
// the server's parser.
func TestHTTPExchangeVectors(t *testing.T) {
	type vec struct{ psk, k, up, upR, down, downR string }
	vecs := []vec{
		{"", "8e08f7810b828921459c8d8c4f1be67963269408f5e7179f7f0df94d417723ef",
			"000102030405060708090a0b0c0d0e0f010000006955b9000102030405060708507a0b4c9f35406fc018ecb298ac332068656c6c6f",
			"477e5b04a29e7fc80fd53eb36fd0715400",
			"000102030405060708090a0b0c0d0e0f0200412c412b6955b9000102030405060708e285ca274bac052e7c0a98cf3174ee5b",
			"0ccf05f246c40f075271dd7e52d2420401776f726c64"},
		{hex.EncodeToString(bytes.Repeat([]byte{0x6b}, 32)), "68489b10ee27b348be274432395cb2107dcfc8cf3f9f69f2cd4cf7cd7a0f8c95",
			"000102030405060708090a0b0c0d0e0f010000006955b90001020304050607089b83951cb9d54b75d8d0785f3eb2d3d668656c6c6f",
			"c5b18d1988c3d46dbfee70883026026000",
			"000102030405060708090a0b0c0d0e0f0200412c412b6955b9000102030405060708d48a0293326ba2533431172737bc96a7",
			"f900c6512f2edcc31275640df468edf201776f726c64"},
	}
	var cid [exCIDLen]byte
	for i := range cid {
		cid[i] = byte(i)
	}
	nonce := [exNonceLen]byte{1, 2, 3, 4, 5, 6, 7, 8}
	for _, v := range vecs {
		psk, _ := hex.DecodeString(v.psk)
		key := newExKey(psk, cid[:])
		if got := hex.EncodeToString(key.k[:]); got != v.k {
			t.Fatalf("K = %s, want %s", got, v.k)
		}
		p, err := (&HTTPExchangeConfig{PSK: psk, AllowInsecure: len(psk) == 0}).resolve()
		if err != nil {
			t.Fatal(err)
		}
		h := &HTTPExchangeHandler{p: p}
		for _, x := range []struct {
			kind, respFlags byte
			seq, ack        uint64
			data, respData  string
			req, resp       string
		}{
			{exKindUp, 0, 0, 0, "hello", "", v.up, v.upR},
			{exKindDown, exFlagFin, 300, 299, "", "world", v.down, v.downR},
		} {
			b := appendExReqHdr(nil, &cid, x.kind, 0, x.seq, x.ack, 1767225600, &nonce)
			mac := key.requestMAC(b, []byte(x.data))
			b = append(append(b, mac[:]...), x.data...)
			if got := hex.EncodeToString(b); got != x.req {
				t.Fatalf("request = %s, want %s", got, x.req)
			}
			q, ok := parseExReq(b)
			if !ok || q.seq != x.seq || q.ack != x.ack || string(q.data) != x.data {
				t.Fatalf("parse %s: %+v %v", x.req, q, ok)
			}
			if m := key.requestMAC(q.hdr, q.data); !bytes.Equal(m[:], q.mac) {
				t.Fatal("request MAC does not verify")
			}
			rec := httptest.NewRecorder()
			h.respond(rec, key, &q, x.respFlags, []byte(x.respData))
			if got := hex.EncodeToString(rec.Body.Bytes()); got != x.resp {
				t.Fatalf("response = %s, want %s", got, x.resp)
			}
		}
	}
}

// newExHandler returns a handler of l or fails the test.
func newExHandler(t testing.TB, l *Listener, cfg *HTTPExchangeConfig) *HTTPExchangeHandler {
	t.Helper()
	h, err := NewHTTPExchangeHandler(l, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

// TestHTTPExchangeConfigAuth checks that authentication is off only when
// explicitly disabled.
func TestHTTPExchangeConfigAuth(t *testing.T) {
	for _, cfg := range []*HTTPExchangeConfig{nil, {}, {PSK: []byte("k"), AllowInsecure: true}} {
		if _, err := NewHTTPExchangeHandler(nil, cfg); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("handler %+v: %v", cfg, err)
		}
		if _, err := HTTPExchangePipe(context.Background(), nil, "http://x/", cfg); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("pipe %+v: %v", cfg, err)
		}
	}
}

// exEnv is a listener behind an HTTPExchangeHandler on an httptest server,
// a client session on an exchange carrier, and its server session.
type exEnv struct {
	lst    *Listener
	h      *HTTPExchangeHandler
	srv    *httptest.Server
	tr     *http.Transport
	client *Conn
	server *Conn
}

// newExEnv starts the environment; front, when set, is the URL clients
// use instead of the server's (a proxy in front of it).
func newExEnv(t *testing.T, ccfg, scfg *HTTPExchangeConfig, front func(origin string) string) *exEnv {
	t.Helper()
	e := &exEnv{lst: NewListener(&ListenerConfig{Config: mpConfig()})}
	e.h = newExHandler(t, e.lst, scfg)
	e.srv = httptest.NewServer(e.h)
	e.tr = &http.Transport{MaxIdleConnsPerHost: 16}
	u := e.srv.URL + "/x/{instance}/{nonce}"
	if front != nil {
		u = front(e.srv.URL) + "/x/{instance}"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// ctx ends when this returns: it bounds the setting up only.
	p, err := HTTPExchangePipe(ctx, &http.Client{Transport: e.tr}, u, ccfg)
	if err != nil {
		t.Fatal(err)
	}
	if e.client, err = Start(ctx, p, RoleClient, mpConfig()); err != nil {
		t.Fatal(err)
	}
	if e.server, err = e.lst.Accept(ctx); err != nil {
		t.Fatal(err)
	}
	return e
}

// close ends both sessions and the handler and checks that no memory stays
// charged.
func (e *exEnv) close(t *testing.T) {
	t.Helper()
	_ = e.client.Close()
	_ = e.server.Close()
	waitDone(t, e.client)
	waitDone(t, e.server)
	_ = e.h.Close()
	_ = e.lst.Close()
	e.srv.Close()
	e.tr.CloseIdleConnections()
	if m := e.lst.MemoryUsed(); m != 0 {
		t.Errorf("%d bytes still charged to the memory budget", m)
	}
}

// echo sends n random bytes on a new stream to an echoing server and
// checks the hash of what comes back.
func (e *exEnv) echo(t *testing.T, n int, timeout time.Duration) {
	t.Helper()
	if err := e.echoErr(n, timeout); err != nil {
		t.Fatal(err)
	}
}

func (e *exEnv) echoErr(n int, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	go func() {
		st, err := e.server.AcceptStream(ctx)
		if err == nil {
			_ = st.SetDeadline(time.Now().Add(timeout))
			_, _ = io.Copy(st, st)
			_ = st.Close()
		}
	}()
	st, err := e.client.OpenStreamSync(ctx)
	if err != nil {
		return err
	}
	_ = st.SetDeadline(time.Now().Add(timeout))
	msg := make([]byte, n)
	_, _ = rand.Read(msg)
	want := sha256.Sum256(msg)
	go func() { _, _ = st.Write(msg); _ = st.Close() }()
	hsh := sha256.New()
	got, err := io.Copy(hsh, st)
	if err != nil || got != int64(n) || !bytes.Equal(hsh.Sum(nil), want[:]) {
		return fmt.Errorf("echo: %d of %d bytes, hash match %v, %w", got, n, bytes.Equal(hsh.Sum(nil), want[:]), err)
	}
	return nil
}

// TestHTTPExchangeEcho moves a few MiB each way over one carrier, binary
// and base64, and checks that nothing leaks after close.
func TestHTTPExchangeEcho(t *testing.T) {
	for _, b64 := range []bool{false, true} {
		name := "binary"
		if b64 {
			name = "base64"
		}
		t.Run(name, func(t *testing.T) {
			leakcheck.Check(t)
			cfg := &HTTPExchangeConfig{PSK: bytes.Repeat([]byte{9}, 32), Base64: b64,
				UpPieceSize: 256 << 10, DownPieceSize: 256 << 10, HoldTime: 2 * time.Second}
			e := newExEnv(t, cfg, cfg, nil)
			e.echo(t, 4<<20, 30*time.Second)
			e.close(t)
		})
	}
}

// TestHTTPExchangeServerRules checks the server's answers that the
// end-to-end runs do not reach: a bad MAC changes nothing, a DOWN beyond
// the window is BUSY, a Down Ack above the assigned pieces fails the
// carrier, a failed carrier answers GONE, and an authenticated request
// that is malformed fails its carrier.
func TestHTTPExchangeServerRules(t *testing.T) {
	leakcheck.Check(t)
	lst := NewListener(&ListenerConfig{Config: mpConfig(), FirstRecordTimeout: 10 * time.Second})
	defer func() { _ = lst.Close() }()
	cfg := &HTTPExchangeConfig{PSK: []byte("k"), DownWindow: 4, HoldTime: 50 * time.Millisecond}
	h := newExHandler(t, lst, cfg)
	defer func() { _ = h.Close() }()
	var cid [exCIDLen]byte
	_, _ = rand.Read(cid[:])
	key := newExKey([]byte("k"), cid[:])
	send := func(kind byte, seq, ack uint64, corrupt bool) (int, byte) {
		var nonce [exNonceLen]byte
		_, _ = rand.Read(nonce[:])
		b := appendExReqHdr(nil, &cid, kind, 0, seq, ack, uint32(time.Now().Unix()), &nonce)
		mac := key.requestMAC(b, nil)
		if corrupt {
			mac[0] ^= 1
		}
		b = append(b, mac[:]...)
		req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(b))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		body := rec.Body.Bytes()
		if rec.Code != http.StatusOK {
			return rec.Code, 0
		}
		q, _ := parseExReq(b)
		want := key.responseMAC(cid[:], kind, q.seqRaw, q.nonce, body[exMACLen], body[exRespHdr:])
		if !bytes.Equal(want[:], body[:exMACLen]) {
			t.Fatal("response MAC does not verify")
		}
		return rec.Code, body[exMACLen]
	}
	if code, _ := send(exKindDown, 0, 0, true); code != http.StatusNotFound {
		t.Fatalf("bad MAC: status %d", code)
	}
	h.mu.Lock()
	n := len(h.carriers)
	h.mu.Unlock()
	if n != 0 {
		t.Fatal("a request with a bad MAC created a carrier")
	}
	if _, f := send(exKindDown, 0, 0, false); f != 0 {
		t.Fatalf("held DOWN: flags %#x, want an empty piece", f)
	}
	if _, f := send(exKindDown, 4, 0, false); f != exFlagBusy {
		t.Fatalf("DOWN beyond the window: flags %#x", f)
	}
	if _, f := send(exKindDown, 1, 5, false); f != exFlagGone {
		t.Fatalf("Down Ack above the assigned pieces: flags %#x", f)
	}
	if _, f := send(exKindUp, 0, 0, false); f != exFlagGone {
		t.Fatalf("after failure: flags %#x", f)
	}
	// A valid MAC over a request that is malformed (flags in a DOWN)
	// fails a live carrier.
	_, _ = rand.Read(cid[:])
	key = newExKey([]byte("k"), cid[:])
	if _, f := send(exKindDown, 0, 0, false); f != 0 {
		t.Fatalf("new carrier: flags %#x", f)
	}
	bad := appendExReqHdr(nil, &cid, exKindDown, exFlagFin, 1, 0, uint32(time.Now().Unix()), &[exNonceLen]byte{1})
	mac := key.requestMAC(bad, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(append(bad, mac[:]...))))
	if rec.Code != http.StatusOK || rec.Body.Bytes()[exMACLen] != exFlagGone {
		t.Fatalf("invalid request: status %d", rec.Code)
	}
	if _, f := send(exKindDown, 1, 0, false); f != exFlagGone {
		t.Fatalf("after an invalid request: flags %#x", f)
	}
}

// TestHTTPExchangeInterop runs one end against another implementation:
//
//	ZMUX_EXCHANGE=client ZMUX_EXCHANGE_URL=http://host:port/x go test -run TestHTTPExchangeInterop
//	ZMUX_EXCHANGE=server ZMUX_EXCHANGE_ADDR=127.0.0.1:8080 go test -run TestHTTPExchangeInterop
//
// ZMUX_EXCHANGE_PSK (hex) and ZMUX_EXCHANGE_BASE64=1 configure the
// carrier; ZMUX_EXCHANGE_DOWN_URL the client's DOWN target (default
// ZMUX_EXCHANGE_URL); ZMUX_EXCHANGE_BYTES the client's echo size (1 MiB);
// ZMUX_EXCHANGE_FOR how long the server serves (60s). The server echoes
// every stream; the client checks the echo's hash.
func TestHTTPExchangeInterop(t *testing.T) {
	mode := os.Getenv("ZMUX_EXCHANGE")
	if mode == "" {
		t.Skip("set ZMUX_EXCHANGE=client or server")
	}
	psk, err := hex.DecodeString(os.Getenv("ZMUX_EXCHANGE_PSK"))
	if err != nil {
		t.Fatal(err)
	}
	// An empty ZMUX_EXCHANGE_PSK runs without authentication.
	cfg := &HTTPExchangeConfig{PSK: psk, AllowInsecure: len(psk) == 0, Base64: os.Getenv("ZMUX_EXCHANGE_BASE64") == "1",
		DownURL: os.Getenv("ZMUX_EXCHANGE_DOWN_URL")}
	zcfg := &Config{}
	switch mode {
	case "server":
		d, err := time.ParseDuration(envOr("ZMUX_EXCHANGE_FOR", "60s"))
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), d)
		defer cancel()
		lst := NewListener(&ListenerConfig{Config: zcfg})
		h := newExHandler(t, lst, cfg)
		srv := &http.Server{Addr: envOr("ZMUX_EXCHANGE_ADDR", "127.0.0.1:8080"), Handler: h, ReadHeaderTimeout: 10 * time.Second}
		go func() { _ = srv.ListenAndServe() }()
		go func() {
			for {
				c, err := lst.Accept(ctx)
				if err != nil {
					return
				}
				go func() {
					for {
						st, err := c.AcceptStream(ctx)
						if err != nil {
							return
						}
						go func() { _, _ = io.Copy(st, st); _ = st.Close() }()
					}
				}()
			}
		}()
		<-ctx.Done()
		_ = srv.Close()
		_ = h.Close()
		_ = lst.Close()
	case "client":
		n := 1 << 20
		if s := os.Getenv("ZMUX_EXCHANGE_BYTES"); s != "" {
			var err error
			if n, err = strconv.Atoi(s); err != nil {
				t.Fatal(err)
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		p, err := HTTPExchangePipe(ctx, nil, os.Getenv("ZMUX_EXCHANGE_URL"), cfg)
		if err != nil {
			t.Fatal(err)
		}
		c, err := Start(ctx, p, RoleClient, zcfg)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = c.Close() }()
		st, err := c.OpenStreamSync(ctx)
		if err != nil {
			t.Fatal(err)
		}
		msg := make([]byte, n)
		_, _ = rand.Read(msg)
		go func() { _, _ = st.Write(msg); _ = st.Close() }()
		got, err := io.ReadAll(st)
		if err != nil || sha256.Sum256(got) != sha256.Sum256(msg) {
			t.Fatalf("echo: %d of %d bytes, %v", len(got), n, err)
		}
		t.Logf("echoed %d bytes", n)
	default:
		t.Fatalf("ZMUX_EXCHANGE=%q", mode)
	}
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

// TestHTTPExchangeCompletion drives a server carrier to its graceful end
// with raw requests: the uplink FIN is delivered, the downlink FIN is
// fetched, and the client's final CLOSE acknowledging it completes the
// carrier instead of aborting it.
func TestHTTPExchangeCompletion(t *testing.T) {
	leakcheck.Check(t)
	lst := NewListener(&ListenerConfig{Config: mpConfig()})
	defer func() { _ = lst.Close() }()
	h := newExHandler(t, lst, &HTTPExchangeConfig{HoldTime: time.Second, AllowInsecure: true})
	defer func() { _ = h.Close() }()
	var cid [exCIDLen]byte
	_, _ = rand.Read(cid[:])
	key := newExKey(nil, cid[:])
	send := func(kind, flags byte, seq, ack uint64, data []byte) byte {
		var nonce [exNonceLen]byte
		_, _ = rand.Read(nonce[:])
		b := appendExReqHdr(nil, &cid, kind, flags, seq, ack, uint32(time.Now().Unix()), &nonce)
		mac := key.requestMAC(b, data)
		b = append(append(b, mac[:]...), data...)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(b)))
		if rec.Code != http.StatusOK {
			t.Fatalf("status %d", rec.Code)
		}
		return rec.Body.Bytes()[exMACLen]
	}
	// Not a zmux record: the listener rejects the carrier and closes both
	// halves gracefully, which ends the downlink with FIN.
	if f := send(exKindUp, exFlagFin, 0, 0, []byte{0xff}); f != 0 {
		t.Fatalf("UP FIN: flags %#x", f)
	}
	h.mu.Lock()
	c := h.carriers[cid]
	h.mu.Unlock()
	deadline := time.Now().Add(5 * time.Second)
	for {
		c.mu.Lock()
		w := c.wclosed
		c.mu.Unlock()
		if w {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the downlink did not end")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if f := send(exKindDown, 0, 0, 0, nil); f != exFlagFin {
		t.Fatalf("DOWN 0: flags %#x, want FIN", f)
	}
	if f := send(exKindClose, 0, 0, 1, nil); f != 0 {
		t.Fatalf("final CLOSE: flags %#x", f)
	}
	c.mu.Lock()
	ended, err := c.ended, c.err
	c.mu.Unlock()
	if !ended || err != nil {
		t.Fatalf("carrier ended %v with %v, want completion", ended, err)
	}
}

// TestHTTPExchangeListenerClosed checks that a closed listener's handler
// refuses new carriers with GONE and creates none.
func TestHTTPExchangeListenerClosed(t *testing.T) {
	leakcheck.Check(t)
	lst := NewListener(&ListenerConfig{Config: mpConfig()})
	h := newExHandler(t, lst, &HTTPExchangeConfig{HoldTime: time.Second, AllowInsecure: true})
	defer func() { _ = h.Close() }()
	_ = lst.Close()
	var cid [exCIDLen]byte
	_, _ = rand.Read(cid[:])
	key := newExKey(nil, cid[:])
	var nonce [exNonceLen]byte
	b := appendExReqHdr(nil, &cid, exKindUp, 0, 0, 0, uint32(time.Now().Unix()), &nonce)
	mac := key.requestMAC(b, []byte{1})
	b = append(append(b, mac[:]...), 1)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(b)))
	if rec.Code != http.StatusOK || rec.Body.Bytes()[exMACLen] != exFlagGone {
		t.Fatalf("status %d, body %x: want GONE", rec.Code, rec.Body.Bytes())
	}
	h.mu.Lock()
	n := len(h.carriers)
	h.mu.Unlock()
	if n != 0 {
		t.Fatalf("%d carriers created", n)
	}
}

// TestHTTPExchangeBodyLimits checks bodies over the other end's limit: an
// UP request over the server's is not authenticated (it is not read) and
// the carrier fails at the client's deadline; a DOWN response over the
// client's fails the carrier at once.
func TestHTTPExchangeBodyLimits(t *testing.T) {
	leakcheck.Check(t)
	read := func(p PipeIO) error {
		errc := make(chan error, 1)
		go func() {
			_, err := p.R.Read(make([]byte, 16))
			errc <- err
		}()
		defer closePipeIO(p)
		select {
		case err := <-errc:
			return err
		case <-time.After(5 * time.Second):
			return nil
		}
	}
	ctx := context.Background()
	t.Run("up", func(t *testing.T) {
		lst := NewListener(&ListenerConfig{Config: mpConfig()})
		h := newExHandler(t, lst, &HTTPExchangeConfig{UpPieceSize: 16 << 10, HoldTime: 100 * time.Millisecond, AllowInsecure: true})
		srv := httptest.NewServer(h)
		defer func() { srv.Close(); _ = h.Close(); _ = lst.Close() }()
		tr := &http.Transport{}
		defer tr.CloseIdleConnections()
		p, err := HTTPExchangePipe(ctx, &http.Client{Transport: tr}, srv.URL, &HTTPExchangeConfig{UpPieceSize: 2 << 20, Coalesce: -1, AllowInsecure: true, Deadline: time.Second})
		if err != nil {
			t.Fatal(err)
		}
		_, _ = p.W.Write(make([]byte, 2<<20))
		if err := read(p); !errors.Is(err, errExchangeDeadline) {
			t.Fatalf("oversized UP: %v, want the deadline", err)
		}
	})
	t.Run("down", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			w.Header().Set("Content-Length", strconv.Itoa(2<<20))
			_, _ = w.Write(make([]byte, 2<<20))
		}))
		defer srv.Close()
		tr := &http.Transport{}
		defer tr.CloseIdleConnections()
		p, err := HTTPExchangePipe(ctx, &http.Client{Transport: tr}, srv.URL, &HTTPExchangeConfig{DownPieceSize: 16 << 10, AllowInsecure: true})
		if err != nil {
			t.Fatal(err)
		}
		if err := read(p); !errors.Is(err, errExchangeProtocol) {
			t.Fatalf("oversized DOWN: %v, want a protocol failure", err)
		}
	})
}

// TestHTTPExchangeSplitTargets sends UP and DOWN requests to different
// hosts and paths in front of one handler.
func TestHTTPExchangeSplitTargets(t *testing.T) {
	leakcheck.Check(t)
	lst := NewListener(&ListenerConfig{Config: mpConfig()})
	h := newExHandler(t, lst, &HTTPExchangeConfig{HoldTime: time.Second, AllowInsecure: true})
	var mu sync.Mutex
	hits := map[string]int{}
	front := func(prefix string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !strings.HasPrefix(r.URL.Path, prefix) {
				http.NotFound(w, r)
				return
			}
			mu.Lock()
			hits[prefix]++
			mu.Unlock()
			h.ServeHTTP(w, r)
		}))
	}
	up, down := front("/up/"), front("/down/")
	tr := &http.Transport{MaxIdleConnsPerHost: 8}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	p, err := HTTPExchangePipe(context.Background(), &http.Client{Transport: tr}, up.URL+"/up/{instance}",
		&HTTPExchangeConfig{DownURL: down.URL + "/down/{instance}/{nonce}", AllowInsecure: true})
	if err != nil {
		t.Fatal(err)
	}
	e := &exEnv{lst: lst, h: h, srv: up, tr: tr}
	if e.client, err = Start(ctx, p, RoleClient, mpConfig()); err != nil {
		t.Fatal(err)
	}
	if e.server, err = lst.Accept(ctx); err != nil {
		t.Fatal(err)
	}
	e.echo(t, 1<<20, 20*time.Second)
	e.close(t)
	down.Close()
	mu.Lock()
	defer mu.Unlock()
	if hits["/up/"] == 0 || hits["/down/"] == 0 {
		t.Fatalf("requests per front: %v", hits)
	}
}

// TestHTTPExchangeBodySize checks that the default piece size is the
// largest whose bodies stay at most 1,000,000 bytes, and moves MiBs each
// way with it, binary and base64, through a front that records every
// body: none is over the bound.
func TestHTTPExchangeBodySize(t *testing.T) {
	for _, b64 := range []bool{false, true} {
		name := "binary"
		if b64 {
			name = "base64"
		}
		t.Run(name, func(t *testing.T) {
			leakcheck.Check(t)
			p, err := (&HTTPExchangeConfig{AllowInsecure: true, Base64: b64}).resolve()
			if err != nil {
				t.Fatal(err)
			}
			body := func(piece int) int {
				if b64 {
					return b64Len(exReqHdrMax + piece)
				}
				return exReqHdrMax + piece
			}
			if p.upPiece != p.downPiece || body(p.upPiece) > 1_000_000 || body(p.upPiece+3) <= 1_000_000 {
				t.Fatalf("default pieces %d/%d: body %d", p.upPiece, p.downPiece, body(p.upPiece))
			}
			if p.upLimit != 1<<20 || p.downLimit != 1<<20 {
				t.Fatalf("limits %d/%d, want 1 MiB", p.upLimit, p.downLimit)
			}
			var mu sync.Mutex
			var maxReq, maxResp int64
			var fronts []*httptest.Server
			front := func(origin string) string {
				u, _ := url.Parse(origin)
				rp := httputil.NewSingleHostReverseProxy(u)
				rp.ErrorLog = log.New(io.Discard, "", 0)
				rp.ModifyResponse = func(resp *http.Response) error {
					mu.Lock()
					maxResp = max(maxResp, resp.ContentLength)
					mu.Unlock()
					return nil
				}
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					mu.Lock()
					maxReq = max(maxReq, r.ContentLength)
					mu.Unlock()
					rp.ServeHTTP(w, r)
				}))
				fronts = append(fronts, srv)
				return srv.URL
			}
			// A long coalescing delay fills the pieces.
			cfg := &HTTPExchangeConfig{PSK: bytes.Repeat([]byte{9}, 32), Base64: b64, HoldTime: 2 * time.Second,
				Coalesce: 200 * time.Millisecond}
			e := newExEnv(t, cfg, cfg, front)
			e.echo(t, 3<<20, 30*time.Second)
			e.close(t)
			for _, f := range fronts {
				f.Close()
			}
			mu.Lock()
			defer mu.Unlock()
			if maxReq > 1_000_000 || maxResp > 1_000_000 || maxReq <= 0 || maxResp <= 0 {
				t.Fatalf("largest request body %d, response body %d", maxReq, maxResp)
			}
		})
	}
}

// TestExchangeHandlerClass serves a request that ClassifyHTTP does not
// make an exchange request, such as a split uplink with a Content-Length,
// with the fallback before any admission.
func TestExchangeHandlerClass(t *testing.T) {
	decided := false
	l := NewListener(&ListenerConfig{Config: mpConfig(), Admission: &Admission{Decide: func(context.Context, *CarrierInfo, []byte) Decision {
		decided = true
		return Decision{Verdict: VerdictAccept}
	}}})
	defer func() { _ = l.Close() }()
	h := newExHandler(t, l, &HTTPExchangeConfig{PSK: bytes.Repeat([]byte{5}, 32),
		Next: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) })})
	defer func() { _ = h.Close() }()
	r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("xxxx"))
	r.Header.Set(HeaderPair, "AAAA")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusTeapot || decided {
		t.Fatalf("status %d, decided %v", w.Code, decided)
	}
}

// TestHTTPExchangeTruncatedFallback gives the fallback an exchange request
// whose client aborted it before its Content-Length with the bytes that
// arrived and then the read error, not a body that seems complete.
func TestHTTPExchangeTruncatedFallback(t *testing.T) {
	leakcheck.Check(t)
	lst := NewListener(&ListenerConfig{Config: mpConfig()})
	type seen struct {
		body []byte
		err  error
	}
	got := make(chan seen, 1)
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := io.ReadAll(r.Body)
		got <- seen{b, err}
	})
	h := newExHandler(t, lst, &HTTPExchangeConfig{PSK: bytes.Repeat([]byte{5}, 32), Next: next})
	srv := httptest.NewServer(h)
	defer func() { srv.Close(); _ = h.Close(); _ = lst.Close() }()
	c, err := net.Dial("tcp", srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.WriteString(c, "POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 52\r\n\r\npartial")
	_ = c.Close()
	select {
	case s := <-got:
		if string(s.body) != "partial" || !errors.Is(s.err, io.ErrUnexpectedEOF) {
			t.Fatalf("fallback read %q, %v", s.body, s.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the fallback was not called")
	}
}

// TestHTTPExchangeOutOfOrderCost counts a fixed cost per UP piece held out
// of order, so that empty ones are bounded too, while the piece the
// carrier waits for is still taken.
func TestHTTPExchangeOutOfOrderCost(t *testing.T) {
	leakcheck.Check(t)
	lst := NewListener(&ListenerConfig{Config: mpConfig(), FirstRecordTimeout: 10 * time.Second})
	defer func() { _ = lst.Close() }()
	h := newExHandler(t, lst, &HTTPExchangeConfig{PSK: []byte("k"), UpStoreLimit: 1})
	defer func() { _ = h.Close() }()
	var cid [exCIDLen]byte
	_, _ = rand.Read(cid[:])
	key := newExKey([]byte("k"), cid[:])
	up := func(seq uint64) byte {
		var nonce [exNonceLen]byte
		_, _ = rand.Read(nonce[:])
		b := appendExReqHdr(nil, &cid, exKindUp, 0, seq, 0, uint32(time.Now().Unix()), &nonce)
		mac := key.requestMAC(b, nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(append(b, mac[:]...))))
		if rec.Code != http.StatusOK {
			t.Fatalf("UP %d: status %d", seq, rec.Code)
		}
		return rec.Body.Bytes()[exMACLen]
	}
	held := uint64((h.p.upStore - h.p.upLimit) / exPieceCost)
	for n := uint64(1); n <= held; n++ {
		if f := up(n); f != 0 {
			t.Fatalf("UP %d of %d held: flags %#x", n, held, f)
		}
	}
	if f := up(held + 1); f != exFlagBusy {
		t.Fatalf("UP beyond the store: flags %#x", f)
	}
	if f := up(0); f != 0 {
		t.Fatalf("the awaited UP: flags %#x", f)
	}
	if f := up(held + 1); f != 0 {
		t.Fatalf("UP after the held ones were released: flags %#x", f)
	}
}

// TestHTTPExchangePendingShare refuses a new carrier beyond its source's
// share of pending carriers at once: BUSY, with no live or gone state, so
// that it is served once the share is free.
func TestHTTPExchangePendingShare(t *testing.T) {
	leakcheck.Check(t)
	lst := NewListener(&ListenerConfig{Config: mpConfig(), MaxPendingPerSource: 1, FirstRecordTimeout: 10 * time.Second})
	defer func() { _ = lst.Close() }()
	h := newExHandler(t, lst, &HTTPExchangeConfig{PSK: []byte("k")})
	defer func() { _ = h.Close() }()
	send := func(cid *[exCIDLen]byte, kind byte, seq uint64) byte {
		key := newExKey([]byte("k"), cid[:])
		var nonce [exNonceLen]byte
		_, _ = rand.Read(nonce[:])
		b := appendExReqHdr(nil, cid, kind, 0, seq, 0, uint32(time.Now().Unix()), &nonce)
		mac := key.requestMAC(b, nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(append(b, mac[:]...))))
		if rec.Code != http.StatusOK {
			t.Fatalf("status %d", rec.Code)
		}
		return rec.Body.Bytes()[exMACLen]
	}
	state := func(cid *[exCIDLen]byte) (live, gone bool) {
		h.mu.Lock()
		defer h.mu.Unlock()
		_, live = h.carriers[*cid]
		_, gone = h.gone[*cid]
		return
	}
	c1, c2 := &[exCIDLen]byte{1}, &[exCIDLen]byte{2}
	if f := send(c1, exKindUp, 1); f != 0 {
		t.Fatalf("first carrier: flags %#x", f)
	}
	if f := send(c2, exKindUp, 1); f != exFlagBusy {
		t.Fatalf("beyond the share: flags %#x", f)
	}
	if live, gone := state(c2); live || gone {
		t.Fatalf("refused carrier left state: live %v, gone %v", live, gone)
	}
	if f := send(c1, exKindClose, 0); f != 0 {
		t.Fatalf("CLOSE: flags %#x", f)
	}
	deadline := time.Now().Add(5 * time.Second)
	for send(c2, exKindUp, 1) != 0 {
		if time.Now().After(deadline) {
			t.Fatal("the freed share was not given to the refused carrier")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
