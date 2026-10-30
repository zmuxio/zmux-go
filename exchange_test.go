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
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

// TestHTTPExchangeVectors checks the test vectors of HTTP-EXCHANGE.md
// Section 9: the carrier key, both requests and both responses, built by
// the client's encoder and the server's response writer, and verified by
// the server's parser.
func TestHTTPExchangeVectors(t *testing.T) {
	type vec struct{ psk, k, up, upR, down, downR string }
	vecs := []vec{
		{"", "8e08f7810b828921459c8d8c4f1be67963269408f5e7179f7f0df94d417723ef",
			"000102030405060708090a0b0c0d0e0f0100000001020304050607083a03565a66d053fcaa22184304620da468656c6c6f",
			"477e5b04a29e7fc80fd53eb36fd0715400",
			"000102030405060708090a0b0c0d0e0f0200412c412b01020304050607088897fdc82804aa8d78f2c52a8e9a9f4c",
			"0ccf05f246c40f075271dd7e52d2420401776f726c64"},
		{hex.EncodeToString(bytes.Repeat([]byte{0x6b}, 32)), "68489b10ee27b348be274432395cb2107dcfc8cf3f9f69f2cd4cf7cd7a0f8c95",
			"000102030405060708090a0b0c0d0e0f010000000102030405060708bace4bcb910362f6efd873612d5cfbbe68656c6c6f",
			"c5b18d1988c3d46dbfee70883026026000",
			"000102030405060708090a0b0c0d0e0f0200412c412b0102030405060708f5da46bda25a34468f71ad8de3505f08",
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
		h := &HTTPExchangeHandler{p: (&HTTPExchangeConfig{PSK: psk}).resolve()}
		for _, x := range []struct {
			kind, respFlags byte
			seq, ack        uint64
			data, respData  string
			req, resp       string
		}{
			{exKindUp, 0, 0, 0, "hello", "", v.up, v.upR},
			{exKindDown, exFlagFin, 300, 299, "", "world", v.down, v.downR},
		} {
			b := appendExReqHdr(nil, &cid, x.kind, 0, x.seq, x.ack, &nonce)
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
	e.h = NewHTTPExchangeHandler(e.lst, scfg)
	e.srv = httptest.NewServer(e.h)
	e.tr = &http.Transport{MaxIdleConnsPerHost: 16}
	u := e.srv.URL + "/x/{instance}/{nonce}"
	if front != nil {
		u = front(e.srv.URL) + "/x/{instance}"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	p, err := HTTPExchangePipe(context.Background(), &http.Client{Transport: e.tr}, u, ccfg)
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
		return fmt.Errorf("echo: %d of %d bytes, hash match %v, %v", got, n, bytes.Equal(hsh.Sum(nil), want[:]), err)
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
// that violates Section 3 fails its carrier.
func TestHTTPExchangeServerRules(t *testing.T) {
	leakcheck.Check(t)
	lst := NewListener(&ListenerConfig{Config: mpConfig(), FirstRecordTimeout: 10 * time.Second})
	defer func() { _ = lst.Close() }()
	cfg := &HTTPExchangeConfig{PSK: []byte("k"), DownWindow: 4, HoldTime: 50 * time.Millisecond}
	h := NewHTTPExchangeHandler(lst, cfg)
	defer func() { _ = h.Close() }()
	var cid [exCIDLen]byte
	_, _ = rand.Read(cid[:])
	key := newExKey([]byte("k"), cid[:])
	send := func(kind byte, seq, ack uint64, corrupt bool) (int, byte) {
		var nonce [exNonceLen]byte
		_, _ = rand.Read(nonce[:])
		b := appendExReqHdr(nil, &cid, kind, 0, seq, ack, &nonce)
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
	if code, _ := send(exKindDown, 0, 0, true); code != http.StatusForbidden {
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
	// A valid MAC over a request that violates Section 3 (flags in a DOWN)
	// fails a live carrier.
	_, _ = rand.Read(cid[:])
	key = newExKey([]byte("k"), cid[:])
	if _, f := send(exKindDown, 0, 0, false); f != 0 {
		t.Fatalf("new carrier: flags %#x", f)
	}
	bad := appendExReqHdr(nil, &cid, exKindDown, exFlagFin, 1, 0, &[exNonceLen]byte{1})
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
// carrier; ZMUX_EXCHANGE_BYTES the client's echo size (1 MiB);
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
	cfg := &HTTPExchangeConfig{PSK: psk, Base64: os.Getenv("ZMUX_EXCHANGE_BASE64") == "1"}
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
		h := NewHTTPExchangeHandler(lst, cfg)
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
	h := NewHTTPExchangeHandler(lst, &HTTPExchangeConfig{HoldTime: time.Second})
	defer func() { _ = h.Close() }()
	var cid [exCIDLen]byte
	_, _ = rand.Read(cid[:])
	key := newExKey(nil, cid[:])
	send := func(kind, flags byte, seq, ack uint64, data []byte) byte {
		var nonce [exNonceLen]byte
		_, _ = rand.Read(nonce[:])
		b := appendExReqHdr(nil, &cid, kind, flags, seq, ack, &nonce)
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

// TestHTTPExchangeBodyLimits checks that bodies over the other end's limit
// fail the carrier at once instead of being retried until the deadline.
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
		h := NewHTTPExchangeHandler(lst, &HTTPExchangeConfig{UpPieceSize: 16 << 10, HoldTime: 100 * time.Millisecond})
		srv := httptest.NewServer(h)
		defer func() { srv.Close(); _ = h.Close(); _ = lst.Close() }()
		tr := &http.Transport{}
		defer tr.CloseIdleConnections()
		p, err := HTTPExchangePipe(ctx, &http.Client{Transport: tr}, srv.URL, &HTTPExchangeConfig{UpPieceSize: 64 << 10, Coalesce: -1})
		if err != nil {
			t.Fatal(err)
		}
		_, _ = p.W.Write(make([]byte, 64<<10))
		if err := read(p); !errors.Is(err, errExchangeGone) {
			t.Fatalf("oversized UP: %v, want GONE", err)
		}
	})
	t.Run("down", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			w.Header().Set("Content-Length", strconv.Itoa(64<<10))
			_, _ = w.Write(make([]byte, 64<<10))
		}))
		defer srv.Close()
		tr := &http.Transport{}
		defer tr.CloseIdleConnections()
		p, err := HTTPExchangePipe(ctx, &http.Client{Transport: tr}, srv.URL, &HTTPExchangeConfig{DownPieceSize: 16 << 10})
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
	h := NewHTTPExchangeHandler(lst, &HTTPExchangeConfig{HoldTime: time.Second})
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
		&HTTPExchangeConfig{DownURL: down.URL + "/down/{instance}/{nonce}"})
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
