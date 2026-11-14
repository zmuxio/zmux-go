package zmuxhttp

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/admit"
	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

// TestHTTPCarriers runs sessions over HTTP request carriers admitted by
// prefix tokens: in the body of a duplex request and a split bootstrap's
// uplink, in the Zmux-Prefix-Auth field of its bodiless downlink GET and
// of a downlink joined out of band.
func TestHTTPCarriers(t *testing.T) {
	leakcheck.Check(t)
	auth := &admit.PrefixAuth{Key: []byte("k"), Slot: 30 * time.Second}
	v, err := admit.NewPrefixAuthVerifier(auth, admit.PrefixAuthVerifierOptions{Skew: 1})
	if err != nil {
		t.Fatal(err)
	}
	lst := mustNewListener(&zmux.ListenerConfig{Config: mpConfig(), Admission: &zmux.Admission{Peek: admit.PrefixTokenLen, Decide: v.Decide,
		PeekTimeout: 2 * time.Second, StallMax: 100 * time.Millisecond}})
	hh, err := NewHandler(lst, nil)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(hh)
	defer srv.Close()
	defer func() { _ = lst.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, mode := range []string{"duplex", "split"} {
		t.Run(mode, func(t *testing.T) {
			var p zmux.PipeIO
			var err error
			if mode == "duplex" {
				p, err = Streaming{Client: srv.Client(), URL: srv.URL, CarrierCredential: auth}.Pipe(ctx, zmux.DirectionBidirectional)
			} else {
				p, err = Split{Client: srv.Client(), URL: srv.URL, CarrierCredential: auth}.Pipe(ctx)
			}
			if err != nil {
				t.Fatal(err)
			}
			c, err := zmux.Start(ctx, p, zmux.RoleClient, mpConfig())
			if err != nil {
				t.Fatal(err)
			}
			s, err := lst.Accept(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				_ = c.Close()
				_ = s.Close()
				waitDone(t, c)
				waitDone(t, s)
			}()
			go func() {
				st, err := s.AcceptStream(ctx)
				if err == nil {
					_, _ = io.Copy(st, st)
					_ = st.Close()
				}
			}()
			st, err := c.OpenStreamSync(ctx)
			if err != nil {
				t.Fatal(err)
			}
			msg := make([]byte, 256<<10)
			_, _ = rand.Read(msg)
			go func() { _, _ = st.Write(msg); _ = st.Close() }()
			got, err := io.ReadAll(st)
			if err != nil || !bytes.Equal(got, msg) {
				t.Fatalf("echo over HTTP %s: %d bytes, %v", mode, len(got), err)
			}
			// A downlink joined over two requests.
			if mode == "duplex" {
				if _, err := (Split{Client: srv.Client(), URL: srv.URL, CarrierCredential: auth}).Join(ctx, c, zmux.PipeOptions{Direction: zmux.DirectionReceiveOnly}); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

// A refused uplink reports the status through its write half.
func TestHTTPPipeSendOnlyStatus(t *testing.T) {
	leakcheck.Check(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "no", http.StatusForbidden)
	}))
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	p, err := Streaming{Client: srv.Client(), URL: srv.URL}.Pipe(ctx, zmux.DirectionSendOnly)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.W.Close() }()
	var se *StatusError
	for !errors.As(err, &se) {
		if _, err = p.W.Write([]byte("x")); err != nil && !errors.As(err, &se) {
			t.Fatalf("write: %v", err)
		}
	}
	if se.StatusCode != http.StatusForbidden {
		t.Fatalf("status %d", se.StatusCode)
	}
}

// TestBodyPipe: a batch reaches the reader whole through reads of any
// size, and closing either side ends a blocked call on the other.
func TestBodyPipe(t *testing.T) {
	leakcheck.Check(t)
	p := newBodyPipe()
	w := &httpReqWriter{p: p, cancel: func() {}}
	bufs := [][]byte{[]byte("ab"), nil, []byte("cdefg"), []byte("h")}
	done := make(chan error, 1)
	go func() {
		n, err := w.WriteBuffers(bufs)
		if err == nil && n != 8 {
			err = errors.New("short batch")
		}
		if err == nil {
			_, err = w.Write([]byte("ij"))
		}
		_ = w.Close()
		done <- err
	}()
	var got []byte
	buf := make([]byte, 3)
	for {
		n, err := p.read(buf, nil)
		got = append(got, buf[:n]...)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := <-done; err != nil || string(got) != "abcdefghij" {
		t.Fatalf("got %q, %v", got, err)
	}

	// The reader gives up: a blocked write fails with its error.
	p = newBodyPipe()
	w = &httpReqWriter{p: p, cancel: func() {}}
	stop := errors.New("refused")
	go func() { time.Sleep(10 * time.Millisecond); p.closeRead(stop) }()
	if _, err := w.WriteBuffers([][]byte{[]byte("x")}); !errors.Is(err, stop) {
		t.Fatalf("write after the reader closed: %v", err)
	}
	// The writer aborts: a blocked read fails with its error.
	p = newBodyPipe()
	w = &httpReqWriter{p: p, cancel: func() {}}
	go func() { time.Sleep(10 * time.Millisecond); w.Abort(stop) }()
	if _, err := p.read(buf, nil); !errors.Is(err, stop) {
		t.Fatalf("read after the writer aborted: %v", err)
	}
}

// A Join runs across a pair of requests, and the pipe does not live by the context of
// the request that arrived second.
func TestSplitJoinHandlerLifetime(t *testing.T) {
	t.Parallel()
	scfg, ccfg := atkConfig(), atkConfig()
	lst := mustNewListener(&zmux.ListenerConfig{Config: scfg})
	defer func() { _ = lst.Close() }()
	hh, err := NewHandler(lst, nil)
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var cancels []context.CancelFunc
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithCancel(r.Context())
		defer cancel()
		mu.Lock()
		cancels = append(cancels, cancel)
		mu.Unlock()
		hh.ServeHTTP(w, r.WithContext(ctx))
	}))
	defer srv.Close()
	defer srv.CloseClientConnections()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	p0, err := Streaming{Client: srv.Client(), URL: srv.URL}.Pipe(ctx, zmux.DirectionBidirectional)
	if err != nil {
		t.Fatal(err)
	}
	c, err := zmux.Start(ctx, p0, zmux.RoleClient, ccfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	s, err := lst.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	mu.Lock()
	before := len(cancels)
	mu.Unlock()
	p, err := Split{Client: srv.Client(), URL: srv.URL}.Join(ctx, c, zmux.PipeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 30*time.Second, "the origin's pipe", func() bool { _, ok := s.Pipe(p.ID()); return ok })
	mu.Lock()
	second := cancels[len(cancels)-1]
	if len(cancels) != before+2 {
		mu.Unlock()
		t.Fatalf("%d requests for a split join", len(cancels)-before)
	}
	mu.Unlock()
	second()
	first, _ := c.Pipe(0)
	_ = first.Abort(nil)
	echoOnce(t, ctx, c, s)
	if st, ok := p.Stats(); !ok || st.FailureCount != 0 {
		t.Fatalf("the pipe died with the request's context: %+v", st)
	}
	if st, _ := p.Stats(); st.Carrier != zmux.CarrierSplit {
		t.Fatalf("carrier %v", st.Carrier)
	}
}
