package zmuxhttp

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/admit"
	"github.com/zmuxio/zmux-go/internal/leakcheck"
	"github.com/zmuxio/zmux-go/internal/prefixtest"
)

func TestServerTCPCarrierSample(t *testing.T) {
	wantSource := zmux.CarrierSourceUnknown
	switch runtime.GOOS {
	case "linux":
		wantSource = zmux.CarrierSourceLinuxTCPInfo
	case "darwin":
		wantSource = zmux.CarrierSourceDarwinTCPConnectionInfo
	case "windows":
		wantSource = zmux.CarrierSourceWindowsSIOTCPInfo
	}
	for _, tc := range []struct {
		name  string
		proto int
	}{{"HTTP/1.1", 1}, {"HTTP/2", 2}} {
		t.Run(tc.name, func(t *testing.T) {
			lst := mustNewListener(&zmux.ListenerConfig{Config: mpConfig()})
			h, err := NewHandler(lst, nil)
			if err != nil {
				_ = lst.Close()
				t.Fatal(err)
			}
			srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.ProtoMajor != tc.proto {
					t.Errorf("HTTP protocol = %s, want %s", r.Proto, tc.name)
				}
				h.ServeHTTP(w, r)
			}))
			defer srv.Close()
			defer func() { _ = lst.Close() }()
			srv.Config.ConnContext = ConnContext
			if tc.proto == 2 {
				srv.EnableHTTP2 = true
				srv.StartTLS()
			} else {
				srv.Start()
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			p, err := Streaming{Client: srv.Client(), URL: srv.URL}.Pipe(ctx, zmux.DirectionBidirectional)
			if err != nil {
				t.Fatal(err)
			}
			c, err := zmux.Start(ctx, p, zmux.RoleClient, mpConfig())
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				_ = c.Close()
				waitDone(t, c)
			}()
			s, err := lst.Accept(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				_ = s.Close()
				waitDone(t, s)
			}()

			stats := s.PipeStats()
			if len(stats) != 1 {
				t.Fatalf("server pipes = %d, want 1", len(stats))
			}
			sample := stats[0].CarrierSample
			if sample == nil || sample.Source != wantSource || sample.SampledAt.IsZero() {
				t.Fatalf("server carrier sample = %+v, want source %s and an attachment timestamp", sample, wantSource)
			}
		})
	}
}

// TestHTTPCarriers runs sessions over HTTP request carriers admitted by
// prefix tokens: in the body of a duplex request and a split bootstrap's
// uplink, in the Zmux-Prefix-Auth field of its bodiless downlink GET and
// of a downlink joined out of band.
func TestHTTPCarriers(t *testing.T) {
	leakcheck.Check(t)
	auth, v, _ := prefixtest.New(t, admit.PrefixCarrierOptions{}, zmux.VerdictClose)
	admission := v.Admission()
	admission.PeekTimeout, admission.StallMax = 2*time.Second, 100*time.Millisecond
	lst := mustNewListener(&zmux.ListenerConfig{Config: mpConfig(), Admission: admission})
	hh, err := NewHandler(lst, nil)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewUnstartedServer(hh)
	srv.EnableHTTP2 = true
	srv.StartTLS()
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
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
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
	w := &httpReqWriter{p: p, call: settledCall(), cancel: func() {}}
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

	synctest.Test(t, func(t *testing.T) {
		stop := errors.New("refused")
		p := newBodyPipe()
		w := &httpReqWriter{p: p, call: settledCall(), cancel: func() {}}
		written := make(chan error, 1)
		go func() { _, err := w.WriteBuffers([][]byte{[]byte("x")}); written <- err }()
		synctest.Wait()
		p.closeRead(stop)
		if err := <-written; !errors.Is(err, stop) {
			t.Fatalf("write after the reader closed: %v", err)
		}

		p = newBodyPipe()
		w = &httpReqWriter{p: p, call: settledCall(), cancel: func() {}}
		read := make(chan error, 1)
		go func() { _, err := p.read(make([]byte, 1), nil); read <- err }()
		synctest.Wait()
		w.Abort(stop)
		if err := <-read; !errors.Is(err, stop) {
			t.Fatalf("read after the writer aborted: %v", err)
		}
	})
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
	if err := p.WaitAttached(ctx); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.Pipe(p.ID()); !ok {
		t.Fatalf("origin acknowledged pipe %d without installing it", p.ID())
	}
	mu.Lock()
	second := cancels[len(cancels)-1]
	if len(cancels) != before+2 {
		mu.Unlock()
		t.Fatalf("%d requests for a split join", len(cancels)-before)
	}
	mu.Unlock()
	second()
	first, _ := c.Pipe(0)
	_ = first.Abort()
	echoOnce(t, ctx, c, s)
	if st, ok := p.Stats(); !ok || st.FailureCount != 0 {
		t.Fatalf("the pipe died with the request's context: %+v", st)
	}
	if st, _ := p.Stats(); st.Carrier != zmux.CarrierSplit {
		t.Fatalf("carrier %v", st.Carrier)
	}
}

// settledCall is the call of a request whose response already came.
func settledCall() *httpCall {
	c := &httpCall{done: make(chan struct{})}
	close(c.done)
	return c
}

// A send-only joiner can finish its downlink before the origin reads the
// Confirm on the uplink. Cancel the GET as the origin writes its Reply.
func TestSplitJoinDownlinkEndsBeforeConfirm(t *testing.T) {
	lst := mustNewListener(&zmux.ListenerConfig{Config: atkConfig()})
	defer func() { _ = lst.Close() }()
	h, err := NewHandler(lst, nil)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.Header.Get(HeaderPair) != "" {
			ctx, cancel := context.WithCancel(r.Context())
			defer cancel()
			w = cancelOnWrite{w, cancel}
			r = r.WithContext(ctx)
		}
		h.ServeHTTP(w, r)
	}))
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	base, err := (Streaming{Client: srv.Client(), URL: srv.URL}).Pipe(ctx, zmux.DirectionBidirectional)
	if err != nil {
		t.Fatal(err)
	}
	c, err := zmux.Start(ctx, base, zmux.RoleClient, atkConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close(); waitDone(t, c) }()
	s, err := lst.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close(); waitDone(t, s) }()
	p, err := (Split{Client: srv.Client(), URL: srv.URL}).Join(ctx, c, zmux.PipeOptions{Direction: zmux.DirectionSendOnly})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.WaitAttached(ctx); err != nil {
		t.Fatal(err)
	}
}

type cancelOnWrite struct {
	http.ResponseWriter
	cancel context.CancelFunc
}

func (w cancelOnWrite) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w cancelOnWrite) Write(p []byte) (int, error) {
	w.cancel()
	return w.ResponseWriter.Write(p)
}
