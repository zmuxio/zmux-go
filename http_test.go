package zmux

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

// TestHTTPCarriers runs sessions over HTTP request carriers admitted by
// prefix tokens: in the body of a duplex request and a split bootstrap's
// uplink, in the Zmux-Prefix-Auth field of its bodiless downlink GET and
// of a downlink joined out of band.
func TestHTTPCarriers(t *testing.T) {
	leakcheck.Check(t)
	auth := &PrefixAuth{Key: []byte("k"), Slot: 30 * time.Second, Skew: 1}
	v, err := NewPrefixAuthVerifier(auth)
	if err != nil {
		t.Fatal(err)
	}
	lst := NewListener(&ListenerConfig{Config: mpConfig(), Admission: &Admission{Peek: PrefixTokenLen, Decide: v.Decide,
		PeekTimeout: 2 * time.Second, StallMax: 100 * time.Millisecond}})
	hh, err := NewHTTPHandler(lst, nil)
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
			var p PipeIO
			var err error
			if mode == "duplex" {
				p, err = HTTPPipe(ctx, srv.Client(), srv.URL, nil, DirBidirectional, auth)
			} else {
				p, err = HTTPSplitPipe(ctx, srv.Client(), srv.URL, nil, auth)
			}
			if err != nil {
				t.Fatal(err)
			}
			c, err := Start(ctx, p, RoleClient, mpConfig())
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
			// A downlink joined out of band.
			if mode == "duplex" {
				r, err := c.ReservePipe(DirReceiveOnly, PipeOptions{})
				if err != nil {
					t.Fatal(err)
				}
				h := map[string][]string{HeaderJoin: {r.Header()}}
				dp, err := HTTPPipe(ctx, srv.Client(), srv.URL, h, DirReceiveOnly, auth)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := r.Attach(ctx, dp); err != nil {
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
	p, err := HTTPPipe(ctx, srv.Client(), srv.URL, nil, DirSendOnly, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.W.Close() }()
	var se *HTTPStatusError
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
		n, err := p.read(buf)
		got = append(got, buf[:n]...)
		if err == io.EOF {
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
	if _, err := p.read(buf); !errors.Is(err, stop) {
		t.Fatalf("read after the writer aborted: %v", err)
	}
}
