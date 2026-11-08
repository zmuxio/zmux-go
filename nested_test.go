package zmux

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"runtime"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

// TestNestedSession runs a session over a stream of another session.
func TestNestedSession(t *testing.T) {
	leakcheck.Check(t)
	a, b := tcpPair(t)
	oc, os := pair(t, a, b, nil, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	type res struct {
		c   *Conn
		err error
	}
	srv := make(chan res, 1)
	go func() {
		st, err := os.AcceptStream(ctx)
		if err != nil {
			srv <- res{nil, err}
			return
		}
		if !st.PipeCarrier() {
			srv <- res{nil, errors.New("no PIPE_CARRIER")}
			return
		}
		c, err := Start(ctx, StreamPipe(st), RoleServer, nil)
		srv <- res{c, err}
	}()
	p, err := oc.OpenPipeStream(ctx, &PipeStreamOptions{Info: []byte("info"), TLVs: []TLV{{Type: 0x1001, Value: []byte("route")}}})
	if err != nil {
		t.Fatal(err)
	}
	nc, err := Start(ctx, p, RoleClient, nil)
	if err != nil {
		t.Fatal(err)
	}
	r := <-srv
	if r.err != nil {
		t.Fatal(r.err)
	}
	ns := r.c
	defer func() {
		_ = nc.Close()
		_ = ns.Close()
		waitDone(t, nc)
		waitDone(t, ns)
	}()
	msg := make([]byte, 8<<20) // the outer stream references the nested blocks
	_, _ = rand.Read(msg)
	go func() {
		s, err := ns.AcceptStream(ctx)
		if err != nil {
			return
		}
		_, _ = io.Copy(s, s)
		_ = s.Close()
	}()
	s, err := nc.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _, _ = s.Write(msg); _ = s.Close() }()
	got, err := io.ReadAll(s)
	if err != nil || !bytes.Equal(got, msg) {
		t.Fatalf("nested echo: %d bytes, %v", len(got), err)
	}
}

func TestNestingLoopRefused(t *testing.T) {
	a, b := new(Conn), new(Conn)
	if err := nestPipe(a, outerRef{b}); err != nil {
		t.Fatal(err)
	}
	if err := nestPipe(b, outerRef{a}); !errors.Is(err, errNestingLoop) {
		t.Fatalf("loop accepted: %v", err)
	}
	forgetNesting(a)
	if err := nestPipe(b, outerRef{a}); err != nil {
		t.Fatal(err)
	}
	forgetNesting(b)
}

// Nested lanes take at most half of the outer peer's bidirectional
// streams and never credit an application opener waits for.
func TestNestedLaneCredit(t *testing.T) {
	leakcheck.Check(t)
	scfg := mpConfig()
	scfg.MaxIncomingStreams = 4
	e := newMPEnv(t, mpConfig(), scfg)
	e.addPipe(t, PipeOptions{}) // lanes pay off: the outer session has two pipes
	c := e.client
	var lanes []*Stream
	for {
		s, ok := c.openNestedLane()
		if !ok {
			break
		}
		lanes = append(lanes, s)
	}
	if len(lanes) != 2 {
		t.Fatalf("%d nested lanes, want 2 (half of 4)", len(lanes))
	}
	_ = (&laneStream{s: lanes[0], outer: c}).Close()
	// Use the remaining credit, then wait as an application opener.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var app []*Stream
	for {
		s, err := c.OpenStream()
		if err != nil {
			break
		}
		app = append(app, s)
	}
	opened := make(chan *Stream, 1)
	go func() {
		s, _ := c.OpenStreamSync(ctx)
		opened <- s
	}()
	time.Sleep(50 * time.Millisecond)
	if s, ok := c.openNestedLane(); ok {
		t.Fatalf("a nested lane took credit an opener waits for: %v", s)
	}
	cancel()
	<-opened
	runtime.KeepAlive(lanes)
	runtime.KeepAlive(app)
}

// TestStreamPipeLocalOpen wraps a locally opened stream that carries
// OPEN_INFO and PIPE_CARRIER; the peer sees the preamble and runs the
// nested session over the stream.
func TestStreamPipeLocalOpen(t *testing.T) {
	leakcheck.Check(t)
	a, b := tcpPair(t)
	oc, os := pair(t, a, b, nil, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	type res struct {
		c    *Conn
		info []byte
		err  error
	}
	srv := make(chan res, 1)
	go func() {
		st, err := os.AcceptStream(ctx)
		if err != nil {
			srv <- res{err: err}
			return
		}
		c, err := Start(ctx, StreamPipe(st), RoleServer, nil)
		srv <- res{c: c, info: st.Preamble(), err: err}
	}()
	s, err := oc.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetPreamble([]byte("route=/x")); err != nil {
		t.Fatal(err)
	}
	if err := s.SetPipeCarrier(); err != nil {
		t.Fatal(err)
	}
	nc, err := Start(ctx, StreamPipe(s), RoleClient, nil)
	if err != nil {
		t.Fatal(err)
	}
	r := <-srv
	if r.err != nil {
		t.Fatal(r.err)
	}
	defer func() {
		_ = nc.Close()
		_ = r.c.Close()
		waitDone(t, nc)
		waitDone(t, r.c)
	}()
	if string(r.info) != "route=/x" {
		t.Fatalf("OPEN_INFO = %q", r.info)
	}
	go func() {
		st, err := r.c.AcceptStream(ctx)
		if err != nil {
			return
		}
		_, _ = io.Copy(st, st)
		_ = st.Close()
	}()
	ns, err := nc.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _, _ = ns.Write([]byte("ping")); _ = ns.Close() }()
	if got, err := io.ReadAll(ns); err != nil || string(got) != "ping" {
		t.Fatalf("nested echo: %q, %v", got, err)
	}
}

// TestPreamblePriority reads the PRIORITY the opener sent in the preamble.
func TestPreamblePriority(t *testing.T) {
	leakcheck.Check(t)
	a, b := tcpPair(t)
	oc, os := pair(t, a, b, nil, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s, err := oc.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetPeerPriority(UrgencyHighest, true); err != nil {
		t.Fatal(err)
	}
	_, _ = s.Write([]byte("x"))
	r, err := os.AcceptStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if u, inc, ok := r.PreamblePriority(); !ok || u != UrgencyHighest || !inc {
		t.Fatalf("PreamblePriority = %v %v %v", u, inc, ok)
	}
	s2, err := oc.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = s2.Write([]byte("y"))
	r2, err := os.AcceptStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, ok := r2.PreamblePriority(); ok {
		t.Fatal("PRIORITY reported for a stream that sent none")
	}
}
