package zmux

import (
	"context"
	"errors"
	"fmt"
	"io"
	"runtime"
	"sync"

	"github.com/zmuxio/zmux-go/internal/core"
)

// Streams as pipes (API.md 4.6, EXTENSIONS.md 10): a bidirectional stream
// of an outer session carries a pipe pair of a nested session.

// OpenPipeStream opens a stream of c to carry a pipe pair of another
// session; tlvs are application TLVs of its preamble (for example routing
// metadata for relays), to which PIPE_CARRIER is added. Nothing is sent
// before the nested session writes its first record.
func (c *Conn) OpenPipeStream(ctx context.Context, tlvs ...TLV) (PipeIO, error) {
	s, err := c.OpenStreamSync(ctx)
	if err != nil {
		return PipeIO{}, err
	}
	pre := c.ConnectionState().PeerIsZmux
	if pre {
		if err := s.SetPipeCarrier(); err != nil && !errors.Is(err, ErrNotNegotiated) {
			s.CancelWrite(0)
			return PipeIO{}, err
		}
	}
	for _, t := range tlvs {
		if err := s.AddPreambleTLV(t.Type, t.Value); err != nil {
			s.CancelWrite(0)
			s.CancelRead(0)
			return PipeIO{}, fmt.Errorf("zmux: pipe stream TLV: %w", err)
		}
	}
	return streamPipe(s, c, true), nil
}

// StreamPipe wraps a bidirectional stream that carries a pipe pair (one
// the peer opened with PIPE_CARRIER, or any stream the application knows
// to be a carrier).
func StreamPipe(s *Stream) PipeIO { return streamPipe(s, s.h.c, false) }

func streamPipe(s *Stream, outer *Conn, opener bool) PipeIO {
	sp := &stPipe{s: s, outer: outer, opener: opener}
	return PipeIO{R: (*stPipeR)(sp), W: (*stPipeW)(sp)}
}

// stPipe adapts a stream to the pipe operations of EXTENSIONS.md 10.2.
type stPipe struct {
	s      *Stream
	outer  *Conn
	opener bool
	once   sync.Once // the read half is closed once
	wonce  sync.Once // the write half is closed once
}

type stPipeR stPipe
type stPipeW stPipe

// Read reads the inbound pipe; FIN is its graceful end, a reset its
// failure.
func (r *stPipeR) Read(p []byte) (int, error) { return r.s.Read(p) }

// Close of the receiving half before its end stops it with
// ZMUX_CARRIER_FAILED.
func (r *stPipeR) Close() error {
	r.once.Do(func() { r.s.CancelRead(CodeCarrierFailed) })
	return nil
}

// CarrierConn reports the outer session as the carrier connection (lanes).
func (r *stPipeR) CarrierConn() any { return r.outer }

func (w *stPipeW) Write(p []byte) (int, error) { return w.s.Write(p) }

// WriteBatch hands a batch of the nested session to the outer stream
// without copying its payload.
func (w *stPipeW) WriteBatch(b *core.Batch) (int, error) {
	defer runtime.KeepAlive(w.s)
	return w.s.h.writeBatch(b)
}

// Close is the graceful end of the sending half (FIN).
func (w *stPipeW) Close() error {
	var err error
	w.wonce.Do(func() { err = w.s.Close() })
	return err
}

// Abort resets the sending part with ZMUX_CARRIER_FAILED (RESET_STREAM_AT
// with the preamble as Reliable Size on the opener's part) and stops the
// receiving part.
func (w *stPipeW) Abort(error) {
	w.wonce.Do(func() { w.s.CancelWrite(CodeCarrierFailed) })
	w.once.Do(func() { w.s.CancelRead(CodeCarrierFailed) })
}

// CarrierConn reports the outer session as the carrier connection.
func (w *stPipeW) CarrierConn() any { return w.outer }

// OpenLane opens a further carrier stream of the outer session as a lane,
// only where lanes pay off (the outer session has lanes or more than one
// pipe) and only with stream credit that leaves the application at least
// half of the outer peer's streams (API.md 4.6).
func (w *stPipeW) OpenLane(context.Context) (io.WriteCloser, error) {
	o := w.outer
	s, ok := o.openNestedLane()
	if !ok {
		return nil, errNoLane
	}
	_ = s.SetPipeCarrier()
	s.CancelRead(0) // the reverse half of a lane is unused
	return &laneStream{s: s, outer: o}, nil
}

var errNoLane = errors.New("zmux: no lane now")

// laneStream is an outbound lane over an outer stream.
type laneStream struct {
	s     *Stream
	outer *Conn
	once  sync.Once
}

func (l *laneStream) Write(p []byte) (int, error) { return l.s.Write(p) }

// WriteBatch hands a batch of the nested session to the lane without
// copying its payload.
func (l *laneStream) WriteBatch(b *core.Batch) (int, error) {
	defer runtime.KeepAlive(l.s)
	return l.s.h.writeBatch(b)
}

func (l *laneStream) Close() error {
	var err error
	l.once.Do(func() {
		err = l.s.Close()
		l.outer.releaseNestedLane()
	})
	return err
}

func (l *laneStream) Abort(error) {
	l.once.Do(func() {
		l.s.CancelWrite(CodeCarrierFailed)
		l.outer.releaseNestedLane()
	})
}

// lanesPayOff: over a single TCP pipe the outer streams share one ordered
// byte stream and lanes would recover nothing.
func (c *Conn) lanesPayOff() bool {
	c.lock()
	defer c.unlock()
	n := 0
	for _, id := range c.s.PipeIDs(nil) {
		if pi, ok := c.s.PipeInfo(id); ok && pi.HasOut && (pi.OutState == "ACTIVE" || pi.OutState == "JOINING") {
			n++
			if pi.Lanes > 1 {
				return true
			}
		}
	}
	return n > 1
}

// openNestedLane opens an outer stream as a lane of a nested pipe in one
// step under the outer session's lock (API.md 4.6): only with stream
// credit that no application opener waits for, never waiting itself, and
// while the lanes of all nested pipes on c stay at most half of the outer
// peer's initial_max_streams_bidi.
func (c *Conn) openNestedLane() (*Stream, bool) {
	if !c.lanesPayOff() {
		return nil, false
	}
	c.lock()
	defer c.unlock()
	p, ok := c.s.PeerParams()
	if !ok || uint64(c.nestedLanes+1) > p.MaxStreamsBidi/2 || c.s.OpenersWaiting(true) {
		return nil, false
	}
	st, err := c.s.OpenStream(true)
	if err != nil {
		return nil, false
	}
	c.nestedLanes++
	return newStream(c.newHandle(st)), true
}

func (c *Conn) releaseNestedLane() {
	c.lock()
	defer c.unlock()
	if c.nestedLanes > 0 {
		c.nestedLanes--
	}
}

// ---- loops (EXTENSIONS.md 10.2) ----

// nesting records which sessions run over which: an edge n -> o for every
// pipe of n carried by a stream of o. No pipe of a session may run over
// that session, directly or through others. The lock is a leaf.
var nesting = struct {
	mu    sync.Mutex
	edges map[*Conn]map[*Conn]int
}{edges: map[*Conn]map[*Conn]int{}}

// errNestingLoop refuses a pipe whose carrier runs over its own session.
var errNestingLoop = errors.New("zmux: the carrier runs over the session it would join")

// outerOf returns the outer session of a stream-backed carrier.
func outerOf(p PipeIO) *Conn {
	for _, x := range []any{p.W, p.R} {
		if cc, ok := x.(carrierConner); ok {
			if o, ok := cc.CarrierConn().(*Conn); ok {
				return o
			}
		}
	}
	return nil
}

// nestPipe records that n runs over o, unless o runs over n already; the
// check and the record are one atomic step.
func nestPipe(n, o *Conn) error {
	if o == nil {
		return nil
	}
	nesting.mu.Lock()
	defer nesting.mu.Unlock()
	if o == n || reaches(o, n, map[*Conn]bool{}) {
		return errNestingLoop
	}
	m := nesting.edges[n]
	if m == nil {
		m = map[*Conn]int{}
		nesting.edges[n] = m
	}
	m[o]++
	return nil
}

// unnestPipe drops one edge n -> o (the pipe ended).
func unnestPipe(n, o *Conn) {
	if o == nil {
		return
	}
	nesting.mu.Lock()
	defer nesting.mu.Unlock()
	m := nesting.edges[n]
	if m[o]--; m[o] <= 0 {
		delete(m, o)
	}
	if len(m) == 0 {
		delete(nesting.edges, n)
	}
}

// forgetNesting drops every edge of a closed session.
func forgetNesting(c *Conn) {
	nesting.mu.Lock()
	defer nesting.mu.Unlock()
	delete(nesting.edges, c)
}

func reaches(from, to *Conn, seen map[*Conn]bool) bool {
	if from == to {
		return true
	}
	if seen[from] {
		return false
	}
	seen[from] = true
	for o := range nesting.edges[from] {
		if reaches(o, to, seen) {
			return true
		}
	}
	return false
}
