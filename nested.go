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

// Streams as pipes: a bidirectional stream
// of an outer session carries a pipe pair of a nested session.

// PipeStreamOptions is the opening preamble of a carrier stream.
type PipeStreamOptions struct {
	// Info is the stream's OPEN_INFO (nil: none).
	Info []byte
	// TLVs are application TLVs (for example routing metadata for
	// relays); PIPE_CARRIER is added.
	TLVs []TLV
}

// OpenPipeStream opens a stream of c to carry a pipe pair of another
// session, with the preamble o (nil: PIPE_CARRIER only). Nothing is sent
// before the nested session writes its first record. When the peer does not
// use the preamble, o must be empty.
func (c *Conn) OpenPipeStream(ctx context.Context, o *PipeStreamOptions) (PipeIO, error) {
	s, err := c.OpenStreamSync(ctx)
	if err != nil {
		return PipeIO{}, err
	}
	fail := func(err error) (PipeIO, error) {
		s.CancelWrite(0)
		s.CancelRead(0)
		return PipeIO{}, err
	}
	if c.ConnectionState().PeerIsZmux {
		if err := s.SetPipeCarrier(); err != nil && !errors.Is(err, ErrNotNegotiated) {
			return fail(err)
		}
	}
	if o != nil {
		if o.Info != nil {
			if err := s.SetPreamble(o.Info); err != nil {
				return fail(fmt.Errorf("zmux: pipe stream OPEN_INFO: %w", err))
			}
		}
		for _, t := range o.TLVs {
			if err := s.AddPreambleTLV(t.Type, t.Value); err != nil {
				return fail(fmt.Errorf("zmux: pipe stream TLV: %w", err))
			}
		}
	}
	return streamPipe(s, c), nil
}

// StreamPipe wraps a bidirectional stream that carries a pipe pair (one
// the peer opened with PIPE_CARRIER, or any stream the application knows
// to be a carrier). A stream opened locally may be wrapped too: set its
// preamble first (SetPreamble for OPEN_INFO, SetPipeCarrier, AddPreambleTLV),
// before the nested session writes.
func StreamPipe(s *Stream) PipeIO { return streamPipe(s, s.h.c) }

func streamPipe(s *Stream, outer *Conn) PipeIO {
	pair := new(struct {
		write stPipeW
		read  stPipeR
	})
	pair.read.s, pair.read.outer = s, outer
	pair.write.s, pair.write.read = s, &pair.read
	return PipeIO{R: &pair.read, W: &pair.write}
}

type stPipeR struct {
	s     *Stream
	outer *Conn
	once  sync.Once
}

type stPipeW struct {
	read *stPipeR
	nestedWriter
}

type nestedWriter struct {
	s    *Stream
	once sync.Once
}

func (w *nestedWriter) Write(p []byte) (int, error) { return w.s.Write(p) }

// WriteBatch passes nested records to the outer stream without a payload copy.
func (w *nestedWriter) WriteBatch(b *core.Batch) (int, error) {
	defer runtime.KeepAlive(w.s)
	return w.s.h.writeBatch(b)
}

func (w *nestedWriter) finish(abort bool, release func()) error {
	var err error
	w.once.Do(func() {
		if abort {
			w.s.CancelWrite(StreamCarrierFailed)
		} else {
			err = w.s.Close()
		}
		if release != nil {
			release()
		}
	})
	return err
}

// Read reads the inbound pipe; FIN is its graceful end, a reset its
// failure.
func (r *stPipeR) Read(p []byte) (int, error) { return r.s.Read(p) }

// readSpan transfers a retained range from the outer receive queue. A
// busy application's outstanding loans cannot stop an inner control read.
func (r *stPipeR) readSpan(limit int) (core.Span, bool, error) {
	defer runtime.KeepAlive(r.s)
	v, err := r.s.h.readLoan(context.Background(), limit)
	if !errors.Is(err, ErrReceiveLoanLimit) {
		return v, true, err
	}
	b := core.GetRecvBuf(limit)
	n, err := r.s.Read(b.B[:limit])
	if n == 0 {
		b.Unref()
		return core.Span{}, false, err
	}
	return b.Span(0, n), false, err
}

// byteSpanReader follows only views that preserve the underlying bytes.
func byteSpanReader(r io.Reader) func(int) (core.Span, bool, error) {
	switch v := r.(type) {
	case *stPipeR:
		return v.readSpan
	case readOnly:
		return byteSpanReader(v.r)
	case *ownedReader:
		return byteSpanReader(v.r)
	case *prefixReader:
		return byteSpanReader(v.r)
	default:
		return nil
	}
}

// Close of the receiving half before its end stops it with
// ZMUX_CARRIER_FAILED.
func (r *stPipeR) Close() error {
	r.once.Do(func() { r.s.CancelRead(StreamCarrierFailed) })
	return nil
}

// CarrierConn reports the outer session as the carrier connection (lanes).
func (r *stPipeR) CarrierConn() any { return r.outer }

// ReportsFailure reports true: the stream fails (a reset, the end of the outer
// session) when its path fails, so a stall behind the outer session's
// queues or rate limits never fails the nested pipe.
func (r *stPipeR) ReportsFailure() bool { return true }

// ReportsFailure reports true, as the receiving half does for the same stream.
func (w *stPipeW) ReportsFailure() bool { return true }

// Close is the graceful end of the sending half (FIN).
func (w *stPipeW) Close() error { return w.finish(false, nil) }

// Abort resets the sending part with ZMUX_CARRIER_FAILED (RESET_STREAM_AT
// with the preamble as Reliable Size on the opener's part) and stops the
// receiving part.
func (w *stPipeW) Abort(_ error) {
	_ = w.finish(true, nil)
	_ = w.read.Close()
}

// CarrierConn reports the outer session as the carrier connection.
func (w *stPipeW) CarrierConn() any { return w.read.outer }

// OpenLane opens a further carrier stream of the outer session as a lane,
// only where lanes pay off (the outer session has lanes or more than one
// pipe) and only with stream credit that leaves the application at least
// half of the outer peer's streams.
func (w *stPipeW) OpenLane(_ context.Context) (io.WriteCloser, error) {
	o := w.read.outer
	s, ok := o.openNestedLane()
	if !ok {
		return nil, errNoLane
	}
	_ = s.SetPipeCarrier()
	s.CancelRead(0) // the reverse half of a lane is unused
	return newLaneStream(s, o), nil
}

var errNoLane = errors.New("zmux: no lane now")

// laneStream is an outbound lane over an outer stream.
type laneStream struct {
	outer *Conn
	nestedWriter
}

func newLaneStream(s *Stream, outer *Conn) *laneStream {
	lane := &laneStream{outer: outer}
	lane.s = s
	return lane
}

func (l *laneStream) Close() error { return l.finish(false, l.outer.releaseNestedLane) }

func (l *laneStream) Abort(_ error) { _ = l.finish(true, l.outer.releaseNestedLane) }

// lanesPayOff: over a single TCP pipe the outer streams share one ordered
// byte stream and lanes would recover nothing.
func (c *Conn) lanesPayOff() bool {
	c.lock()
	defer c.unlock()
	n := 0
	for _, id := range c.s.PipeIDs(nil) {
		if pi, ok := c.s.PipeInfo(id); ok && pi.HasOut && (pi.OutState == core.OutActive || pi.OutState == core.OutJoining) {
			n++
			if pi.LanesOut > 1 {
				return true
			}
		}
	}
	return n > 1
}

// openNestedLane opens an outer stream as a lane of a nested pipe in one
// step under the outer session's lock: only with stream
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

// ---- loops ----

// nesting records which sessions run over which: an edge n -> o for every
// pipe of n carried by a stream of o. No pipe of a session may run over
// that session, directly or through others. The lock is a leaf.
var nesting = struct {
	edges map[*Conn]*nestOut
	mu    sync.Mutex
}{edges: map[*Conn]*nestOut{}}

// nestOut is the outer sessions one session runs over, each with the
// number of its edges to it. forgetNesting discards it whole; a session's
// later edges count in a new one.
type nestOut struct{ n map[*Conn]int }

// errNestingLoop refuses a pipe whose carrier runs over its own session.
var errNestingLoop = errors.New("zmux: the carrier runs over the session it would join")

// outerRef names the outer session a stream-backed carrier runs over. It
// is a reference for the nesting graph only: its holder neither owns nor
// closes that session.
type outerRef struct{ c *Conn }

// carrierIdentity samples the lane identity and the enclosing session, if any.
// The writer has precedence for lane identity. Either half may identify nesting.
func carrierIdentity(p PipeIO) (carrier any, outer outerRef) {
	seen := false
	for _, x := range []any{p.W, p.R} {
		if cc, ok := x.(CarrierConner); ok {
			id := cc.CarrierConn()
			if !seen {
				carrier, seen = id, true
			}
			if o, ok := id.(*Conn); ok {
				return carrier, outerRef{o}
			}
		}
	}
	return carrier, outer
}

// nestEdge is one recorded edge n -> o of the nesting graph, held by
// exactly one owner at a time (a join attempt, then the pipe it made); drop
// releases it once, whoever calls it and however late (forgetNesting may
// have dropped the session's edges already).
type nestEdge struct {
	n, o    *Conn
	out     *nestOut // what the edge counts in
	dropped bool     // under nesting.mu
}

// nestPipe records that n runs over ref, unless ref runs over n already;
// the check and the record are one atomic step. The edge is nil when the
// carrier runs over no session.
func nestPipe(n *Conn, ref outerRef) (*nestEdge, error) {
	o := ref.c
	if o == nil {
		return nil, nil //nolint:nilnil // no outer session, no edge: a nil edge is "none", which drop takes
	}
	nesting.mu.Lock()
	defer nesting.mu.Unlock()
	if o == n || reaches(o, n, map[*Conn]bool{}) {
		return nil, errNestingLoop
	}
	out := nesting.edges[n]
	if out == nil {
		out = &nestOut{n: map[*Conn]int{}}
		nesting.edges[n] = out
	}
	out.n[o]++
	return &nestEdge{n: n, o: o, out: out}, nil
}

// drop releases the edge (nil: none); a second drop does nothing.
func (e *nestEdge) drop() {
	if e == nil {
		return
	}
	nesting.mu.Lock()
	defer nesting.mu.Unlock()
	if e.dropped {
		return
	}
	e.dropped = true
	if nesting.edges[e.n] != e.out { // forgetNesting dropped them all
		return
	}
	m := e.out.n
	if m[e.o]--; m[e.o] <= 0 {
		delete(m, e.o)
	}
	if len(m) == 0 {
		delete(nesting.edges, e.n)
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
	out := nesting.edges[from]
	if out == nil {
		return false
	}
	for o := range out.n {
		if reaches(o, to, seen) {
			return true
		}
	}
	return false
}
