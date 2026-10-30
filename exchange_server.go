package zmux

import (
	"context"
	"crypto/hmac"
	"encoding/base64"
	"io"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// HTTPExchangeHandler serves HTTP exchange carriers (see
// HTTPExchangeConfig) into a Listener: every carrier is handed to
// Listener.HandlePipe as a bidirectional carrier that starts or joins a
// session. Mount it at the path (or path prefix) that the clients' URL
// template points to; the request target and headers are otherwise
// ignored. The bytes it holds for its carriers count against the
// listener's memory budget.
type HTTPExchangeHandler struct {
	l      *Listener
	p      exParams
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup // HandlePipe goroutines

	mu       sync.Mutex
	closed   bool
	carriers map[[exCIDLen]byte]*exServer
	gone     map[[exCIDLen]byte]*exKey
	goneQ    []exGone // in expiry order
}

type exGone struct {
	cid   [exCIDLen]byte
	until time.Time
}

// NewHTTPExchangeHandler returns a handler that serves exchange carriers
// into l. cfg may be nil.
func NewHTTPExchangeHandler(l *Listener, cfg *HTTPExchangeConfig) *HTTPExchangeHandler {
	h := &HTTPExchangeHandler{l: l, p: cfg.resolve(),
		carriers: map[[exCIDLen]byte]*exServer{}, gone: map[[exCIDLen]byte]*exKey{}}
	h.ctx, h.cancel = context.WithCancel(context.Background())
	return h
}

// Close fails every carrier, refuses new requests and returns once the
// carriers' pipes have ended.
func (h *HTTPExchangeHandler) Close() error {
	h.mu.Lock()
	h.closed = true
	cs := make([]*exServer, 0, len(h.carriers))
	for _, c := range h.carriers {
		cs = append(cs, c)
	}
	h.mu.Unlock()
	for _, c := range cs {
		c.fail(net.ErrClosed)
	}
	h.cancel()
	h.wg.Wait()
	return nil
}

// ServeHTTP answers one exchange request.
func (h *HTTPExchangeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	limit := exReqHdrMax + h.p.upPiece
	if h.p.b64 {
		limit = b64Len(limit)
	}
	switch {
	case r.ContentLength < 0:
		http.Error(w, "length required", http.StatusLengthRequired)
		return
	case r.ContentLength > int64(limit):
		h.tooLarge(w, r)
		return
	}
	buf, body, err := readExBody(r.Body, int(r.ContentLength), h.p.b64)
	if err != nil {
		http.Error(w, "bad body", http.StatusBadRequest)
		return
	}
	// The request's fields alias buf until the response is written.
	defer buf.unref()
	q, ok := parseExReq(body)
	if !ok {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if !q.valid {
		h.invalid(w, &q)
		return
	}
	c, key, flags := h.lookup(r, &q)
	switch {
	case key == nil:
		http.Error(w, "forbidden", http.StatusForbidden)
	case c == nil:
		h.respond(w, key, &q, flags, nil)
	default:
		buf.ref() // serve's own reference: an UP piece keeps it
		c.serve(w, r, &q, buf)
	}
}

// tooLarge answers an UP request over the local body limit with GONE
// (Section 8): a configuration mismatch the client does not retry. Only
// the request's header is read; the request is not authenticated, so it
// changes nothing.
func (h *HTTPExchangeHandler) tooLarge(w http.ResponseWriter, r *http.Request) {
	n := exReqHdrMax
	if h.p.b64 {
		n = b64Len(n)
	}
	buf, b, err := readExBody(r.Body, n, h.p.b64)
	if err != nil {
		http.Error(w, "too large", http.StatusRequestEntityTooLarge)
		return
	}
	defer buf.unref()
	q, ok := parseExReq(b)
	if !ok || !q.valid || q.kind != exKindUp {
		http.Error(w, "too large", http.StatusRequestEntityTooLarge)
		return
	}
	h.mu.Lock()
	key, gone := h.goneLocked(q.cid)
	if c := h.carriers[q.cid]; c != nil {
		key, gone = c.key, true
	}
	h.mu.Unlock()
	if !gone {
		key = newExKey(h.p.psk, q.cid[:])
	}
	h.respond(w, key, &q, exFlagGone, nil)
}

// invalid answers an authenticated request whose fields violate Section 3
// with GONE and fails its carrier, if any; it creates none.
func (h *HTTPExchangeHandler) invalid(w http.ResponseWriter, q *exReq) {
	h.mu.Lock()
	c := h.carriers[q.cid]
	key, gone := h.goneLocked(q.cid)
	h.mu.Unlock()
	switch {
	case c != nil:
		key = c.key
	case !gone:
		key = newExKey(h.p.psk, q.cid[:])
	}
	if m := key.requestMAC(q.hdr, q.data); !hmac.Equal(m[:], q.mac) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	if c != nil {
		c.fail(errExchangeProtocol)
	}
	h.respond(w, key, q, exFlagGone, nil)
}

// lookup finds or creates the carrier of a request whose MAC verifies. key
// is nil when the MAC does not verify. Without a carrier, flags is the
// answer: GONE for a remembered carrier, BUSY for one that cannot be
// created, processed for a CLOSE of an unknown carrier.
func (h *HTTPExchangeHandler) lookup(r *http.Request, q *exReq) (c *exServer, key *exKey, flags byte) {
	h.mu.Lock()
	c = h.carriers[q.cid]
	g, gone := h.goneLocked(q.cid)
	closed := h.closed
	h.mu.Unlock()
	verify := func(k *exKey) bool {
		m := k.requestMAC(q.hdr, q.data)
		return hmac.Equal(m[:], q.mac)
	}
	switch {
	case c != nil:
		if !verify(c.key) {
			return nil, nil, 0
		}
		return c, c.key, 0
	case gone:
		if !verify(g) {
			return nil, nil, 0
		}
		return nil, g, exFlagGone
	}
	key = newExKey(h.p.psk, q.cid[:])
	if !verify(key) {
		return nil, nil, 0
	}
	if q.kind == exKindClose {
		h.mu.Lock()
		h.forgetLocked(q.cid, key)
		h.mu.Unlock()
		return nil, key, 0
	}
	if closed || h.l.budget.Pressure() {
		return nil, key, exFlagBusy
	}
	nc := newExServer(h, q.cid, key)
	h.mu.Lock()
	if old := h.carriers[q.cid]; old != nil {
		h.mu.Unlock()
		return old, old.key, 0 // created concurrently
	}
	if h.closed {
		h.mu.Unlock()
		return nil, key, exFlagBusy
	}
	h.carriers[q.cid] = nc
	h.wg.Add(1)
	h.mu.Unlock()
	hint := JoinHint{}
	if a, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		hint.Source = a
	}
	go func() {
		defer h.wg.Done()
		_ = h.l.HandlePipe(h.ctx, PipeIO{R: &exServerReader{nc}, W: &exServerWriter{nc}}, hint)
	}()
	return nc, key, 0
}

// goneLocked looks a remembered carrier up, forgetting the expired ones.
func (h *HTTPExchangeHandler) goneLocked(cid [exCIDLen]byte) (*exKey, bool) {
	now := time.Now()
	for len(h.goneQ) > 0 && now.After(h.goneQ[0].until) {
		delete(h.gone, h.goneQ[0].cid)
		h.goneQ[0] = exGone{}
		h.goneQ = h.goneQ[1:]
	}
	k, ok := h.gone[cid]
	return k, ok
}

// forgetLocked removes a carrier and remembers it for GoneMemory.
func (h *HTTPExchangeHandler) forgetLocked(cid [exCIDLen]byte, key *exKey) {
	delete(h.carriers, cid)
	if _, ok := h.gone[cid]; !ok {
		h.gone[cid] = key
		h.goneQ = append(h.goneQ, exGone{cid: cid, until: time.Now().Add(h.p.goneFor)})
	}
}

// respond writes a response with flags and data to q.
func (h *HTTPExchangeHandler) respond(w http.ResponseWriter, key *exKey, q *exReq, flags byte, data []byte) {
	mac := key.responseMAC(q.cid[:], q.kind, q.seqRaw, q.nonce, flags, data)
	var hdr [exRespHdr]byte
	copy(hdr[:], mac[:])
	hdr[exMACLen] = flags
	hh := w.Header()
	hh.Set("Content-Type", "application/octet-stream")
	hh.Set("Cache-Control", "no-store")
	n := exRespHdr + len(data)
	if h.p.b64 {
		bin := getExBuf(n)
		copy(bin.b, hdr[:])
		copy(bin.b[exRespHdr:], data)
		enc := getExBuf(b64Len(n))
		base64.StdEncoding.Encode(enc.b, bin.b[:n])
		bin.unref()
		hh.Set("Content-Length", strconv.Itoa(b64Len(n)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(enc.b[:b64Len(n)]) // the client repeats what it lacks
		enc.unref()
		return
	}
	hh.Set("Content-Length", strconv.Itoa(n))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(hdr[:])
	if len(data) > 0 {
		_, _ = w.Write(data)
	}
}

// exServer is the server end of one exchange carrier. Every field below mu
// is owned by mu. Held DOWN requests wait in their HTTP handlers, each
// bounded by the hold time; zmux reads and writes through the halves.
type exServer struct {
	h   *HTTPExchangeHandler
	cid [exCIDLen]byte
	key *exKey

	mu       sync.Mutex
	rc, wc   sync.Cond     // uplink reader, downlink writer
	assigned chan struct{} // closed and replaced when a piece is assigned
	dead     chan struct{} // closed when the carrier ends
	ended    bool
	err      error // failure seen by zmux (nil: completed)
	last     time.Time
	idle     *time.Timer
	coal     *time.Timer
	mem      int64 // charged to the listener's budget

	// uplink
	upNext  uint64              // lowest number not received
	upMax   uint64              // highest number received plus one
	upOOO   map[uint64]*exPiece // received above upNext
	upQ     []*exPiece          // contiguous, not yet read by zmux
	upLen   int                 // bytes in upOOO and upQ
	upFin   uint64
	upFinOK bool
	rclosed bool

	// downlink
	ack     uint64               // highest Down Ack received
	pieces  map[uint64]*exAssign // assigned, not yet released
	asgEnd  uint64               // highest assigned number plus one
	fill    uint64               // lowest unassigned number: the piece bytes go to
	cur     *exBuf               // bytes of piece fill
	curLen  int
	curAt   time.Time
	nheld   int // held DOWN requests
	wclosed bool
	finLow  uint64 // lowest FIN piece, when finOK
	finOK   bool
}

// exAssign is an assigned downlink piece; refs counts the responses being
// written from it.
type exAssign struct {
	buf      *exBuf
	data     []byte
	fin      bool
	refs     int
	released bool
}

func newExServer(h *HTTPExchangeHandler, cid [exCIDLen]byte, key *exKey) *exServer {
	c := &exServer{h: h, cid: cid, key: key, assigned: make(chan struct{}), dead: make(chan struct{}),
		upOOO: map[uint64]*exPiece{}, pieces: map[uint64]*exAssign{}, last: time.Now()}
	c.rc.L, c.wc.L = &c.mu, &c.mu
	c.idle = time.AfterFunc(h.p.idle, c.onIdle)
	return c
}

func (c *exServer) charge(d int64) {
	c.mem += d
	c.h.l.budget.Add(d)
}

func (c *exServer) onIdle() {
	c.mu.Lock()
	if c.ended {
		c.mu.Unlock()
		return
	}
	if c.nheld == 0 {
		if rest := c.h.p.idle - time.Since(c.last); rest <= 0 {
			// Both FINs on their way: released as complete (Section 7.4).
			err := errExchangeIdle
			if c.upFinOK && c.upNext > c.upFin && c.finOK {
				err = nil
			}
			c.endLocked(err)
			c.mu.Unlock()
			c.forget()
			return
		}
	}
	c.idle.Reset(max(c.h.p.idle-time.Since(c.last), time.Millisecond))
	c.mu.Unlock()
}

// fail ends the carrier with err.
func (c *exServer) fail(err error) {
	c.mu.Lock()
	ended := c.endLocked(err)
	c.mu.Unlock()
	if ended {
		c.forget()
	}
}

func (c *exServer) forget() {
	c.h.mu.Lock()
	if c.h.carriers[c.cid] == c {
		c.h.forgetLocked(c.cid, c.key)
	}
	c.h.mu.Unlock()
}

// endLocked ends the carrier (err nil: completed) and releases what it
// holds, except uplink bytes zmux may still read after a completion. The
// caller then calls forget.
func (c *exServer) endLocked(err error) bool {
	if c.ended {
		return false
	}
	c.ended = true
	c.err = err
	close(c.dead)
	c.idle.Stop()
	if c.coal != nil {
		c.coal.Stop()
	}
	for n, a := range c.pieces {
		c.releaseLocked(a)
		delete(c.pieces, n)
	}
	if c.cur != nil {
		c.charge(-int64(len(c.cur.b)))
		c.cur.unref()
		c.cur, c.curLen = nil, 0
	}
	if err != nil {
		c.dropUplinkLocked()
	}
	c.rc.Broadcast()
	c.wc.Broadcast()
	return true
}

func (c *exServer) dropUplinkLocked() {
	for _, p := range c.upQ {
		c.freePieceLocked(p)
	}
	c.upQ = nil
	for n, p := range c.upOOO {
		c.freePieceLocked(p)
		delete(c.upOOO, n)
	}
	c.upLen = 0
}

func (c *exServer) freePieceLocked(p *exPiece) {
	if p.buf != nil {
		c.charge(-int64(len(p.buf.b)))
		p.buf.unref()
		p.buf = nil
	}
}

func (c *exServer) releaseLocked(a *exAssign) {
	if a.released {
		return
	}
	a.released = true
	if a.buf != nil {
		c.charge(-int64(len(a.buf.b)))
		if a.refs == 0 {
			a.buf.unref()
		}
	}
}

// completeLocked ends a completed carrier (Section 7.4).
func (c *exServer) completeLocked() bool {
	if c.upFinOK && c.upNext > c.upFin && c.finOK && c.ack > c.finLow {
		return c.endLocked(nil)
	}
	return false
}

// serve processes a request whose MAC verified. buf holds q's body.
func (c *exServer) serve(w http.ResponseWriter, r *http.Request, q *exReq, buf *exBuf) {
	h := c.h
	c.mu.Lock()
	c.last = time.Now()
	if c.ended {
		c.mu.Unlock()
		buf.unref()
		h.respond(w, c.key, q, exFlagGone, nil)
		return
	}
	if q.ack > c.ack {
		if q.ack > c.asgEnd {
			c.endLocked(errExchangeProtocol)
			c.mu.Unlock()
			c.forget()
			buf.unref()
			h.respond(w, c.key, q, exFlagGone, nil)
			return
		}
		for n, a := range c.pieces {
			if n < q.ack {
				c.releaseLocked(a)
				delete(c.pieces, n)
			}
		}
		c.ack = q.ack
		c.wc.Broadcast()
	}
	c.completeLocked()
	var flags byte
	switch {
	case c.ended:
		// Completed by this acknowledgement: the request is a repetition.
		buf.unref()
		buf = nil
		if q.kind == exKindDown {
			flags = exFlagGone
		}
	case q.kind == exKindUp:
		flags = c.upLocked(q, buf)
		buf = nil
	case q.kind == exKindClose:
		c.endLocked(errExchangeClosed)
	default:
		buf.unref()
		c.down(w, r, q) // unlocks
		return
	}
	ended := c.completeLocked() || c.ended
	gone := c.ended && c.err != nil && q.kind != exKindClose
	c.mu.Unlock()
	if buf != nil {
		buf.unref()
	}
	if ended {
		c.forget()
	}
	if gone {
		flags = exFlagGone
	}
	h.respond(w, c.key, q, flags, nil)
}

// upLocked stores an UP piece (Section 7.2); it owns buf and returns the
// response flags.
func (c *exServer) upLocked(q *exReq, buf *exBuf) byte {
	n, fin := q.seq, q.flags&exFlagFin != 0
	switch {
	case fin && (c.upFinOK && c.upFin != n || c.upMax > n+1),
		!fin && c.upFinOK && n > c.upFin:
		buf.unref()
		c.endLocked(errExchangeProtocol)
		return exFlagGone
	}
	if _, dup := c.upOOO[n]; n < c.upNext || dup {
		buf.unref()
		return 0
	}
	p := &exPiece{fin: fin}
	if len(q.data) > 0 && !c.rclosed {
		room := c.h.p.upStore
		if n != c.upNext {
			// Out of order: leave room for the piece zmux waits for.
			room -= c.h.p.upPiece
			if c.h.l.budget.Pressure() {
				room = 0
			}
		}
		if c.upLen+len(q.data) > room {
			buf.unref()
			return exFlagBusy
		}
		p.buf, p.data = buf, q.data
		c.upLen += len(q.data)
		c.charge(int64(len(buf.b)))
	} else {
		buf.unref()
	}
	if fin {
		c.upFin, c.upFinOK = n, true
	}
	c.upMax = max(c.upMax, n+1)
	if n != c.upNext {
		c.upOOO[n] = p
		return 0
	}
	c.upQ = append(c.upQ, p)
	c.upNext++
	for {
		x, ok := c.upOOO[c.upNext]
		if !ok {
			break
		}
		delete(c.upOOO, c.upNext)
		c.upQ = append(c.upQ, x)
		c.upNext++
	}
	c.rc.Broadcast()
	return 0
}

// down answers a DOWN request (Section 7.3); it is called locked and
// unlocks.
func (c *exServer) down(w http.ResponseWriter, r *http.Request, q *exReq) {
	h := c.h
	n := q.seq
	var hold *time.Timer
	defer func() {
		if hold != nil {
			hold.Stop()
		}
	}()
	held, expired := false, false
	for {
		a := c.pieces[n]
		var flags byte
		switch {
		case c.ended:
			flags = exFlagGone
		case n < c.ack:
			flags = exFlagBusy
		case a != nil:
		case n >= c.ack+uint64(h.p.window):
			flags = exFlagBusy
		case c.wclosed && c.curLen == 0:
			a = c.assignLocked(n, nil, 0, true)
		case expired:
			// The hold time ran out: the open piece, or an empty one.
			if n == c.fill && c.curLen > 0 {
				a = c.assignCurLocked()
			} else {
				a = c.assignLocked(n, nil, 0, false)
			}
		default:
			if !held {
				held = true
				c.nheld++
				hold = time.NewTimer(h.p.hold)
				c.coalesceLocked(time.Now())
				continue // coalescing may have assigned it
			}
			ch := c.assigned
			c.mu.Unlock()
			select {
			case <-ch:
			case <-hold.C:
				expired = true
			case <-c.dead:
			case <-r.Context().Done():
				c.mu.Lock()
				c.nheld--
				c.mu.Unlock()
				return
			}
			c.mu.Lock()
			continue
		}
		if held {
			c.nheld--
		}
		if a != nil {
			a.refs++
		}
		c.last = time.Now()
		c.mu.Unlock()
		if a == nil {
			h.respond(w, c.key, q, flags, nil)
			return
		}
		if a.fin {
			flags = exFlagFin
		}
		h.respond(w, c.key, q, flags, a.data)
		c.mu.Lock()
		a.refs--
		if a.released && a.refs == 0 && a.buf != nil {
			a.buf.unref()
		}
		c.mu.Unlock()
		return
	}
}

// assignLocked assigns piece n and wakes the held requests.
func (c *exServer) assignLocked(n uint64, buf *exBuf, l int, fin bool) *exAssign {
	a := &exAssign{buf: buf, fin: fin}
	if buf != nil {
		a.data = buf.b[:l]
	}
	c.pieces[n] = a
	c.asgEnd = max(c.asgEnd, n+1)
	if fin && (!c.finOK || n < c.finLow) {
		c.finLow, c.finOK = n, true
	}
	for c.pieces[c.fill] != nil {
		c.fill++
	}
	close(c.assigned)
	c.assigned = make(chan struct{})
	c.wc.Broadcast()
	return a
}

// assignCurLocked assigns the bytes of piece fill (FIN once zmux closed
// the write side).
func (c *exServer) assignCurLocked() *exAssign {
	buf, l := c.cur, c.curLen
	c.cur, c.curLen = nil, 0
	return c.assignLocked(c.fill, buf, l, c.wclosed)
}

// coalesceLocked assigns piece fill when it is full, or when a request can
// take it and its coalescing delay ran out; otherwise it arms the timer.
func (c *exServer) coalesceLocked(now time.Time) {
	if c.curLen == 0 || c.ended {
		return
	}
	if c.curLen >= c.h.p.downPiece || c.wclosed {
		c.assignCurLocked()
		return
	}
	if c.nheld == 0 {
		return
	}
	due := c.curAt.Add(c.h.p.coalesce)
	if !now.Before(due) {
		c.assignCurLocked()
		return
	}
	if c.coal == nil {
		c.coal = time.AfterFunc(due.Sub(now), c.onCoalesce)
	} else {
		c.coal.Reset(due.Sub(now))
	}
}

func (c *exServer) onCoalesce() {
	c.mu.Lock()
	c.coalesceLocked(time.Now())
	c.mu.Unlock()
}

// exServerReader is the uplink half.
type exServerReader struct{ c *exServer }

func (r *exServerReader) Read(p []byte) (int, error) {
	c := r.c
	c.mu.Lock()
	defer c.mu.Unlock()
	for {
		if c.rclosed {
			return 0, net.ErrClosed
		}
		for len(c.upQ) > 0 {
			x := c.upQ[0]
			if len(x.data) == 0 {
				c.freePieceLocked(x)
				c.upQ[0] = nil
				c.upQ = c.upQ[1:]
				continue
			}
			n := copy(p, x.data)
			x.data = x.data[n:]
			c.upLen -= n
			if len(x.data) == 0 {
				c.freePieceLocked(x)
				c.upQ[0] = nil
				c.upQ = c.upQ[1:]
			}
			return n, nil
		}
		if c.err != nil {
			return 0, c.err
		}
		if c.upFinOK && c.upNext > c.upFin {
			return 0, io.EOF
		}
		c.rc.Wait()
	}
}

func (r *exServerReader) Close() error {
	c := r.c
	c.mu.Lock()
	c.rclosed = true
	c.dropUplinkLocked()
	c.rc.Broadcast()
	c.mu.Unlock()
	return nil
}

// exServerWriter is the downlink half.
type exServerWriter struct{ c *exServer }

func (w *exServerWriter) Write(p []byte) (int, error) {
	c := w.c
	lim := c.h.p.downPiece
	c.mu.Lock()
	defer c.mu.Unlock()
	total := 0
	for len(p) > 0 {
		switch {
		case c.err != nil:
			return total, c.err
		case c.wclosed || c.ended:
			return total, net.ErrClosed
		case c.fill >= c.ack+uint64(c.h.p.window):
			c.wc.Wait()
			continue
		}
		n := min(lim-c.curLen, len(p))
		old := 0
		if c.cur != nil {
			old = len(c.cur.b)
		}
		c.cur = exGrow(c.cur, c.curLen, c.curLen+n, lim)
		c.charge(int64(len(c.cur.b) - old))
		copy(c.cur.b[c.curLen:], p[:n])
		if c.curLen == 0 {
			c.curAt = time.Now()
		}
		c.curLen += n
		total += n
		p = p[n:]
		c.coalesceLocked(time.Now())
	}
	return total, nil
}

// Close ends the downlink gracefully: the last piece carries FIN.
func (w *exServerWriter) Close() error {
	c := w.c
	c.mu.Lock()
	if c.wclosed || c.ended {
		c.mu.Unlock()
		return nil
	}
	c.wclosed = true
	if c.curLen > 0 {
		c.assignCurLocked()
	} else {
		// Held requests take an empty FIN piece.
		close(c.assigned)
		c.assigned = make(chan struct{})
	}
	ended := c.completeLocked()
	c.mu.Unlock()
	if ended {
		c.forget()
	}
	return nil
}

// Abort fails the carrier; later requests get GONE.
func (w *exServerWriter) Abort(err error) {
	if err == nil {
		err = net.ErrClosed
	}
	w.c.fail(err)
}
