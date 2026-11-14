package zmuxhttp

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	mrand "math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/internal/labels"
)

// Pipe opens the exchange carrier to the ExchangeHandler at e.URL and
// returns it as a bidirectional pipe for zmux.Start or zmux.Conn.AddPipe. It
// sends nothing before zmux writes; the carrier is ready at once. ctx
// bounds the setting up: ending it before the first verified response fails
// the carrier, which otherwise lives until zmux closes it. An invalid
// configuration (no PSK and no AllowInsecure, a negative field, a URL that
// does not parse) is a zmux.ErrInvalidConfig error.
//
// Every request follows redirects as Streaming.Pipe does; a pure scheme
// upgrade (CheckRedirect) is remembered, and the carrier's later requests
// go to the new origin at once.
func (e Exchange) Pipe(ctx context.Context) (zmux.PipeIO, error) {
	return e.pipe(ctx, nil)
}

// pipe is Pipe; with t, the carrier starts at t's URL and records in t the
// URL redirects led it to.
func (e Exchange) pipe(ctx context.Context, t *Target) (zmux.PipeIO, error) {
	p, err := e.Config.resolve(&e, nil)
	if err != nil {
		return zmux.PipeIO{}, err
	}
	client, rawURL := e.Client, e.URL
	if t != nil {
		rawURL = t.URL()
	}
	c := &exClient{p: p, hc: client, nc: noRedirectClient(client), ht: t, wake: make(chan struct{}, 1),
		downs: map[uint64]*exReqState{}, held: map[uint64]*exPiece{}}
	if _, err := rand.Read(c.cid[:]); err != nil {
		return zmux.PipeIO{}, err
	}
	c.key = newExKey(c.p.psk, c.cid[:])
	inst := c.p.instance
	if inst == "" {
		var t [8]byte
		_, _ = rand.Read(t[:])
		inst = hex.EncodeToString(t[:])
	}
	downURL := c.p.downURL
	if downURL == "" {
		downURL = rawURL
	}
	for i, u := range []string{rawURL, downURL} {
		tg := exTarget{tmpl: u, param: c.p.nonceParam, instance: inst,
			bust: c.p.cacheBust && !strings.Contains(u, "{nonce}"), origin: new(atomic.Pointer[string])}
		var probe [exNonceLen]byte
		if _, err := url.Parse(tg.build(probe[:])); err != nil {
			return zmux.PipeIO{}, fmt.Errorf("%w: exchange URL: %w", zmux.ErrInvalidConfig, err)
		}
		if i == 0 {
			c.target = tg
		} else {
			c.downTarget = tg
		}
	}
	c.rc.L, c.wc.L = &c.mu, &c.mu
	c.upLimit, c.downLimit = c.p.ups, c.p.downs
	// ctx bounds the setting up of the carrier only: from the first
	// verified response on, the carrier lives until it is closed.
	c.ctx, c.cancel = context.WithCancel(context.WithoutCancel(ctx))
	c.setup = context.AfterFunc(ctx, c.cancel)
	labels.Go(ctx, "exchange-client", func() { c.run() })
	return zmux.PipeIO{R: &exClientReader{c}, W: &exClientWriter{c}, Carrier: zmux.CarrierExchange}, nil
}

// exTarget builds request targets from the URL template.
type exTarget struct {
	tmpl, param, instance string
	bust                  bool
	// origin replaces the template's scheme and authority once a request
	// was upgraded to https (nil: none).
	origin *atomic.Pointer[string]
}

// tmplRest is the template after its scheme and authority.
func tmplRest(tmpl string) string {
	i := strings.Index(tmpl, "://")
	if i < 0 {
		return tmpl
	}
	j := strings.IndexAny(tmpl[i+3:], "/?#")
	if j < 0 {
		return ""
	}
	return tmpl[i+3+j:]
}

// upgraded records that a request to from was upgraded to to.
func (t *exTarget) upgraded(to *url.URL) string {
	o := to.Scheme + "://" + to.Host
	t.origin.Store(&o)
	return o + tmplRest(t.tmpl)
}

func (t *exTarget) build(nonce []byte) string {
	n := hex.EncodeToString(nonce)
	tmpl := t.tmpl
	if o := t.origin.Load(); o != nil {
		tmpl = *o + tmplRest(tmpl)
	}
	u := strings.NewReplacer("{instance}", t.instance, "{nonce}", n).Replace(tmpl)
	if t.bust {
		sep := "?"
		if strings.Contains(u, "?") {
			sep = "&"
		}
		u += sep + t.param + "=" + n
	}
	return u
}

// exClient is the client end of an exchange carrier. Every field below mu
// is owned by mu; the run goroutine schedules requests, one goroutine per
// attempt in flight (at most DownRequests+UpRequests, plus a final CLOSE)
// performs it and applies its result under mu.
type exClient struct {
	setup      func() bool // stops ctx from ending the carrier (context.AfterFunc)
	p          exParams
	hc         *http.Client // the caller's client (nil: the default)
	nc         *http.Client // hc returning redirects
	ht         *Target      // the candidate's target, if any
	cid        [exCIDLen]byte
	key        *exKey
	target     exTarget // UP and CLOSE requests
	downTarget exTarget
	ctx        context.Context
	cancel     context.CancelFunc
	wake       chan struct{} // capacity 1: run has something to do

	mu      sync.Mutex
	rc, wc  sync.Cond // Read and Write waiters
	err     error     // the carrier failed
	ended   bool      // run has stopped: no more requests
	sentAny bool
	gone    bool // the server answered GONE
	// The flags of the uplink and the downlink, kept with the others.
	wclosed  bool // uplink: zmux closed the write side
	finSeal  bool // uplink: the FIN piece is sealed
	finKnown bool // downlink: fin is set
	rclosed  bool // downlink: zmux closed the read side

	// uplink
	open     *exBuf // the piece being filled (nil: none)
	openLen  int
	openAt   time.Time // first byte of the open piece
	upSeq    uint64    // number of the next sealed piece
	ups      []*exUp   // sealed pieces not yet processed, by number
	upFlight int
	upLimit  int

	// downlink
	ack       uint64                 // smallest downlink number not received
	deliv     uint64                 // next piece to give to the reader
	held      map[uint64]*exPiece    // received, not yet given to the reader
	fin       uint64                 // lowest FIN piece, when finKnown
	downs     map[uint64]*exReqState // outstanding DOWN numbers
	dnFlight  int
	downLimit int
	rbuf      *exBuf // the piece the reader is consuming
	rdata     []byte
}

// exReqState is one request number's retry state.
type exReqState struct {
	inflight bool
	cancel   context.CancelFunc
	retryAt  time.Time
	since    time.Time // first attempt without a result since the last result
	backoff  time.Duration
}

type exUp struct {
	exReqState
	seq  uint64
	buf  *exBuf // nil for an empty piece
	data []byte
	fin  bool
}

// exPiece is a received downlink piece.
type exPiece struct {
	buf  *exBuf
	data []byte
	fin  bool
}

func (c *exClient) poke() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

// failLocked fails the carrier with err (the first cause wins).
func (c *exClient) failLocked(err error) {
	if c.err == nil {
		c.err = err
	}
	c.rc.Broadcast()
	c.wc.Broadcast()
	c.poke()
}

// run schedules the carrier's requests until it ends.
func (c *exClient) run() {
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	var launch []*exAttempt
	for {
		c.mu.Lock()
		if c.ctx.Err() != nil && !c.ended {
			c.failLocked(c.ctx.Err())
		}
		now := time.Now()
		launch = launch[:0]
		next, done := c.stepLocked(now, &launch)
		c.mu.Unlock()
		for _, a := range launch {
			labels.Go(c.ctx, "exchange-attempt", func() { c.attempt(a) })
		}
		if done {
			return
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		if !next.IsZero() {
			timer.Reset(max(next.Sub(now), 0))
		}
		select {
		case <-c.wake:
		case <-timer.C:
		case <-c.ctx.Done():
		}
	}
}

// completeLocked reports a completed carrier.
func (c *exClient) completeLocked() bool {
	return c.finSeal && len(c.ups) == 0 && c.finKnown && c.ack > c.fin
}

// stepLocked advances the schedule at now: it seals UP pieces, collects the
// attempts to launch and returns when it next has to run.
func (c *exClient) stepLocked(now time.Time, launch *[]*exAttempt) (next time.Time, done bool) {
	soon := func(t time.Time) {
		if next.IsZero() || t.Before(next) {
			next = t
		}
	}
	if c.err == nil {
		check := func(s *exReqState) {
			if !s.since.IsZero() {
				if now.Sub(s.since) > c.p.deadline {
					c.failLocked(errExchangeDeadline)
				}
				soon(s.since.Add(c.p.deadline + time.Millisecond))
			}
		}
		for _, u := range c.ups {
			check(&u.exReqState)
		}
		for _, s := range c.downs {
			check(s)
		}
	}
	complete := c.completeLocked()
	upEnded := c.finSeal && len(c.ups) == 0
	if c.err != nil || complete || (upEnded && c.rclosed) {
		c.endLocked(complete, launch)
		return next, true
	}

	// Uplink: seal the open piece when it is full, when its coalescing
	// delay ran out, or at the end of the write side.
	c.sealLocked(now)
	if c.openLen > 0 && len(c.ups) < c.upLimit {
		soon(c.openAt.Add(c.p.coalesce))
	}
	for _, u := range c.ups {
		if c.upFlight >= c.upLimit {
			break
		}
		if u.inflight {
			continue
		}
		if u.retryAt.After(now) {
			soon(u.retryAt)
			continue
		}
		u.inflight = true
		c.upFlight++
		if u.buf != nil {
			u.buf.ref() // the attempt's reference, dropped when its body is closed
		}
		*launch = append(*launch, c.newAttemptLocked(exKindUp, u.seq, &u.exReqState, u.buf, u.data, u.fin, now))
	}

	// Downlink: consecutive numbers from the acknowledgement on, up to the
	// FIN, the outstanding limit and a read-ahead bound.
	if !c.rclosed {
		want := 0
		last := c.ack
		for n := c.ack; want < c.downLimit; n++ {
			if c.finKnown && n > c.fin || n >= c.deliv+uint64(2*c.p.downs) {
				break
			}
			if _, ok := c.held[n]; ok {
				continue
			}
			want++
			last = n
			s := c.downs[n]
			if s == nil {
				s = &exReqState{backoff: c.p.backoffMin}
				c.downs[n] = s
			}
			if s.inflight {
				continue
			}
			if s.retryAt.After(now) {
				soon(s.retryAt)
				continue
			}
			s.inflight = true
			c.dnFlight++
			*launch = append(*launch, c.newAttemptLocked(exKindDown, n, s, nil, nil, false, now))
		}
		for n, s := range c.downs {
			// Numbers no longer wanted (the limit fell) start afresh later.
			if !s.inflight && (n > last || n < c.ack) {
				delete(c.downs, n)
			}
		}
	}
	return next, false
}

// sealLocked seals the open piece when it is due and a slot is free, and
// the empty FIN piece after the write side ended.
func (c *exClient) sealLocked(now time.Time) {
	if len(c.ups) >= c.upLimit || c.finSeal {
		return
	}
	due := c.wclosed || c.openLen == c.p.upPiece || c.openLen > 0 && !now.Before(c.openAt.Add(c.p.coalesce))
	if !due {
		return
	}
	u := &exUp{seq: c.upSeq, fin: c.wclosed}
	u.backoff = c.p.backoffMin
	if c.openLen > 0 {
		u.buf, u.data = c.open, c.open.b[:c.openLen]
		c.open, c.openLen = nil, 0
	}
	c.upSeq++
	c.finSeal = c.wclosed
	c.ups = append(c.ups, u)
	c.wc.Broadcast()
}

// endLocked stops the carrier: it cancels the attempts in flight, releases
// the uplink buffers and, unless the server forgot it, sends a CLOSE
// without waiting for its result. After a completion that CLOSE carries the
// Down Ack past the downlink FIN, which completes the server's side too.
func (c *exClient) endLocked(complete bool, launch *[]*exAttempt) {
	c.ended = true
	c.cancel()
	if c.err == nil && !complete {
		c.err = net.ErrClosed
	}
	for _, u := range c.ups {
		if u.buf != nil {
			u.buf.unref()
		}
	}
	c.ups = nil
	if c.open != nil {
		c.open.unref()
		c.open, c.openLen = nil, 0
	}
	if !complete {
		// The reader sees the failure; pieces it has not taken are dropped.
		for n, p := range c.held {
			if p.buf != nil {
				p.buf.unref()
			}
			delete(c.held, n)
		}
	}
	c.rc.Broadcast()
	c.wc.Broadcast()
	if c.sentAny && !c.gone {
		s := &exReqState{inflight: true}
		*launch = append(*launch, c.newAttemptLocked(exKindClose, 0, s, nil, nil, false, time.Now()))
	}
}

// exAttempt is one HTTP exchange of one request.
type exAttempt struct {
	ctx    context.Context
	cancel context.CancelFunc
	st     *exReqState
	kind   byte
	flags  byte
	seq    uint64
	ack    uint64
	buf    *exBuf // the attempt's reference to data's buffer, if any
	data   []byte
}

func (c *exClient) newAttemptLocked(kind byte, seq uint64, st *exReqState, buf *exBuf, data []byte, fin bool, now time.Time) *exAttempt {
	a := &exAttempt{st: st, kind: kind, seq: seq, ack: c.ack, buf: buf, data: data}
	if fin {
		a.flags = exFlagFin
	}
	parent := c.ctx
	if kind == exKindClose {
		parent = context.WithoutCancel(c.ctx)
	}
	a.ctx, a.cancel = context.WithTimeout(parent, c.p.reqTimeout)
	st.cancel = a.cancel
	if st.since.IsZero() {
		st.since = now
	}
	c.sentAny = true
	return a
}

// exResult is what an attempt brought back.
type exResult struct {
	ok    bool // a verified response
	bad   bool // a verified response that fails the carrier
	flags byte
	err   error // the failure of bad, if not a protocol error
	buf   *exBuf
	data  []byte
}

func (c *exClient) attempt(a *exAttempt) {
	defer a.cancel()
	r := c.exchange(a)
	if r.ok {
		c.setup()
	}
	if a.kind == exKindClose {
		if r.buf != nil {
			r.buf.unref()
		}
		return
	}
	c.mu.Lock()
	c.applyLocked(a, r, time.Now())
	c.mu.Unlock()
	c.poke()
}

// exchange performs one attempt: it sends the request with a fresh nonce
// and returns the verified response, or no result.
func (c *exClient) exchange(a *exAttempt) (res exResult) {
	release := func() {
		if a.buf != nil {
			a.buf.unref()
		}
	}
	var nonce [exNonceLen]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		release()
		return res
	}
	// One allocation holds the authentication header and the Data MAC.
	hdr := appendExAuth(make([]byte, 0, exReqOverhead), c.key, &c.cid, a.kind, a.flags, a.seq, a.ack, uint32(time.Now().Unix()), &nonce, len(a.data))
	mac := c.key.dataMAC(hdr[exFieldsLen:], a.data)
	tail := append(hdr[exAuthLen:], mac[:]...)
	var body io.ReadCloser
	n := exReqOverhead + len(a.data)
	if c.p.b64 {
		enc := getExBuf(b64Len(n))
		// The header's 66 bytes encode to 88 characters on their own; the
		// data and the Data MAC follow in one run.
		base64.StdEncoding.Encode(enc.b, hdr)
		bin := getExBuf(len(a.data) + exMACLen)
		copy(bin.b, a.data)
		copy(bin.b[len(a.data):], tail)
		base64.StdEncoding.Encode(enc.b[b64Len(exAuthLen):], bin.b[:len(a.data)+exMACLen])
		bin.unref()
		release()
		n = b64Len(exAuthLen) + b64Len(len(a.data)+exMACLen)
		body = &exBody{data: enc.b[:n], done: enc.unref}
	} else {
		body = &exBody{hdr: hdr, data: a.data, tail: tail, done: release}
	}
	tg := &c.target
	if a.kind == exKindDown {
		tg = &c.downTarget
	}
	// Every hop of a redirect sends the same body; it is given up once
	// the transport closed every one.
	var hops atomic.Int32
	hops.Store(1)
	unhop := func() {
		if hops.Add(-1) == 0 {
			_ = body.Close()
		}
	}
	defer unhop()
	src := body.(*exBody)
	from := tg.build(nonce[:])
	resp, final, err := doHTTP(a.ctx, c.hc, c.nc, from, func(u *url.URL) (*http.Request, error) {
		hops.Add(1)
		hb := &exBody{hdr: src.hdr, data: src.data, tail: src.tail, done: unhop}
		req, err := http.NewRequestWithContext(a.ctx, http.MethodPost, u.String(), hb)
		if err != nil {
			_ = hb.Close()
			return nil, err
		}
		req.ContentLength = int64(n)
		addHeader(req.Header, c.p.header)
		req.Header.Set("Content-Type", "application/octet-stream")
		req.Header.Set("Cache-Control", "no-store")
		req.Header.Set("Pragma", "no-cache")
		return req, nil
	})
	if err != nil {
		if _, ok := errors.AsType[*RedirectError](err); ok {
			// A redirect the carrier must not follow fails it.
			res.ok, res.bad, res.err = true, true, err
		}
		return res
	}
	if f, perr := url.Parse(from); perr == nil && final.String() != from && SchemeUpgradeOf(f, final) {
		up := tg.upgraded(final)
		if c.ht != nil && tg == &c.target {
			c.ht.Redirected(up)
		}
	}
	defer func() { _ = resp.Body.Close() }() // read in full or abandoned
	limit := int64(exRespHdr + c.p.downLimit)
	if c.p.b64 {
		limit = int64(b64Len(int(limit)))
	}
	if resp.StatusCode != http.StatusOK || resp.ContentLength < 0 {
		return res
	}
	if resp.ContentLength > limit {
		// Over the local limit: a configuration mismatch, never retried.
		res.ok, res.bad = true, true
		return res
	}
	buf, b, err := readExBody(resp.Body, int(resp.ContentLength), c.p.b64)
	if err != nil {
		return res
	}
	if len(b) < exRespHdr {
		buf.unref()
		return res
	}
	flags, data := b[exMACLen], b[exRespHdr:]
	want := c.key.responseMAC(c.cid[:], a.kind, a.seq, nonce[:], flags, data)
	if !c.p.skipRespMAC && !hmac.Equal(want[:], b[:exMACLen]) {
		buf.unref()
		return res
	}
	res.ok = true
	res.flags = flags
	if !exRespValid(a.kind, flags, data) {
		res.bad = true
		buf.unref()
		return res
	}
	if len(data) == 0 {
		buf.unref()
		return res
	}
	res.buf, res.data = buf, data
	return res
}

// applyLocked applies an attempt's result.
func (c *exClient) applyLocked(a *exAttempt, r exResult, now time.Time) {
	st := a.st
	st.inflight = false
	st.cancel = nil
	if a.kind == exKindUp {
		c.upFlight--
	} else {
		c.dnFlight--
	}
	drop := func() {
		if r.buf != nil {
			r.buf.unref()
		}
	}
	if c.ended {
		drop()
		return
	}
	retry := func() {
		d := time.Duration(float64(st.backoff) * (1 - c.p.backoffJitter*mrand.Float64()))
		st.retryAt = now.Add(d)
		st.backoff = min(2*st.backoff, c.p.backoffMax)
	}
	switch {
	case !r.ok:
		retry()
		return
	case r.bad && r.err != nil:
		c.failLocked(r.err)
		return
	case r.bad:
		c.failLocked(errExchangeProtocol)
		return
	case r.flags == exFlagGone:
		c.gone = true
		c.failLocked(errExchangeGone)
		return
	}
	st.since = time.Time{}
	if r.flags == exFlagBusy {
		retry()
		if a.kind == exKindUp {
			c.upLimit = max(1, c.upLimit-1)
		} else {
			c.downLimit = max(1, c.downLimit-1)
		}
		return
	}
	st.backoff = c.p.backoffMin
	if a.kind == exKindUp {
		c.upLimit = min(c.p.ups, c.upLimit+1)
		for i, u := range c.ups {
			if u.seq == a.seq {
				if u.buf != nil {
					u.buf.unref()
				}
				c.ups = append(c.ups[:i], c.ups[i+1:]...)
				break
			}
		}
		c.wc.Broadcast()
		return
	}
	c.downLimit = min(c.p.downs, c.downLimit+1)
	c.receiveLocked(a.seq, r.buf, r.data, r.flags&exFlagFin != 0)
}

// receiveLocked takes downlink piece n.
func (c *exClient) receiveLocked(n uint64, buf *exBuf, data []byte, fin bool) {
	delete(c.downs, n)
	_, dup := c.held[n]
	if dup || n < c.ack || c.finKnown && n > c.fin {
		if buf != nil {
			buf.unref()
		}
		return
	}
	if fin && (!c.finKnown || n < c.fin) {
		c.fin, c.finKnown = n, true
		for m, p := range c.held {
			if m > n {
				if p.buf != nil {
					p.buf.unref()
				}
				delete(c.held, m)
			}
		}
	}
	c.held[n] = &exPiece{buf: buf, data: data, fin: fin}
	for {
		if _, ok := c.held[c.ack]; !ok || c.finKnown && c.ack > c.fin {
			break
		}
		c.ack++
	}
	c.rc.Broadcast()
}

// exBody is a request body over a header and data that it does not own;
// done runs once the transport closed it.
type exBody struct {
	hdr, data, tail []byte
	done            func()
	once            sync.Once
}

func (b *exBody) Read(p []byte) (int, error) {
	for _, part := range []*[]byte{&b.hdr, &b.data, &b.tail} {
		if len(*part) > 0 {
			n := copy(p, *part)
			*part = (*part)[n:]
			return n, nil
		}
	}
	return 0, io.EOF
}

func (b *exBody) Close() error {
	b.once.Do(b.done)
	return nil
}

// exClientReader is the downlink half.
type exClientReader struct{ c *exClient }

func (r *exClientReader) Read(p []byte) (int, error) {
	c := r.c
	c.mu.Lock()
	defer c.mu.Unlock()
	for {
		if c.rclosed {
			return 0, net.ErrClosed
		}
		if len(c.rdata) > 0 {
			n := copy(p, c.rdata)
			c.rdata = c.rdata[n:]
			if len(c.rdata) == 0 {
				c.rbuf.unref()
				c.rbuf, c.rdata = nil, nil
			}
			return n, nil
		}
		if c.err != nil {
			return 0, c.err
		}
		if c.finKnown && c.deliv > c.fin {
			return 0, io.EOF
		}
		if pc, ok := c.held[c.deliv]; ok {
			delete(c.held, c.deliv)
			c.deliv++
			c.poke() // the read-ahead bound moved
			if len(pc.data) > 0 {
				c.rbuf, c.rdata = pc.buf, pc.data
			}
			continue
		}
		c.rc.Wait()
	}
}

// Close stops the downlink; the carrier ends once the uplink is done.
func (r *exClientReader) Close() error {
	c := r.c
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.rclosed {
		return nil
	}
	c.rclosed = true
	if c.rbuf != nil {
		c.rbuf.unref()
		c.rbuf, c.rdata = nil, nil
	}
	for _, s := range c.downs {
		if s.cancel != nil {
			s.cancel()
		}
	}
	c.rc.Broadcast()
	c.poke()
	return nil
}

// exClientWriter is the uplink half.
type exClientWriter struct{ c *exClient }

func (w *exClientWriter) Write(p []byte) (int, error) {
	c := w.c
	c.mu.Lock()
	defer c.mu.Unlock()
	total := 0
	for len(p) > 0 {
		switch {
		case c.err != nil:
			return total, c.err
		case c.wclosed || c.ended:
			return total, net.ErrClosed
		}
		space := c.p.upPiece - c.openLen
		if space == 0 {
			c.sealLocked(time.Now())
			c.poke()
			if c.openLen > 0 {
				c.wc.Wait()
			}
			continue
		}
		n := min(space, len(p))
		if c.openLen == 0 {
			c.openAt = time.Now()
			c.poke()
		}
		c.open = exGrow(c.open, c.openLen, c.openLen+n, c.p.upPiece)
		copy(c.open.b[c.openLen:], p[:n])
		c.openLen += n
		total += n
		p = p[n:]
		if c.openLen == c.p.upPiece {
			c.sealLocked(time.Now())
			c.poke()
		}
	}
	return total, nil
}

// Close ends the uplink gracefully: the pieces written are sent, the last
// one with FIN.
func (w *exClientWriter) Close() error {
	c := w.c
	c.mu.Lock()
	c.wclosed = true
	c.wc.Broadcast()
	c.mu.Unlock()
	c.poke()
	return nil
}

// Abort fails the carrier; the server is told with a CLOSE request.
func (w *exClientWriter) Abort(err error) {
	if err == nil {
		err = net.ErrClosed
	}
	c := w.c
	c.mu.Lock()
	c.failLocked(err)
	c.mu.Unlock()
}
