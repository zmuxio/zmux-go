package zmux

import (
	"context"
	"crypto/hmac"
	crand "crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/djylb/timeshim"

	"github.com/zmuxio/zmux-go/internal/core"
)

// Admission is a decision taken on a new carrier before any byte is sent on
// it and before the Listener classifies it. It is configured on the
// Listener (ListenerConfig.Admission), and every entry point of that
// listener runs it: Serve, HandlePipe, the HTTP, exchange and upgrade
// handlers, and the QUIC and WebTransport adapters. The listener reads up
// to Peek bytes of the carrier (a stream's first bytes after its TLS
// handshake, an HTTP request's body), never more, and calls Decide with
// what it read and the carrier's metadata; the Decision says what happens
// to the carrier.
//
// The fields are read by NewListener; an Admission must not be changed or
// copied afterwards.
type Admission struct {
	// Peek is the number of bytes read before Decide (0: none). Decide
	// receives fewer when the carrier ends first.
	Peek int
	// PeekTimeout bounds the reading of the Peek bytes and the decision,
	// from the end of the carrier's outer handshake (TLS). Zero selects a
	// value drawn once per Admission, from a secure random source, between
	// 5 s and 30 s; a negative value is no timeout.
	PeekTimeout time.Duration
	// Decide returns the decision for a carrier. prefix is what was read,
	// valid only during the call. ctx ends at what remains of PeekTimeout
	// and when the listener stops taking carriers (Close, or the end of
	// Shutdown); a decision not returned by then is VerdictClose, and so
	// is a panic. It is called at most once per carrier, concurrently for
	// different carriers, outside every lock.
	Decide func(ctx context.Context, info *CarrierInfo, prefix []byte) Decision
	// StallMin and StallMax bound the duration of a VerdictStall. The
	// duration is a function of the carrier's source alone for the
	// Listener's lifetime (Listener.StallDuration). When both are zero,
	// the bounds are drawn once per Admission from a secure random source:
	// StallMax between 30 s and 120 s, StallMin between a quarter and
	// three quarters of it.
	StallMin, StallMax time.Duration

	once sync.Once
	err  error
	r    admParams // resolved
}

// admParams are the resolved bounds of an Admission.
type admParams struct {
	peekTimeout        time.Duration
	stallMin, stallMax time.Duration
}

// CarrierInfo is the metadata of a carrier under admission, or of one a
// client opens (Credential). Fields that do not apply to the carrier are
// zero.
type CarrierInfo struct {
	// Source is the carrier's client address: what a trusted intermediary
	// reports (TrustedProxies; port 0 when it reports none), what the
	// host gave as JoinHint.Source, and otherwise Peer. The listener's
	// per-source limits and the stall duration count it aggregated: an
	// IPv6 address to its /64 prefix, the port ignored.
	Source netip.AddrPort
	// Peer is the address the carrier arrived from: the remote address of
	// its connection, or an HTTP request's RemoteAddr. Zero when it is not
	// an IP address and port, such as a Unix socket.
	Peer netip.AddrPort
	// TLS is the state of the carrier's TLS (or QUIC) handshake, which has
	// completed; ExportKeyingMaterial binds data to it. For a carrier
	// decided on an HTTP request head it is the state of the connection
	// that carries the request. It is nil where TLS ended at an
	// intermediary.
	TLS *tls.ConnectionState
	// Request is the request of an HTTP carrier. Its body must not be read
	// by Decide; the bytes the listener read are the prefix.
	Request *http.Request
	// Head marks a carrier decided on its HTTP request head, before any
	// body byte is read: an HTTP upgrade such as WebSocket or
	// WebTransport, an exchange carrier, or an HTTP request carrier
	// without a body, such as a downlink GET. There is no prefix, and a
	// credential travels in a request header field.
	Head bool
}

// Verdict is the outcome of an admission.
type Verdict uint8

const (
	// VerdictAccept hands the carrier to the Listener, without the first
	// Decision.Skip bytes of the prefix.
	VerdictAccept Verdict = iota
	// VerdictFallback hands the carrier, with every byte read replayed, to
	// the fallback target of its kind (ListenerConfig.Fallback for byte
	// streams, HTTPOptions.Next for HTTP carriers, the QUIC adapter's).
	// Without one a stream carrier is stalled, and an HTTP request gets
	// the server's not-found response.
	VerdictFallback
	// VerdictStall reads and discards what arrives, sends nothing, and
	// closes the carrier gracefully when the peer ends it or after a
	// duration between StallMin and StallMax. A stalled carrier keeps its
	// source's slot (ListenerConfig.MaxPendingPerSource) until it is
	// closed; a source holds at most MaxPendingPerSource - 1 stalled
	// carriers, and one beyond that is closed at once.
	VerdictStall
	// VerdictClose closes the carrier at once.
	VerdictClose
)

// verdictFailed is the outcome of a carrier whose outer handshake failed:
// it reached no decision and is closed.
const verdictFailed Verdict = 255

// Decision is what Admission.Decide returns.
type Decision struct {
	Verdict Verdict
	// Skip is the number of leading prefix bytes that are not carrier bytes
	// (VerdictAccept only); the rest is replayed to the Listener.
	Skip int
	// Value is the host's value for an accepted carrier (VerdictAccept
	// only; ignored otherwise): zmux hands it on without looking at it to
	// WrapPipe (JoinHint.Admitted) and to the session (Conn.Admitted) or
	// pipe (Pipe.Admitted) the carrier makes, and holds it until their
	// state is released. Whether a join comes from the same client as its
	// session is the host's to decide: zmux compares no values.
	Value any
}

// AdmissionStats are the outcomes of a listener's admission (API.md 7.2),
// each carrier that took its source slot and whose outcome is known
// counted once, so that the sum of the fields is the number of such
// carriers. All are zero without an Admission.
type AdmissionStats struct {
	Accepted uint64 // VerdictAccept
	// Fallback counts VerdictFallback, also where the carrier has no
	// fallback target.
	Fallback uint64
	Stalled  uint64 // VerdictStall, counted at the decision
	// Closed counts VerdictClose, a decision that panicked, was cancelled,
	// or had a Skip outside the prefix or an unknown verdict.
	Closed uint64
	// StallOverflow counts VerdictStall closed at once: the source held
	// MaxPendingPerSource - 1 stalled carriers.
	StallOverflow uint64
	// PeekTimedOut counts carriers whose prefix or decision did not
	// arrive within PeekTimeout.
	PeekTimedOut uint64
	// HandshakeFailed counts carriers whose outer handshake failed or
	// timed out, without a decision.
	HandshakeFailed uint64
}

type admissionCounters struct {
	accepted, fallback, stalled, closed, stallOverflow, peekTimedOut, handshakeFailed atomic.Uint64
}

func (c *admissionCounters) snapshot() AdmissionStats {
	return AdmissionStats{Accepted: c.accepted.Load(), Fallback: c.fallback.Load(), Stalled: c.stalled.Load(),
		Closed: c.closed.Load(), StallOverflow: c.stallOverflow.Load(), PeekTimedOut: c.peekTimedOut.Load(),
		HandshakeFailed: c.handshakeFailed.Load()}
}

// Validate checks a (nil is valid) and resolves its bounds, once; a must
// not be copied afterwards. NewListener calls it.
func (a *Admission) Validate() error {
	if a == nil {
		return nil
	}
	a.once.Do(func() {
		switch {
		case a.Decide == nil:
			a.err = fmt.Errorf("%w: Admission.Decide is nil", ErrInvalidConfig)
		case a.Peek < 0 || a.StallMin < 0 || a.StallMax < 0:
			a.err = fmt.Errorf("%w: negative Admission field", ErrInvalidConfig)
		case a.StallMin > a.StallMax:
			a.err = fmt.Errorf("%w: Admission.StallMin above StallMax", ErrInvalidConfig)
		}
		if a.err != nil {
			return
		}
		r := admParams{peekTimeout: a.PeekTimeout, stallMin: a.StallMin, stallMax: a.StallMax}
		if r.peekTimeout == 0 {
			r.peekTimeout = drawDuration(5*time.Second, 30*time.Second)
		}
		r.peekTimeout = timeout(r.peekTimeout, 0)
		if r.stallMax == 0 {
			r.stallMax = drawDuration(30*time.Second, 120*time.Second)
			r.stallMin = drawDuration(r.stallMax/4, r.stallMax*3/4)
		}
		a.r = r
	})
	return a.err
}

// drawDuration is uniform in [lo, hi] from a secure random source.
func drawDuration(lo, hi time.Duration) time.Duration {
	var b [8]byte
	_, _ = crand.Read(b[:])
	return lo + time.Duration(binary.LittleEndian.Uint64(b[:])%uint64(hi-lo+1))
}

// sourceKey is the one aggregation of a source for the listener's
// per-source state (MULTIPATH.md 5.6): an IPv6 address to its /64, an
// IPv4 address itself, the port ignored; "" for an invalid address.
func sourceKey(src netip.AddrPort) string {
	if !src.IsValid() {
		return ""
	}
	return core.SourceKey(src.Addr().String())
}

// addrPortOf is a's address and port, an IPv4-mapped address as IPv4;
// zero when it has none (a Unix socket, a pipe).
func addrPortOf(a net.Addr) netip.AddrPort {
	var ap netip.AddrPort
	switch t := a.(type) {
	case nil:
	case *net.TCPAddr:
		ap = t.AddrPort()
	case *net.UDPAddr:
		ap = t.AddrPort()
	default:
		ap, _ = netip.ParseAddrPort(a.String())
	}
	return unmapped(ap)
}

// unmapped is ap with an IPv4-mapped address as IPv4, without a zone.
func unmapped(ap netip.AddrPort) netip.AddrPort {
	if !ap.IsValid() {
		return ap
	}
	return netip.AddrPortFrom(ap.Addr().Unmap().WithZone(""), ap.Port())
}

// peerOf is the remote address of p's halves, where one has one.
func peerOf(p PipeIO) netip.AddrPort {
	for _, x := range []any{p.R, p.W} {
		if ra, ok := x.(interface{ RemoteAddr() net.Addr }); ok {
			if ap := addrPortOf(ra.RemoteAddr()); ap.IsValid() {
				return ap
			}
		}
	}
	return netip.AddrPort{}
}

// StallDuration is the duration of a VerdictStall of l's admission for a
// carrier from source: in [StallMin, StallMax], and the same for every
// carrier from one source (aggregated as CarrierInfo.Source says) during
// l's lifetime. It is HMAC-SHA-256, under a key NewListener drew, of the
// source, reduced to the interval; no state is kept per source. It is 0
// without an Admission.
func (l *Listener) StallDuration(source netip.AddrPort) time.Duration {
	if l.cfg.Admission == nil {
		return 0
	}
	return l.stallDuration(sourceKey(source))
}

func (l *Listener) stallDuration(key string) time.Duration {
	a := l.cfg.Admission
	lo, hi := a.r.stallMin, a.r.stallMax
	if hi <= lo {
		return hi
	}
	m := hmac.New(sha256.New, l.stallKey[:])
	m.Write([]byte(key))
	v := binary.BigEndian.Uint64(m.Sum(nil))
	return lo + time.Duration(v%uint64(hi-lo+1))
}

// ---- arrivals ----

// Arrival is a carrier that an entry point of a Listener took: it holds a
// slot of its source (ListenerConfig.MaxPendingPerSource) from its
// arrival, before any handshake, through the admission and until the
// listener classifies the carrier. Listener.Serve, HandlePipe and the
// HTTP handlers make their own; an adapter with its own accept loop takes
// one with Arrive (or AdmitUpgrade for an HTTP upgrade) as soon as it
// accepts a connection, and hands its carriers on with HandlePipe.
type Arrival struct {
	l    *Listener
	src  netip.AddrPort
	peer netip.AddrPort
	key  string

	mu       sync.Mutex
	held     bool // the source slot Arrive took is held and not yet handed on
	accepted bool // the admission accepted the carrier
	value    any  // Decision.Value of that acceptance
}

// Arrive takes a slot of source for a carrier that arrived from peer
// (source zero: peer). A source without a free slot, or a closed listener
// (net.ErrClosed), refuses it (ListenerStats.Refused): the caller closes
// the carrier without a handshake.
func (l *Listener) Arrive(source, peer netip.AddrPort) (*Arrival, error) {
	source, peer = unmapped(source), unmapped(peer)
	if !source.IsValid() {
		source = peer
	}
	key := sourceKey(source)
	l.mu.Lock()
	closed := l.closed
	ok := !closed && l.pending.Enter(key)
	l.mu.Unlock()
	if !ok {
		l.st.refused.Add(1)
		if closed {
			return nil, net.ErrClosed
		}
		return nil, errRefused
	}
	return &Arrival{l: l, src: source, peer: peer, key: key, held: true}, nil
}

// Release gives back the arrival's source slot when no carrier of it is
// handed on (the connection ended first). It may be called more than
// once, also after HandlePipe.
func (a *Arrival) Release() {
	a.mu.Lock()
	held := a.held
	a.held = false
	a.mu.Unlock()
	if held {
		a.l.leave(a.key)
	}
}

// HandshakeFailed records that the arrival's outer handshake (an HTTP
// upgrade, a QUIC handshake) failed or timed out, and releases it: with
// an Admission it counts as AdmissionStats.HandshakeFailed, otherwise as
// a setup failure.
func (a *Arrival) HandshakeFailed() {
	if a.l.cfg.Admission != nil {
		a.l.st.adm.handshakeFailed.Add(1)
	} else {
		a.l.CountSetupFailure()
	}
	a.Release()
}

func (l *Listener) leave(key string) {
	l.mu.Lock()
	l.pending.Leave(key)
	l.mu.Unlock()
}

// takeSlot hands the arrival's source slot on to a carrier: the one Arrive
// took for the first, a new one for each further carrier of an accepted
// arrival (a QUIC connection's further streams).
func (a *Arrival) takeSlot() bool {
	a.mu.Lock()
	held := a.held
	a.held = false
	a.mu.Unlock()
	if held {
		return true
	}
	l := a.l
	l.mu.Lock()
	ok := !l.closed && l.pending.Enter(a.key)
	l.mu.Unlock()
	return ok
}

// Admitted reports whether the arrival's carriers are handed on without
// an admission: the listener has none, or it accepted the arrival.
func (a *Arrival) Admitted() bool { return a.isAccepted() }

func (a *Arrival) isAccepted() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.accepted || a.l.cfg.Admission == nil
}

func (a *Arrival) accept() {
	a.mu.Lock()
	a.accepted = true
	a.mu.Unlock()
}

// HandlePipe hands a carrier of the arrival to the listener and blocks for
// the lifetime of its pipe, as Listener.HandlePipe does. The first carrier
// passes the listener's admission (unless AdmitUpgrade decided it
// already): its outer TLS handshake is completed, its prefix read and the
// decision taken, and VerdictFallback hands a net.Conn carrier to
// ListenerConfig.Fallback. Once the arrival is accepted, further
// carriers of it (a QUIC connection's further streams) are handed on
// without an admission, each taking a slot of the source. hint.Source is
// ignored: the arrival's source applies.
func (a *Arrival) HandlePipe(ctx context.Context, p PipeIO, hint JoinHint) error {
	l := a.l
	if err := p.checkOverhead(); err != nil {
		a.Release()
		closePipeIO(p)
		l.CountSetupFailure()
		return err
	}
	if !a.isAccepted() {
		q, prefix, v := a.admitPipe(ctx, p)
		switch v {
		case VerdictAccept:
			p = q
			a.accept()
		case VerdictFallback:
			if nc, ok := p.R.(net.Conn); ok && any(p.W) == any(p.R) && l.cfg.Fallback != nil {
				a.Release()
				l.cfg.Fallback(ctx, PrefixedConn(nc, prefix))
				return errRejected
			}
			a.stallPipe(ctx, p, false)
			return errRejected
		default:
			return errRejected
		}
	}
	if !a.takeSlot() {
		closePipeIO(p)
		l.st.refused.Add(1)
		return errRefused
	}
	hint.Source = a.src
	a.mu.Lock()
	hint.Admitted = a.value
	a.mu.Unlock()
	run, err := l.take(p, hint, a.key)
	if err != nil {
		return err
	}
	return run(ctx)
}

// Admit runs the listener's admission on p, the first carrier of the
// arrival, for an adapter with a fallback target of its own: on
// VerdictAccept it returns p without the skipped bytes and true, to be
// handed on with HandlePipe. Otherwise it has acted on the verdict and
// returns false: VerdictFallback hands p, with every byte read replayed,
// to fallback (nil: the carrier is stalled), VerdictStall stalls it and
// VerdictClose closes it. Without an Admission it returns p and true.
func (a *Arrival) Admit(ctx context.Context, p PipeIO, fallback func(context.Context, PipeIO)) (PipeIO, bool) {
	if a.isAccepted() {
		return p, true
	}
	q, prefix, v := a.admitPipe(ctx, p)
	switch v {
	case VerdictAccept:
		a.accept()
		return q, true
	case VerdictFallback:
		if fallback != nil {
			a.Release()
			fallback(ctx, PrefixedPipe(p, prefix))
		} else {
			a.stallPipe(ctx, p, false)
		}
	}
	return PipeIO{}, false
}

// stopContext is ctx, also ended when the listener stops taking carriers.
func (l *Listener) stopContext(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(l.stopCtx, cancel)
	return ctx, func() { stop(); cancel() }
}

// admitPipe completes p's outer TLS handshake, reads its prefix and takes
// the decision. On VerdictAccept q is p without the skipped bytes; on
// VerdictFallback p is untouched and prefix what was read; otherwise p is
// closed (VerdictStall: after nothing, the caller stalls only a fallback
// without target; the decision's own stall has run).
func (a *Arrival) admitPipe(ctx context.Context, p PipeIO) (q PipeIO, prefix []byte, v Verdict) {
	l := a.l
	adm := l.cfg.Admission
	ctx, cancel := l.stopContext(ctx)
	defer cancel()
	if hs := tlsPending(p); hs != nil {
		hctx, hcancel := timeshim.WithTimeout(ctx, l.clk, l.cfg.Config.handshakeTimeout())
		err := hs.HandshakeContext(hctx)
		hcancel()
		if err != nil {
			closePipeIO(p)
			l.st.adm.handshakeFailed.Add(1)
			a.Release()
			return PipeIO{}, nil, verdictFailed
		}
	}
	deadline := l.clk.Now().Add(adm.r.peekTimeout)
	info := &CarrierInfo{Source: a.src, Peer: a.peer}
	if cs, ok := carrierIsTLS(p); ok {
		info.TLS = &cs
	}
	if adm.Peek > 0 && p.R != nil && p.Framing == FramingByteStream {
		var fired bool
		prefix, fired = a.peekPipe(p, adm.Peek, deadline)
		if fired {
			closePipeIO(p)
			l.st.adm.peekTimedOut.Add(1)
			a.Release()
			return PipeIO{}, nil, VerdictClose
		}
	}
	d := a.decide(ctx, info, prefix, deadline)
	switch d.Verdict {
	case VerdictAccept:
		return withPrefix(p, prefix[d.Skip:]), nil, VerdictAccept
	case VerdictFallback:
		return p, prefix, VerdictFallback
	case VerdictStall:
		a.stallPipe(ctx, p, true)
		return PipeIO{}, nil, VerdictStall
	}
	closePipeIO(p)
	a.Release()
	return PipeIO{}, nil, VerdictClose
}

// tlsPending is p's TLS connection whose handshake has not completed.
func tlsPending(p PipeIO) *tls.Conn {
	for _, x := range []any{p.R, p.W} {
		if tc, ok := x.(*tls.Conn); ok && !tc.ConnectionState().HandshakeComplete {
			return tc
		}
	}
	return nil
}

// peekPipe reads up to n bytes of p by deadline; fired: the deadline
// passed first (the carrier may be closed).
func (a *Arrival) peekPipe(p PipeIO, n int, deadline time.Time) (prefix []byte, fired bool) {
	var once atomic.Bool
	t := a.l.clk.AfterFunc(deadline.Sub(a.l.clk.Now()), func() {
		once.Store(true)
		interruptRead(p)
	})
	prefix = peek(p.R, n)
	if !t.Stop() && once.Load() {
		return nil, true
	}
	return prefix, false
}

// interruptRead ends a blocked read of p: a passed read deadline where
// the receiving half takes one, a close otherwise.
func interruptRead(p PipeIO) {
	if d, ok := p.R.(interface{ SetReadDeadline(time.Time) error }); ok && d.SetReadDeadline(time.Now()) == nil {
		return
	}
	closePipeIO(p)
}

// decide calls Decide with ctx ending at deadline, outside every lock,
// bounds the result and counts the outcome (a stall is counted by
// stallSlot). A decision not returned in time is VerdictClose.
func (a *Arrival) decide(ctx context.Context, info *CarrierInfo, prefix []byte, deadline time.Time) Decision {
	l := a.l
	dctx, cancel := timeshim.WithDeadline(ctx, l.clk, deadline)
	defer cancel()
	res := make(chan Decision, 1)
	go func() {
		d := Decision{Verdict: VerdictClose}
		defer func() {
			_ = recover() // a panicking decision is VerdictClose
			res <- d
		}()
		d = l.cfg.Admission.Decide(dctx, info, prefix)
	}()
	var d Decision
	select {
	case d = <-res:
	case <-dctx.Done():
	}
	// A decision that returns after ctx or the deadline ended is not
	// used, however the select above chose.
	if dctx.Err() != nil {
		if errors.Is(dctx.Err(), context.DeadlineExceeded) {
			l.st.adm.peekTimedOut.Add(1)
		} else {
			l.st.adm.closed.Add(1)
		}
		return Decision{Verdict: VerdictClose}
	}
	switch {
	case d.Verdict > VerdictClose:
		d = Decision{Verdict: VerdictClose}
	case d.Verdict != VerdictAccept:
		d.Skip, d.Value = 0, nil
	case d.Skip < 0 || d.Skip > len(prefix):
		d = Decision{Verdict: VerdictClose}
	}
	switch d.Verdict {
	case VerdictAccept:
		a.mu.Lock()
		a.value = d.Value
		a.mu.Unlock()
		l.st.adm.accepted.Add(1)
	case VerdictFallback:
		l.st.adm.fallback.Add(1)
	case VerdictClose:
		l.st.adm.closed.Add(1)
	}
	return d
}

// stallSlot counts a stalled carrier of the arrival; ok false: the source
// holds its share of stalled carriers (StallOverflow), close it at once.
func (a *Arrival) stallSlot(counted bool) (d time.Duration, release func(), ok bool) {
	l := a.l
	l.mu.Lock()
	ok = !l.closed && l.pending.Stall(a.key)
	l.mu.Unlock()
	if counted {
		if ok {
			l.st.adm.stalled.Add(1)
		} else {
			l.st.adm.stallOverflow.Add(1)
		}
	}
	if !ok {
		return 0, nil, false
	}
	return l.stallDuration(a.key), func() {
		l.mu.Lock()
		l.pending.Unstall(a.key)
		l.mu.Unlock()
	}, true
}

// stallPipe stalls p: it discards what arrives until the peer ends it or
// the stall duration passes on the listener's clock, then closes it
// gracefully; the arrival is released at the end.
// counted: the decision was VerdictStall, not a fallback without target.
func (a *Arrival) stallPipe(ctx context.Context, p PipeIO, counted bool) {
	defer a.Release()
	d, release, ok := a.stallSlot(counted)
	if !ok || p.R == nil {
		closePipeIO(p)
		if ok {
			release()
		}
		return
	}
	defer release()
	t := a.l.clk.AfterFunc(d, func() { interruptRead(p) })
	stop := context.AfterFunc(ctx, func() { interruptRead(p) })
	discard(p.R)
	stop()
	t.Stop()
	if cw, ok := p.W.(closeWriter); ok {
		_ = cw.CloseWrite()
	}
	closePipeIO(p)
}

// peek reads up to n bytes from r into a buffer of n bytes; it stops early
// at the end of r or an error (an interrupted read).
func peek(r io.Reader, n int) []byte {
	if n == 0 {
		return nil
	}
	b := make([]byte, n)
	got, _ := io.ReadFull(r, b)
	return b[:got]
}

// stallBuf is the discard buffer of stalled carriers.
var stallBuf = sync.Pool{New: func() any { return new([512]byte) }}

// discard reads r until it ends or fails.
func discard(r io.Reader) {
	b := stallBuf.Get().(*[512]byte)
	for {
		if _, err := r.Read(b[:]); err != nil {
			break
		}
	}
	stallBuf.Put(b)
}

// PrefixedPipe returns p with prefix read before the rest of its receiving
// half; the sending half, and the carrier capabilities zmux discovers on it,
// are unchanged. It is how bytes read before HandlePipe are given back.
func PrefixedPipe(p PipeIO, prefix []byte) PipeIO { return withPrefix(p, prefix) }

// PrefixedConn returns c with prefix read before the rest of c. Once the
// prefix is consumed, WriteTo and ReadFrom use c's own (splice and
// sendfile where c offers them), and Unwrap returns c.
func PrefixedConn(c net.Conn, prefix []byte) net.Conn {
	if len(prefix) == 0 {
		return c
	}
	return &prefixedConn{Conn: c, pre: prefix}
}

type prefixedConn struct {
	net.Conn
	pre []byte
}

func (p *prefixedConn) Read(b []byte) (int, error) {
	if len(p.pre) > 0 {
		n := copy(b, p.pre)
		p.pre = p.pre[n:]
		return n, nil
	}
	return p.Conn.Read(b)
}

// WriteTo writes the prefix, then copies c to w through w's ReadFrom.
func (p *prefixedConn) WriteTo(w io.Writer) (int64, error) {
	var n int64
	if len(p.pre) > 0 {
		m, err := w.Write(p.pre)
		n += int64(m)
		p.pre = p.pre[m:]
		if err != nil {
			return n, err
		}
	}
	m, err := io.Copy(w, p.Conn)
	return n + m, err
}

// ReadFrom writes r to c through c's ReadFrom where it has one.
func (p *prefixedConn) ReadFrom(r io.Reader) (int64, error) {
	if rf, ok := p.Conn.(io.ReaderFrom); ok {
		return rf.ReadFrom(r)
	}
	return io.Copy(struct{ io.Writer }{p.Conn}, r)
}

// CloseWrite half-closes c where it can.
func (p *prefixedConn) CloseWrite() error {
	if cw, ok := p.Conn.(interface{ CloseWrite() error }); ok {
		return cw.CloseWrite()
	}
	return errors.ErrUnsupported
}

// Unwrap returns the connection under the prefix.
func (p *prefixedConn) Unwrap() net.Conn { return p.Conn }

// ---- HTTP carriers ----

// HTTPOptions configure the HTTP adapters (NewHTTPHandler, the exchange
// handler, and HTTP upgrades such as WebSocket and WebTransport); the
// admission they run is their listener's (ListenerConfig.Admission).
type HTTPOptions struct {
	// Next serves the requests that are not carriers: VerdictFallback, and
	// requests the adapter does not take (nil: http.NotFound).
	// An HTTP/1.1 request with a body reaches it full duplex
	// (http.ResponseController.EnableFullDuplex), so its response can go
	// out while the client still streams the body.
	Next http.Handler
	// Trusted names the intermediaries whose forwarding header field gives
	// a request's source (nil: the request's peer is its source). Without
	// it zmux takes r.RemoteAddr as it finds it, so a host that resolves
	// the source itself rewrites r.RemoteAddr before the handler runs and
	// leaves Trusted nil.
	Trusted *TrustedProxies
}

// Validate checks o (nil is valid); the HTTP adapters call it when they
// are set up.
func (o *HTTPOptions) Validate() error {
	if o == nil {
		return nil
	}
	return o.Trusted.validate()
}

// trusted returns o.Trusted; nil for a nil o.
func (o *HTTPOptions) trusted() *TrustedProxies {
	if o == nil {
		return nil
	}
	return o.Trusted
}

// Fallback serves r with Next (http.NotFound without one). An HTTP/1.1
// request with a body reaches Next full duplex (see serveFallback).
func (o *HTTPOptions) Fallback(w http.ResponseWriter, r *http.Request) {
	var next http.Handler
	if o != nil {
		next = o.Next
	}
	serveFallback(next, w, r)
}

// serveFallback serves r with next (http.NotFound when nil). An HTTP/1.1
// request with a body is switched to full duplex first: net/http would
// otherwise read the rest of the body before the response header goes
// out, and a streaming body that the client keeps open until it sees a
// response would stall both sides. The error of a writer that cannot
// switch is ignored.
func serveFallback(next http.Handler, w http.ResponseWriter, r *http.Request) {
	if r.ProtoMajor == 1 && r.ContentLength != 0 && r.Body != nil && r.Body != http.NoBody {
		_ = http.NewResponseController(w).EnableFullDuplex()
	}
	if next == nil {
		http.NotFound(w, r)
		return
	}
	next.ServeHTTP(w, r)
}

// AdmitUpgrade takes r, the request of an HTTP upgrade (WebSocket,
// WebTransport), as a carrier of l and runs l's admission on its head,
// before anything is written to w: the decision gets no prefix,
// CarrierInfo.Head is set, and the body is not read. Its source is what
// o.Trusted resolves. A request that an intermediary forwarded from TLS
// early data (Early-Data: 1, RFC 8470), or read before the TLS handshake
// completed (0-RTT on HTTP/3), is answered 425 (Too Early) first and is
// no carrier. On VerdictAccept (or without an Admission) it returns the
// arrival and true: the caller completes the upgrade and hands the
// carrier on with Arrival.HandlePipe, or calls Arrival.HandshakeFailed.
// Otherwise it has answered r (VerdictFallback, or a carrier the listener
// refuses, with o.Next) or ended it (VerdictStall, VerdictClose, which
// abort the request with http.ErrAbortHandler), and the caller returns.
func (l *Listener) AdmitUpgrade(w http.ResponseWriter, r *http.Request, o *HTTPOptions) (*Arrival, bool) {
	return l.admitRequest(w, r, o, false)
}

// earlyRequest reports whether r may have been received in TLS early
// data: an intermediary forwarded it from early data (Early-Data: 1, RFC
// 8470), or the server read it on a connection whose handshake has not
// completed, as an HTTP/3 server listening for 0-RTT does.
func earlyRequest(r *http.Request) bool {
	if r.TLS != nil && !r.TLS.HandshakeComplete {
		return true
	}
	for _, v := range r.Header.Values("Early-Data") {
		if strings.TrimSpace(v) == "1" {
			return true
		}
	}
	return false
}

// admitRequest is AdmitUpgrade, and with body set the admission of a
// request carrier: its prefix is the start of the body (none for a
// request without one, decided on its head), and on VerdictAccept r.Body
// continues after the skipped bytes.
func (l *Listener) admitRequest(w http.ResponseWriter, r *http.Request, o *HTTPOptions, body bool) (*Arrival, bool) {
	if earlyRequest(r) {
		w.WriteHeader(http.StatusTooEarly)
		l.CountSetupFailure() // it needs the handshake: no pipe input
		return nil, false
	}
	a, err := l.Arrive(requestSource(o.trusted(), r))
	if err != nil {
		// Refused (its source holds its share, or the listener is
		// closed): nothing was read or written, the host serves it.
		o.Fallback(w, r)
		return nil, false
	}
	if !a.decideRequest(w, r, body, func() { o.Fallback(w, r) }) {
		return nil, false
	}
	return a, true
}

// decideRequest runs the admission of the arrival of r (see
// admitRequest); fallback serves r on VerdictFallback, r.Body replaying
// the bytes read. It returns true on VerdictAccept, or without an
// Admission.
func (a *Arrival) decideRequest(w http.ResponseWriter, r *http.Request, body bool, fallback func()) bool {
	l := a.l
	adm := l.cfg.Admission
	if adm == nil {
		return true
	}
	ctx, cancel := l.stopContext(r.Context())
	defer cancel()
	deadline := l.clk.Now().Add(adm.r.peekTimeout)
	// A request without a body is decided on its head: one known to have
	// none, or one whose body ends before its first byte.
	head := !body || r.Body == nil || r.Body == http.NoBody
	var prefix []byte
	if !head && adm.Peek > 0 {
		rc := http.NewResponseController(w)
		var fired atomic.Bool
		t := l.clk.AfterFunc(deadline.Sub(l.clk.Now()), func() {
			fired.Store(true)
			_ = rc.SetReadDeadline(time.Now())
		})
		b := make([]byte, adm.Peek)
		n, err := io.ReadFull(r.Body, b)
		if !t.Stop() && fired.Load() {
			l.st.adm.peekTimedOut.Add(1)
			a.Release()
			panic(http.ErrAbortHandler)
		}
		prefix, head = b[:n], n == 0 && err == io.EOF
	}
	info := &CarrierInfo{Source: a.src, Peer: a.peer, TLS: r.TLS, Request: r, Head: head}
	d := a.decide(ctx, info, prefix, deadline)
	switch d.Verdict {
	case VerdictAccept:
		if rest := prefix[d.Skip:]; len(rest) > 0 {
			r.Body = &replayBody{Reader: io.MultiReader(bytesReader(rest), r.Body), c: r.Body}
		}
		a.accept()
		return true
	case VerdictFallback:
		if len(prefix) > 0 {
			r.Body = &replayBody{Reader: io.MultiReader(bytesReader(prefix), r.Body), c: r.Body}
		}
		a.Release()
		fallback()
	case VerdictStall:
		if d, release, ok := a.stallSlot(true); ok {
			t := timeshim.NewTimer(l.clk, d)
			select {
			case <-t.C:
			case <-ctx.Done():
			}
			t.Stop()
			release()
		}
		a.Release()
		panic(http.ErrAbortHandler)
	default:
		a.Release()
		panic(http.ErrAbortHandler)
	}
	return false
}

// replayBody is a request body with bytes read earlier put back in front.
type replayBody struct {
	io.Reader
	c io.Closer
}

func (b *replayBody) Close() error { return b.c.Close() }

type sliceReader struct{ b []byte }

func (s *sliceReader) Read(p []byte) (int, error) {
	if len(s.b) == 0 {
		return 0, io.EOF
	}
	n := copy(p, s.b)
	s.b = s.b[n:]
	return n, nil
}

func bytesReader(b []byte) io.Reader { return &sliceReader{b: b} }

// errReader returns its error from every Read.
type errReader struct{ err error }

func (e errReader) Read([]byte) (int, error) { return 0, e.err }
