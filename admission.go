package zmux

import (
	"bytes"
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
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/djylb/timeshim"

	"github.com/zmuxio/zmux-go/internal/core"
	"github.com/zmuxio/zmux-go/internal/httpx"
	"github.com/zmuxio/zmux-go/internal/labels"
	"github.com/zmuxio/zmux-go/internal/setting"
)

// Admission is a decision taken on a new carrier before any byte is sent on
// it and before the Listener classifies it. It is configured on the
// Listener (ListenerConfig.Admission), and every entry point of that
// listener runs it: Serve, HandlePipe, the HTTP, exchange and upgrade
// handlers, and the QUIC and WebTransport adapters. The listener reads up
// to Peek bytes of the carrier (a stream's first bytes after its TLS
// handshake, an HTTP request's body), never more, and calls Decide with
// what it read and the carrier's metadata; the Decision says what happens
// to the carrier. On a message-framed carrier (FramingRecords, such as
// WebSocket) the prefix is the carrier's first message, whole. Decide may
// ask for more of a byte stream (VerdictMore, up to MaxPeek bytes), as a
// host that tells several kinds of carrier apart by their first bytes
// does (admit.ClassifyFirstBytes).
//
// The fields are read by NewListener; an Admission must not be changed or
// copied afterwards.
type Admission struct {
	// Decide returns the decision for a carrier. prefix is what was read,
	// valid only during the call. ctx ends at what remains of PeekTimeout
	// and when the listener stops taking carriers (Close, or the end of
	// Shutdown); a decision not returned by then is VerdictClose, and so
	// is a panic. It is called once per carrier and once more after each
	// VerdictMore, concurrently for different carriers, outside every
	// lock.
	Decide func(ctx context.Context, info *CarrierInfo, prefix []byte) Decision
	// StallMin and StallMax bound the duration of a VerdictStall. The
	// duration is a function of the carrier's source alone for the
	// Listener's lifetime (Listener.StallDuration). When both are zero,
	// the bounds are drawn once per Admission from a secure random source:
	// StallMax between 30 s and 120 s, StallMin between a quarter and
	// three quarters of it.
	StallMin, StallMax time.Duration
	// Peek is the number of bytes read before Decide (0: none). Decide
	// receives fewer when the carrier ends first. On a message-framed
	// carrier any Peek above 0 reads the first message, of at most
	// max(Peek, MaxPeek) bytes; a longer one is VerdictClose.
	Peek int
	// MaxPeek bounds Decision.Need, the prefix that VerdictMore may ask
	// for, and a first message (0: Peek, so that VerdictMore is
	// VerdictClose). It is not below Peek, or ErrInvalidConfig.
	MaxPeek int
	// PeekTimeout bounds the reading of the prefix, every VerdictMore
	// round included, and the decisions, from the end of the carrier's
	// outer handshake (TLS). Zero selects a
	// value drawn once per Admission, from a secure random source, between
	// 5 s and 30 s; a negative value is no timeout.
	PeekTimeout time.Duration
	// FallbackOnPeekTimeout makes a byte-stream carrier whose prefix does
	// not arrive within PeekTimeout, in the first read or in a
	// VerdictMore round, take VerdictFallback with every byte read so far
	// replayed (without a fallback target it is stalled, as for any
	// fallback). It needs a receiving half that continues after its read
	// deadline passed (a net.Conn, a TLS connection); a carrier that does
	// not, a message-framed one and an HTTP request carrier are closed on
	// the timeout as when it is false. A Decide that does not return in
	// time is VerdictClose, and a failed outer handshake is not affected.
	FallbackOnPeekTimeout bool

	once sync.Once // before err: it fills the padding after the bool
	err  error
	r    admParams // resolved
}

// admParams are the resolved bounds of an Admission.
type admParams struct {
	maxPeek            int // max(Peek, MaxPeek)
	peekTimeout        time.Duration
	stallMin, stallMax time.Duration
}

// CarrierInfo is the metadata of a carrier under admission, or of one a
// client opens (CarrierCredential). Fields that do not apply to the carrier are
// zero.
type CarrierInfo struct {
	// TLS is the state of the carrier's TLS (or QUIC) handshake, which has
	// completed; ExportKeyingMaterial binds data to it. For a carrier
	// decided on an HTTP request head it is the state of the connection
	// that carries the request. It is nil where TLS ended at an
	// intermediary.
	TLS *tls.ConnectionState
	// Request is the request of an HTTP carrier. Its body must not be read
	// by Decide; the bytes the listener read are the prefix.
	Request *http.Request
	// Source is the carrier's client address: what a trusted intermediary
	// reports (TrustedProxies; port 0 when it reports none), what the
	// host gave as PipeHint.Source, and otherwise Peer. The listener's
	// per-source limits and the stall duration count it aggregated to
	// its source prefix (ListenerConfig.SourcePrefixV4, SourcePrefixV6),
	// the port ignored.
	Source netip.AddrPort
	// Peer is the address the carrier arrived from: the remote address of
	// its connection, or an HTTP request's RemoteAddr. Zero when it is not
	// an IP address and port, such as a Unix socket.
	Peer netip.AddrPort
	// Head marks a carrier decided on its HTTP request head, before any
	// body byte is read: an HTTP upgrade such as WebSocket or
	// WebTransport, an exchange carrier, or an HTTP request carrier
	// without a body, such as a downlink GET. There is no prefix, and a
	// credential travels in a request header field.
	Head bool
}

// AdmissionVerdict is the outcome of an admission.
type AdmissionVerdict uint8

const (
	// VerdictAccept hands the carrier to the Listener, without the first
	// Decision.Skip bytes of the prefix.
	VerdictAccept AdmissionVerdict = iota
	// VerdictFallback hands the carrier, with every byte read replayed, to
	// the fallback target of its kind (ListenerConfig.Fallback for byte
	// streams, zmuxhttp.Options.Next for HTTP carriers, the QUIC adapter's).
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
	// VerdictMore asks for a longer prefix of a byte stream: the listener
	// reads until it has Decision.Need bytes, the carrier ends or
	// PeekTimeout passes, and calls Decide again with the longer prefix.
	// Need must exceed the prefix and not exceed MaxPeek, and VerdictMore
	// after a carrier ended short of Need, on a message-framed carrier or
	// on one decided on its request head is VerdictClose.
	VerdictMore
)

// verdictFailed is the outcome of a carrier whose outer handshake failed:
// it reached no decision and is closed.
const verdictFailed AdmissionVerdict = 255

// Decision is what Admission.Decide returns.
type Decision struct {
	// Value is the host's value for an accepted carrier (VerdictAccept
	// only; ignored otherwise): zmux hands it on without looking at it to
	// WrapPipe (PipeHint.Admitted) and to the session (AdmittedAs) or
	// pipe (AdmittedAs) the carrier makes, and holds it until their
	// state is released. Whether a join comes from the same client as its
	// session is the host's to decide: zmux compares no values.
	Value any
	// Variant labels the carrier with any verdict, for
	// Listener.VariantStats; on VerdictAccept it is the PipeIO.Variant
	// handed to WrapPipe (which may change it). "" for none. Use one of a
	// few constants of the host, never a value derived from the peer's
	// bytes: at most MaxAdmissionVariants are counted apart.
	Variant string
	// Skip is the number of leading prefix bytes that are not carrier bytes
	// (VerdictAccept only); the rest is replayed to the Listener.
	Skip int
	// Need is, with VerdictMore, the total prefix length Decide needs.
	Need    int
	Verdict AdmissionVerdict
}

// VariantStat are the admission outcomes of one Decision.Variant
// (Listener.VariantStats).
type VariantStat struct {
	// Name is the Decision.Variant ("" for none; also "" for the
	// Overflow bucket).
	Name string
	// Overflow marks the bucket of the variants past MaxAdmissionVariants.
	Overflow bool

	Accepted, Fallback uint64
	// Stalled counts VerdictStall decisions, StallOverflow included.
	Stalled uint64
	Closed  uint64
}

// MaxAdmissionVariants bounds the distinct Decision.Variant values a
// listener counts apart; the outcomes of further values are counted in one
// VariantStat with Overflow set. Variants are meant to be a few constants
// of the host, never derived from what the peer sent.
const MaxAdmissionVariants = 64

// AdmissionStats are the outcomes of a listener's admission,
// each carrier that took its source slot and whose outcome is known
// counted once, so that the sum of the fields is the number of such
// carriers, except that a carrier Admission.FallbackOnPeekTimeout takes to
// fallback is counted in both PeekTimedOut and Fallback. All are zero
// without an Admission.
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
	// arrive within PeekTimeout. A carrier that Admission.FallbackOnPeekTimeout
	// takes to fallback is counted both here and in Fallback: the two
	// overlap for it.
	PeekTimedOut uint64
	// HandshakeFailed counts carriers whose outer handshake failed or
	// timed out, without a decision.
	HandshakeFailed uint64
}

type admissionCounters struct {
	accepted, fallback, stalled, closed, stallOverflow, peekTimedOut, handshakeFailed atomic.Uint64

	variants map[string]*VariantStat
	overflow *VariantStat // the variants past MaxAdmissionVariants
	mu       sync.Mutex
}

// variant counts a final verdict of variant v.
func (c *admissionCounters) variant(v string, verdict AdmissionVerdict) {
	c.mu.Lock()
	defer c.mu.Unlock()
	vs := c.variants[v]
	switch {
	case vs != nil:
	case len(c.variants) < MaxAdmissionVariants:
		if c.variants == nil {
			c.variants = map[string]*VariantStat{}
		}
		vs = &VariantStat{Name: v}
		c.variants[v] = vs
	default:
		if c.overflow == nil {
			c.overflow = &VariantStat{Overflow: true}
		}
		vs = c.overflow
	}
	switch verdict {
	case VerdictAccept:
		vs.Accepted++
	case VerdictFallback:
		vs.Fallback++
	case VerdictStall:
		vs.Stalled++
	default:
		vs.Closed++
	}
}

func (c *admissionCounters) snapshot() AdmissionStats {
	return AdmissionStats{Accepted: c.accepted.Load(), Fallback: c.fallback.Load(), Stalled: c.stalled.Load(),
		Closed: c.closed.Load(), StallOverflow: c.stallOverflow.Load(), PeekTimedOut: c.peekTimedOut.Load(),
		HandshakeFailed: c.handshakeFailed.Load()}
}

// variantStats copies the per-variant counts, sorted by Name, the Overflow
// bucket last.
func (c *admissionCounters) variantStats() []VariantStat {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.variants) == 0 {
		return nil
	}
	out := make([]VariantStat, 0, len(c.variants)+1)
	for _, v := range c.variants {
		out = append(out, *v)
	}
	slices.SortFunc(out, func(a, b VariantStat) int { return strings.Compare(a.Name, b.Name) })
	if c.overflow != nil {
		out = append(out, *c.overflow)
	}
	return out
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
		case a.Peek < 0 || a.MaxPeek < 0 || a.StallMin < 0 || a.StallMax < 0:
			a.err = fmt.Errorf("%w: negative Admission field", ErrInvalidConfig)
		case a.MaxPeek != 0 && a.MaxPeek < a.Peek:
			a.err = fmt.Errorf("%w: Admission.MaxPeek below Peek", ErrInvalidConfig)
		case a.StallMin > a.StallMax:
			a.err = fmt.Errorf("%w: Admission.StallMin above StallMax", ErrInvalidConfig)
		}
		if a.err != nil {
			return
		}
		r := admParams{maxPeek: max(a.Peek, a.MaxPeek), peekTimeout: a.PeekTimeout, stallMin: a.StallMin, stallMax: a.StallMax}
		if r.peekTimeout == 0 {
			r.peekTimeout = drawDuration(5*time.Second, 30*time.Second)
		}
		r.peekTimeout = setting.Timeout(r.peekTimeout, 0)
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
// per-source state (the source slots, the count of stalled carriers, the
// stall duration): the address, an IPv4-mapped one as IPv4, to its source
// prefix (ListenerConfig.SourcePrefixV4, SourcePrefixV6), the port and the
// zone ignored; the zero Prefix for an invalid address. Nothing else
// aggregates a source.
func (l *Listener) sourceKey(src netip.AddrPort) netip.Prefix {
	if !src.IsValid() {
		return netip.Prefix{}
	}
	a := src.Addr().Unmap().WithZone("")
	bits := l.cfg.SourcePrefixV6
	if a.Is4() {
		bits = l.cfg.SourcePrefixV4
	}
	p, _ := a.Prefix(bits) // bits is in range (resolve)
	return p
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
func (a *ListenerAdapter) StallDuration(source netip.AddrPort) time.Duration {
	l := (*Listener)(a)
	if l.cfg.Admission == nil {
		return 0
	}
	return l.stallDuration(l.sourceKey(source))
}

func (l *Listener) stallDuration(key netip.Prefix) time.Duration {
	a := l.cfg.Admission
	lo, hi := a.r.stallMin, a.r.stallMax
	if hi <= lo {
		return hi
	}
	m := hmac.New(sha256.New, l.stallKey[:])
	b, _ := key.MarshalBinary() // the address and the prefix length
	m.Write(b)
	v := binary.BigEndian.Uint64(m.Sum(nil))
	return lo + time.Duration(v%uint64(hi-lo+1))
}

// ---- arrivals ----

// Arrival is a carrier that an entry point of a Listener took: it holds a
// slot of its source (ListenerConfig.MaxPendingPerSource) and a slot of
// the listener (MaxPending) from its arrival, before any handshake,
// through the admission and until the carrier enters a session, is closed
// or is handed to a fallback. Listener.Serve, HandlePipe and the
// HTTP handlers make their own; an adapter with its own accept loop takes
// one with Arrive (or AdmitUpgrade for an HTTP upgrade) as soon as it
// accepts a connection, and hands its carriers on with HandlePipe.
type Arrival struct {
	l    *Listener
	src  netip.AddrPort
	peer netip.AddrPort
	key  netip.Prefix

	value    any               // Decision.Value of that acceptance
	slot     *core.PendingSlot // the slots Arrive took, until handed on
	variant  string            // Decision.Variant of that acceptance
	mu       sync.Mutex
	accepted bool // the admission accepted the carrier
}

// Arrive takes a slot of source and a slot of the listener for a carrier
// that arrived from peer (source zero: peer). When every slot of the
// listener is taken, the oldest stalled carrier is closed to make room; a
// source without a free slot or a closed listener (net.ErrClosed) refuses
// the carrier (ListenerStats.Refused), and so does a full listener with no
// stalled carrier (ListenerStats.Rejected): the caller closes the carrier
// without a handshake.
func (a *ListenerAdapter) Arrive(source, peer netip.AddrPort) (*Arrival, error) {
	return (*Listener)(a).arrive(source, peer)
}

func (l *Listener) arrive(source, peer netip.AddrPort) (*Arrival, error) {
	source, peer = unmapped(source), unmapped(peer)
	if !source.IsValid() {
		source = peer
	}
	key := l.sourceKey(source)
	slot, err := l.enter(key)
	if err != nil {
		return nil, err
	}
	l.mu.Lock()
	watch := l.startWatchLocked()
	l.mu.Unlock()
	if watch != nil {
		watch()
	}
	return &Arrival{l: l, src: source, peer: peer, key: key, slot: slot}, nil
}

// enter takes the slots of a carrier from the source of key, counting a
// refusal; the error is net.ErrClosed, errRefused (the source's share) or
// errRejected (every slot taken, none stalled).
func (l *Listener) enter(key netip.Prefix) (*core.PendingSlot, error) {
	l.mu.Lock()
	closed := l.closed
	var slot *core.PendingSlot
	var evicted func()
	r := core.EnterSourceFull
	if !closed {
		slot, evicted, r = l.pending.Enter(key)
	}
	l.mu.Unlock()
	if evicted != nil {
		evicted()
	}
	switch {
	case closed:
		l.st.refused.Add(1)
		return nil, net.ErrClosed
	case r == core.EnterFull:
		l.st.rejected.Add(1)
		return nil, errRejected
	case r != core.EnterOK:
		l.st.refused.Add(1)
		return nil, errRefused
	}
	return slot, nil
}

// Release gives back the arrival's slots when no carrier of it is handed
// on (the connection ended first). It may be called more than once, also
// after HandlePipe.
func (a *Arrival) Release() {
	if a == nil {
		return
	}
	a.mu.Lock()
	slot := a.slot
	a.slot = nil
	a.mu.Unlock()
	a.l.leave(slot)
}

// HandshakeFailed records that the arrival's outer handshake (an HTTP
// upgrade, a QUIC handshake) failed or timed out, and releases it: with
// an Admission it counts as AdmissionStats.HandshakeFailed, otherwise as
// a setup failure.
func (a *Arrival) HandshakeFailed() {
	if a.l.cfg.Admission != nil {
		a.l.st.adm.handshakeFailed.Add(1)
	} else {
		a.l.countSetupFailure()
	}
	a.Release()
}

// leave gives back the slots of a carrier; nil, or slots given back
// already or evicted, do nothing.
func (l *Listener) leave(slot *core.PendingSlot) {
	if slot == nil {
		return
	}
	l.mu.Lock()
	l.pending.Leave(slot)
	l.mu.Unlock()
}

// takeSlot hands the arrival's slots on to a carrier: the ones Arrive took
// for the first, new ones for each further carrier of an accepted arrival
// (a QUIC connection's further streams). The error is that of enter.
func (a *Arrival) takeSlot() (*core.PendingSlot, error) {
	a.mu.Lock()
	slot := a.slot
	a.slot = nil
	a.mu.Unlock()
	if slot != nil {
		return slot, nil
	}
	return a.l.enter(a.key)
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
// ignored: the arrival's source applies. Hand on the first carrier alone
// and further ones only once it has been accepted (Admitted): first
// carriers handed on concurrently are each admitted on their own, with a
// decision each.
func (a *Arrival) HandlePipe(ctx context.Context, p PipeIO, hint PipeHint) error {
	l := a.l
	if err := p.checkOverhead(); err != nil {
		a.Release()
		closePipeIO(p)
		l.countSetupFailure()
		return err
	}
	if !a.isAccepted() {
		q, prefix, v := a.admitPipe(ctx, p)
		switch v {
		case VerdictAccept:
			p = q
			a.accept()
		case VerdictFallback:
			if nc, ok := p.R.(net.Conn); ok && sameHalf(p.W, p.R) && l.cfg.Fallback != nil {
				a.Release()
				l.cfg.Fallback(ctx, prefixConn(nc, prefix))
				return errRejected
			}
			a.stallPipe(ctx, p, false)
			return errRejected
		default:
			return errRejected
		}
	}
	run, err := a.Take(ctx, p, hint)
	if err != nil {
		return err
	}
	return run(ctx)
}

// Take hands p, a carrier of the arrival that its admission accepted
// (Admitted), to the listener, or refuses it at once, before it waits for
// anything: the listener is closed, it is shutting down and p is a split
// half, or the source has no slot for a further carrier. p takes the
// arrival's slots, as with HandlePipe, and is closed when refused. run
// does the rest of HandlePipe: it blocks for the lifetime of the pipe.
// Take serves an adapter that answers a carrier's request according to
// whether the listener took it (the exchange carrier); HandlePipe is Take
// and run in one.
func (a *Arrival) Take(ctx context.Context, p PipeIO, hint PipeHint) (run func(context.Context) error, err error) {
	l := a.l
	if !a.isAccepted() {
		closePipeIO(p)
		return nil, errRejected
	}
	slot, err := a.takeSlot()
	if err != nil {
		if errors.Is(err, errRefused) && !l.isClosed() {
			return nil, l.refusePending(ctx, p, a.src, a.peer)
		}
		closePipeIO(p)
		return nil, err
	}
	hint.Source = a.src
	a.mu.Lock()
	hint.Admitted = a.value
	if p.Variant == "" {
		p.Variant = a.variant // the admission labels the carrier
	}
	a.mu.Unlock()
	return l.take(p, hint, slot)
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
	default:
		// VerdictStall, VerdictClose and a failed handshake: admitPipe
		// has acted.
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
func (a *Arrival) admitPipe(ctx context.Context, p PipeIO) (q PipeIO, prefix []byte, v AdmissionVerdict) {
	l := a.l
	adm := l.cfg.Admission
	ctx, cancel := l.stopContext(ctx)
	defer cancel()
	if handshake := pendingHandshake(p); handshake != nil {
		hctx, hcancel := timeshim.WithTimeout(ctx, l.clk, l.cfg.Config.handshakeTimeout())
		err := handshake(hctx)
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
	if cs, ok := TerminatedTLS(p); ok {
		info.TLS = &cs
	}
	var more peekMore
	records := p.Framing == FramingRecords
	tooLong := false
	if adm.Peek > 0 && p.R != nil {
		var fired, resumable bool
		if records {
			prefix, tooLong, fired = a.peekMessage(p, adm.r.maxPeek, deadline)
		} else {
			prefix, fired, resumable = a.peekPipe(p, prefix, adm.Peek, deadline)
			more = func(have []byte, need int) ([]byte, bool, bool) { return a.peekPipe(p, have, need, deadline) }
		}
		if fired {
			l.st.adm.peekTimedOut.Add(1)
			if !resumable {
				closePipeIO(p)
				a.Release()
				return PipeIO{}, nil, VerdictClose
			}
			a.count(Decision{Verdict: VerdictFallback})
			return p, prefix, VerdictFallback
		}
	}
	var dd decided
	if tooLong {
		dd = decided{a.count(Decision{Verdict: VerdictClose}), prefix}
	} else {
		dd = a.decide(ctx, info, prefix, deadline, more, records)
	}
	d := dd.Decision
	prefix = dd.prefix
	switch d.Verdict {
	case VerdictAccept:
		return withPrefix(p, prefix[d.Skip:]), nil, VerdictAccept
	case VerdictFallback:
		return p, prefix, VerdictFallback
	case VerdictStall:
		a.stallPipe(ctx, p, true)
		return PipeIO{}, nil, VerdictStall
	default: // VerdictClose
		closePipeIO(p)
		a.Release()
		return PipeIO{}, nil, VerdictClose
	}
}

// pendingHandshake is the HandshakeContext of p's half whose own
// handshake has not run (a Handshaker; a *tls.Conn only while its
// handshake is incomplete), nil without one. The half stays p's.
func pendingHandshake(p PipeIO) func(context.Context) error {
	for _, x := range []any{p.R, p.W} {
		if tc, ok := x.(*tls.Conn); ok {
			if !tc.ConnectionState().HandshakeComplete {
				return tc.HandshakeContext
			}
			continue
		}
		if h, ok := x.(Handshaker); ok {
			return h.HandshakeContext
		}
	}
	return nil
}

// peekMore reads on until the prefix holds need bytes (see decide): the
// prefix, whether the deadline passed first, and, then, whether the carrier
// goes on to fallback with that prefix (otherwise it is closed).
type peekMore func(have []byte, need int) (prefix []byte, fired, resumable bool)

// peekPipe reads p by deadline until have holds n bytes, and returns
// them (fewer when the carrier ends); fired: the deadline passed first.
// Then resumable: Admission.FallbackOnPeekTimeout is set and p's read
// deadline is cleared again, so that prefix, the bytes read, is the carrier's
// to fall back with; otherwise the carrier is closed.
func (a *Arrival) peekPipe(p PipeIO, have []byte, n int, deadline time.Time) (prefix []byte, fired, resumable bool) {
	var deadlined bool
	done := make(chan struct{})
	t := a.l.clk.AfterFunc(deadline.Sub(a.l.clk.Now()), func() {
		defer close(done)
		fired = true
		deadlined = interruptRead(p)
	})
	prefix = peek(p.R, have, n)
	if t.Stop() {
		return prefix, false, false
	}
	<-done // the timer's function has run
	if !fired {
		return prefix, false, false
	}
	complete := len(prefix) >= n // the prefix arrived in time after all
	if deadlined && (complete || a.l.cfg.Admission.FallbackOnPeekTimeout) {
		if d, ok := p.R.(interface{ SetReadDeadline(time.Time) error }); ok && d.SetReadDeadline(time.Time{}) == nil {
			return prefix, !complete, !complete
		}
	}
	closePipeIO(p)
	return nil, true, false
}

// peekMessage reads the first message of a message-framed p by deadline
// (none when the carrier ends or its half is no MessageReader), at most
// limit; tooLong: it held more (read no further than limit+1 bytes with a
// MessageLimitReader).
func (a *Arrival) peekMessage(p PipeIO, limit int, deadline time.Time) (msg []byte, tooLong, fired bool) {
	mr, ok := p.R.(MessageReader)
	if !ok {
		return nil, false, false
	}
	done := make(chan struct{})
	t := a.l.clk.AfterFunc(deadline.Sub(a.l.clk.Now()), func() {
		defer close(done)
		interruptRead(p)
	})
	var err error
	if lr, ok := p.R.(MessageLimitReader); ok {
		msg, err = lr.ReadMessageLimit(limit)
	} else {
		msg, err = mr.ReadMessage()
	}
	if !t.Stop() {
		<-done // it fired: the carrier's reads have ended
		return nil, false, true
	}
	if errors.Is(err, ErrMessageTooLarge) || len(msg) > limit {
		return nil, true, false
	}
	return bytes.Clone(msg), false, false
}

// interruptRead ends a blocked read of p: a passed read deadline where
// the receiving half takes one (deadlined), a close otherwise.
func interruptRead(p PipeIO) (deadlined bool) {
	if d, ok := p.R.(interface{ SetReadDeadline(time.Time) error }); ok && d.SetReadDeadline(time.Now()) == nil {
		return true
	}
	closePipeIO(p)
	return false
}

// decided is a final Decision with the prefix it was taken on.
type decided struct {
	Decision
	prefix []byte
}

// decide calls Decide with ctx ending at deadline, outside every lock,
// again after each VerdictMore with the longer prefix that more reads (nil:
// VerdictMore is VerdictClose), bounds the result and counts the outcome
// (a stall's aggregate count is stallSlot's); whole: the prefix is a
// message, replayed whole or not at all. A decision not returned in
// time is VerdictClose.
func (a *Arrival) decide(ctx context.Context, info *CarrierInfo, prefix []byte, deadline time.Time,
	more peekMore, whole bool) decided {
	l := a.l
	dctx, cancel := timeshim.WithDeadline(ctx, l.clk, deadline)
	defer cancel()
	ended := false // the carrier ended short of a Need
	for {
		d, ok := a.callDecide(dctx, info, prefix)
		if !ok {
			if errors.Is(dctx.Err(), context.DeadlineExceeded) {
				l.st.adm.peekTimedOut.Add(1)
			} else {
				l.st.adm.closed.Add(1)
				l.st.adm.variant("", VerdictClose)
			}
			return decided{Decision{Verdict: VerdictClose}, prefix}
		}
		if d.Verdict != VerdictMore {
			d = d.bound(len(prefix))
			if whole && d.Verdict == VerdictAccept && d.Skip != 0 && d.Skip != len(prefix) {
				// A message is replayed whole or not at all.
				d = Decision{Verdict: VerdictClose, Variant: d.Variant}
			}
			return decided{a.count(d), prefix}
		}
		if more == nil || ended || d.Need <= len(prefix) || d.Need > l.cfg.Admission.r.maxPeek {
			return decided{a.count(Decision{Verdict: VerdictClose, Variant: d.Variant}), prefix}
		}
		next, fired, resumable := more(prefix, d.Need)
		if fired {
			l.st.adm.peekTimedOut.Add(1)
			if resumable {
				return decided{a.count(Decision{Verdict: VerdictFallback, Variant: d.Variant}), next}
			}
			return decided{Decision{Verdict: VerdictClose}, prefix}
		}
		prefix, ended = next, len(next) < d.Need
	}
}

// callDecide runs Decide once; ok false: dctx ended first (a decision
// returned after that is not used, however the select chose).
func (a *Arrival) callDecide(dctx context.Context, info *CarrierInfo, prefix []byte) (Decision, bool) {
	res := make(chan Decision, 1)
	labels.Go(dctx, "admission", func() {
		d := Decision{Verdict: VerdictClose}
		defer func() { res <- d }()
		// A panicking decision is VerdictClose.
		safeLog(a.l.cfg.Logger, "Admission.Decide", func() { d = a.l.cfg.Admission.Decide(dctx, info, prefix) })
	})
	var d Decision
	select {
	case d = <-res:
	case <-dctx.Done():
	}
	return d, dctx.Err() == nil
}

// bound is d with the fields its verdict does not use cleared; an unknown
// verdict, or a Skip outside a prefix of n bytes, is VerdictClose.
func (d Decision) bound(n int) Decision {
	switch {
	case d.Verdict >= VerdictMore:
		return Decision{Verdict: VerdictClose, Variant: d.Variant}
	case d.Verdict != VerdictAccept:
		return Decision{Verdict: d.Verdict, Variant: d.Variant}
	case d.Skip < 0 || d.Skip > n:
		return Decision{Verdict: VerdictClose, Variant: d.Variant}
	}
	d.Need = 0
	return d
}

// count counts a final decision and keeps the value of an acceptance.
func (a *Arrival) count(d Decision) Decision {
	l := a.l
	switch d.Verdict {
	case VerdictAccept:
		a.mu.Lock()
		a.value, a.variant = d.Value, d.Variant
		a.mu.Unlock()
		l.st.adm.accepted.Add(1)
	case VerdictFallback:
		l.st.adm.fallback.Add(1)
	case VerdictStall:
		// The aggregate is counted by stallSlot.
	case VerdictClose:
		l.st.adm.closed.Add(1)
	case VerdictMore:
		// Never final: decide turns it into a longer prefix or a close.
	}
	l.st.adm.variant(d.Variant, d.Verdict)
	return d
}

// stallSlot marks the arrival's carrier stalled, evict ending the stall
// when a new carrier takes its slots; ok false: the source holds its share
// of stalled carriers, or the listener half of MaxPending
// (StallOverflow), close it at once.
func (a *Arrival) stallSlot(counted bool, evict func()) (d time.Duration, release func(), ok bool) {
	l := a.l
	a.mu.Lock()
	slot := a.slot
	a.mu.Unlock()
	l.mu.Lock()
	ok = !l.closed && l.pending.Stall(slot, evict)
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
		l.pending.Unstall(slot)
		l.mu.Unlock()
	}, true
}

// stallPipe stalls p: it discards what arrives until the peer ends it or
// the stall duration passes on the listener's clock or the listener
// stops taking carriers, then closes it gracefully; the arrival is
// released at the end.
// counted: the decision was VerdictStall, not a fallback without target.
func (a *Arrival) stallPipe(ctx context.Context, p PipeIO, counted bool) {
	defer a.Release()
	d, release, ok := a.stallSlot(counted, func() { interruptRead(p) })
	if !ok || p.R == nil {
		closePipeIO(p)
		if ok {
			release()
		}
		return
	}
	defer release()
	ctx, cancel := a.l.stopContext(ctx) // Listener.Close ends a stall
	defer cancel()
	t := a.l.clk.AfterFunc(d, func() { interruptRead(p) })
	stop := context.AfterFunc(ctx, func() { interruptRead(p) })
	discard(p.R)
	stop()
	t.Stop()
	if cw, ok := p.W.(CloseWriter); ok {
		_ = cw.CloseWrite()
	}
	closePipeIO(p)
}

// peek reads from r until have holds n bytes (a new buffer when have has
// no room); it stops early at the end of r or an error (an interrupted
// read).
func peek(r io.Reader, have []byte, n int) []byte {
	if n <= len(have) {
		return have
	}
	b := have
	if cap(b) < n {
		b = make([]byte, len(have), n)
		copy(b, have)
	}
	got, _ := io.ReadFull(r, b[len(have):n])
	return b[:len(have)+got]
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

// prefixConn returns c with prefix read before the rest of c. Once the
// prefix is consumed, WriteTo and ReadFrom use c's own (splice and
// sendfile where c offers them), and Unwrap returns c.
func prefixConn(c net.Conn, prefix []byte) net.Conn {
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

// ---- HTTP requests as carriers ----

// AdmitUpgrade takes r, the request of an HTTP upgrade (WebSocket,
// WebTransport), as a carrier of l and runs l's admission on its head,
// before anything is written to w: the decision gets no prefix,
// CarrierInfo.Head is set, and the body is not read. Its source is what
// trusted resolves (TrustedProxies.RequestSource). A request that an
// intermediary forwarded from TLS early data (Early-Data: 1, RFC 8470), or
// read before the TLS handshake completed (0-RTT on HTTP/3), is answered
// 425 (Too Early) first and is no carrier (EarlyRequest). On VerdictAccept
// (or without an Admission) it returns the arrival and true: the caller
// completes the upgrade and hands the carrier on with Arrival.HandlePipe,
// or calls Arrival.HandshakeFailed. Otherwise it has answered r
// (VerdictFallback, or a carrier the listener refuses, with fallback;
// nil: http.NotFound) or ended it (VerdictStall, VerdictClose, which abort
// the request with http.ErrAbortHandler), and the caller returns.
func (a *ListenerAdapter) AdmitUpgrade(w http.ResponseWriter, r *http.Request, trusted *TrustedProxies, fallback http.Handler) (*Arrival, bool) {
	return (*Listener)(a).admitRequest(w, r, trusted, false, fallback)
}

// AdmitRequest is AdmitUpgrade for a request whose body is the carrier:
// the decision's prefix is the start of the body (none for a request
// without one, decided on its head), and on VerdictAccept r.Body
// continues after the skipped bytes. On VerdictFallback r.Body replays
// the bytes read.
func (a *ListenerAdapter) AdmitRequest(w http.ResponseWriter, r *http.Request, trusted *TrustedProxies, fallback http.Handler) (*Arrival, bool) {
	return (*Listener)(a).admitRequest(w, r, trusted, true, fallback)
}

// EarlyRequest reports whether r may have been received in TLS early
// data: an intermediary forwarded it from early data (Early-Data: 1, RFC
// 8470), or the server read it on a connection whose handshake has not
// completed, as an HTTP/3 server listening for 0-RTT does. Such a request
// is no carrier; AdmitUpgrade and AdmitRequest answer it 425.
func EarlyRequest(r *http.Request) bool {
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

// admitRequest is AdmitUpgrade, and with body set AdmitRequest.
func (l *Listener) admitRequest(w http.ResponseWriter, r *http.Request, trusted *TrustedProxies, body bool, fallback http.Handler) (*Arrival, bool) {
	if EarlyRequest(r) {
		w.WriteHeader(http.StatusTooEarly)
		l.countSetupFailure() // it needs the handshake: no pipe input
		return nil, false
	}
	a, err := l.arrive(trusted.RequestSource(r))
	if err != nil {
		// Refused (its source holds its share, or the listener is
		// closed): nothing was read or written, the host serves it.
		serveHTTP(fallback, w, r)
		return nil, false
	}
	if !a.decideRequest(w, r, body, fallback) {
		return nil, false
	}
	return a, true
}

// serveHTTP serves r with h, http.NotFound when h is nil.
func serveHTTP(h http.Handler, w http.ResponseWriter, r *http.Request) {
	if h == nil {
		http.NotFound(w, r)
		return
	}
	h.ServeHTTP(w, r)
}

// DecideRequest runs the admission of the arrival of r, a request taken
// as a carrier on its head (see AdmitUpgrade), for a host that checks the
// request between Listener.Arrive and the decision. On VerdictFallback it
// serves r with fallback (nil: http.NotFound); on VerdictStall and
// VerdictClose it aborts the request with http.ErrAbortHandler. It returns
// true on VerdictAccept, or without an Admission.
func (a *Arrival) DecideRequest(w http.ResponseWriter, r *http.Request, fallback http.Handler) bool {
	return a.decideRequest(w, r, false, fallback)
}

// decideRequest is DecideRequest, and with body set the decision on the
// start of the body (see AdmitRequest).
func (a *Arrival) decideRequest(w http.ResponseWriter, r *http.Request, body bool, fallback http.Handler) bool {
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
		done := make(chan struct{})
		t := l.clk.AfterFunc(deadline.Sub(l.clk.Now()), func() {
			defer close(done)
			_ = rc.SetReadDeadline(time.Now())
		})
		b := make([]byte, adm.Peek)
		n, err := io.ReadFull(r.Body, b)
		if !t.Stop() {
			<-done // it fired: the request's reads have ended
			l.st.adm.peekTimedOut.Add(1)
			a.Release()
			panic(http.ErrAbortHandler)
		}
		prefix, head = b[:n], n == 0 && err == io.EOF
	}
	var more peekMore
	if !head && adm.Peek > 0 {
		more = func(have []byte, need int) ([]byte, bool, bool) {
			rc := http.NewResponseController(w)
			done := make(chan struct{})
			t := l.clk.AfterFunc(deadline.Sub(l.clk.Now()), func() {
				defer close(done)
				_ = rc.SetReadDeadline(time.Now())
			})
			b := peek(r.Body, have, need)
			if !t.Stop() {
				<-done // it fired: the request's reads have ended
				return nil, true, false
			}
			return b, false, false
		}
	}
	info := &CarrierInfo{Source: a.src, Peer: a.peer, TLS: r.TLS, Request: r, Head: head}
	dd := a.decide(ctx, info, prefix, deadline, more, false)
	d, prefix := dd.Decision, dd.prefix
	switch d.Verdict {
	case VerdictAccept:
		if rest := prefix[d.Skip:]; len(rest) > 0 {
			r.Body = httpx.ReplayBody(rest, r.Body, r.Body)
		}
		a.accept()
		return true
	case VerdictFallback:
		if len(prefix) > 0 {
			r.Body = httpx.ReplayBody(prefix, r.Body, r.Body)
		}
		a.Release()
		serveHTTP(fallback, w, r)
	case VerdictStall:
		evicted := make(chan struct{})
		if d, release, ok := a.stallSlot(true, sync.OnceFunc(func() { close(evicted) })); ok {
			t := timeshim.NewTimer(l.clk, d)
			select {
			case <-t.C:
			case <-ctx.Done():
			case <-evicted:
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
