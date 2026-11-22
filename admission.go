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

	"github.com/zmuxio/zmux-go/internal/carrierio"
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
// WebSocket) the prefix is the carrier's first message, whole (at most
// MaxMessage bytes). Decide may ask for more of a byte stream
// (VerdictMore, up to MaxPeek bytes), as a host that tells several kinds
// of carrier apart by their first bytes does (admit.ClassifyFirstBytes
// with CarrierInfo.Framing). An HTTP request carrier is decided on its
// request head (CarrierInfo.Stage StageHead); on an upgrade's head
// VerdictDefer decides again on the carrier's first message or bytes
// once the upgrade completed (CarrierInfo.Upgrade).
//
// The fields are read by NewListener; an Admission must not be changed or
// copied afterwards.
type Admission struct {
	// Decide returns the decision for a carrier. prefix is what was read,
	// valid only during the call. ctx ends at what remains of PeekTimeout
	// and when the listener stops taking carriers (Close, or the end of
	// Shutdown); a decision not returned by then is VerdictClose, and so
	// is a panic. It is called once per carrier, once more after each
	// VerdictMore and once more after a VerdictDefer, concurrently for
	// different carriers, outside every lock; info is each call's own
	// copy. Decide must return once ctx ends: one that does not keeps its
	// goroutine, the request and prefix past the carrier's end.
	Decide func(ctx context.Context, info CarrierInfo, prefix []byte) Decision
	// StallMin and StallMax bound the duration of a VerdictStall. The
	// duration is a function of the carrier's source alone for the
	// Listener's lifetime (Listener.StallDuration). When both are zero,
	// the bounds are drawn once per Admission from a secure random source:
	// StallMax between 30 s and 120 s, StallMin between a quarter and
	// three quarters of it.
	StallMin, StallMax time.Duration
	// Peek is the number of bytes of a byte stream read before Decide (0:
	// none). Decide receives fewer when the carrier ends first. On a
	// message-framed carrier any Peek above 0, and a decision deferred to
	// the carrier (VerdictDefer) with any Peek, reads the first message
	// instead (MaxMessage).
	Peek int
	// MaxPeek bounds Decision.Need, the prefix of a byte stream that
	// VerdictMore may ask for (0: Peek, so that VerdictMore is
	// VerdictClose). It is not below Peek, or ErrInvalidConfig.
	MaxPeek int
	// MaxMessage bounds the first message that the decision on a
	// message-framed carrier reads: a longer one is closed without a
	// decision (Closed in ordinary admission, TooLarge after Defer). Zero is
	// MaxRecordSize, the largest zmux record; a host that
	// decides on messages of its own sets the largest it accepts. Above
	// MaxRecordSize it is ErrInvalidConfig.
	MaxMessage int
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
	// not, a message-framed one, an HTTP request carrier and a deferred
	// HTTP upgrade are closed on the timeout as when it is false. A Decide
	// that does not return in time is VerdictClose, and a failed outer
	// handshake is not affected.
	FallbackOnPeekTimeout bool

	once sync.Once // before err: it fills the padding after the bool
	err  error
	r    admParams // resolved
}

// admParams are the resolved bounds of an Admission.
type admParams struct {
	maxPeek            int // max(Peek, MaxPeek)
	maxMessage         int // MaxMessage, MaxRecordSize for 0
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
	// Stage is what of the carrier the decision is taken on: its request
	// head (StageHead) or its start (StageCarrier, the zero value).
	Stage AdmissionStage
	// Framing is the carrier's framing at StageCarrier (the prefix is the
	// start of a byte stream, or a first message with FramingRecords), as
	// admit.ClassifyFirstBytes takes it; zero on a head.
	Framing Framing
	// Upgrade marks the carrier of an HTTP upgrade (WebSocket,
	// WebTransport; ListenerAdapter.AdmitUpgrade), on its head and on the
	// decision a VerdictDefer on that head takes on the carrier.
	Upgrade bool
}

// AdmissionStage is what of a carrier a decision is taken on
// (CarrierInfo.Stage).
type AdmissionStage uint8

const (
	// StageCarrier is the carrier's start: the prefix is its first bytes
	// (Peek, VerdictMore) or its first message.
	StageCarrier AdmissionStage = iota
	// StageHead is an HTTP request's head, before any body byte is read
	// or the upgrade is answered: an HTTP upgrade such as WebSocket or
	// WebTransport, an exchange carrier, or an HTTP request carrier
	// without a body, such as a downlink GET. There is no prefix, and a
	// credential travels in a request header field.
	StageHead
)

// Takeover is a carrier that the admission hands to the host
// (ListenerConfig.Fallback) on VerdictTakeover, on a stream or message
// carrier’s VerdictFallback, or on a prefix that
// Admission.FallbackOnPeekTimeout gave up on. What was read of it is
// replayed: the bytes of a byte stream, the first message of a
// message-framed carrier (always a whole, non-empty binary message: a
// carrier whose first read fails or returns an empty message is closed
// without a decision).
//
// The carrier is the host's for the call of Fallback, whose context ends
// with the call at every entry point, and zmux closes it when Fallback
// returns, unless the host called Detach during the call:
// the carrier, and what an adapter runs it on (the QUIC connection of a
// stream, zmuxquic.TakeoverConn; the WebTransport session; the HTTP request
// of a request carrier), is then the host's for good, to be served after
// Fallback returned, on any goroutine and with a context of the host's,
// and closed by the host, which must close it once done.
//
// Conn and Pipe are views of the one carrier, valid for as long as it is
// open: the host closes it through either (Conn's Close, or ClosePipeIO on
// Pipe). The views forward the capabilities of the carrier's halves
// (ConnectionStater, CarrierSecure, CloseWriter, Aborter, ...; see
// carrierview.go for the complete set and what a view reports for a
// capability its half lacks), and Unwrap returns the half under a view.
type Takeover struct {
	// Info is the carrier's metadata, as Decide saw it.
	Info CarrierInfo
	// Value is Decision.Value of the verdict that handed the carrier over (nil after FallbackOnPeekTimeout): the host's routing
	// of the carrier, as an accepted carrier's Value is its session's.
	Value any
	conn  *prefixedConn
	owner *halfOwner
	pipe  PipeIO
	mu    sync.Mutex
	state takeoverState
}

// takeoverState is where a Takeover is in its hand-over.
type takeoverState uint8

const (
	takeoverHeld     takeoverState = iota // Fallback runs: zmux closes the carrier when it returns
	takeoverDetached                      // the host owns the carrier
	takeoverReturned                      // Fallback returned without Detach: zmux closed it
)

// ErrTakeoverReturned is Takeover.Detach called after Fallback returned
// without it: zmux has closed the carrier.
var ErrTakeoverReturned = errors.New("zmux: Takeover.Detach after Fallback returned")

// Detach makes the carrier the host's: zmux does not close it when
// Fallback returns, and never touches it again; the host must close it
// once done (an HTTP request carrier's handler waits for that, bounded
// only by the request's context). It is called during Fallback, which may
// then hand Conn or Pipe to another goroutine and return; the context
// Fallback got ends with the call, so the host serves the carrier on with
// a context of its own. A second Detach returns nil. After Fallback
// returned without Detach it returns ErrTakeoverReturned (the carrier is
// closed), and net.ErrClosed when the host closed every half of the
// carrier before.
func (t *Takeover) Detach() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	switch t.state {
	case takeoverDetached:
		return nil
	case takeoverReturned:
		return ErrTakeoverReturned
	case takeoverHeld:
	}
	if t.owner.closed() {
		return net.ErrClosed
	}
	t.state = takeoverDetached
	return nil
}

// returned ends the call of Fallback: whether the host detached the
// carrier (otherwise Detach is refused from now on).
func (t *Takeover) returned() (detached bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state == takeoverDetached {
		return true
	}
	t.state = takeoverReturned
	return false
}

// Pipe returns the carrier with what was read replayed: its receiving
// half is a view that returns what was read first (on a message-framed
// carrier, Framing FramingRecords, a MessageReader that returns the first
// message again first); its sending half is the carrier's own, except
// where Conn applies, whose view both halves then are. Each half is a
// view that closes the carrier's half once, whoever closes it (the host,
// or zmux when Fallback returns without Detach): ClosePipeIO closes the
// carrier, and a half can be closed on its own (close the sending half,
// read the receiving one to its end).
func (t *Takeover) Pipe() PipeIO { return t.pipe }

// Conn returns the carrier as a net.Conn when it is a byte stream on a
// single net.Conn (TCP, TLS), sharing Pipe's replay: once the replayed
// bytes are read, WriteTo and ReadFrom use the connection's own (splice
// and sendfile where it offers them). It is a view of the connection
// that forwards its capabilities (ConnectionState, NetConn, CarrierSecure,
// CloseWrite, Abort, HandshakeContext, ...), its Close closing the
// connection once, and Unwrap returns the connection (read through it
// only once the replayed bytes were read).
func (t *Takeover) Conn() (net.Conn, bool) {
	if t.conn == nil {
		return nil, false
	}
	return t.conn, true
}

// Messages returns the halves of a message-framed carrier (Framing
// FramingRecords, such as WebSocket), the reader returning the first
// message again first; false for a byte stream. A half the carrier does
// not have is nil.
func (t *Takeover) Messages() (MessageReader, MessageWriter, bool) {
	if t.pipe.Framing != FramingRecords {
		return nil, nil, false
	}
	mr, _ := t.pipe.R.(MessageReader)
	mw, _ := t.pipe.W.(MessageWriter)
	return mr, mw, true
}

// AdmissionVerdict is the outcome of an admission.
type AdmissionVerdict uint8

const (
	// VerdictAccept hands the carrier to the Listener, without the first
	// Decision.Skip bytes of the prefix.
	VerdictAccept AdmissionVerdict = iota
	// VerdictFallback selects the carrier's normal non-zmux handling.
	// An HTTP request goes to the adapter's Next handler with its headers
	// and complete streaming body, or receives HTTP 404 when Next is nil.
	// Byte streams and message-framed carriers, including QUIC first streams
	// and already upgraded connections, go to ListenerConfig.Fallback with
	// the prefix replayed and Decision.Value. Without that target they stall.
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
	// on a request head is VerdictClose.
	VerdictMore
	// VerdictDefer completes an HTTP upgrade whose head is being decided
	// (CarrierInfo.Upgrade, Stage StageHead) and decides again on the
	// carrier: Decide is called with the same metadata at StageCarrier and
	// the carrier's first message, or its first Peek bytes (the first
	// stream of a WebTransport session). PeekTimeout runs on from the
	// head's decision, and the carrier keeps its slots throughout. On any
	// other decision it is VerdictClose.
	VerdictDefer
	// VerdictTakeover hands the carrier itself to ListenerConfig.Fallback.
	// HTTP adapters commit the carrier response before the handoff. With
	// no byte-stream target the carrier stalls.
	VerdictTakeover
)

// verdictFailed is the outcome of a carrier whose outer handshake failed:
// it reached no decision and is closed.
const verdictFailed AdmissionVerdict = 255

// Decision is what Admission.Decide returns.
type Decision struct {
	// Value is the host's value for an accepted carrier or one it takes
	// over (VerdictAccept, VerdictFallback or VerdictTakeover): zmux
	// hands it on without looking at it, on VerdictAccept to WrapPipe
	// (PipeHint.Admitted) and to the session (AdmittedAs) or pipe
	// (AdmittedAs) the carrier makes, holding it until their state is
	// released, and on stream handoff to ListenerConfig.Fallback
	// (Takeover.Value), where it routes the carrier: a listener has one
	// Fallback, and the Value says which of the host's services the
	// carrier goes to. Whether a join comes from the same client as its
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
	// Name is the label, empty for the Overflow bucket.
	Name string
	// Overflow marks the bucket of the variants past MaxAdmissionVariants.
	Overflow bool

	Accepted, Fallback uint64
	// Takeover counts carriers delivered to the byte-stream target on VerdictTakeover.
	Takeover uint64
	// Stalled counts stall outcomes, including Takeover without a target
	// and stalls that exceed the quota.
	Stalled uint64
	// Closed counts Close outcomes labeled with this variant.
	Closed uint64
}

// MaxAdmissionVariants bounds the distinct Decision.Variant values a
// listener counts apart; the outcomes of further values are counted in one
// VariantStat with Overflow set. Variants are meant to be a few constants
// of the host, never derived from what the peer sent.
const MaxAdmissionVariants = 64

// AdmissionStats are the outcomes of a listener's admission,
// each carrier that took its source slot and whose outcome is known
// counted once, so that the sum of the fields is the number of such
// carriers. All are zero without an Admission.
type AdmissionStats struct {
	Accepted uint64 // VerdictAccept
	// Fallback counts carriers handed to a fallback target.
	Fallback uint64
	// Takeover counts VerdictTakeover at entry to the host callback.
	Takeover uint64
	// Stalled counts carriers admitted to a stall, including Takeover
	// without a byte-stream target.
	Stalled uint64
	// Closed counts VerdictClose, a decision that panicked, was cancelled,
	// or had a Skip outside the prefix or an unknown verdict. It also
	// counts prepared takeovers closed before delivery.
	Closed uint64
	// StallOverflow counts stall attempts closed because they could not
	// acquire or commit a stall slot.
	StallOverflow uint64
	// TooLarge counts oversized first messages after Defer.
	TooLarge uint64
	// PeekTimedOut counts carriers whose admission ended on expiry.
	PeekTimedOut uint64
	// HandshakeFailed counts carriers whose outer handshake failed or
	// timed out, including a request whose accepted response could not be committed.
	HandshakeFailed uint64
}

type admissionCounters struct {
	accepted, fallback, takeover, stalled, closed, stallOverflow, tooLarge, peekTimedOut, handshakeFailed atomic.Uint64

	variants map[string]*VariantStat
	overflow *VariantStat // the variants past MaxAdmissionVariants
	mu       sync.Mutex
}

// variant counts a final verdict of variant v.
func (c *admissionCounters) variant(v string, verdict AdmissionVerdict) {
	if v == "" {
		return
	}
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
	case VerdictTakeover:
		vs.Takeover++
	case VerdictStall:
		vs.Stalled++
	default:
		vs.Closed++
	}
}

func (c *admissionCounters) snapshot() AdmissionStats {
	return AdmissionStats{Accepted: c.accepted.Load(), Fallback: c.fallback.Load(), Takeover: c.takeover.Load(), Stalled: c.stalled.Load(),
		Closed: c.closed.Load(), StallOverflow: c.stallOverflow.Load(), TooLarge: c.tooLarge.Load(), PeekTimedOut: c.peekTimedOut.Load(),
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
		case a.MaxMessage < 0 || a.MaxMessage > MaxRecordSize:
			a.err = fmt.Errorf("%w: Admission.MaxMessage outside [0, %d]", ErrInvalidConfig, MaxRecordSize)
		case a.MaxPeek != 0 && a.MaxPeek < a.Peek:
			a.err = fmt.Errorf("%w: Admission.MaxPeek below Peek", ErrInvalidConfig)
		case a.StallMin > a.StallMax:
			a.err = fmt.Errorf("%w: Admission.StallMin above StallMax", ErrInvalidConfig)
		}
		if a.err != nil {
			return
		}
		r := admParams{maxPeek: max(a.Peek, a.MaxPeek), maxMessage: a.MaxMessage, peekTimeout: a.PeekTimeout,
			stallMin: a.StallMin, stallMax: a.StallMax}
		if r.maxMessage == 0 {
			r.maxMessage = MaxRecordSize
		}
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

	value    any         // Decision.Value of that acceptance
	stopSlot func() bool // unregister the initial reservation at transfer
	first    *attachment // reservation for the first carrier
	deferred *deferral   // a head decision deferred to the first carrier
	variant  string      // Decision.Variant of that acceptance
	mu       sync.Mutex
	accepted bool // the admission accepted the carrier
	detached bool // the host detached the carrier it took over
}

// An accepted HTTP response or host handoff can span adapter stages.
// Preparation settles the response. Dispatch settles a handoff.
type admissionHandoff struct {
	info        *CarrierInfo // non-nil for a carrier handoff
	decision    Decision
	prepared    atomic.Bool
	preparation sync.Once
	delivered   bool // written by the dispatch owner, read after its last release
}

func (h *admissionHandoff) prepare(l *Listener, ok bool) bool {
	if h == nil {
		return true
	}
	h.preparation.Do(func() {
		if !ok {
			l.st.adm.handshakeFailed.Add(1)
		} else if h.decision.Verdict == VerdictAccept {
			l.st.adm.count(h.decision)
		}
		h.prepared.Store(ok)
	})
	return h.prepared.Load()
}

// The attachment's last reference settles a prepared handoff which never
// reached the host, including cancellation between adapter stages.
func (h *admissionHandoff) end(l *Listener) {
	if h.prepare(l, false) && h.info != nil && !h.delivered {
		l.st.adm.count(Decision{Verdict: VerdictClose, Variant: h.decision.Variant})
	}
}

// deferral is an upgrade's head decision deferred to its carrier
// (VerdictDefer): the metadata of the second decision and the deadline
// that runs on from the head's.
type deferral struct {
	deadline time.Time
	info     CarrierInfo
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
	l.startWatch()
	a := &Arrival{l: l, src: source, peer: peer, key: key}
	owner := newAttachment(l, false)
	owner.slot = slot
	if !a.keep(owner) {
		owner.release()
		return nil, net.ErrClosed
	}
	return a, nil
}

// keep parks the setup reference between adapter stages. Once it has a
// carrier, only that carrier can consume it; later streams reserve afresh.
func (a *Arrival) keep(owner *attachment) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.first != nil || a.l.stopCtx.Err() != nil {
		return false
	}
	a.first = owner
	a.stopSlot = context.AfterFunc(a.l.stopCtx, a.Release)
	return true
}

func (a *Arrival) moveLocked() *attachment {
	owner := a.first
	a.first = nil
	if a.stopSlot != nil {
		a.stopSlot()
		a.stopSlot = nil
	}
	return owner
}

// enter takes the slots of a carrier from the source of key, counting a
// refusal; the error is net.ErrClosed, errRefused (the source's share) or
// errRejected (every slot taken, none stalled).
func (l *Listener) enter(key netip.Prefix) (*core.PendingSlot, error) {
	l.pmu.Lock()
	closed := l.stopCtx.Err() != nil
	var slot *core.PendingSlot
	var evicted func()
	r := core.EnterSourceFull
	if !closed {
		slot, evicted, r = l.pending.Enter(key)
	}
	l.pmu.Unlock()
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
	owner := a.moveLocked()
	dfr := a.deferred
	a.deferred = nil
	a.mu.Unlock()
	a.countAbandoned(dfr)
	if owner != nil {
		owner.handoff.prepare(a.l, false)
		if owner.pipe.own != nil {
			closePipe(owner.pipe)
		}
		owner.release()
	}
}

func (a *Arrival) countAbandoned(dfr *deferral) {
	if dfr != nil {
		// A deferred decision that never came: its carrier did not arrive
		// in time (PeekTimedOut) or ended first, also by the listener's
		// stop (Closed).
		if !a.l.clk.Now().Before(dfr.deadline) {
			a.l.st.adm.peekTimedOut.Add(1)
		} else {
			a.l.st.adm.closed.Add(1)
		}
	}
}

// A cancellation may win after begin moved the reservation but before
// admitPipe has consumed the deferred head decision.
func (a *Arrival) abandonDeferral() {
	a.mu.Lock()
	dfr := a.deferred
	a.deferred = nil
	a.mu.Unlock()
	a.countAbandoned(dfr)
}

// SetupContext bounds an adapter's work before it hands off a carrier.
// It ends with parent or when the listener stops setup. Call unlink when
// setup has finished so the established carrier has its own lifetime.
func (a *Arrival) SetupContext(parent context.Context) (ctx context.Context, unlink context.CancelFunc) {
	return a.l.stopContext(parent)
}

// PrepareCarrier runs an adapter's pre-handoff operation while the arrival
// still accounts for its work. Its context ends on listener stop, parent
// cancellation, or the earlier of the handshake and deferred peek deadlines.
func (a *Arrival) PrepareCarrier(ctx context.Context, prepare func(context.Context) error) (err error) {
	a.mu.Lock()
	owner := a.first
	if owner == nil || !owner.reserve() {
		a.mu.Unlock()
		return net.ErrClosed
	}
	deadline := a.l.clk.Now().Add(a.l.cfg.Config.handshakeTimeout())
	if d := a.deferred; d != nil && d.deadline.Before(deadline) {
		deadline = d.deadline
	}
	a.mu.Unlock()
	defer owner.release()
	defer func() { owner.handoff.prepare(a.l, err == nil) }()
	setup, unlink := a.SetupContext(ctx)
	defer unlink()
	bound, cancel := timeshim.WithDeadline(setup, a.l.clk, deadline)
	defer cancel()
	if err := bound.Err(); err != nil {
		return err
	}
	err = hostCall(a.l.cfg.Logger, "prepare carrier", func() error { return prepare(bound) })
	if bound.Err() != nil {
		return bound.Err()
	}
	if a.l.stopCtx.Err() != nil {
		return net.ErrClosed
	}
	return err
}

// AdmitFirst is Admit for an adapter whose first carrier comes after the
// arrival's head decision (an HTTP upgrade): first completes the upgrade
// or waits for the carrier (the first stream of a WebTransport session)
// and returns it, within the effective HandshakeTimeout. While the decision is deferred (VerdictDefer) first
// runs under a context that also ends at the decision's PeekTimeout and
// when the listener stops taking carriers; a carrier that does not come
// in time is counted (AdmissionStats.PeekTimedOut, or Closed on the
// listener's stop). An error of first otherwise is the failure of the
// arrival's outer handshake (HandshakeFailed), unless ctx ended. On an
// error first returns no carrier: it closes what it upgraded, and the
// arrival is released. The carrier then passes the admission as with Admit: on
// VerdictAccept, and when the head was accepted already, AdmitFirst
// returns it and true, to be handed on with HandlePipe.
func (a *Arrival) AdmitFirst(ctx context.Context, first func(context.Context) (PipeIO, error)) (PipeIO, bool) {
	a.mu.Lock()
	preparedRequest := a.first != nil && a.first.handoff != nil
	a.mu.Unlock()
	var p PipeIO
	err := a.PrepareCarrier(ctx, func(bound context.Context) error {
		var err error
		p, err = first(bound)
		return err
	})
	if err != nil {
		closePipe(owned(p))
		a.mu.Lock()
		dfr := a.deferred
		a.mu.Unlock()
		peekExpired := dfr != nil && !a.l.clk.Now().Before(dfr.deadline)
		if preparedRequest || ctx.Err() != nil || a.l.stopCtx.Err() != nil || peekExpired {
			a.Release()
		} else {
			a.HandshakeFailed()
		}
		return PipeIO{}, false
	}
	return a.Admit(ctx, p)
}

// HandshakeFailed records that the arrival's outer handshake (an HTTP
// upgrade, a QUIC handshake) failed or timed out, and releases it: with
// an Admission it counts as AdmissionStats.HandshakeFailed, otherwise as
// a setup failure.
func (a *Arrival) HandshakeFailed() {
	a.mu.Lock()
	owner := a.moveLocked()
	dfr := a.deferred
	a.deferred = nil
	a.mu.Unlock()
	if owner == nil {
		return
	}
	defer owner.release()
	if owner.pipe.own != nil {
		closePipe(owner.pipe)
	}
	switch {
	case owner.handoff != nil:
		owner.handoff.prepare(a.l, false)
	case dfr != nil && !a.l.clk.Now().Before(dfr.deadline):
		a.l.st.adm.peekTimedOut.Add(1)
	case a.l.cfg.Admission != nil:
		a.l.st.adm.handshakeFailed.Add(1)
	default:
		a.l.countSetupFailure()
	}
}

// leave gives back the slots of a carrier; nil, or slots given back
// already or evicted, do nothing.
func (l *Listener) leave(slot *core.PendingSlot) {
	if slot == nil {
		return
	}
	l.pmu.Lock()
	l.pending.Leave(slot)
	l.pmu.Unlock()
}

// begin moves the first reservation, or obtains one for another stream of
// an accepted connection, before inspecting any host capability.
func (a *Arrival) begin(p PipeIO, observe bool) (*attachment, error) {
	p = owned(p)
	var owner *attachment
	a.mu.Lock()
	if first := a.first; first != nil && (first.pipe.own == nil || first.pipe.own == p.own) {
		owner = a.moveLocked()
	}
	a.mu.Unlock()
	if owner == nil {
		slot, err := a.l.enter(a.key)
		if err != nil {
			if errors.Is(err, errRefused) && !a.l.isClosed() {
				return nil, a.l.refusePending(p, a.src, a.peer)
			}
			closePipe(p)
			return nil, err
		}
		owner = newAttachment(a.l, false)
		owner.slot = slot
	}
	if observe {
		owner.done = make(chan struct{})
	}
	if owner.pipe.own != nil {
		return owner, nil
	}
	owner.pipe = p
	half := any(p.W)
	if half == nil {
		half = p.R
	}
	if adapter, ok := half.(interface{ RequestTail() *carrierio.RequestTail }); ok {
		if !safeLog(a.l.cfg.Logger, "request tail", func() {
			if tail := adapter.RequestTail(); tail != nil {
				owner.reserve()
				tail.Bind(owner.release)
				owner.head = tail.Accept
			}
		}) {
			closePipe(owner.pipe)
			owner.release()
			return nil, errRejected
		}
	}
	return owner, nil
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

// HandlePipe takes ownership of p and returns after handoff or refusal,
// with the same setup-only context as Listener.HandlePipe. The first carrier
// passes the listener's admission (unless it was accepted already; one
// whose decision AdmitUpgrade deferred is decided here, on its first
// message or bytes, although AdmitFirst is the upgrade adapters' entry,
// which also bounds the wait for that carrier): its outer TLS handshake is completed, its
// prefix read and the decision taken, and VerdictFallback hands it to
// ListenerConfig.Fallback. Once the arrival is accepted, further
// carriers of it (a QUIC connection's further streams) are handed on
// without an admission, each taking a slot of the source. hint.Source is
// ignored: the arrival's source applies. Hand on the first carrier alone
// and further ones only once it has been accepted (Admitted): first
// carriers handed on concurrently are each admitted on their own, with a
// decision each.
func (a *Arrival) HandlePipe(ctx context.Context, p PipeIO, hint PipeHint) error {
	owner, err := a.begin(p, false)
	if err != nil {
		return err
	}
	defer owner.release()
	var result admissionResult
	err = owner.inspect(ctx, func() error {
		if err := owner.pipe.validate(); err != nil {
			a.l.countSetupFailure()
			return err
		}
		result = a.admit(ctx, owner, true)
		return result.err
	})
	if err != nil {
		if owner.rollback() {
			closePipe(owner.pipe)
		}
		return err
	}
	if result.kind == admissionFallback {
		return nil
	}
	run, err := a.take(hint, owner)
	if err != nil {
		return err
	}
	return run(ctx)
}

// Take hands p, a carrier of the arrival that its admission accepted
// (Admitted), to the listener, or refuses it at once, before it waits for
// anything: the listener has stopped setup, or the source has no slot for a further
// carrier. p takes the
// arrival's slots, as with HandlePipe, and is closed when refused. run
// completes setup and waits for the attached pipe or lane to end, even when
// setup failed after attachment. Its context bounds setup only.
// Take serves an adapter that answers a carrier's request according to
// whether the listener took it (the exchange carrier).
func (a *Arrival) Take(ctx context.Context, p PipeIO, hint PipeHint) (run func(context.Context) error, err error) {
	owner, err := a.begin(p, true)
	if err != nil {
		return nil, err
	}
	if err := owner.inspect(ctx, owner.pipe.validate); err != nil {
		closePipe(owner.pipe)
		owner.release()
		return nil, err
	}
	var setup func(context.Context) error
	if owner.handoff != nil && owner.handoff.info != nil {
		setup = func(ctx context.Context) error {
			return owner.inspect(ctx, func() error { return a.admit(ctx, owner, true).err })
		}
	} else {
		setup, err = a.take(hint, owner)
	}
	if err != nil {
		owner.release()
		return nil, err
	}
	var once sync.Once
	var result error
	stop := context.AfterFunc(a.l.stopCtx, func() {
		once.Do(func() { result = net.ErrClosed; closePipe(owner.pipe); owner.release() })
	})
	return func(ctx context.Context) error {
		once.Do(func() {
			stop()
			if r := owner.handoff; r != nil && r.info == nil && !r.prepare(a.l, ctx.Err() == nil) {
				result = errRejected
			} else {
				result = setup(ctx)
			}
			if result != nil && owner.rollback() {
				closePipe(owner.pipe)
			}
			owner.release()
		})
		<-owner.done
		if owner.pair != nil {
			<-owner.pair.done
		}
		return result
	}, nil
}

func (a *Arrival) take(hint PipeHint, owner *attachment) (func(context.Context) error, error) {
	if !a.isAccepted() {
		closePipe(owner.pipe)
		return nil, errRejected
	}
	hint.Source = a.src
	a.mu.Lock()
	hint.Admitted = a.value
	if owner.pipe.Variant == "" {
		owner.pipe.Variant = a.variant
	}
	a.mu.Unlock()
	return a.l.take(hint, owner)
}

// Admit runs the listener's admission on p, the first carrier of the
// arrival, for an adapter that must know the outcome before it hands
// further carriers on (a QUIC connection, a WebTransport session): on
// VerdictAccept it returns p without the skipped bytes and true, to be
// handed on with HandlePipe or Take. Otherwise it has acted on the
// verdict and returns false: VerdictFallback hands p, with every byte or
// message read replayed, to ListenerConfig.Fallback (without one the
// carrier is stalled) and returns once the fallback did (Detached reports
// whether the host kept the carrier), VerdictStall
// stalls it and VerdictClose closes it. Without an Admission, or once the
// arrival is accepted, it returns p and true. HandlePipe runs admission and setup in one call.
func (a *Arrival) Admit(ctx context.Context, p PipeIO) (PipeIO, bool) {
	if ctx.Err() != nil || a.l.stopCtx.Err() != nil {
		closePipe(owned(p))
		a.Release()
		return PipeIO{}, false
	}
	owner, err := a.begin(p, false)
	if err != nil {
		return PipeIO{}, false
	}

	var result admissionResult
	err = owner.inspect(ctx, func() error {
		result = a.admit(ctx, owner, false)
		return result.err
	})
	if err != nil || result.kind != admissionContinue {
		a.abandonDeferral()
		if owner.rollback() {
			closePipe(owner.pipe)
		}
		owner.release()
		return PipeIO{}, false
	}
	// Admit is the adapter's first stage. Keep its reservation for Take or
	// HandlePipe (or Release if the outer connection ends first).
	if !a.keep(owner) {
		closePipe(owner.pipe)
		owner.release()
		return PipeIO{}, false
	}
	return owner.pipe, true
}

type admissionKind uint8

const (
	admissionFailed admissionKind = iota
	admissionContinue
	admissionFallback
)

type admissionResult struct {
	err  error
	kind admissionKind
}

func (a *Arrival) admit(ctx context.Context, owner *attachment, async bool) admissionResult {
	p := owner.pipe
	if tk := owner.handoff; tk != nil && tk.info != nil {
		// A request carrier decided on its start (AdmitRequest).
		if !tk.prepared.Load() {
			tk.prepare(a.l, false)
			closePipe(owner.pipe)
			return admissionResult{kind: admissionFailed, err: errRejected}
		}
		return a.fallback(ctx, owner, nil)
	}
	if a.isAccepted() {
		if !owner.handoff.prepare(a.l, true) {
			closePipe(p)
			return admissionResult{kind: admissionFailed, err: errRejected}
		}
		return admissionResult{kind: admissionContinue}
	}
	var q PipeIO
	var prefix []byte
	var d Decision
	if !safeLog(a.l.cfg.Logger, "carrier admission", func() { q, prefix, d = a.admitPipe(ctx, owner) }) {
		closePipe(p)
		a.l.st.adm.closed.Add(1)
		return admissionResult{kind: admissionFailed, err: errRejected}
	}
	switch d.Verdict {
	case VerdictAccept:
		a.accept()
		owner.pipe = q
		return admissionResult{kind: admissionContinue}
	case VerdictFallback, VerdictTakeover:
		return a.fallback(ctx, owner, prefix)
	case VerdictStall:
		a.stallPipe(ctx, owner, d.Variant, async)
	default:
		// VerdictStall, VerdictClose and a failed handshake: admitPipe
		// has acted.
	}
	return admissionResult{kind: admissionFailed, err: errRejected}
}

// fallback hands p, with prefix replayed, to ListenerConfig.Fallback as a
// Takeover and closes it when the call returns unless the host detached
// it, the arrival's slots given back first. A panicking host callback
// closes the carrier unless the host detached it.
func (a *Arrival) fallback(ctx context.Context, owner *attachment, prefix []byte) admissionResult {
	l := a.l
	h := owner.handoff
	p := owner.pipe
	if err := hostCall(l.cfg.Logger, "CarrierTaken", func() error { markTaken(p); return nil }); err != nil {
		closePipe(p)
		return admissionResult{kind: admissionFailed, err: err}
	}
	if err := owner.commitFallback(ctx); err != nil {
		closePipe(p)
		return admissionResult{kind: admissionFailed, err: err}
	}
	// Nothing more is replayed once the host has the carrier.
	o := newHalfOwner(p)
	if !releasePipe(p, o.closeAll) {
		return admissionResult{kind: admissionFailed, err: net.ErrClosed} // closed already (the listener's stop)
	}
	t := &Takeover{Info: *h.info, Value: h.decision.Value, owner: o}
	t.pipe, t.conn = takeoverPipe(p, prefix, o)
	// The call's context ends with the call, at every entry point: a host
	// that serves a detached carrier on uses a context of its own.
	fctx, cancel := context.WithCancel(ctx)
	err := hostCall(l.cfg.Logger, "ListenerConfig.Fallback", func() error {
		h.delivered = true
		l.st.adm.count(h.decision)
		l.cfg.Fallback(fctx, t)
		return nil
	})
	cancel()
	if t.returned() {
		a.mu.Lock()
		a.detached = true
		a.mu.Unlock()
	} else {
		o.closeAll() // the halves the host has not closed
	}
	if err != nil {
		return admissionResult{kind: admissionFailed, err: err}
	}
	return admissionResult{kind: admissionFallback}
}

// Detached reports whether the host detached the carrier of the arrival
// that Admit, AdmitFirst or HandlePipe handed to ListenerConfig.Fallback
// (Takeover.Detach): the adapter then leaves the carrier, and what it runs
// on, to the host. An adapter whose carrier lives only as long as a call
// of its own (an HTTP handler serving an HTTP/2 stream or a request
// carrier) keeps that call until the host closed the carrier.
func (a *Arrival) Detached() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.detached
}

// stopContext is ctx, also ended when the listener stops taking carriers.
func (l *Listener) stopContext(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(l.stopCtx, cancel)
	if l.stopCtx.Err() != nil {
		cancel()
	}
	return ctx, func() { stop(); cancel() }
}

// admitPipe completes p's outer TLS handshake, reads its prefix and takes
// the decision, or the decision an upgrade's head deferred to it. On
// VerdictAccept q is p without the skipped bytes; on VerdictFallback p is
// untouched and prefix holds what was read.
// VerdictStall transfers it to stallPipe.
func (a *Arrival) admitPipe(ctx context.Context, owner *attachment) (q PipeIO, prefix []byte, d Decision) {
	p := owner.pipe
	l := a.l
	adm := l.cfg.Admission
	ctx, cancel := l.stopContext(ctx)
	defer cancel()
	a.mu.Lock()
	dfr := a.deferred
	a.deferred = nil
	a.mu.Unlock()
	if handshake := pendingHandshake(p); handshake != nil {
		deadline := l.clk.Now().Add(l.cfg.Config.handshakeTimeout())
		if dfr != nil && dfr.deadline.Before(deadline) {
			deadline = dfr.deadline
		}
		hctx, hcancel := timeshim.WithDeadline(ctx, l.clk, deadline)
		err := hostCall(l.cfg.Logger, "HandshakeContext", func() error { return handshake(hctx) })
		hcancel()
		if err != nil {
			closePipe(p)
			switch {
			case dfr != nil && !l.clk.Now().Before(dfr.deadline):
				l.st.adm.peekTimedOut.Add(1)
			case dfr != nil && ctx.Err() != nil:
				l.st.adm.closed.Add(1)
			default:
				l.st.adm.handshakeFailed.Add(1)
			}
			return PipeIO{}, nil, Decision{Verdict: verdictFailed}
		}
	}
	deadline := l.clk.Now().Add(adm.r.peekTimeout)
	info := &CarrierInfo{Source: a.src, Peer: a.peer}
	if cs, ok := TerminatedTLS(p); ok {
		info.TLS = &cs
	}
	if dfr != nil {
		info, deadline = &dfr.info, dfr.deadline
		if !l.clk.Now().Before(deadline) {
			l.st.adm.peekTimedOut.Add(1)
			closePipe(p)
			return PipeIO{}, nil, Decision{Verdict: VerdictClose}
		}
	}
	info.Framing = p.Framing
	var more peekMore
	records := p.Framing == FramingRecords
	if !records && p.R != nil && (adm.Peek > 0 || dfr != nil) {
		fallbackOnTimeout := dfr == nil && adm.FallbackOnPeekTimeout
		more = func(have []byte, need int) ([]byte, bool, bool, bool) {
			return a.peekPipe(ctx, p, have, need, deadline, fallbackOnTimeout)
		}
	}
	tooLong := false
	if (adm.Peek > 0 || records && dfr != nil) && p.R != nil {
		var fired, resumable, failed bool
		if records {
			prefix, tooLong, failed, fired = a.peekMessage(ctx, p, adm.r.maxMessage, deadline)
		} else {
			prefix, fired, resumable, failed = more(prefix, adm.Peek)
		}
		if fired {
			if !resumable {
				l.st.adm.peekTimedOut.Add(1)
				closePipe(p)
				return PipeIO{}, nil, Decision{Verdict: VerdictClose}
			}
			d := a.planPipeDecision(owner, info, Decision{Verdict: VerdictFallback})
			if d.Verdict == VerdictStall {
				return PipeIO{}, nil, d
			}
			return p, prefix, d
		}
		if failed {
			// Nothing to decide on, and nothing a fallback could be given
			// back whole.
			a.count(Decision{Verdict: VerdictClose})
			closePipe(p)
			return PipeIO{}, nil, Decision{Verdict: VerdictClose}
		}
	}
	var dd decided
	if tooLong {
		if dfr != nil {
			l.st.adm.tooLarge.Add(1)
		} else {
			l.st.adm.closed.Add(1)
		}
		dd = decided{Verdict: VerdictClose, prefix: prefix}
	} else {
		dd = a.decide(ctx, owner, info, prefix, deadline, more, records, false, func(d Decision) Decision {
			return a.planPipeDecision(owner, info, d)
		})
	}
	d = dd.Decision
	prefix = dd.prefix
	switch d.Verdict {
	case VerdictAccept:
		return withPrefix(p, prefix[d.Skip:]), nil, d
	case VerdictFallback, VerdictTakeover:
		return p, prefix, d
	case VerdictStall:
		return PipeIO{}, nil, d
	default: // VerdictClose
		closePipe(p)
		return PipeIO{}, nil, Decision{Verdict: VerdictClose}
	}
}

// planPipeDecision reserves handoff accounting until the host receives the carrier.
func (a *Arrival) planPipeDecision(owner *attachment, info *CarrierInfo, d Decision) Decision {
	if d.Verdict == VerdictFallback || d.Verdict == VerdictTakeover {
		if a.l.cfg.Fallback != nil {
			owner.handoff = &admissionHandoff{decision: d, info: info}
			owner.handoff.prepare(a.l, true)
			return d
		}
		d.Verdict = VerdictStall
	}
	return a.count(d)
}

// pendingHandshake is the HandshakeContext of p's half whose own
// handshake has not run (a Handshaker; a *tls.Conn only while its
// handshake is incomplete), nil without one. The half stays p's.
func pendingHandshake(p PipeIO) func(context.Context) error {
	for _, x := range []any{p.R, p.W} {
		x = unprefixed(x)
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
// goes on to fallback with that prefix. Failed marks an interrupted read
// or a failure in the carrier callback.
type peekMore func(have []byte, need int) (prefix []byte, fired, resumable, failed bool)

// peekPipe reads p by deadline until have holds n bytes, and returns
// them (fewer when the carrier ends or ctx ends, which interrupts the
// read); fired: the deadline passed first.
// Then resumable: fallbackOnTimeout is set and p's read
// deadline is cleared again, so that prefix, the bytes read, is the carrier's
// to fall back with; otherwise the carrier is closed.
func (a *Arrival) peekPipe(ctx context.Context, p PipeIO, have []byte, n int, deadline time.Time, fallbackOnTimeout bool) (prefix []byte, fired, resumable, failed bool) {
	var deadlined bool
	done := make(chan struct{})
	t := a.l.clk.AfterFunc(deadline.Sub(a.l.clk.Now()), func() {
		defer close(done)
		fired = true
		deadlined = a.interruptRead(p)
	})
	stop := interruptOnCancel(ctx, func() { a.interruptRead(p) })
	err := hostCall(a.l.cfg.Logger, "admission read", func() error { prefix = peek(p.R, have, n); return nil })
	if err != nil {
		closePipe(p)
	}
	interrupted := !stop() // ctx ended: the read may have been cut short
	if t.Stop() {
		if interrupted || err != nil {
			closePipe(p)
			return nil, false, false, true
		}
		return prefix, false, false, false
	}
	<-done // the timer's function has run
	if err != nil || interrupted {
		return nil, true, false, false
	}
	complete := len(prefix) >= n // the prefix arrived in time after all
	if deadlined && (complete || fallbackOnTimeout) {
		if d, ok := p.R.(interface{ SetReadDeadline(time.Time) error }); ok && d.SetReadDeadline(time.Time{}) == nil {
			return prefix, !complete, !complete, false
		}
	}
	closePipe(p)
	return nil, true, false, false
}

// peekMessage reads the first message of a message-framed p by deadline,
// at most limit; tooLong: it held more (read no further than limit+1
// bytes); failed: there is no first message to decide on (the carrier
// ended or failed, sent a message that is not binary, has no
// MessageReader, or ctx ended, which interrupts the read); an empty first
// message is failed too.
func (a *Arrival) peekMessage(ctx context.Context, p PipeIO, limit int, deadline time.Time) (msg []byte, tooLong, failed, fired bool) {
	mr, ok := p.R.(MessageReader)
	if !ok {
		return nil, false, true, false
	}
	done := make(chan struct{})
	t := a.l.clk.AfterFunc(deadline.Sub(a.l.clk.Now()), func() {
		defer close(done)
		a.interruptRead(p)
	})
	stop := interruptOnCancel(ctx, func() { a.interruptRead(p) })
	err := hostCall(a.l.cfg.Logger, "admission message", func() error {
		var readErr error
		msg, readErr = mr.ReadMessage(limit)
		return readErr
	})
	interrupted := !stop() // ctx ended: the read may have been cut short
	if !t.Stop() {
		<-done // it fired: the carrier's reads have ended
		return nil, false, false, true
	}
	switch {
	case interrupted:
		return nil, false, true, false
	case errors.Is(err, ErrMessageTooLarge) || err == nil && len(msg) > limit:
		return nil, true, false, false
	case err != nil, len(msg) == 0:
		// An empty first message is no prefix: a takeover could not
		// replay it, so the carrier closes without a decision.
		return nil, false, true, false
	}
	return bytes.Clone(msg), false, false, false
}

// interruptRead ends a blocked read of p: a passed read deadline where
// the receiving half takes one (deadlined), a close otherwise.
func (a *Arrival) interruptRead(p PipeIO) (deadlined bool) {
	if d, ok := p.R.(interface{ SetReadDeadline(time.Time) error }); ok && hostCall(a.l.cfg.Logger, "SetReadDeadline", func() error { return d.SetReadDeadline(time.Now()) }) == nil {
		return true
	}
	closePipe(p)
	return false
}

// decided is a final Decision with the prefix it was taken on.
type decided struct {
	prefix []byte
	Decision
}

// decide calls Decide with ctx ending at deadline, outside every lock,
// again after each VerdictMore with the longer prefix that more reads (nil:
// VerdictMore is VerdictClose), bounds the result and counts the outcome
// (a stall's aggregate count is stallSlot's); whole: the prefix is a
// message, replayed whole or not at all. With deferrable (an upgrade's
// head) VerdictDefer is returned as it is, uncounted: the decision is
// deferred to the carrier; elsewhere it is VerdictClose. A decision not
// returned in time is VerdictClose.
func (a *Arrival) decide(ctx context.Context, owner *attachment, info *CarrierInfo, prefix []byte, deadline time.Time,
	more peekMore, whole, deferrable bool, record func(Decision) Decision) decided {
	l := a.l
	dctx, cancel := timeshim.WithDeadline(ctx, l.clk, deadline)
	defer cancel()
	ended := false // the carrier ended short of a Need
	for {
		d, ok := a.callDecide(dctx, owner, info, prefix)
		if !ok {
			if errors.Is(dctx.Err(), context.DeadlineExceeded) {
				l.st.adm.peekTimedOut.Add(1)
			} else {
				l.st.adm.closed.Add(1)
			}
			return decided{Verdict: VerdictClose, prefix: prefix}
		}
		if d.Verdict == VerdictDefer {
			if deferrable {
				return decided{Verdict: VerdictDefer, prefix: prefix}
			}
			return decided{Decision: a.count(Decision{Verdict: VerdictClose, Variant: d.Variant}), prefix: prefix}
		}
		if d.Verdict != VerdictMore {
			d = d.bound(len(prefix))
			if d.Verdict == VerdictTakeover && l.cfg.Fallback == nil {
				d.Verdict = VerdictStall
			}
			if whole && d.Verdict == VerdictAccept && d.Skip != 0 && d.Skip != len(prefix) {
				// A message is replayed whole or not at all.
				d = Decision{Verdict: VerdictClose, Variant: d.Variant}
			}
			return decided{Decision: record(d), prefix: prefix}
		}
		if more == nil || ended || d.Need <= len(prefix) || d.Need > l.cfg.Admission.r.maxPeek {
			return decided{Decision: a.count(Decision{Verdict: VerdictClose, Variant: d.Variant}), prefix: prefix}
		}
		next, fired, resumable, failed := more(prefix, d.Need)
		if failed {
			return decided{Decision: a.count(Decision{Verdict: VerdictClose, Variant: d.Variant}), prefix: prefix}
		}
		if fired {
			if resumable {
				return decided{Decision: record(Decision{Verdict: VerdictFallback, Variant: d.Variant}), prefix: next}
			}
			l.st.adm.peekTimedOut.Add(1)
			return decided{Verdict: VerdictClose, prefix: prefix}
		}
		prefix, ended = next, len(next) < d.Need
	}
}

// callDecide runs Decide once; ok false: dctx ended first (a decision
// returned after that is not used, however the select chose).
func (a *Arrival) callDecide(dctx context.Context, owner *attachment, info *CarrierInfo, prefix []byte) (Decision, bool) {
	if dctx.Err() != nil || a.l.stopCtx.Err() != nil {
		return Decision{}, false // ended before the call: Decide is not run
	}
	owner.reserve()
	res := make(chan Decision, 1)
	labels.Go(dctx, "admission", func() {
		d := Decision{Verdict: VerdictClose}
		defer func() { owner.release(); res <- d }()
		// A panicking decision is VerdictClose.
		safeLog(a.l.cfg.Logger, "Admission.Decide", func() { d = a.l.cfg.Admission.Decide(dctx, *info, prefix) })
	})
	var d Decision
	select {
	case d = <-res:
	case <-dctx.Done():
	}
	return d, dctx.Err() == nil && a.l.stopCtx.Err() == nil
}

// bound is d with the fields its verdict does not use cleared. Accept,
// Fallback and Takeover retain Value. An unknown
// verdict, or a Skip outside a prefix of n bytes, is VerdictClose.
func (d Decision) bound(n int) Decision {
	switch {
	case d.Verdict > VerdictTakeover || d.Verdict == VerdictMore || d.Verdict == VerdictDefer:
		return Decision{Verdict: VerdictClose, Variant: d.Variant}
	case d.Verdict == VerdictFallback || d.Verdict == VerdictTakeover:
		return Decision{Verdict: d.Verdict, Variant: d.Variant, Value: d.Value}
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
	if d.Verdict == VerdictAccept {
		a.mu.Lock()
		a.value, a.variant = d.Value, d.Variant
		a.mu.Unlock()
	}
	a.l.st.adm.count(d)
	return d
}

func (c *admissionCounters) count(d Decision) {
	switch d.Verdict {
	case VerdictAccept:
		c.accepted.Add(1)
	case VerdictFallback:
		c.fallback.Add(1)
	case VerdictTakeover:
		c.takeover.Add(1)
	case VerdictClose:
		c.closed.Add(1)
	case VerdictStall, VerdictMore, VerdictDefer, verdictFailed:
		// Stalls are counted when their slot is settled.
		return
	}
	c.variant(d.Variant, d.Verdict)
}

// stallPipe discards incoming bytes while the verdict holds its slot.
// A request or upgrade adapter can keep its outer transport alive by
// completing the stall synchronously.
func (a *Arrival) stallPipe(ctx context.Context, owner *attachment, variant string, async bool) {
	p := owner.pipe
	if !owner.stall(ctx, a.l.stallDuration(a.key), &variant, async && p.R != nil, func(ctx context.Context) {
		defer closePipe(p)
		if p.R == nil {
			return
		}
		ended := make(chan struct{})
		stop := context.AfterFunc(ctx, func() { defer close(ended); closePipe(p) })
		defer func() {
			if !stop() {
				<-ended
			}
		}()
		safeLog(a.l.cfg.Logger, "stall read", func() {
			if mr, ok := p.R.(MessageReader); ok && p.Framing == FramingRecords {
				for {
					if _, err := mr.ReadMessage(MaxRecordSize); err != nil {
						break
					}
				}
			} else {
				discard(p.R)
			}
			if cw, ok := p.W.(CloseWriter); ok {
				_ = cw.CloseWrite()
			}
		})
	}) {
		closePipe(p)
	}
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

// ---- HTTP requests as carriers ----

// AdmitUpgrade takes r, the request of an HTTP upgrade (WebSocket,
// WebTransport), as a carrier of l and runs l's admission on its head,
// before anything is written to w: the decision gets no prefix,
// CarrierInfo.Stage is StageHead and Upgrade is set, and the body is not
// read. Its source is what
// trusted resolves (TrustedProxies.RequestSource). A request that an
// intermediary forwarded from TLS early data (Early-Data: 1, RFC 8470), or
// read before the TLS handshake completed (0-RTT on HTTP/3), is answered
// 425 (Too Early) first and is no carrier (EarlyRequest). On VerdictAccept
// (or without an Admission), on VerdictTakeover with a byte-stream target,
// and on VerdictDefer, which defers the
// decision to the carrier, it returns the arrival and true: the caller
// completes the upgrade and takes its first carrier through
// Arrival.AdmitFirst, which also takes a deferred decision, and hands it
// on with Arrival.HandlePipe. Otherwise it has answered r
// (VerdictFallback, or a carrier the listener refuses, with fallback;
// nil: http.NotFound) or ended it (VerdictStall, VerdictClose, which abort
// the request with http.ErrAbortHandler), and the caller returns.
func (a *ListenerAdapter) AdmitUpgrade(w http.ResponseWriter, r *http.Request, trusted *TrustedProxies, fallback http.Handler) (*Arrival, bool) {
	return (*Listener)(a).admitRequest(w, r, trusted, reqUpgrade, fallback)
}

// AdmitRequest is AdmitUpgrade for a request whose body is the carrier:
// the decision's prefix is the start of the body (none for a request
// without one, decided on its head), and on VerdictAccept r.Body
// continues after the skipped bytes. On VerdictFallback r.Body replays
// the bytes read. A configured HTTP fallback receives the original request
// with that prefix and the unread body, before any carrier response. A nil
// HTTP fallback serves http.NotFound. VerdictTakeover selects the byte-stream
// target after the adapter commits the accepted response.
func (a *ListenerAdapter) AdmitRequest(w http.ResponseWriter, r *http.Request, trusted *TrustedProxies, fallback http.Handler) (*Arrival, bool) {
	return (*Listener)(a).admitRequest(w, r, trusted, reqBody, fallback)
}

// reqKind is what of an HTTP request is its carrier.
type reqKind uint8

const (
	reqHead    reqKind = iota // nothing after the head (an exchange carrier)
	reqBody                   // the body (AdmitRequest)
	reqUpgrade                // the connection after an upgrade (AdmitUpgrade)
)

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

// admitRequest is AdmitUpgrade and AdmitRequest.
func (l *Listener) admitRequest(w http.ResponseWriter, r *http.Request, trusted *TrustedProxies, kind reqKind, fallback http.Handler) (*Arrival, bool) {
	if EarlyRequest(r) {
		w.WriteHeader(http.StatusTooEarly)
		l.countSetupFailure() // it needs the handshake: no pipe input
		return nil, false
	}
	a, err := l.arrive(trusted.RequestSource(r))
	if err != nil {
		// Refused (its source holds its share, or the listener is
		// closed): nothing was read or written, the host serves it.
		httpx.ServeFallback(fallback, w, r)
		return nil, false
	}
	if ok, _ := a.decideRequest(w, r, kind, fallback); !ok {
		return nil, false
	}
	return a, true
}

func (a *Arrival) requestAttachment() *attachment {
	a.mu.Lock()
	defer a.mu.Unlock()
	if owner := a.moveLocked(); owner != nil {
		return owner
	}
	return newAttachment(a.l, false) // listener stop already released it
}

// DecideRequest runs the admission of the arrival of r, a request taken
// as a carrier on its head (see AdmitUpgrade), for a host that checks the
// request between Listener.Arrive and the decision. On VerdictFallback it
// serves r with fallback (nil: http.NotFound); on VerdictStall and
// VerdictClose it aborts the request with http.ErrAbortHandler. It returns
// true without an Admission, on VerdictAccept, or on VerdictTakeover when
// the listener has a byte-stream target.
func (a *Arrival) DecideRequest(w http.ResponseWriter, r *http.Request, fallback http.Handler) bool {
	ok, _ := a.decideRequest(w, r, reqHead, fallback)
	return ok
}

// decideRequest is DecideRequest, the decision on the start of the body
// (reqBody, see AdmitRequest) and on an upgrade's head, which may defer
// it (reqUpgrade, see AdmitUpgrade).
func (a *Arrival) decideRequest(w http.ResponseWriter, r *http.Request, kind reqKind, fallback http.Handler) (bool, error) {
	l := a.l
	adm := l.cfg.Admission
	if l.stopCtx.Err() != nil || r.Context().Err() != nil {
		a.Release()
		if err := r.Context().Err(); err != nil {
			return false, err
		}
		return false, net.ErrClosed
	}
	a.mu.Lock()
	owner := a.first
	reserved := owner != nil && owner.reserve()
	a.mu.Unlock()
	if !reserved {
		return false, net.ErrClosed
	}
	defer owner.release()
	if adm == nil {
		return true, nil
	}
	ctx, cancel := l.stopContext(r.Context())
	defer cancel()
	deadline := l.clk.Now().Add(adm.r.peekTimeout)
	// A request without a body is decided on its head: one known to have
	// none, or one whose body ends before its first byte.
	head := kind != reqBody || r.Body == nil || r.Body == http.NoBody
	var prefix []byte
	var more peekMore
	if r.Body != nil && r.Body != http.NoBody {
		body := r.Body
		closeBody := sync.OnceFunc(func() { carrierio.CloseHalf(body) })
		interrupt := func(closing bool) {
			err := hostCall(l.cfg.Logger, "HTTP read deadline", func() error {
				return http.NewResponseController(w).SetReadDeadline(time.Now())
			})
			if closing || err != nil {
				closeBody()
			}
		}
		stop := interruptOnCancel(ctx, func() { interrupt(true) })
		defer stop()
		if !head && adm.Peek > 0 {
			var readErr error
			more = func(have []byte, need int) ([]byte, bool, bool, bool) {
				done := make(chan struct{})
				timer := l.clk.AfterFunc(deadline.Sub(l.clk.Now()), func() {
					defer close(done)
					interrupt(false)
				})
				b := make([]byte, need)
				n := copy(b, have)
				err := hostCall(l.cfg.Logger, "HTTP body read", func() error {
					var added int
					added, readErr = io.ReadFull(body, b[n:])
					n += added
					return nil
				})
				if !timer.Stop() {
					<-done
					return nil, true, false, false
				}
				if err != nil {
					return nil, false, false, true
				}
				return b[:n], false, false, false
			}
			var fired, failed bool
			prefix, fired, _, failed = more(nil, adm.Peek)
			if failed {
				l.st.adm.closed.Add(1)
				a.Release()
				panic(http.ErrAbortHandler)
			}
			if fired {
				l.st.adm.peekTimedOut.Add(1)
				a.Release()
				panic(http.ErrAbortHandler)
			}
			head = len(prefix) == 0 && readErr == io.EOF
		}
	}
	upgrade := kind == reqUpgrade
	// The decision can outlive the handler after a timeout. Give it only
	// request metadata, and count its work against this reservation.
	metadata := r.Clone(r.Context())
	metadata.Body, metadata.GetBody, metadata.Response = http.NoBody, nil, nil
	info := &CarrierInfo{Source: a.src, Peer: a.peer, TLS: r.TLS, Request: metadata, Upgrade: upgrade}
	if head {
		info.Stage = StageHead
	}
	record := func(d Decision) Decision {
		takeover := d.Verdict == VerdictTakeover
		if d.Verdict == VerdictFallback {
			return d
		}
		if d.Verdict != VerdictAccept && !takeover {
			return a.count(d)
		}
		request := &admissionHandoff{decision: d}
		if takeover {
			request.info = info
		}
		a.mu.Lock()
		parked := a.first == owner
		if parked {
			owner.handoff = request
			if !takeover {
				a.accepted = true
				a.value, a.variant = d.Value, d.Variant
			}
		}
		a.mu.Unlock()
		if !parked {
			request.prepare(a.l, false)
		}
		return d
	}
	dd := a.decide(ctx, owner, info, prefix, deadline, more, false, upgrade, record)
	d, prefix := dd.Decision, dd.prefix
	if ctx.Err() != nil || l.stopCtx.Err() != nil {
		if d.Verdict == VerdictFallback || d.Verdict == VerdictStall {
			l.st.adm.count(Decision{Verdict: VerdictClose, Variant: d.Variant})
		}
		a.Release()
		panic(http.ErrAbortHandler)
	}
	switch d.Verdict {
	case VerdictDefer: // the decision is the carrier's (AdmitFirst, HandlePipe)
		dfr := &deferral{info: *info, deadline: deadline}
		dfr.info.Stage = StageCarrier
		a.mu.Lock()
		a.deferred = dfr
		a.mu.Unlock()
		return true, nil
	case VerdictAccept, VerdictTakeover:
		// Only Accept can skip prefix bytes. Takeover replays the whole prefix.
		if rest := prefix[d.Skip:]; len(rest) > 0 {
			r.Body = httpx.ReplayBody(rest, r.Body, r.Body)
		}
		return true, nil
	case VerdictFallback:
		if len(prefix) > 0 {
			r.Body = httpx.ReplayBody(prefix, r.Body, r.Body)
		}
		owner := a.requestAttachment()
		defer owner.release()
		delivered := false
		defer func() {
			if !delivered {
				l.st.adm.count(Decision{Verdict: VerdictClose, Variant: d.Variant})
			}
		}()
		if owner.commitFallback(ctx) == nil {
			// Keep the request headers, streaming body and response writer.
			// Close and Shutdown also cancel this delegated HTTP request.
			stop := context.AfterFunc(l.shutdownCtx, cancel)
			defer stop()
			if l.shutdownCtx.Err() != nil {
				cancel()
			}
			httpx.ServeFallback(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				delivered = true
				if fallback == nil {
					l.st.adm.stalled.Add(1)
					l.st.adm.variant(d.Variant, VerdictStall)
					http.NotFound(w, r)
				} else {
					l.st.adm.count(d)
					fallback.ServeHTTP(w, r)
				}
			}), w, httpx.DelegateRequest(ctx, r))
		}
	case VerdictStall:
		owner := a.requestAttachment()
		owner.stall(ctx, l.stallDuration(a.key), &d.Variant, false, func(ctx context.Context) { <-ctx.Done() })
		owner.release()
		panic(http.ErrAbortHandler)
	default:
		a.Release()
		panic(http.ErrAbortHandler)
	}
	return false, nil
}
