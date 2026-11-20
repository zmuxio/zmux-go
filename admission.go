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
	// decision (AdmissionStats.TooLarge). Zero is MaxRecordSize, the
	// largest record (the most a zmux first record can be); a host that
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
// (ListenerConfig.Fallback): on VerdictFallback, or on a prefix that
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
	// Value is Decision.Value of the VerdictFallback that handed the
	// carrier over (nil after FallbackOnPeekTimeout): the host's routing
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
	// VerdictFallback hands the carrier, with every byte or message read
	// replayed and Decision.Value, to ListenerConfig.Fallback (a Takeover;
	// without a Fallback it is stalled). The stage decides what an HTTP
	// request is: decided on its head (StageHead: an upgrade's head, an
	// exchange request, a request without a body) it is a request, served
	// by zmuxhttp.Options.Next (without one: the server's not-found
	// response); decided on its start as a carrier (StageCarrier: the
	// carrier of an upgrade after VerdictDefer, the body of a request
	// carrier, AdmitRequest) it is a carrier, taken over by
	// ListenerConfig.Fallback; a request carrier's Takeover is the full
	// duplex byte stream of its request body (Pipe's R, the bytes read
	// replayed) and its response body (Pipe's W), the response head
	// written by zmux. Without a ListenerConfig.Fallback such a request,
	// to which nothing was written, is served as a request by
	// zmuxhttp.Options.Next.
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
)

// verdictFailed is the outcome of a carrier whose outer handshake failed:
// it reached no decision and is closed.
const verdictFailed AdmissionVerdict = 255

// Decision is what Admission.Decide returns.
type Decision struct {
	// Value is the host's value for an accepted carrier or one it takes
	// over (VerdictAccept and VerdictFallback; ignored otherwise): zmux
	// hands it on without looking at it, on VerdictAccept to WrapPipe
	// (PipeHint.Admitted) and to the session (AdmittedAs) or pipe
	// (AdmittedAs) the carrier makes, holding it until their state is
	// released, and on VerdictFallback to ListenerConfig.Fallback
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
	// Name is the Decision.Variant ("" for none; also "" for the
	// Overflow bucket).
	Name string
	// Overflow marks the bucket of the variants past MaxAdmissionVariants.
	Overflow bool

	Accepted, Fallback uint64
	// Stalled counts VerdictStall decisions, StallOverflow included.
	Stalled uint64
	// Closed counts VerdictClose and the carriers closed without a
	// decision of the variant (AdmissionStats.TooLarge: variant "").
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
	// TooLarge counts message-framed carriers closed because their first
	// message held more than Admission.MaxMessage bytes, without a
	// decision.
	TooLarge uint64
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
	accepted, fallback, stalled, closed, stallOverflow, tooLarge, peekTimedOut, handshakeFailed atomic.Uint64

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

	value    any               // Decision.Value of that acceptance
	slot     *core.PendingSlot // the slots Arrive took, until handed on
	deferred *deferral         // a head decision deferred to the first carrier
	takeover *pendingTakeover  // a request carrier's VerdictFallback, taken over by Admit
	variant  string            // Decision.Variant of that acceptance
	mu       sync.Mutex
	accepted bool // the admission accepted the carrier
	detached bool // the host detached the carrier it took over
}

// pendingTakeover is the VerdictFallback of a request carrier decided on
// its start (AdmitRequest), which the carrier the adapter then builds of
// the request is taken over with (Admit).
type pendingTakeover struct {
	value any
	info  CarrierInfo
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
	return &Arrival{l: l, src: source, peer: peer, key: key, slot: slot}, nil
}

// enter takes the slots of a carrier from the source of key, counting a
// refusal; the error is net.ErrClosed, errRefused (the source's share) or
// errRejected (every slot taken, none stalled).
func (l *Listener) enter(key netip.Prefix) (*core.PendingSlot, error) {
	l.pmu.Lock()
	closed := l.pclosed
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
	slot := a.slot
	a.slot = nil
	dfr := a.deferred
	a.deferred = nil
	a.takeover = nil
	a.mu.Unlock()
	if dfr != nil {
		// A deferred decision that never came: its carrier did not arrive
		// in time (PeekTimedOut) or ended first, also by the listener's
		// stop (Closed).
		if !a.l.clk.Now().Before(dfr.deadline) {
			a.l.st.adm.peekTimedOut.Add(1)
		} else {
			a.l.st.adm.closed.Add(1)
			a.l.st.adm.variant("", VerdictClose)
		}
	}
	a.l.leave(slot)
}

// deferralContext is ctx, also ended when the listener stops taking
// carriers and, while the arrival's decision is deferred to its carrier,
// at that decision's deadline.
func (a *Arrival) deferralContext(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := a.l.stopContext(ctx)
	a.mu.Lock()
	dfr := a.deferred
	a.mu.Unlock()
	if dfr == nil {
		return ctx, cancel
	}
	dctx, dcancel := timeshim.WithDeadline(ctx, a.l.clk, dfr.deadline)
	return dctx, func() { dcancel(); cancel() }
}

// AdmitFirst is Admit for an adapter whose first carrier comes after the
// arrival's head decision (an HTTP upgrade): first completes the upgrade
// or waits for the carrier (the first stream of a WebTransport session)
// and returns it. While the decision is deferred (VerdictDefer) first
// runs under a context that also ends at the decision's PeekTimeout and
// when the listener stops taking carriers; a carrier that does not come
// in time is counted (AdmissionStats.PeekTimedOut, or Closed on the
// listener's stop). An error of first otherwise is the failure of the
// arrival's outer handshake (HandshakeFailed), unless ctx ended. On an
// error the arrival is released and the adapter closes what it
// upgraded. The carrier then passes the admission as with Admit: on
// VerdictAccept, and when the head was accepted already, AdmitFirst
// returns it and true, to be handed on with HandlePipe.
func (a *Arrival) AdmitFirst(ctx context.Context, first func(context.Context) (PipeIO, error)) (PipeIO, bool) {
	fctx, cancel := a.deferralContext(ctx)
	p, err := first(fctx)
	ended := fctx.Err() != nil
	cancel()
	if err != nil {
		if p.R != nil || p.W != nil {
			closePipe(p)
		}
		if ended {
			a.Release() // counts a deferral that the bound or ctx ended
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
	a.deferred = nil // counted as the handshake's failure, not as a decision
	a.mu.Unlock()
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
	l.pmu.Lock()
	l.pending.Leave(slot)
	l.pmu.Unlock()
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
	p = owned(p) // the peeks, their timers, a stall and the verdict close it through one closer
	l := a.l
	if err := p.validate(); err != nil {
		a.Release()
		closePipe(p)
		l.countSetupFailure()
		return err
	}
	q, ok := a.Admit(ctx, p)
	if !ok {
		return errRejected
	}
	run, err := a.Take(ctx, q, hint)
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
	p = owned(p) // the peeks, their timers, a stall and the verdict close it through one closer
	l := a.l
	if !a.isAccepted() {
		closePipe(p)
		return nil, errRejected
	}
	slot, err := a.takeSlot()
	if err != nil {
		if errors.Is(err, errRefused) && !l.isClosed() {
			return nil, l.refusePending(ctx, p, a.src, a.peer)
		}
		closePipe(p)
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
// arrival, for an adapter that must know the outcome before it hands
// further carriers on (a QUIC connection, a WebTransport session): on
// VerdictAccept it returns p without the skipped bytes and true, to be
// handed on with HandlePipe or Take. Otherwise it has acted on the
// verdict and returns false: VerdictFallback hands p, with every byte or
// message read replayed, to ListenerConfig.Fallback (without one the
// carrier is stalled) and returns once the fallback did (Detached reports
// whether the host kept the carrier), VerdictStall
// stalls it and VerdictClose closes it. Without an Admission, or once the
// arrival is accepted, it returns p and true. HandlePipe is Admit, Take
// and the pipe's run in one.
func (a *Arrival) Admit(ctx context.Context, p PipeIO) (PipeIO, bool) {
	p = owned(p) // the peeks, their timers, a stall and the verdict close it through one closer
	a.mu.Lock()
	tk := a.takeover
	a.takeover = nil
	a.mu.Unlock()
	if tk != nil {
		// A request carrier decided on its start (AdmitRequest).
		a.fallback(ctx, p, nil, &tk.info, tk.value)
		return PipeIO{}, false
	}
	if a.isAccepted() {
		return p, true
	}
	q, prefix, info, value, v := a.admitPipe(ctx, p)
	switch v {
	case VerdictAccept:
		a.accept()
		return q, true
	case VerdictFallback:
		a.fallback(ctx, p, prefix, info, value)
	default:
		// VerdictStall, VerdictClose and a failed handshake: admitPipe
		// has acted.
	}
	return PipeIO{}, false
}

// fallback hands p, with prefix replayed, to ListenerConfig.Fallback as a
// Takeover and closes it when the call returns unless the host detached
// it, the arrival's slots given back first; without a Fallback it stalls
// p (not counted as a stall: the decision was VerdictFallback). A panic of
// the fallback closes the carrier, not the process (a detached one stays
// the host's): a peer reaches it with any non-zmux bytes.
func (a *Arrival) fallback(ctx context.Context, p PipeIO, prefix []byte, info *CarrierInfo, value any) {
	l := a.l
	if l.cfg.Fallback == nil {
		a.stallPipe(ctx, p, false)
		return
	}
	a.Release()
	markTaken(p) // nothing more is replayed once the host has it
	o := newHalfOwner(p)
	if !releasePipe(p, o.closeAll) {
		return // closed already (the listener's stop)
	}
	t := &Takeover{Info: *info, Value: value, owner: o}
	t.pipe, t.conn = takeoverPipe(p, prefix, o)
	// The call's context ends with the call, at every entry point: a host
	// that serves a detached carrier on uses a context of its own.
	fctx, cancel := context.WithCancel(ctx)
	_ = hostCall(l.cfg.Logger, "ListenerConfig.Fallback", func() error { l.cfg.Fallback(fctx, t); return nil })
	cancel()
	if t.returned() {
		a.mu.Lock()
		a.detached = true
		a.mu.Unlock()
		return
	}
	o.closeAll() // the halves the host has not closed
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
	return ctx, func() { stop(); cancel() }
}

// admitPipe completes p's outer TLS handshake, reads its prefix and takes
// the decision, or the decision an upgrade's head deferred to it. On
// VerdictAccept q is p without the skipped bytes; on VerdictFallback p is
// untouched, prefix what was read, info the carrier's metadata and value
// the decision's Value;
// otherwise p is closed (VerdictStall: the decision's stall has run).
func (a *Arrival) admitPipe(ctx context.Context, p PipeIO) (q PipeIO, prefix []byte, info *CarrierInfo, value any, v AdmissionVerdict) {
	l := a.l
	adm := l.cfg.Admission
	ctx, cancel := l.stopContext(ctx)
	defer cancel()
	if handshake := pendingHandshake(p); handshake != nil {
		hctx, hcancel := timeshim.WithTimeout(ctx, l.clk, l.cfg.Config.handshakeTimeout())
		err := handshake(hctx)
		hcancel()
		if err != nil {
			closePipe(p)
			l.st.adm.handshakeFailed.Add(1)
			a.Release()
			return PipeIO{}, nil, nil, nil, verdictFailed
		}
	}
	a.mu.Lock()
	dfr := a.deferred
	a.deferred = nil
	a.mu.Unlock()
	deadline := l.clk.Now().Add(adm.r.peekTimeout)
	info = &CarrierInfo{Source: a.src, Peer: a.peer}
	if cs, ok := TerminatedTLS(p); ok {
		info.TLS = &cs
	}
	if dfr != nil {
		info, deadline = &dfr.info, dfr.deadline
	}
	info.Framing = p.Framing
	var more peekMore
	records := p.Framing == FramingRecords
	tooLong := false
	if (adm.Peek > 0 || records && dfr != nil) && p.R != nil {
		var fired, resumable, failed bool
		if records {
			prefix, tooLong, failed, fired = a.peekMessage(ctx, p, adm.r.maxMessage, deadline)
		} else {
			prefix, fired, resumable = a.peekPipe(ctx, p, prefix, adm.Peek, deadline)
			more = func(have []byte, need int) ([]byte, bool, bool) { return a.peekPipe(ctx, p, have, need, deadline) }
		}
		if fired {
			l.st.adm.peekTimedOut.Add(1)
			if !resumable {
				closePipe(p)
				a.Release()
				return PipeIO{}, nil, nil, nil, VerdictClose
			}
			a.count(Decision{Verdict: VerdictFallback})
			return p, prefix, info, nil, VerdictFallback
		}
		if failed {
			// Nothing to decide on, and nothing a fallback could be given
			// back whole.
			a.count(Decision{Verdict: VerdictClose})
			closePipe(p)
			a.Release()
			return PipeIO{}, nil, nil, nil, VerdictClose
		}
	}
	var dd decided
	if tooLong {
		l.st.adm.tooLarge.Add(1)
		l.st.adm.variant("", VerdictClose)
		dd = decided{Decision{Verdict: VerdictClose}, prefix}
	} else {
		dd = a.decide(ctx, info, prefix, deadline, more, records, false)
	}
	d := dd.Decision
	prefix = dd.prefix
	switch d.Verdict {
	case VerdictAccept:
		return withPrefix(p, prefix[d.Skip:]), nil, nil, nil, VerdictAccept
	case VerdictFallback:
		return p, prefix, info, d.Value, VerdictFallback
	case VerdictStall:
		a.stallPipe(ctx, p, true)
		return PipeIO{}, nil, nil, nil, VerdictStall
	default: // VerdictClose
		closePipe(p)
		a.Release()
		return PipeIO{}, nil, nil, nil, VerdictClose
	}
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
// goes on to fallback with that prefix (otherwise it is closed).
type peekMore func(have []byte, need int) (prefix []byte, fired, resumable bool)

// peekPipe reads p by deadline until have holds n bytes, and returns
// them (fewer when the carrier ends or ctx ends, which interrupts the
// read); fired: the deadline passed first.
// Then resumable: Admission.FallbackOnPeekTimeout is set and p's read
// deadline is cleared again, so that prefix, the bytes read, is the carrier's
// to fall back with; otherwise the carrier is closed.
func (a *Arrival) peekPipe(ctx context.Context, p PipeIO, have []byte, n int, deadline time.Time) (prefix []byte, fired, resumable bool) {
	var deadlined bool
	done := make(chan struct{})
	t := a.l.clk.AfterFunc(deadline.Sub(a.l.clk.Now()), func() {
		defer close(done)
		fired = true
		deadlined = a.interruptRead(p)
	})
	stop := context.AfterFunc(ctx, func() { a.interruptRead(p) })
	prefix = peek(p.R, have, n)
	interrupted := !stop() // ctx ended: the read may have been cut short
	if t.Stop() {
		if interrupted {
			// The carrier is no longer whole: close it; the decision
			// that follows sees ctx ended and is VerdictClose.
			closePipe(p)
			return nil, false, false
		}
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
	closePipe(p)
	return nil, true, false
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
	stop := context.AfterFunc(ctx, func() { a.interruptRead(p) })
	msg, err := mr.ReadMessage(limit)
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
	if d, ok := p.R.(interface{ SetReadDeadline(time.Time) error }); ok && d.SetReadDeadline(time.Now()) == nil {
		return true
	}
	closePipe(p)
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
// message, replayed whole or not at all. With deferrable (an upgrade's
// head) VerdictDefer is returned as it is, uncounted: the decision is
// deferred to the carrier; elsewhere it is VerdictClose. A decision not
// returned in time is VerdictClose.
func (a *Arrival) decide(ctx context.Context, info *CarrierInfo, prefix []byte, deadline time.Time,
	more peekMore, whole, deferrable bool) decided {
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
		if d.Verdict == VerdictDefer {
			if deferrable {
				return decided{Decision{Verdict: VerdictDefer}, prefix}
			}
			return decided{a.count(Decision{Verdict: VerdictClose, Variant: d.Variant}), prefix}
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
	if dctx.Err() != nil {
		return Decision{}, false // ended before the call: Decide is not run
	}
	res := make(chan Decision, 1)
	labels.Go(dctx, "admission", func() {
		d := Decision{Verdict: VerdictClose}
		defer func() { res <- d }()
		// A panicking decision is VerdictClose.
		safeLog(a.l.cfg.Logger, "Admission.Decide", func() { d = a.l.cfg.Admission.Decide(dctx, *info, prefix) })
	})
	var d Decision
	select {
	case d = <-res:
	case <-dctx.Done():
	}
	return d, dctx.Err() == nil
}

// bound is d with the fields its verdict does not use cleared (Value is
// VerdictAccept's and VerdictFallback's); an unknown
// verdict, or a Skip outside a prefix of n bytes, is VerdictClose.
func (d Decision) bound(n int) Decision {
	switch {
	case d.Verdict >= VerdictMore:
		return Decision{Verdict: VerdictClose, Variant: d.Variant}
	case d.Verdict == VerdictFallback:
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
	l := a.l
	switch d.Verdict {
	case VerdictAccept:
		a.mu.Lock()
		a.value, a.variant = d.Value, d.Variant
		a.mu.Unlock()
		l.st.adm.accepted.Add(1)
	case VerdictFallback:
		l.st.adm.fallback.Add(1)
	case VerdictClose:
		l.st.adm.closed.Add(1)
	case VerdictStall, VerdictMore, VerdictDefer, verdictFailed:
		// A stall's aggregate is counted by stallSlot; More is never
		// final: decide turns it into a longer prefix or a close, and
		// bound turns a failure into a close.
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
	l.pmu.Lock()
	ok = !l.pclosed && l.pending.Stall(slot, evict)
	l.pmu.Unlock()
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
		l.pmu.Lock()
		l.pending.Unstall(slot)
		l.pmu.Unlock()
	}, true
}

// stallPipe stalls p: it discards what arrives until the peer ends it or
// the stall duration passes on the listener's clock or the listener
// stops taking carriers, then closes it gracefully; the arrival is
// released at the end.
// counted: the decision was VerdictStall, not a fallback without target.
func (a *Arrival) stallPipe(ctx context.Context, p PipeIO, counted bool) {
	defer a.Release()
	d, release, ok := a.stallSlot(counted, func() { a.interruptRead(p) })
	if !ok || p.R == nil {
		closePipe(p)
		if ok {
			release()
		}
		return
	}
	defer release()
	ctx, cancel := a.l.stopContext(ctx) // Listener.Close ends a stall
	defer cancel()
	t := a.l.clk.AfterFunc(d, func() { a.interruptRead(p) })
	stop := context.AfterFunc(ctx, func() { a.interruptRead(p) })
	if mr, ok := p.R.(MessageReader); ok && p.Framing == FramingRecords {
		for {
			if _, err := mr.ReadMessage(MaxRecordSize); err != nil {
				break
			}
		}
	} else {
		discard(p.R)
	}
	stop()
	t.Stop()
	if cw, ok := p.W.(CloseWriter); ok {
		_ = cw.CloseWrite()
	}
	closePipe(p)
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
// (or without an Admission), and on VerdictDefer, which defers the
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
// the bytes read. A VerdictFallback on the start of a body
// (StageCarrier), with a ListenerConfig.Fallback, takes the request over
// as a carrier: AdmitRequest returns the arrival and true, nothing is
// written, and the caller hands the carrier it builds of the request on
// with Arrival.HandlePipe (or Admit), which hands it to the Fallback as a
// Takeover instead of the listener (with the decision's Value).
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
		serveHTTP(fallback, w, r)
		return nil, false
	}
	if !a.decideRequest(w, r, kind, fallback) {
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
	return a.decideRequest(w, r, reqHead, fallback)
}

// decideRequest is DecideRequest, the decision on the start of the body
// (reqBody, see AdmitRequest) and on an upgrade's head, which may defer
// it (reqUpgrade, see AdmitUpgrade).
func (a *Arrival) decideRequest(w http.ResponseWriter, r *http.Request, kind reqKind, fallback http.Handler) bool {
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
	head := kind != reqBody || r.Body == nil || r.Body == http.NoBody
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
	upgrade := kind == reqUpgrade
	info := &CarrierInfo{Source: a.src, Peer: a.peer, TLS: r.TLS, Request: r, Upgrade: upgrade}
	if head {
		info.Stage = StageHead
	}
	dd := a.decide(ctx, info, prefix, deadline, more, false, upgrade)
	d, prefix := dd.Decision, dd.prefix
	switch d.Verdict {
	case VerdictDefer: // the decision is the carrier's (AdmitFirst, HandlePipe)
		dfr := &deferral{info: *info, deadline: deadline}
		dfr.info.Stage = StageCarrier
		a.mu.Lock()
		a.deferred = dfr
		a.mu.Unlock()
		return true
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
		if kind == reqBody && !head && l.cfg.Fallback != nil {
			// A carrier decided on its start: taken over as a carrier
			// (Admit), not served as a request.
			a.mu.Lock()
			a.takeover = &pendingTakeover{info: *info, value: d.Value}
			a.mu.Unlock()
			return true
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
