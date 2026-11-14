package zmux

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/zmuxio/zmux-go/internal/core"
	"github.com/zmuxio/zmux-go/varint"
)

// capsuleDatagram is the RFC 9297 DATAGRAM capsule type.
const capsuleDatagram = 0x00

// DefaultMaxPacket is the default PacketOptions.MaxPacket.
const DefaultMaxPacket = 65535

// packetChunk is the most a PacketStream reads from its stream at once.
const packetChunk = 64 << 10

// PacketPolicy chooses how a PacketStream sends a packet.
type PacketPolicy uint8

const (
	// PacketPreferDatagram sends a packet as a datagram when the stream has
	// datagrams and the packet fits the stream's MaxDatagramPayload at
	// that moment, otherwise as a capsule in the stream. A datagram that
	// the send queue or a limiter drops is lost; it is never resent as a
	// capsule.
	PacketPreferDatagram PacketPolicy = iota
	// PacketStreamOnly sends every packet as a capsule: in order and
	// reliably.
	PacketStreamOnly
	// PacketDatagramOnly sends every packet as a datagram; one that does
	// not fit fails with *DatagramTooLargeError, and without datagrams
	// every packet fails with ErrNotNegotiated.
	PacketDatagramOnly
)

// String returns the policy's name ("prefer datagram").
func (p PacketPolicy) String() string {
	switch p {
	case PacketPreferDatagram:
		return "prefer datagram"
	case PacketStreamOnly:
		return "stream only"
	case PacketDatagramOnly:
		return "datagram only"
	}
	return "PacketPolicy(?)"
}

// PacketOptions configure a PacketStream; the zero value is the default.
type PacketOptions struct {
	// MaxPacket is the largest payload, in bytes and without the Context
	// ID, that the stream sends and accepts (0: DefaultMaxPacket). Both
	// ends use the same value: a received capsule above it ends the stream
	// with ZMUX_PACKET_ERROR. Negative is invalid.
	MaxPacket int
	// Policy chooses how packets are sent (PacketPreferDatagram).
	Policy PacketPolicy
}

// Packet is a packet of a PacketStream.
type Packet struct {
	// Data is the payload.
	Data []byte
	// ContextID is the RFC 9297 Context ID (0 by default).
	ContextID uint64
	// Datagram reports that the packet arrived as a datagram rather than
	// as a capsule in the stream.
	Datagram bool
}

// PacketStats are the counts of a PacketStream.
type PacketStats struct {
	// SentDatagrams and SentCapsules count the packets sent each way.
	SentDatagrams, SentCapsules uint64
	// DroppedTooLarge counts the packets PacketDatagramOnly did not send
	// because they did not fit; DroppedLimited those the session's
	// datagram send limiter refused. Datagrams dropped later, from a full
	// send queue, are counted by the session (DatagramsDroppedSend).
	DroppedTooLarge, DroppedLimited uint64
	// ReceivedDatagrams and ReceivedCapsules count the packets received
	// each way; SkippedCapsules the capsules of other types skipped.
	ReceivedDatagrams, ReceivedCapsules, SkippedCapsules uint64
}

var (
	// ErrPacketTooLarge is returned for a packet above MaxPacket.
	ErrPacketTooLarge = errors.New("zmux: packet above MaxPacket")
	// ErrMalformedPacket is returned once a PacketStream received bytes
	// that are not a valid capsule sequence (a capsule above MaxPacket, or
	// the stream ending inside a capsule); the stream was reset and
	// stopped with ZMUX_PACKET_ERROR.
	ErrMalformedPacket = errors.New("zmux: malformed packet stream")

	errCapsuleOversized = fmt.Errorf("%w: capsule above the bound", ErrMalformedPacket)
	errCapsuleTruncated = fmt.Errorf("%w: stream ended inside a capsule", ErrMalformedPacket)
)

// PacketStream carries packets on a bidirectional stream (EXTENSIONS.md
// Section 12): each packet goes either as a datagram of the stream or as
// an RFC 9297 DATAGRAM capsule in its bytes, with the same Context ID and
// payload. Packets sent as datagrams may be lost or reordered relative to
// the others; those sent as capsules arrive in order. Capsules of other
// types are skipped.
//
// A PacketStream owns its stream: once wrapped, the stream's bytes and
// datagrams are read and written only through it. Its deadlines still
// apply. ReadPacket and WritePacket may run concurrently with each other
// (one reader and one writer); concurrent calls of the same method are
// serialized. Close, CancelRead, CancelWrite and Stats may be called at
// any time.
type PacketStream struct {
	st   *Stream
	dsig *core.Signal
	wbuf []byte // capsule header being written (under wmu)
	werr error  // the write side's capsule framing broke (under wmu)
	rerr error  // the read side ended for good (under rmu)
	dst  []byte // payload of the capsule being read
	ch   Chunk  // bytes read from the stream, from off on unparsed
	cur  capsuleHeader
	off  int
	got  uint64 // value bytes of cur consumed
	max  int
	hn   int // bytes in hdr
	rmu  sync.Mutex
	wmu  sync.Mutex
	cnt  packetCounters
	hdr  [3 * 8]byte
	// inValue: the header of cur is complete; dstOwned: dst is not the
	// caller's buffer.
	inValue, dstOwned bool
	policy            PacketPolicy
}

type packetCounters struct {
	sentDgram, sentCapsule, tooLarge, limited, recvDgram, recvCapsule, skipped atomic.Uint64
}

// NewPacketStream wraps a bidirectional stream, opened or accepted, as a
// packet stream (opts nil: the defaults). The application agrees on the
// stream's use with its peer, for example in the preamble.
func NewPacketStream(st *Stream, opts *PacketOptions) (*PacketStream, error) {
	var o PacketOptions
	if opts != nil {
		o = *opts
	}
	if o.MaxPacket < 0 {
		return nil, fmt.Errorf("%w: negative PacketOptions.MaxPacket", ErrInvalidConfig)
	}
	if o.Policy > PacketDatagramOnly {
		return nil, fmt.Errorf("%w: unknown PacketOptions.Policy %d", ErrInvalidConfig, o.Policy)
	}
	if o.MaxPacket == 0 {
		o.MaxPacket = DefaultMaxPacket
	}
	return &PacketStream{st: st, max: o.MaxPacket, policy: o.Policy, dsig: core.NewSignal()}, nil
}

// WritePacket sends a packet with Context ID contextID (0 when the
// application uses none) as the policy says. A datagram never blocks; a
// capsule waits for room in the stream until ctx ends or the write
// deadline passes. A ctx that has ended before the call sends nothing, and
// a capsule given up before its first byte is never sent later. A capsule
// interrupted part-way leaves the stream
// unusable for packets: its sending part is reset with ZMUX_CANCELLED and
// later writes fail.
func (p *PacketStream) WritePacket(ctx context.Context, contextID uint64, b []byte) error {
	defer runtime.KeepAlive(p.st)
	if len(b) > p.max {
		return ErrPacketTooLarge
	}
	if err := ctx.Err(); err != nil {
		return err // ended before any byte: nothing is sent later
	}
	if contextID > varint.MaxVarint {
		return fmt.Errorf("%w: Context ID above 2^62-1", ErrInvalidConfig)
	}
	h := p.st.h
	if p.policy != PacketStreamOnly {
		if p.policy == PacketPreferDatagram && len(b) > p.st.MaxDatagramPayload(contextID) {
			return p.writeCapsule(ctx, contextID, b)
		}
		limited, err := h.c.sendDatagramLimited(h.st, contextID, b)
		var tl *DatagramTooLargeError
		switch {
		case limited:
			p.cnt.limited.Add(1)
			return nil
		case err == nil:
			p.cnt.sentDgram.Add(1)
			return nil
		case p.policy == PacketPreferDatagram && (errors.As(err, &tl) || errors.Is(err, ErrNotNegotiated)):
			// The bound shrank since it was read.
		case errors.As(err, &tl):
			p.cnt.tooLarge.Add(1)
			return err
		default:
			return err
		}
	}
	return p.writeCapsule(ctx, contextID, b)
}

func (p *PacketStream) writeCapsule(ctx context.Context, cx uint64, b []byte) error {
	p.wmu.Lock()
	defer p.wmu.Unlock()
	if p.werr != nil {
		return p.werr
	}
	p.wbuf = appendCapsuleHeader(p.wbuf[:0], cx, len(b))
	h := p.st.h
	n, err := h.writeCtx(ctx, p.wbuf)
	if err == nil {
		var m int
		m, err = h.writeCtx(ctx, b)
		n += m
	}
	if err != nil {
		if n > 0 {
			// The peer would read the next capsule from inside this one.
			h.cancelWrite(StreamCancelled)
			p.werr = fmt.Errorf("zmux: packet stream write interrupted: %w", err)
		}
		return err
	}
	p.cnt.sentCapsule.Add(1)
	return nil
}

// appendCapsuleHeader appends the Type, Length and Context ID of a
// DATAGRAM capsule with an n-byte payload.
func appendCapsuleHeader(b []byte, cx uint64, n int) []byte {
	b = varint.AppendVarint(b, capsuleDatagram)
	b = varint.AppendVarint(b, uint64(varint.SizeVarint(cx)+n))
	return varint.AppendVarint(b, cx)
}

// ReadPacket returns the next packet, waiting until ctx ends or the read
// deadline passes; io.EOF once the peer finished the stream at a capsule
// boundary and no datagram is queued. A capsule's payload is read into buf
// when it fits (Data is then buf[:n]) and into a new slice otherwise; a
// datagram's payload is returned in its own slice. Malformed stream bytes
// reset and stop the stream with ZMUX_PACKET_ERROR and fail this and every
// later call with ErrMalformedPacket. An interrupted call loses nothing:
// the next one continues the capsule in progress.
func (p *PacketStream) ReadPacket(ctx context.Context, buf []byte) (Packet, error) {
	defer runtime.KeepAlive(p.st)
	p.rmu.Lock()
	defer p.rmu.Unlock()
	h := p.st.h
	if p.rerr != nil {
		if errors.Is(p.rerr, io.EOF) {
			// A datagram that arrived before the end still goes first.
			if cx, d, ok := h.tryPacket(); ok {
				p.cnt.recvDgram.Add(1)
				return Packet{Data: d, ContextID: cx, Datagram: true}, nil
			}
		}
		return Packet{}, p.rerr
	}
	for {
		if !p.inValue && p.hn == 0 && p.off == len(p.ch.Data) {
			// At a capsule boundary with nothing buffered: a queued
			// datagram goes first.
			p.ch.Release()
			p.off = 0
			cx, d, ok, err := h.waitPacket(ctx, p.dsig)
			if err != nil {
				return Packet{}, err
			}
			if ok {
				p.cnt.recvDgram.Add(1)
				return Packet{Data: d, ContextID: cx, Datagram: true}, nil
			}
		}
		if !p.inValue {
			if err := p.readHeader(); err != nil {
				return Packet{}, p.fail(err)
			}
			if !p.inValue {
				if err := p.fill(ctx, true); err != nil {
					return Packet{}, p.readErr(err)
				}
				continue
			}
			if p.cur.typ == capsuleDatagram {
				if p.cur.payload <= uint64(cap(buf)) {
					p.dst, p.dstOwned = buf[:p.cur.payload], false
				} else {
					p.dst, p.dstOwned = make([]byte, p.cur.payload), true
				}
			}
		}
		avail := p.ch.Data[p.off:]
		k := min(uint64(len(avail)), p.cur.payload-p.got)
		if p.cur.typ == capsuleDatagram {
			copy(p.dst[p.got:], avail[:k])
		}
		p.off += int(k)
		p.got += k
		if p.got == p.cur.payload {
			p.inValue, p.hn, p.got = false, 0, 0
			if p.cur.typ != capsuleDatagram {
				p.cnt.skipped.Add(1)
				continue
			}
			pk := Packet{Data: p.dst, ContextID: p.cur.cx}
			p.dst = nil
			p.cnt.recvCapsule.Add(1)
			return pk, nil
		}
		if err := p.fill(ctx, false); err != nil {
			return Packet{}, p.readErr(err)
		}
	}
}

// readHeader moves header bytes from the chunk into hdr until the header
// is complete (inValue) or the chunk is used up.
func (p *PacketStream) readHeader() error {
	for {
		h, need, err := parseCapsuleHeader(p.hdr[:p.hn], p.max)
		if err != nil {
			return err
		}
		if need == 0 {
			p.cur, p.inValue, p.got = h, true, 0
			return nil
		}
		avail := p.ch.Data[p.off:]
		if len(avail) == 0 {
			return nil
		}
		k := copy(p.hdr[p.hn:p.hn+min(need, len(avail))], avail)
		p.hn += k
		p.off += k
	}
}

// fill replaces the used-up chunk with the stream's next bytes. The end of
// the stream inside a capsule is malformed.
func (p *PacketStream) fill(ctx context.Context, header bool) error {
	p.ch.Release()
	p.off = 0
	ch, err := p.st.h.readChunk(ctx, packetChunk)
	if errors.Is(err, io.EOF) && (p.inValue || header && p.hn > 0) {
		return errCapsuleTruncated
	}
	if err != nil {
		return err
	}
	p.ch = ch
	return nil
}

// readErr ends the call on err: a malformed stream is failed, an
// interruption keeps the capsule in progress, any other end is final.
func (p *PacketStream) readErr(err error) error {
	switch {
	case errors.Is(err, ErrMalformedPacket):
		return p.fail(err)
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded),
		errors.Is(err, os.ErrDeadlineExceeded):
		if p.inValue && !p.dstOwned && p.dst != nil {
			// The caller may reuse its buffer before the next call.
			own := make([]byte, len(p.dst))
			copy(own, p.dst[:p.got])
			p.dst, p.dstOwned = own, true
		}
		return err
	}
	p.ch.Release()
	p.rerr = err
	return err
}

// fail ends a malformed stream with ZMUX_PACKET_ERROR in both directions.
func (p *PacketStream) fail(err error) error {
	p.ch.Release()
	p.dst = nil
	p.rerr = err
	h := p.st.h
	h.cancelWrite(StreamPacketError)
	h.cancelRead(StreamPacketError)
	return err
}

// Close finishes the sending part (FIN) once queued capsules are sent;
// packets can still be read.
func (p *PacketStream) Close() error { return p.st.Close() }

// CancelWrite resets the sending part with code; queued capsules are
// discarded.
func (p *PacketStream) CancelWrite(code StreamErrorCode) { p.st.CancelWrite(code) }

// CancelRead stops the receiving part with code.
func (p *PacketStream) CancelRead(code StreamErrorCode) { p.st.CancelRead(code) }

// Stats returns the stream's packet counts.
func (p *PacketStream) Stats() PacketStats {
	c := &p.cnt
	return PacketStats{
		SentDatagrams: c.sentDgram.Load(), SentCapsules: c.sentCapsule.Load(),
		DroppedTooLarge: c.tooLarge.Load(), DroppedLimited: c.limited.Load(),
		ReceivedDatagrams: c.recvDgram.Load(), ReceivedCapsules: c.recvCapsule.Load(),
		SkippedCapsules: c.skipped.Load(),
	}
}

// capsuleHeader is a parsed capsule header; for DATAGRAM it includes the
// Context ID, and payload is the rest of the Value.
type capsuleHeader struct {
	typ, cx, payload uint64
	n                int // header bytes
}

// varintNeed returns how many more bytes the varint starting b needs.
func varintNeed(b []byte) int {
	if len(b) == 0 {
		return 1
	}
	return 1<<(b[0]>>6) - len(b)
}

// parseCapsuleHeader parses a capsule header (Type, Length, and for
// DATAGRAM the Context ID) at the start of b with a receive bound of max
// payload bytes; need is the bytes still missing (0: complete). A Length
// above max + 8 (any type), a DATAGRAM payload above max, and a DATAGRAM
// Value too short for its Context ID are errors.
func parseCapsuleHeader(b []byte, max int) (h capsuleHeader, need int, err error) {
	typ, i := varint.ConsumeVarint(b)
	if i == 0 {
		return h, varintNeed(b), nil
	}
	ln, k := varint.ConsumeVarint(b[i:])
	if k == 0 {
		return h, varintNeed(b[i:]), nil
	}
	i += k
	if ln > uint64(max)+8 {
		return h, 0, errCapsuleOversized
	}
	h.typ = typ
	if typ != capsuleDatagram {
		h.payload, h.n = ln, i
		return h, 0, nil
	}
	if ln == 0 {
		return h, 0, errCapsuleTruncated
	}
	if len(b) == i {
		return h, 1, nil
	}
	l := 1 << (b[i] >> 6)
	if uint64(l) > ln {
		return h, 0, errCapsuleTruncated
	}
	if len(b) < i+l {
		return h, i + l - len(b), nil
	}
	h.cx, _ = varint.ConsumeVarint(b[i:])
	h.payload, h.n = ln-uint64(l), i+l
	if h.payload > uint64(max) {
		return h, 0, errCapsuleOversized
	}
	return h, 0, nil
}

// waitPacket waits until the stream has a datagram (returned, ok true) or
// bytes or its end to read (ok false).
func (h *handle) waitPacket(ctx context.Context, dsig *core.Signal) (uint64, []byte, bool, error) {
	h.rmu.Lock()
	defer h.rmu.Unlock()
	c := h.c
	for {
		c.lock()
		cx, d, ok := c.s.RecvStreamDatagram(h.st)
		var n uint64
		var end bool
		if !ok {
			n, end = c.s.ReadReady(h.st)
			if n == 0 && !end {
				h.rsig.Drain()
				c.s.WaitRead(h.st, &h.rsig)
				dsig.Drain()
				c.s.WaitStreamDatagram(h.st, dsig)
			}
		}
		c.unlock()
		if ok {
			return cx, d, true, nil
		}
		if n > 0 || end {
			return 0, nil, false, nil
		}
		select {
		case <-h.rsig.C:
		case <-dsig.C:
		case <-ctx.Done():
			return 0, nil, false, ctx.Err()
		case <-h.rwait():
			return 0, nil, false, os.ErrDeadlineExceeded
		}
	}
}

// tryPacket returns a queued stream datagram without waiting.
func (h *handle) tryPacket() (uint64, []byte, bool) {
	h.rmu.Lock()
	defer h.rmu.Unlock()
	h.c.lock()
	defer h.c.unlock()
	return h.c.s.RecvStreamDatagram(h.st)
}

// writeCtx writes all of p (copied), waiting for room until ctx ends or
// the write deadline passes.
func (h *handle) writeCtx(ctx context.Context, p []byte) (int, error) {
	h.wmu.Lock()
	defer h.wmu.Unlock()
	c := h.c
	total := 0
	for total < len(p) {
		c.lock()
		c.s.SetNow(c.clk.now())
		n, err := c.s.Write(h.st, p[total:], false)
		total += n
		wait := err == nil && total < len(p)
		if wait {
			h.wsig.Drain()
			c.s.WaitWrite(h.st, &h.wsig)
		}
		c.unlock()
		if err != nil {
			return total, apiErr(err)
		}
		if !wait {
			break
		}
		select {
		case <-h.wsig.C:
		case <-ctx.Done():
			return total, ctx.Err()
		case <-h.wwait():
			return total, os.ErrDeadlineExceeded
		}
	}
	return total, nil
}
