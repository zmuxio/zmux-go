package seal

import (
	"context"
	"crypto/ecdh"
	"encoding/binary"
	"errors"
	"io"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	"github.com/djylb/timeshim"

	"github.com/zmuxio/zmux-go"
)

// Mode is what a server's admission accepts.
type Mode uint8

const (
	// SealedOnly accepts carriers that run the transform, and no others.
	// An entrance that an intermediary may reach over TLS of its own (a
	// CDN or proxy that connects with HTTPS) uses it: TLS terminated here
	// is no proof that nobody read the carrier on the way.
	SealedOnly Mode = iota
	// Auto also accepts plain zmux carriers (VariantPlain), on a carrier
	// whose TLS this process terminates (zmux.TerminatedTLS); the host
	// proves such carriers end to end by binding to their exporter.
	Auto
)

// ServerConfig configures the server end of carriers.
type ServerConfig struct {
	// Keys are the server's static private keys: the current one first
	// (the bootstrap handshake sends it), then the ones being rotated in
	// or out. A regular msg1 is tried against each.
	Keys []PrivateKey
	// Authorize authenticates the client of a regular handshake, once per
	// msg1 after its static key was decrypted, on the admission's
	// goroutine: a fast lookup that never blocks (ctx ends with the
	// admission's PeekTimeout). It returns the PSK the client must have
	// mixed in (nil: none, else at least 16 bytes), the value its carriers
	// are admitted with (Peer.Value), and whether the client is allowed.
	// src is the carrier's source (zmux.CarrierInfo.Source). nil: no
	// regular handshake is accepted.
	Authorize func(ctx context.Context, peer PublicKey, src netip.AddrPort) (psk []byte, value any, ok bool)
	// Bootstrap is the table of bootstrap clients (nil: no bootstrap
	// handshake is accepted).
	Bootstrap *TagTable
	// Window is how far a msg1's stamp may be from the server's clock for
	// its first flight to be accepted (0: 60 s); a stamp outside it, or a
	// msg1 seen before, completes the handshake with the flight refused.
	Window time.Duration
	// ReplayMax bounds the remembered handshakes, each for at most
	// 2·Window (0: 1<<18); when full, first flights are refused.
	ReplayMax int
	// Stall stalls a carrier whose handshake fails (zmux.VerdictStall)
	// instead of closing it.
	Stall bool
	// PeekTimeout is the admission's (zmux.Admission.PeekTimeout).
	PeekTimeout time.Duration
	// Clock is the server's clock; nil: the host clock.
	Clock timeshim.Clock

	rand io.Reader // ephemeral keys; nil: crypto/rand (tests)
}

// Server is the server end of carriers: an admission that verifies msg1,
// and a ListenerConfig.WrapPipe that completes the handshake.
type Server struct {
	cfg    ServerConfig
	keys   []*ecdh.PrivateKey
	filter *zmux.ReplayFilter
	clk    timeshim.Clock

	st struct {
		handshakes, bootstraps, earlyAccepted, earlyRejected            atomic.Uint64
		failNoKey, failUnauthorized, failAuth, failMalformed            atomic.Uint64
		replays, windowMiss, cacheFull, plainNotTerminated              atomic.Uint64
		handshakeNanos, handshakeMaxNanos, plainAccepted, plainRejected atomic.Uint64
	}
}

// Stats are a server's counts.
type Stats struct {
	// Handshakes counts verified msg1s, Bootstraps those of the bootstrap
	// handshake among them.
	Handshakes, Bootstraps uint64
	// EarlyAccepted and EarlyRejected count first flights accepted and
	// refused (Replays + WindowMiss + CacheFull).
	EarlyAccepted, EarlyRejected uint64
	// FailNoKey counts msg1s that fit no server key and no bootstrap
	// client; FailUnauthorized those whose client Authorize refused;
	// FailAuth those whose PSK or payload failed authentication;
	// FailMalformed those with an invalid payload.
	FailNoKey, FailUnauthorized, FailAuth, FailMalformed uint64
	// Replays, WindowMiss and CacheFull count refused first flights by
	// cause: seen before, a stamp outside the window, a full filter.
	Replays, WindowMiss, CacheFull uint64
	// CacheLen is the number of remembered handshakes.
	CacheLen uint64
	// HandshakeNanos and HandshakeMaxNanos are the sum and the maximum of
	// the time taken to verify a complete msg1.
	HandshakeNanos, HandshakeMaxNanos uint64
	// PlainAccepted counts plain carriers an Auto admission took;
	// PlainNotTerminated those it refused for want of TLS terminated
	// here, and PlainRejected those a SealedOnly admission refused.
	PlainAccepted, PlainNotTerminated, PlainRejected uint64
}

// NewServer returns a server for c; an invalid c is an ErrInvalidConfig
// error.
func NewServer(c *ServerConfig) (*Server, error) {
	if c == nil || len(c.Keys) == 0 || c.Window < 0 || c.ReplayMax < 0 {
		return nil, ErrInvalidConfig
	}
	s := &Server{cfg: *c, clk: timeshim.Or(c.Clock)}
	s.cfg.Keys = append([]PrivateKey(nil), c.Keys...)
	if s.cfg.Window == 0 {
		s.cfg.Window = time.Minute
	}
	if s.cfg.ReplayMax == 0 {
		s.cfg.ReplayMax = 1 << 18
	}
	for _, k := range s.cfg.Keys {
		s.keys = append(s.keys, k.ecdh())
	}
	f, err := zmux.NewReplayFilter(zmux.ReplayConfig{Window: s.cfg.Window, Max: s.cfg.ReplayMax,
		NotBefore: s.clk.Now().Add(-s.cfg.Window), Clock: s.clk})
	if err != nil {
		return nil, errors.Join(ErrInvalidConfig, err)
	}
	s.filter = f
	return s, nil
}

// Stats returns the server's counts.
func (s *Server) Stats() Stats {
	return Stats{Handshakes: s.st.handshakes.Load(), Bootstraps: s.st.bootstraps.Load(),
		EarlyAccepted: s.st.earlyAccepted.Load(), EarlyRejected: s.st.earlyRejected.Load(),
		FailNoKey: s.st.failNoKey.Load(), FailUnauthorized: s.st.failUnauthorized.Load(),
		FailAuth: s.st.failAuth.Load(), FailMalformed: s.st.failMalformed.Load(),
		Replays: s.st.replays.Load(), WindowMiss: s.st.windowMiss.Load(), CacheFull: s.st.cacheFull.Load(),
		CacheLen: uint64(s.filter.Len()), HandshakeNanos: s.st.handshakeNanos.Load(),
		HandshakeMaxNanos: s.st.handshakeMaxNanos.Load(), PlainAccepted: s.st.plainAccepted.Load(),
		PlainNotTerminated: s.st.plainNotTerminated.Load(), PlainRejected: s.st.plainRejected.Load()}
}

// Peer is the zmux.Decision.Value of a carrier that the server admitted:
// zmux.Conn.Admitted and zmux.Pipe.Admitted return it.
type Peer struct {
	// Value is what Authorize or the TagTable gave for the client (nil
	// for a plain carrier).
	Value any
	// Static is the client's static key (regular handshake).
	Static PublicKey
	// Bootstrap marks the bootstrap handshake, and ID is the TagTable
	// identifier of its client.
	Bootstrap bool
	ID        string
	// Plain marks a plain carrier of an Auto admission.
	Plain bool
	// EarlyAccepted reports whether msg1's first flight was accepted.
	EarlyAccepted bool

	pend     atomic.Pointer[admitted] // taken by WrapPipe
	deferred atomic.Bool              // decided on an HTTP request head: WrapPipe reads msg1
	mode     Mode
}

// admitted is a verified msg1 waiting for WrapPipe.
type admitted struct {
	h       *handshake
	records [][]byte // the accepted first flight
	variant string
}

// Admission returns an admission for ListenerConfig.Admission in mode m:
// it tells plain zmux carriers from carriers of the transform by their
// first bytes (zmux.ClassifyFirstBytes; the client's ephemeral key never
// looks like a zmux record), reads and verifies msg1, and closes (or
// stalls, ServerConfig.Stall) a carrier that fails. A failure is never
// answered. Use one Server's admissions on every entrance of a server, so
// that they share its replay filter; set WrapPipe with the same Server.
func (s *Server) Admission(m Mode) *zmux.Admission {
	return &zmux.Admission{Peek: zmux.MaxFirstBytes, MaxPeek: maxMsg1, PeekTimeout: s.cfg.PeekTimeout,
		Decide: func(ctx context.Context, info *zmux.CarrierInfo, prefix []byte) zmux.Decision {
			return s.decide(ctx, m, info, prefix)
		}}
}

func (s *Server) reject(variant string) zmux.Decision {
	if s.cfg.Stall {
		return zmux.Decision{Verdict: zmux.VerdictStall, Variant: variant}
	}
	return zmux.Decision{Verdict: zmux.VerdictClose, Variant: variant}
}

func more(n int, variant string) zmux.Decision {
	return zmux.Decision{Verdict: zmux.VerdictMore, Need: n, Variant: variant}
}

func (s *Server) decide(ctx context.Context, m Mode, info *zmux.CarrierInfo, prefix []byte) zmux.Decision {
	if info.Head {
		// A carrier decided on its request head (WebSocket) has no
		// prefix: WrapPipe reads and verifies msg1, its first message.
		p := &Peer{mode: m}
		p.deferred.Store(true)
		return zmux.Decision{Verdict: zmux.VerdictAccept, Value: p}
	}
	// A prefix that is a zmux first record in either framing is plain:
	// ephemeral keys are drawn so that msg1 is neither.
	cr, _ := zmux.ClassifyFirstBytes(prefix, zmux.FramingRecords)
	cb, need := zmux.ClassifyFirstBytes(prefix, zmux.FramingByteStream)
	switch {
	case cr == zmux.FirstBytesRecord || cb == zmux.FirstBytesRecord:
		if m != Auto {
			s.st.plainRejected.Add(1)
			return s.reject(VariantPlain)
		}
		if info.TLS == nil {
			s.st.plainNotTerminated.Add(1)
			return s.reject(VariantPlain)
		}
		s.st.plainAccepted.Add(1)
		return zmux.Decision{Verdict: zmux.VerdictAccept, Value: &Peer{Plain: true}, Variant: VariantPlain}
	case cb == zmux.FirstBytesNeedMore:
		return more(need, "")
	}
	if len(prefix) < nxMsg1Head {
		return more(nxMsg1Head, VariantSealed)
	}
	if t := s.cfg.Bootstrap; t != nil {
		var tag [nxTagLen]byte
		copy(tag[:], prefix[dhLen:dhLen+nxTagLen])
		if e, ok := t.lookup(tag); ok {
			n := nxMsg1Head + int(binary.BigEndian.Uint16(prefix[nxMsg1Head-lenLen:])) + tagLen
			if len(prefix) < n {
				return more(n, VariantBootstrap)
			}
			if d, ok := s.verifyNX(e, prefix); ok {
				return d
			}
			// A tag that matched by chance: try the regular handshake.
		}
	}
	if len(prefix) < ikMsg1Head {
		return more(ikMsg1Head, VariantSealed)
	}
	n := ikMsg1Head + int(binary.BigEndian.Uint16(prefix[ikMsg1Head-lenLen:])) + tagLen
	if len(prefix) < n {
		return more(n, VariantSealed)
	}
	return s.verifyIK(ctx, info, prefix)
}

// timed adds the time since t0 to the verification time.
func (s *Server) timed(t0 time.Time) {
	d := uint64(max(time.Since(t0), 0))
	s.st.handshakeNanos.Add(d)
	for {
		m := s.st.handshakeMaxNanos.Load()
		if d <= m || s.st.handshakeMaxNanos.CompareAndSwap(m, d) {
			return
		}
	}
}

func (s *Server) verifyNX(e *tagEntry, msg []byte) (zmux.Decision, bool) {
	t0 := time.Now()
	defer s.timed(t0)
	h := &handshake{p: defaultParams, nx: true, psk: e.psk, s: s.keys[0]}
	payload, n, err := h.readNX(msg)
	if err != nil {
		return zmux.Decision{}, false
	}
	return s.admit(h, payload, n, &Peer{Value: e.value, Bootstrap: true, ID: e.id}, VariantBootstrap), true
}

func (s *Server) verifyIK(ctx context.Context, info *zmux.CarrierInfo, msg []byte) zmux.Decision {
	t0 := time.Now()
	defer s.timed(t0)
	var h *handshake
	for _, k := range s.keys {
		try := &handshake{p: defaultParams, s: k}
		if try.readIKStatic(msg) == nil {
			h = try
			break
		}
	}
	if h == nil || s.cfg.Authorize == nil {
		s.st.failNoKey.Add(1)
		return s.reject(VariantSealed)
	}
	psk, value, ok := s.cfg.Authorize(ctx, PublicKey(h.rs), info.Source)
	if !ok || len(psk) != 0 && len(psk) < 16 {
		s.st.failUnauthorized.Add(1)
		return s.reject(VariantSealed)
	}
	h.psk = compressPSK(psk)
	payload, n, err := h.readIKRest(msg)
	if err != nil {
		s.st.failAuth.Add(1)
		return s.reject(VariantSealed)
	}
	return s.admit(h, payload, n, &Peer{Value: value, Static: PublicKey(h.rs)}, VariantSealed)
}

// admit checks a verified msg1's payload and its replay status.
func (s *Server) admit(h *handshake, payload []byte, n int, p *Peer, variant string) zmux.Decision {
	if len(payload) < stampLen {
		s.st.failMalformed.Add(1)
		return s.reject(variant)
	}
	recs, err := parseRecords(payload[stampLen:])
	if err != nil {
		s.st.failMalformed.Add(1)
		return s.reject(variant)
	}
	s.st.handshakes.Add(1)
	if p.Bootstrap {
		s.st.bootstraps.Add(1)
	}
	switch s.filter.Check(h.h1[:], int64(binary.BigEndian.Uint64(payload))) {
	case zmux.ReplayFresh:
		p.EarlyAccepted = true
		s.st.earlyAccepted.Add(1)
	case zmux.ReplaySeen:
		s.st.replays.Add(1)
	case zmux.ReplayOutside:
		s.st.windowMiss.Add(1)
	default:
		s.st.cacheFull.Add(1)
	}
	if !p.EarlyAccepted {
		s.st.earlyRejected.Add(1)
		recs = nil
	}
	p.pend.Store(&admitted{h: h, records: recs, variant: variant})
	return zmux.Decision{Verdict: zmux.VerdictAccept, Skip: n, Value: p, Variant: variant}
}

var errNotAdmitted = errors.New("seal: carrier not admitted by this server's admission")

// WrapPipe is the ListenerConfig.WrapPipe of the server: it completes the
// handshake of a carrier its admission verified, writing msg2 with the
// server time and the verdict on the first flight, and returns the
// carrier's server end, a message carrier (zmux.FramingRecords) whose
// first records are those of the accepted first flight. A plain carrier
// of an Auto admission is returned as it is, on TLS terminated here; any
// other carrier is an error (it is closed).
//
// The halves offer zmux.KeyingMaterialExporter, zmux.EarlyServerCarrier
// (streams of an accepted first flight are replayable until the client's
// first transport frame confirms the handshake), zmux.MessageBatchWriter,
// zmux.CloseWriter and zmux.Aborter, and none of p's other capabilities.
func (s *Server) WrapPipe(ctx context.Context, p zmux.PipeIO, hint zmux.JoinHint) (zmux.PipeIO, error) {
	peer, ok := hint.Admitted.(*Peer)
	if !ok {
		return zmux.PipeIO{}, errNotAdmitted
	}
	if peer.deferred.CompareAndSwap(true, false) {
		var err error
		if p, err = s.readDeferred(ctx, peer, p, hint); err != nil || peer.Plain {
			return p, err
		}
	} else if peer.Plain {
		if _, ok := zmux.TerminatedTLS(p); !ok {
			return zmux.PipeIO{}, errNotAdmitted
		}
		return p, nil
	}
	a := peer.pend.Swap(nil)
	if a == nil {
		return zmux.PipeIO{}, errNotAdmitted // a carrier after the admitted one
	}
	low, err := newLower(p)
	if err != nil {
		return zmux.PipeIO{}, err
	}
	h := a.h
	e, err := genEphemeral(s.cfg.rand)
	if err != nil {
		return zmux.PipeIO{}, err
	}
	h.e = e
	var flags byte
	if peer.EarlyAccepted {
		flags = flagEarlyAccepted
	}
	payload := binary.BigEndian.AppendUint64(make([]byte, 0, stampLen+flagsLen), uint64(s.clk.Now().UnixMilli()))
	payload = append(payload, flags)
	msg, err := h.writeMsg2(nil, payload)
	if err != nil {
		return zmux.PipeIO{}, err
	}
	sc := &serverCarrier{confirmed: make(chan struct{}), early: len(a.records) > 0}
	sc.init(low)
	c1, c2 := h.ss.split()
	sc.recv, sc.send = newFrameState(c1), newFrameState(c2)
	sc.exp = h.exporterSecret()
	sc.expOK.Store(true)
	sc.pending = a.records
	if err := ctx.Err(); err != nil {
		return zmux.PipeIO{}, err
	}
	if err := low.write(msg); err != nil {
		return zmux.PipeIO{}, err
	}
	return zmux.PipeIO{R: sc, W: sc, Framing: zmux.FramingRecords,
		WriteOverhead: p.WriteOverhead + low.overhead(), Variant: a.variant}, nil
}

type serverCarrier struct {
	carrier
	early     bool
	confirmed chan struct{}
	confOnce  sync.Once
}

// WriteMessage writes one record.
func (c *serverCarrier) WriteMessage(m []byte) error {
	one := [1][]byte{m}
	return c.WriteMessages(one[:])
}

// Write writes b as one record.
func (c *serverCarrier) Write(b []byte) (int, error) {
	if err := c.WriteMessage(b); err != nil {
		return 0, err
	}
	return len(b), nil
}

// WriteMessages writes records in one write below.
func (c *serverCarrier) WriteMessages(msgs [][]byte) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	return c.writeFrames(msgs)
}

// ReadMessage returns the next record: those of an accepted first flight,
// then those of the client's transport frames.
func (c *serverCarrier) ReadMessage() ([]byte, error) {
	c.rmu.Lock()
	defer c.rmu.Unlock()
	if m, ok := c.nextPending(); ok {
		return m, nil
	}
	m, err := c.readFrame()
	if err == nil {
		c.confOnce.Do(func() { close(c.confirmed) })
	}
	return m, err
}

// UsedEarlyData reports whether the carrier delivers an accepted first
// flight (zmux.EarlyServerCarrier).
func (c *serverCarrier) UsedEarlyData() bool { return c.early }

// HandshakeComplete is closed when the client's first transport frame
// arrived, which proves that msg1 was no replay.
func (c *serverCarrier) HandshakeComplete() <-chan struct{} { return c.confirmed }

// readDeferred reads the first message of a carrier admitted on its
// request head and decides on it as the admission does a prefix; it fills
// peer. A plain carrier is returned with the message replayed.
func (s *Server) readDeferred(ctx context.Context, peer *Peer, p zmux.PipeIO, hint zmux.JoinHint) (zmux.PipeIO, error) {
	mr, ok := p.R.(zmux.MessageReader)
	if !ok || p.Framing != zmux.FramingRecords {
		return zmux.PipeIO{}, errNotAdmitted
	}
	var msg []byte
	var err error
	if lr, ok := p.R.(zmux.MessageLimitReader); ok {
		msg, err = lr.ReadMessageLimit(maxMsg1)
	} else {
		msg, err = mr.ReadMessage()
	}
	if err != nil {
		return zmux.PipeIO{}, err
	}
	info := &zmux.CarrierInfo{Source: hint.Source}
	if st, ok := zmux.TerminatedTLS(p); ok {
		info.TLS = &st
	}
	d := s.decide(ctx, peer.mode, info, msg)
	got, _ := d.Value.(*Peer)
	if d.Verdict != zmux.VerdictAccept || got == nil {
		return zmux.PipeIO{}, errNotAdmitted
	}
	if got.Plain {
		peer.Plain = true
		return zmux.PrefixedPipe(p, msg), nil
	}
	if d.Skip != len(msg) {
		return zmux.PipeIO{}, errNotAdmitted
	}
	peer.Value, peer.Static, peer.Bootstrap, peer.ID, peer.EarlyAccepted = got.Value, got.Static, got.Bootstrap, got.ID, got.EarlyAccepted
	peer.pend.Store(got.pend.Load())
	return p, nil
}
