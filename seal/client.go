package seal

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"time"

	"github.com/djylb/timeshim"

	"github.com/zmuxio/zmux-go"
)

// Variants of the carriers (zmux.PipeIO.Variant, zmux.Candidate.Variant,
// zmux.Decision.Variant).
const (
	// VariantSealed labels carriers that run the regular handshake.
	VariantSealed = "sealed"
	// VariantBootstrap labels carriers that run the bootstrap handshake.
	VariantBootstrap = "sealed-boot"
	// VariantPlain labels carriers that an Auto admission took without
	// the transform.
	VariantPlain = "plain"
)

// ClientConfig configures the client end of carriers.
type ClientConfig struct {
	// Peers are the server's static public keys the client pins, in order
	// of preference: the first is used. With none, the carrier runs the
	// bootstrap handshake, which authenticates the server by PSK alone and
	// learns its key (OnPeer).
	Peers []PublicKey
	// Key is the client's static private key (regular handshake; ignored
	// by the bootstrap one), such as one from GenerateKey, KeyFromSecret
	// or X25519FromEd25519Private.
	Key PrivateKey
	// PSK is a pre-shared key of at least 16 bytes and 128 bits of uniform
	// randomness, or a value the host derived from one; the bootstrap
	// handshake needs it, the regular one mixes it in when the server's
	// Authorize returns the same. nil: none (regular handshake only).
	PSK []byte
	// OnPeer, when set, is called with the server's static key that a
	// bootstrap handshake learnt and verified, before the carrier
	// delivers anything; the host decides whether to pin it.
	OnPeer func(PublicKey)
	// Skew is the clock estimate shared by the carriers of the servers
	// that share a clock; nil: one shared by every ClientConfig without
	// one.
	Skew *ClockSkew
	// Clock is the local clock; nil: the host clock.
	Clock timeshim.Clock

	rand io.Reader // ephemeral keys; nil: crypto/rand (tests)
}

// ErrInvalidConfig marks an invalid ClientConfig or ServerConfig.
var ErrInvalidConfig = errors.New("seal: invalid config")

func (c *ClientConfig) check() error {
	switch {
	case c == nil:
		return ErrInvalidConfig
	case len(c.PSK) != 0 && len(c.PSK) < 16:
		return errors.Join(ErrInvalidConfig, ErrShortPSK)
	case len(c.Peers) == 0 && len(c.PSK) == 0:
		return errors.Join(ErrInvalidConfig, errors.New("seal: bootstrap handshake without PSK"))
	case len(c.Peers) > 0 && c.Key == (PrivateKey{}):
		return errors.Join(ErrInvalidConfig, errors.New("seal: regular handshake without Key"))
	}
	return nil
}

// Client returns the client end of a carrier over p, a byte-stream or
// message carrier with both halves, which the result owns from then on
// (an error closes nothing). The result is a message carrier
// (zmux.FramingRecords) whose first write sends msg1 with the records of
// that write (the first flight), so use it with zmux.EarlyData{ZmuxPeer,
// Replayable, HoldFirstFlight} to send the first streams in msg1. Writes
// after the first wait for msg2; reads return records once it is
// verified. Its halves offer zmux.KeyingMaterialExporter (after msg2),
// zmux.EarlyClientCarrier, zmux.MessageBatchWriter, zmux.CloseWriter and
// zmux.Aborter, and none of p's other capabilities.
func Client(p zmux.PipeIO, c *ClientConfig) (zmux.PipeIO, error) {
	if err := c.check(); err != nil {
		return zmux.PipeIO{}, err
	}
	low, err := newLower(p)
	if err != nil {
		return zmux.PipeIO{}, err
	}
	cc := &clientCarrier{cfg: *c, ready: make(chan struct{})}
	cc.cfg.Peers = append([]PublicKey(nil), c.Peers...)
	cc.cfg.PSK = append([]byte(nil), c.PSK...)
	if cc.cfg.Skew == nil {
		cc.cfg.Skew = &defaultSkew
	}
	cc.cfg.Clock = timeshim.Or(c.Clock)
	cc.init(low)
	variant := VariantSealed
	if len(c.Peers) == 0 {
		variant = VariantBootstrap
	}
	return zmux.PipeIO{R: cc, W: cc, Framing: zmux.FramingRecords, Carrier: p.Carrier, Label: p.Label,
		FateGroup: p.FateGroup, WriteOverhead: p.WriteOverhead + low.overhead(), Variant: variant}, nil
}

// WrapCandidate returns cand with its carriers wrapped by Client with c,
// labelled VariantSealed or VariantBootstrap (a regular and a bootstrap
// candidate of one kind are different candidates). A split candidate is
// not supported (its halves arrive at the server apart).
func WrapCandidate(cand zmux.Candidate, c *ClientConfig) zmux.Candidate {
	inner := cand.Dial
	out := zmux.Candidate{Kind: cand.Kind, Target: cand.Target, Variant: VariantSealed}
	if len(c.Peers) == 0 {
		out.Variant = VariantBootstrap
	}
	out.Dial = func(ctx context.Context, cred zmux.Credential) (zmux.PipeIO, error) {
		p, err := inner(ctx, cred)
		if err != nil {
			return zmux.PipeIO{}, err
		}
		q, err := Client(p, c)
		if err != nil {
			closePipe(p)
			return zmux.PipeIO{}, err
		}
		return q, nil
	}
	return out
}

func closePipe(p zmux.PipeIO) {
	if p.W != nil {
		_ = p.W.Close()
	}
	if p.R != nil && any(p.R) != any(p.W) {
		_ = p.R.Close()
	}
}

// Dial connects to addr on the named network (TCP), wraps the connection
// with Client and starts a client session on it with cfg (zmux.Start).
// With EarlyData.HoldFirstFlight in cfg, the first flight waits for
// Conn.ReleaseFirstFlight as Start describes.
func Dial(ctx context.Context, network, addr string, c *ClientConfig, cfg *zmux.Config) (*zmux.Conn, error) {
	var d net.Dialer
	nc, err := d.DialContext(ctx, network, addr)
	if err != nil {
		return nil, err
	}
	p, err := Client(zmux.NetPipe(nc), c)
	if err != nil {
		_ = nc.Close()
		return nil, err
	}
	conn, err := zmux.Start(ctx, p, zmux.RoleClient, cfg)
	if err != nil {
		closePipe(p)
		return nil, err
	}
	return conn, nil
}

// Client states.
const (
	csFresh       = iota // the next write sends msg1
	csSent               // msg1 sent; writes wait for msg2
	csEstablished        // transport keys in use
)

type clientCarrier struct {
	carrier
	cfg ClientConfig

	// Under wmu:
	state    int
	hs       *handshake
	sentAt   time.Time
	flight   [][]byte // the records of msg1, sent again if refused
	queue    [][]byte // records of the first write that msg1 did not hold
	accepted bool

	ready chan struct{} // closed when established
}

// WriteMessage writes one record (zmux.MessageWriter).
func (c *clientCarrier) WriteMessage(m []byte) error {
	one := [1][]byte{m}
	return c.WriteMessages(one[:])
}

// Write writes b as one record.
func (c *clientCarrier) Write(b []byte) (int, error) {
	if err := c.WriteMessage(b); err != nil {
		return 0, err
	}
	return len(b), nil
}

// WriteMessages writes records in one write below (zmux.MessageBatchWriter):
// the first call sends msg1 with them; later calls wait for msg2.
func (c *clientCarrier) WriteMessages(msgs [][]byte) error {
	c.wmu.Lock()
	if c.state == csFresh {
		err := c.sendMsg1(msgs)
		c.wmu.Unlock()
		return err
	}
	if c.state == csSent {
		c.wmu.Unlock()
		select {
		case <-c.ready:
		case <-c.closed:
			return c.err()
		}
		c.wmu.Lock()
	}
	defer c.wmu.Unlock()
	if c.state != csEstablished {
		return c.err()
	}
	return c.writeFrames(msgs)
}

// sendMsg1 builds and writes msg1 (wmu held).
func (c *clientCarrier) sendMsg1(msgs [][]byte) error {
	select {
	case <-c.closed:
		return c.err()
	default:
	}
	cfg := &c.cfg
	h := &handshake{p: defaultParams, nx: len(cfg.Peers) == 0, psk: compressPSK(cfg.PSK)}
	e, err := genEphemeral(cfg.rand)
	if err != nil {
		return c.fail(err)
	}
	h.e = e
	if !h.nx {
		h.s = cfg.Key.ecdh()
		h.rs = cfg.Peers[0]
	}
	now := cfg.Clock.Now()
	if _, auth := cfg.Skew.Offset(cfg.Clock); !auth {
		for _, x := range []any{c.low.p.W, c.low.p.R} {
			if rh, ok := x.(zmux.ResponseHeaderer); ok {
				cfg.Skew.hint(rh.ResponseHeader(), now)
				break
			}
		}
	}
	stamp := cfg.Skew.now(cfg.Clock)
	payload := binary.BigEndian.AppendUint64(make([]byte, 0, 256), uint64(stamp))
	payload, n := appendRecords(payload, msgs, maxMsg1-ikMsg1Head-tagLen)
	for _, m := range msgs[:n] {
		c.flight = append(c.flight, append([]byte(nil), m...))
	}
	for _, m := range msgs[n:] {
		c.queue = append(c.queue, append([]byte(nil), m...))
	}
	var tag [nxTagLen]byte
	if h.nx {
		tag = tagFor(&h.psk, stamp)
	}
	msg, err := h.writeMsg1(nil, &tag, payload)
	if err != nil {
		return c.fail(err)
	}
	c.hs, c.state, c.sentAt = h, csSent, cfg.Clock.Now()
	if err := c.low.write(msg); err != nil {
		return c.fail(err)
	}
	return nil
}

// ReadMessage returns the next record (zmux.MessageReader); the first call
// reads and verifies msg2.
func (c *clientCarrier) ReadMessage() ([]byte, error) {
	c.rmu.Lock()
	defer c.rmu.Unlock()
	select {
	case <-c.ready:
	default:
		if err := c.finish(); err != nil {
			return nil, err
		}
	}
	if m, ok := c.nextPending(); ok {
		return m, nil
	}
	return c.readFrame()
}

// finish reads msg2, derives the transport keys and sends what waited:
// the first flight again if the server refused it, then the queue.
func (c *clientCarrier) finish() error {
	nx := len(c.cfg.Peers) == 0
	head := ikMsg2Head
	if nx {
		head = nxMsg2Head
	}
	msg, err := c.low.readHandshake(head)
	if err != nil {
		return c.fail(unexpected(err))
	}
	at := c.cfg.Clock.Now()
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if c.state != csSent {
		return c.fail(errMalformed) // msg2 before msg1
	}
	h := c.hs
	payload, n, err := h.readMsg2(msg)
	if err != nil {
		return c.fail(err)
	}
	if n != len(msg) || len(payload) < stampLen+flagsLen {
		return c.fail(errMalformed)
	}
	recs, err := parseRecords(payload[stampLen+flagsLen:])
	if err != nil {
		return c.fail(err)
	}
	c.cfg.Skew.observe(int64(binary.BigEndian.Uint64(payload)), c.sentAt.Add(at.Sub(c.sentAt)/2), true)
	if nx && c.cfg.OnPeer != nil {
		c.cfg.OnPeer(PublicKey(h.rs))
	}
	c1, c2 := h.ss.split()
	c.send, c.recv = newFrameState(c1), newFrameState(c2)
	c.exp = h.exporterSecret()
	c.expOK.Store(true)
	c.accepted = payload[stampLen]&flagEarlyAccepted != 0
	c.hs = nil
	c.pending = recs
	var out [][]byte
	if !c.accepted {
		out = append(out, c.flight...)
	}
	out = append(out, c.queue...)
	c.flight, c.queue = nil, nil
	if err := c.writeFrames(out); err != nil {
		return err
	}
	c.state = csEstablished
	close(c.ready)
	return nil
}

// HandshakeContext runs the handshake now (zmux.Handshaker): msg1 with no
// first flight, then msg2, so that the exporter is ready before anything
// is written, as a carrier-bound join needs (zmux.PipeOptions.CarrierBound
// runs it). A carrier that sent msg1 already waits for msg2; ctx ending
// closes the carrier.
func (c *clientCarrier) HandshakeContext(ctx context.Context) error {
	stop := context.AfterFunc(ctx, func() { _ = c.fail(ctx.Err()) })
	defer stop()
	c.wmu.Lock()
	if c.state == csFresh {
		if err := c.sendMsg1(nil); err != nil {
			c.wmu.Unlock()
			return err
		}
	}
	c.wmu.Unlock()
	c.rmu.Lock()
	defer c.rmu.Unlock()
	select {
	case <-c.ready:
		return nil
	default:
	}
	return c.finish()
}

// SendsEarlyData reports true: msg1 carries the first flight
// (zmux.EarlyClientCarrier), which a server may see replayed.
func (c *clientCarrier) SendsEarlyData() bool { return true }

// EarlyAccepted waits for msg2 and reports true: a first flight that the
// server refused is sent again under the transport keys before anything
// else, so the bytes stay usable either way.
func (c *clientCarrier) EarlyAccepted(ctx context.Context) (bool, error) {
	select {
	case <-c.ready:
		return true, nil
	case <-c.closed:
		return false, c.err()
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

// RestartEarly is not needed (EarlyAccepted never reports false); it
// returns the carrier itself.
func (c *clientCarrier) RestartEarly(context.Context) (zmux.PipeIO, error) {
	return zmux.PipeIO{R: c, W: c, Framing: zmux.FramingRecords}, nil
}

// EarlyDataAccepted reports whether the server accepted the first flight
// in msg1, and whether msg2 has been verified.
func (c *clientCarrier) EarlyDataAccepted() (accepted, known bool) {
	select {
	case <-c.ready:
	default:
		return false, false
	}
	c.wmu.Lock()
	defer c.wmu.Unlock()
	return c.accepted, true
}

// EarlyDataAccepted reports, for p from Client, whether the server
// accepted the first flight in msg1 and whether msg2 has been verified
// (known); false, false for any other p.
func EarlyDataAccepted(p zmux.PipeIO) (accepted, known bool) {
	if c, ok := p.W.(*clientCarrier); ok {
		return c.EarlyDataAccepted()
	}
	return false, false
}
