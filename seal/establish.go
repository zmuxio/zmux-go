package seal

import (
	"bytes"
	"context"
	"crypto/ecdh"
	"crypto/rand"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/internal/join"
	"github.com/zmuxio/zmux-go/seal/proto"
	"github.com/zmuxio/zmux-go/shaping"
)

// Peer is what an Establish learned of the other end. A client gets it with PeerOf; on a
// server it is Conn.Admitted.
type Peer struct {
	// Static is the peer's static key.
	Static []byte
	// Credential is the credential the peer presented, nil: none.
	Credential *Credential
	// AppData is the application data the peer sent, in message order (from a
	// client messages 1 and 3, from a server message 2).
	AppData []AppData
	// Verdict is the Anchor's; zero when the peer was not judged (a client of a
	// server without ClientAnchor, a server under AllowPSKOnly).
	Verdict Verdict
	// Value is Gate.Commit's result (server only).
	Value any
	// Profile and Mode are those of the handshake.
	Profile proto.Profile
	Mode    zmux.RecordMode
	// PQConfidential reports that the handshake's Profile resists a quantum
	// adversary; PQAuthenticated that the peer's transcript signature, which
	// the verdict asked for, is post-quantum and verified.
	PQConfidential  bool
	PQAuthenticated bool
}

// PeerOf returns the Peer of a carrier Establish returned.
func PeerOf(p zmux.PipeIO) (*Peer, bool) {
	for _, x := range []any{p.W, p.R} {
		if c, ok := x.(zmux.CarrierPeer); ok {
			if v, ok := c.CarrierPeer().(*Peer); ok {
				return v, true
			}
		}
	}
	return nil, false
}

// ClientConfig is the client's side of Establish.
type ClientConfig struct {
	// Anchor judges the server. It is required unless AllowPSKOnly.
	Anchor Anchor
	// AllowPSKOnly accepts any server that knows the pre-shared key. It is an explicit
	// bootstrap and must not override trust the host already holds.
	AllowPSKOnly bool
	// RequirePQAuth fails a server that is not authenticated by a post-quantum
	// transcript signature.
	RequirePQAuth bool
	// Mode is the carrier's record mode; the zero value is Sealed.
	Mode zmux.RecordMode
	// Handle and PSK select a profile with a pre-shared key; both empty
	// selects one without.
	Handle, PSK []byte
	// Key is the client's static key; nil is a key generated once per process.
	Key *ecdh.PrivateKey
	// AllowedProfiles are the profiles this client may use, the first that fits Handle,
	// and PSK being used. nil: both ciphers, the one with hardware support
	// first. A refusal never makes a client choose another profile.
	AllowedProfiles []proto.Profile
	// Credential goes in message 3; nil: none.
	Credential *Credential
	// Signer, when set, signs the transcript in message 3, for a server whose
	// ClientAnchor asks for it.
	Signer Signer
	// LocalContext is passed to the Anchor as VerifyInput.LocalContext.
	LocalContext any
	// AppData supplies the application data of message 1, in Profiles 1 and 3
	// only, and of message 3, after the server is judged; nil: none.
	AppData AppDataSource
	// MaxAppData bounds the application data of a received message; zero is
	// proto.DefaultMaxAppData, negative no bound beyond the message.
	MaxAppData int
	// Shaper is asked for the plan of each handshake message; nil: none.
	Shaper shaping.HandshakeShaper
	// ShapingLimits limit those plans.
	ShapingLimits shaping.Limits
}

var processKey = sync.OnceValues(func() (*ecdh.PrivateKey, error) {
	k, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", proto.ErrRandomFailed, err)
	}
	return k, nil
})

func (c *ClientConfig) profile(withPSK bool) (proto.Profile, error) {
	list := c.AllowedProfiles
	if list == nil {
		list = defaultClientProfiles()
	}
	for _, p := range list {
		if p.HasPSK() == withPSK {
			return p, nil
		}
	}
	return 0, fmt.Errorf("%w: no allowed profile fits the pre-shared key", proto.ErrInvalidConfig)
}

// Establish runs the client's handshake on p and returns the carrier for zmux.Start. On
// a failure p is closed, and nothing is retried: a different choice is a new carrier.
func Establish(ctx context.Context, p zmux.PipeIO, cfg *ClientConfig) (q zmux.PipeIO, err error) {
	stop := context.AfterFunc(ctx, func() { closeCarrier(p) })
	defer func() {
		if !stop() && err == nil {
			err = context.Cause(ctx)
			closeCarrier(q)
		}
		if err != nil {
			if ctx.Err() != nil && !errors.Is(err, ErrNoReply) {
				err = context.Cause(ctx)
			}
			closeCarrier(p)
			q = zmux.PipeIO{}
		}
	}()
	if cfg.Anchor == nil && !cfg.AllowPSKOnly {
		return q, fmt.Errorf("%w: no Anchor", proto.ErrInvalidConfig)
	}
	if len(cfg.Handle) > 0 && len(cfg.PSK) == 0 {
		return q, fmt.Errorf("%w: Handle set without a pre-shared key", proto.ErrInvalidConfig)
	}
	if err := checkPQAuth(cfg.RequirePQAuth, cfg.Anchor); err != nil {
		return q, err
	}
	if cfg.RequirePQAuth && cfg.Anchor == nil {
		return q, fmt.Errorf("%w: RequirePQAuth without an Anchor", proto.ErrInvalidConfig)
	}
	handle, psk := cfg.Handle, cfg.PSK
	profile, err := cfg.profile(len(psk) > 0)
	if err != nil {
		return q, err
	}
	if cfg.AllowPSKOnly && !profile.HasPSK() {
		return q, fmt.Errorf("%w: AllowPSKOnly without a pre-shared key", proto.ErrInvalidConfig)
	}
	key := cfg.Key
	if key == nil {
		if key, err = processKey(); err != nil {
			return q, err
		}
	}
	var binding []byte
	if cfg.Mode == zmux.Bound {
		if binding, err = readBinding(ctx, p, true); err != nil {
			return q, err
		}
	}
	hs, err := proto.NewInitiator(proto.Config{Profile: profile, Mode: proto.Mode(cfg.Mode), Handle: handle, PSK: psk, Binding: binding, Static: key,
		MaxAppData: cfg.MaxAppData})
	if err != nil {
		return q, err
	}
	f := framingOf(p)
	var padded uint64
	first := proto.Payload{}
	if profile.HasPSK() {
		if first.AppData, err = askAppData(ctx, cfg.AppData, AppDataRequest{Message: 1}); err != nil {
			return q, err
		}
	}
	if err := shape(ctx, cfg.Shaper, &cfg.ShapingLimits, p, msgHello, hs, proto.HelloHeaderLen(f, profile), &first, &padded); err != nil {
		return q, err
	}
	msg, err := hs.WriteMessage(nil, first)
	if err != nil {
		return q, err
	}
	hello, err := proto.AppendHello(nil, f, proto.Hello{Profile: profile, Mode: proto.Mode(cfg.Mode), Handle: handle, Msg1: msg})
	if err != nil {
		return q, err
	}
	if err := writeUnit(p, hello); err != nil {
		return q, err
	}
	u := &unitReader{p: p, f: f}
	reply, err := u.frame()
	if err != nil {
		if ctx.Err() != nil {
			err = context.Cause(ctx)
		}
		return q, &NoReplyError{Err: err}
	}
	pl, err := hs.ReadMessage(reply)
	if err != nil {
		return q, err
	}
	peer := &Peer{Static: bytes.Clone(hs.PeerStatic()), Credential: pl.Credential, AppData: appData(2, pl.AppData), Profile: profile, Mode: cfg.Mode,
		PQConfidential: profile.PQConfidential()}
	// The server is judged before anything of this client is written.
	in, err := cfg.verify(ctx, peer, handle, pl)
	if err != nil {
		return q, err
	}
	last := proto.Payload{Credential: cfg.Credential, Proof: proofPlaceholder(cfg.Signer)}
	r := AppDataRequest{Message: 3, Peer: in}
	if cfg.Anchor != nil {
		v := peer.Verdict
		r.Verdict = &v
	}
	if last.AppData, err = askAppData(ctx, cfg.AppData, r); err != nil {
		return q, err
	}
	if err := shape(ctx, cfg.Shaper, &cfg.ShapingLimits, p, msgFinish, hs, proto.FrameHeaderLen(f), &last, &padded); err != nil {
		return q, err
	}
	finish, err := writeProof(ctx, hs, last, cfg.Signer)
	if err != nil {
		return q, err
	}
	if finish, err = proto.AppendFrame(nil, f, finish); err != nil {
		return q, err
	}
	if err := writeUnit(p, finish); err != nil {
		return q, err
	}
	res, err := hs.Finish()
	if err != nil {
		return q, err
	}
	q = wrapResult(p, u.rest(), res, peer)
	q.HandshakePadding += padded
	return q, nil
}

// ErrNoReply matches the error of a client whose carrier ended, or whose
// handshake was cancelled or timed out, before message 2 (*NoReplyError).
var ErrNoReply = errors.New("seal: no reply")

// NoReplyError is a client's handshake that got no message 2. A server says
// nothing about a refused handshake, and one that stalls it leaves the client
// waiting until its own deadline, so give Establish a context with a time limit.
// It wraps the end of the carrier or ctx's error.
type NoReplyError struct{ Err error }

func (e *NoReplyError) Error() string {
	return "seal: no reply from the server (the Profile is not accepted, the Handle is unknown, " +
		"the pre-shared key is wrong, or the record mode is not allowed): " + e.Err.Error()
}

// Unwrap returns Err.
func (e *NoReplyError) Unwrap() error { return e.Err }

// Is matches ErrNoReply.
func (e *NoReplyError) Is(t error) bool { return t == ErrNoReply }

// writeProof writes the next message with a proof of signer, or without one
// when signer is nil.
func writeProof(ctx context.Context, hs *proto.Handshake, pl proto.Payload, signer Signer) ([]byte, error) {
	if signer == nil {
		return hs.WriteMessage(nil, pl)
	}
	return hs.WriteMessageProof(nil, pl, func(m []byte) ([]byte, error) { return signer.SignProof(ctx, m) })
}

// proofPlaceholder is the proof of signer with a signature of zeros, which
// gives the message its length before it is signed.
func proofPlaceholder(signer Signer) *proto.Proof {
	if signer == nil {
		return nil
	}
	alg, pub := signer.ProofKey()
	return &proto.Proof{Alg: alg, Pub: pub, Sig: make([]byte, alg.SigLen())}
}

// verify runs the Anchor over the server's message 2 and returns what it
// judged.
func (c *ClientConfig) verify(ctx context.Context, peer *Peer, handle []byte, pl proto.Payload) (*VerifyInput, error) {
	in := &VerifyInput{Role: RoleServer, Profile: peer.Profile, PeerStatic: peer.Static, PSKIdentity: handle,
		Credential: pl.Credential, PeerAppData: slices.Clone(peer.AppData), LocalContext: c.LocalContext}
	if c.Anchor == nil {
		return in, nil // AllowPSKOnly
	}
	v, pq, err := judge(ctx, c.Anchor, *in, pl.Proof, c.RequirePQAuth)
	if err != nil {
		return nil, err
	}
	peer.Verdict, peer.PQAuthenticated = v, pq
	authenticated(peer.AppData)
	return in, nil
}

// askAppData runs src, nil for none; an error ends the handshake.
func askAppData(ctx context.Context, src AppDataSource, r AppDataRequest) ([]byte, error) {
	if src == nil {
		return nil, nil
	}
	b, err := src(ctx, r)
	if err != nil {
		return nil, fmt.Errorf("seal: application data of message %d: %w", r.Message, err)
	}
	return b, nil
}

// Messages of an Establish.
const (
	msgHello  = "hello"
	msgReply  = "reply"
	msgFinish = "finish"
)

// shape asks for the plan of the next message, applies the limits, waits out the
// delay and sets the padding of pl, whose message has head bytes before its
// Noise message.
func shape(ctx context.Context, s shaping.HandshakeShaper, l *shaping.Limits, p zmux.PipeIO, msg string, hs *proto.Handshake, head int, pl *proto.Payload, padded *uint64) error {
	if s == nil {
		return nil
	}
	kind := ""
	if p.Carrier != 0 {
		kind = p.Carrier.String()
	}
	var plan shaping.UnitPlan
	// Each handshake family has its own name: a plan made for the lengths of one
	// is never applied to another.
	_, _ = shaping.Ask(s, &shaping.HandshakeView{Kind: kind, Handshake: proto.HandshakeEstablish, Message: msg}, &plan)
	shaping.ApplyLimits(&plan, l)
	if plan.Delay > 0 {
		t := time.NewTimer(plan.Delay)
		defer t.Stop()
		select {
		case <-t.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	noise := hs.EmptyMessageLen() + proto.PayloadLen(*pl)
	w := head + noise + max(p.WriteOverhead, 0)
	if v, ok := join.ItemFor(shaping.PadFor(w, &plan)-w, join.MaxMessage-noise); ok {
		pl.Padding, pl.PadEmpty = v, v == 0
		*padded += uint64(join.ItemSize(v))
	}
	return nil
}

// readBinding drives the carrier's handshake and reads its Binding (the Binding of the Bound mode). up says
// whether the writing half is the uplink, as it is at the client. Only a split carrier
// has an exporter per half.
func readBinding(ctx context.Context, p zmux.PipeIO, up bool) ([]byte, error) {
	for _, x := range []any{p.W, p.R} {
		if h, ok := x.(zmux.Handshaker); ok {
			if err := h.HandshakeContext(ctx); err != nil {
				return nil, fmt.Errorf("seal: carrier handshake: %w", err)
			}
		}
	}
	if !zmux.IsCarrierSecure(p) {
		return nil, fmt.Errorf("%w: Bound on a carrier without a Secure declaration", proto.ErrInvalidConfig)
	}
	if p.Carrier != zmux.CarrierSplit {
		v, err := carrierExport(p)
		if err != nil {
			return nil, err
		}
		return proto.BoundExporter(v, v)
	}
	w, err := carrierExport(zmux.PipeIO{W: p.W})
	if err != nil {
		return nil, err
	}
	r, err := carrierExport(zmux.PipeIO{R: p.R})
	if err != nil {
		return nil, err
	}
	if up {
		return proto.BoundExporter(w, r)
	}
	return proto.BoundExporter(r, w)
}

func carrierExport(p zmux.PipeIO) ([]byte, error) {
	e, ok := zmux.Exporter(p)
	if !ok {
		return nil, fmt.Errorf("%w: Bound on a carrier without an exporter", proto.ErrInvalidConfig)
	}
	v, err := e.ExportKeyingMaterial(proto.BoundExporterLabel, nil, proto.BoundExporterLen)
	if err != nil {
		return nil, fmt.Errorf("seal: carrier exporter: %w", err)
	}
	return v, nil
}

// wrapResult makes the carrier of a finished handshake. rest is what was read beyond it.
func wrapResult(p zmux.PipeIO, rest []byte, res *proto.Result, peer *Peer) zmux.PipeIO {
	lower := zmux.PrefixedPipe(p, rest)
	exp := &resultExporter{res: res, peer: peer}
	if peer.Mode == zmux.Sealed {
		return zmux.SealRecords(lower, res.Sealer, res.Opener, zmux.Sealed, exp)
	}
	return rawCarrier(p, lower, peer.Mode, exp)
}

// resultExporter is the handshake's exporter: no exporter of the carrier passes through.
type resultExporter struct {
	res  *proto.Result
	peer *Peer
}

func (e *resultExporter) ExportKeyingMaterial(label string, context []byte, n int) ([]byte, error) {
	return e.res.ExportKeyingMaterial(label, context, n)
}

func (e *resultExporter) CarrierPeer() any { return e.peer }

func framingOf(p zmux.PipeIO) proto.Framing {
	if p.Framing == zmux.FramingRecords {
		return proto.Message
	}
	return proto.Stream
}

func closeCarrier(p zmux.PipeIO) {
	if p.W != nil {
		_ = p.W.Close()
	}
	if p.R != nil && any(p.R) != any(p.W) {
		_ = p.R.Close()
	}
}

func writeUnit(p zmux.PipeIO, b []byte) error {
	if p.Framing == zmux.FramingRecords {
		mw, ok := p.W.(zmux.MessageWriter)
		if !ok {
			return errors.New("seal: Records framing needs a MessageWriter")
		}
		return mw.WriteMessage(b)
	}
	_, err := p.W.Write(b)
	return err
}

// unitReader reads handshake units from a carrier and keeps what is beyond them.
type unitReader struct {
	p   zmux.PipeIO
	f   proto.Framing
	buf []byte
}

// readChunk is the size of a read of a byte stream during the handshake.
const readChunk = 4096

// fill reads once: a whole message on a message carrier, some bytes on a byte stream.
func (u *unitReader) fill() error {
	if u.f == proto.Message {
		mr, ok := u.p.R.(zmux.MessageReader)
		if !ok {
			return errors.New("seal: Records framing needs a MessageReader")
		}
		m, err := mr.ReadMessage()
		if err != nil {
			return err
		}
		u.buf = append(u.buf[:0], m...)
		return nil
	}
	var tmp [readChunk]byte
	n, err := u.p.R.Read(tmp[:])
	u.buf = append(u.buf, tmp[:n]...)
	if n > 0 {
		return nil
	}
	return err
}

// frame returns the next Reply or Finish.
func (u *unitReader) frame() ([]byte, error) {
	if u.f == proto.Message {
		if err := u.fill(); err != nil {
			return nil, err
		}
		unit, _, err := proto.CutFrame(u.buf, u.f)
		u.buf = nil
		return unit, err
	}
	for {
		unit, n, err := proto.CutFrame(u.buf, u.f)
		if err == nil {
			out := bytes.Clone(unit)
			u.buf = u.buf[n:]
			return out, nil
		}
		if !errors.Is(err, proto.ErrIncomplete) {
			return nil, err
		}
		if err := u.fill(); err != nil {
			return nil, err
		}
	}
}

// rest returns what the reader holds beyond the units it returned.
func (u *unitReader) rest() []byte { return bytes.Clone(u.buf) }
