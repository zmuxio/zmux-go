package proto

import (
	"bytes"
	"crypto"
	"fmt"

	"github.com/zmuxio/zmux-go/internal/join"
)

// Config is one end's side of a handshake.
type Config struct {
	// Static is the key of this role, for profiles where it has one: an
	// *ecdh.PrivateKey on X25519 for every profile today.
	Static crypto.PrivateKey
	// Handle names the key, for profiles with a handle: 16 bytes.
	Handle []byte
	// PSK is the key of the length of the profile's suite (32 bytes for every
	// profile today), for profiles with a pre-shared key.
	PSK []byte
	// Binding is the 32-byte BoundExporter of the carrier, exactly for Bound.
	Binding []byte
	Profile Profile
	// MaxAppData bounds the application data of a received message; zero is
	// DefaultMaxAppData, negative no bound beyond the message. More is
	// ErrTooLarge. What is sent is bounded only by
	// the message: application data the message cannot hold is ErrTooLarge.
	MaxAppData int
	Mode       Mode
}

// Handshake is the state machine of one handshake, driven by the pattern of
// its profile. It is not safe for concurrent use.
type Handshake struct {
	pat       *pattern
	cfg       Config
	initiator bool
	finished  bool
	ss        symmetricState
	genKey    func() (keypair, error)
	s, e      keypair
	rs, re    []byte
	// e1 is this initiator's KEM key pair and re1 the encapsulation key a
	// responder received; each lives for one token pair. genKEM and encaps
	// are the KEM's, replaceable by tests.
	e1     keypair
	re1    []byte
	genKEM func() (keypair, error)
	encaps func(ek []byte) (ct, ss []byte, err error)
	idx    int
	// hp is h_p of the last message read.
	hp  []byte
	err error
}

// NewInitiator starts the client's side with a fresh ephemeral key.
func NewInitiator(cfg Config) (*Handshake, error) {
	return start(cfg, true)
}

// NewResponder starts the server's side with a fresh ephemeral key. Its
// Profile, Mode and Handle normally come from ParseHello.
func NewResponder(cfg Config) (*Handshake, error) {
	return start(cfg, false)
}

func start(cfg Config, initiator bool) (*Handshake, error) {
	pat, ok := profiles[cfg.Profile]
	if !ok {
		return nil, ErrUnknownProfile
	}
	return newHandshake(cfg, initiator, pat, pat.suite.kex.generate)
}

// newHandshake is the internal entrance: it takes the pattern, the peer's
// pre-known static key and the source of ephemeral keys.
func newHandshake(cfg Config, initiator bool, pat *pattern, genKey func() (keypair, error)) (*Handshake, error) {
	if cfg.Mode >= modeReserved {
		return nil, fmt.Errorf("%w: mode", ErrInvalidConfig)
	}
	if err := checkHandle(pat, cfg.Handle); err != nil {
		return nil, fmt.Errorf("%w: handle", ErrInvalidConfig)
	}
	if pat.hasPSK() != (len(cfg.PSK) == pat.suite.pskLen) {
		return nil, fmt.Errorf("%w: pre-shared key", ErrInvalidConfig)
	}
	// In Bound a Binding is exactly BoundExporterLen bytes on both ends; a
	// responder may lack one and then fails as ErrAuth when it reads. A
	// sender has none outside Bound; a responder's surplus Binding fails
	// as ErrAuth, its prologue differing from the sender's.
	if (cfg.Mode == Bound && len(cfg.Binding) != BoundExporterLen && (initiator || len(cfg.Binding) != 0)) ||
		(initiator && cfg.Mode != Bound && len(cfg.Binding) != 0) {
		return nil, fmt.Errorf("%w: binding", ErrInvalidConfig)
	}
	hs := &Handshake{pat: pat, cfg: cfg, initiator: initiator, genKey: genKey}
	hs.genKEM, hs.encaps = pat.suite.kem.generate, pat.suite.kem.encapsulate
	if pat.usesStatic(initiator) {
		var err error
		if hs.s, err = pat.suite.kex.importStatic(cfg.Static); err != nil {
			return nil, err
		}
	} else if cfg.Static != nil {
		return nil, fmt.Errorf("%w: static key", ErrInvalidConfig)
	}
	hs.ss.init(pat.noiseName(), pat.suite)
	hs.ss.mixHash(prologue(cfg.Profile, cfg.Mode, cfg.Handle, cfg.Binding))
	return hs, nil
}

func (hs *Handshake) ready(writing bool) error {
	if hs.err != nil {
		return hs.err
	}
	if hs.idx >= len(hs.pat.msgs) || (hs.idx%2 == 0) != (hs.initiator == writing) {
		return ErrState
	}
	return nil
}

func (hs *Handshake) fail(err error) error {
	hs.err = err
	hs.dropKEM()
	return err
}

// dropKEM gives up the KEM state: a decapsulation key serves one handshake
// and one ciphertext.
func (hs *Handshake) dropKEM() {
	hs.e1, hs.re1 = keypair{}, nil
}

// WriteMessage appends the next Noise message, without framing, to dst.
func (hs *Handshake) WriteMessage(dst []byte, p Payload) ([]byte, error) {
	if err := hs.ready(true); err != nil {
		return nil, err
	}
	if p.known()&^hs.pat.known[hs.idx] != 0 {
		return nil, hs.fail(fmt.Errorf("%w: payload type not allowed in this message", ErrInvalidConfig))
	}
	if err := hs.checkAppData(&p); err != nil {
		return nil, hs.fail(err)
	}
	if p.Credential != nil && p.Credential.Type == 0 {
		return nil, hs.fail(fmt.Errorf("%w: CredType 0 is no credential", ErrInvalidConfig))
	}
	plain, err := appendPayload(nil, p, !hs.nextKeyed())
	if err != nil {
		return nil, hs.fail(err)
	}
	return hs.writeRaw(dst, plain)
}

// DefaultMaxAppData is the default bound on the application data of a received
// message.
const DefaultMaxAppData = 256

func (hs *Handshake) maxAppData() int {
	switch {
	case hs.cfg.MaxAppData < 0:
		return maxMessage // the message bounds it
	case hs.cfg.MaxAppData > 0:
		return hs.cfg.MaxAppData
	}
	return DefaultMaxAppData
}

// checkAppData refuses application data the message cannot hold.
func (hs *Handshake) checkAppData(p *Payload) error {
	if len(p.AppData) == 0 {
		return nil
	}
	q := *p
	if hs.minMessage()+PayloadLen(q) > maxMessage {
		return fmt.Errorf("%w: application data of %d bytes does not fit the message", ErrTooLarge, len(p.AppData))
	}
	return nil
}

// Proof labels: the role of the signature is in the signed message.
const (
	proofLabelR = "zmux-seal proof r"
	proofLabelI = "zmux-seal proof i"
)

// ProofMessage is M = label ‖ 0x00 ‖ h_p of message idx (1: message 2, 2:
// message 3).
func ProofMessage(idx int, hp []byte) []byte {
	label := proofLabelR
	if idx == 2 {
		label = proofLabelI
	}
	return append(append([]byte(label), 0), hp...)
}

// WriteMessageProof is WriteMessage with a proof: p.Proof gives its KeyAlg and
// key, and sign signs M, computed from h_p after the message's last token,
// before the payload is encrypted. The signature must have the KeyAlg's length,
// so that the message length is known before it is signed.
func (hs *Handshake) WriteMessageProof(dst []byte, p Payload, sign func(m []byte) ([]byte, error)) ([]byte, error) {
	if err := hs.ready(true); err != nil {
		return nil, err
	}
	if p.Proof == nil || p.known()&^hs.pat.known[hs.idx] != 0 {
		return nil, hs.fail(fmt.Errorf("%w: payload type not allowed in this message", ErrInvalidConfig))
	}
	if err := hs.checkAppData(&p); err != nil {
		return nil, hs.fail(err)
	}
	random := !hs.nextKeyed()
	idx := hs.idx
	return hs.writeRawFunc(dst, func(hp []byte) ([]byte, error) {
		sig, err := sign(ProofMessage(idx, hp))
		if err != nil {
			return nil, err
		}
		if len(sig) != p.Proof.Alg.SigLen() {
			return nil, fmt.Errorf("%w: a proof signature of %d bytes", ErrInvalidConfig, len(sig))
		}
		q := p
		pr := *p.Proof
		pr.Sig = sig
		q.Proof = &pr
		return appendPayload(nil, q, random)
	})
}

// nextKeyed reports whether the next message's payload is encrypted.
func (hs *Handshake) nextKeyed() bool {
	if hs.ss.cs.AEAD != nil {
		return true
	}
	for _, t := range hs.pat.msgs[hs.idx] {
		switch t {
		case tokE:
			if hs.pat.hasPSK() {
				return true
			}
		case tokS, tokE1:
		default:
			return true
		}
	}
	return false
}

// EmptyMessageLen is the Noise length of the next message with an empty payload.
func (hs *Handshake) EmptyMessageLen() int { return hs.minMessage() }

// writeRaw takes the encoded payload as it is.
func (hs *Handshake) writeRaw(dst, plaintext []byte) ([]byte, error) {
	return hs.writeRawFunc(dst, func([]byte) ([]byte, error) { return plaintext, nil })
}

// writeRawFunc processes the tokens of the message, then encodes the payload
// with h_p, the handshake hash at that point.
func (hs *Handshake) writeRawFunc(dst []byte, payload func(hp []byte) ([]byte, error)) ([]byte, error) {
	start := len(dst)
	for _, t := range hs.pat.msgs[hs.idx] {
		var err error
		switch t {
		case tokE:
			if hs.e, err = hs.genKey(); err != nil {
				return nil, hs.fail(err)
			}
			dst = append(dst, hs.e.pub...)
			hs.ss.mixHash(hs.e.pub)
			if hs.pat.hasPSK() {
				hs.ss.mixKey(hs.e.pub)
			}
		case tokS:
			if dst, err = hs.ss.encryptAndHash(dst, hs.s.pub); err != nil {
				return nil, hs.fail(err)
			}
		case tokPSK:
			hs.ss.mixKeyAndHash(hs.cfg.PSK)
		case tokE1:
			// New for every handshake, a retry included; never cached.
			if hs.e1, err = hs.genKEM(); err != nil {
				return nil, hs.fail(err)
			}
			if dst, err = hs.ss.encryptAndHash(dst, hs.e1.pub); err != nil {
				return nil, hs.fail(err)
			}
		case tokEKEM1:
			// The received key is judged here, before this end sends anything.
			ct, ss, err := hs.encaps(hs.re1)
			hs.re1 = nil
			if err != nil {
				return nil, hs.fail(err)
			}
			dst, err = hs.ss.encryptAndHash(dst, ct)
			if err != nil {
				clear(ss)
				return nil, hs.fail(err)
			}
			hs.ss.mixKey(ss)
			clear(ss)
		default:
			if err = hs.mixDH(t); err != nil {
				return nil, hs.fail(err)
			}
		}
	}
	plaintext, err := payload(bytes.Clone(hs.ss.h))
	if err != nil {
		return nil, hs.fail(err)
	}
	dst, err = hs.ss.encryptAndHash(dst, plaintext)
	if err != nil {
		return nil, hs.fail(err)
	}
	if len(dst)-start > maxMessage {
		return nil, hs.fail(ErrTooLarge)
	}
	hs.idx++
	return dst, nil
}

// ReadMessage takes the next Noise message, without framing, and returns its
// validated payload.
func (hs *Handshake) ReadMessage(msg []byte) (Payload, error) {
	if err := hs.ready(false); err != nil {
		return Payload{}, err
	}
	// A first message in Bound and no Binding here: refused, unread.
	if !hs.initiator && hs.idx == 0 && hs.cfg.Mode == Bound && len(hs.cfg.Binding) == 0 {
		return Payload{}, hs.fail(ErrAuth)
	}
	known, idx := hs.pat.known[hs.idx], hs.idx
	plaintext, err := hs.readRaw(msg)
	if err != nil {
		return Payload{}, hs.fail(err)
	}
	// Application data in a message 1 without a key is refused, not skipped.
	refuse := idx == 0 && !hs.pat.hasPSK()
	p, err := parsePayload(plaintext, known, hs.maxAppData(), refuse)
	if err != nil {
		return Payload{}, hs.fail(err)
	}
	if p.Proof != nil {
		p.Proof.Message = ProofMessage(idx, hs.hp)
	}
	return p, nil
}

func (hs *Handshake) readRaw(msg []byte) ([]byte, error) {
	if len(msg) > maxMessage {
		return nil, ErrTooLarge
	}
	// A short message is malformed before anything else happens to it, whatever
	// its bytes would have done to the keys.
	if len(msg) < hs.minMessage() {
		return nil, ErrMalformed
	}
	take := func(n int) ([]byte, error) {
		if len(msg) < n {
			return nil, ErrMalformed
		}
		b := msg[:n]
		msg = msg[n:]
		return b, nil
	}
	for _, t := range hs.pat.msgs[hs.idx] {
		var err error
		switch t {
		case tokE:
			if hs.re, err = take(hs.pat.suite.kex.publicLen()); err != nil {
				return nil, err
			}
			hs.re = append([]byte{}, hs.re...)
			hs.ss.mixHash(hs.re)
			if hs.pat.hasPSK() {
				hs.ss.mixKey(hs.re)
			}
		case tokS:
			b, err := take(hs.keyedLen(hs.pat.suite.kex.publicLen()))
			if err != nil {
				return nil, err
			}
			if hs.rs, err = hs.ss.decryptAndHash(b); err != nil {
				return nil, err
			}
			if hs.s.pub != nil && bytes.Equal(hs.rs, hs.s.pub) {
				return nil, ErrPeerIsSelf
			}
		case tokPSK:
			hs.ss.mixKeyAndHash(hs.cfg.PSK)
		case tokE1:
			b, err := take(hs.keyedLen(hs.pat.suite.kem.encapKeyLen()))
			if err != nil {
				return nil, err
			}
			// Judged at ekem1, when this end writes.
			if hs.re1, err = hs.ss.decryptAndHash(b); err != nil {
				return nil, err
			}
		case tokEKEM1:
			b, err := take(hs.keyedLen(hs.pat.suite.kem.ciphertextLen()))
			if err != nil {
				return nil, err
			}
			ct, err := hs.ss.decryptAndHash(b)
			if err != nil {
				return nil, err
			}
			ss, err := hs.pat.suite.kem.decapsulate(hs.e1.priv, ct)
			hs.e1 = keypair{}
			if err != nil {
				return nil, err
			}
			hs.ss.mixKey(ss)
			clear(ss)
		default:
			if err = hs.mixDH(t); err != nil {
				return nil, err
			}
		}
	}
	hs.hp = bytes.Clone(hs.ss.h)
	plaintext, err := hs.ss.decryptAndHash(msg)
	if err != nil {
		return nil, err
	}
	hs.idx++
	return plaintext, nil
}

// keyedLen is the length of a value of n bytes sent under the current key.
func (hs *Handshake) keyedLen(n int) int {
	if hs.ss.cs.AEAD != nil {
		n += hs.ss.cs.AEAD.Overhead()
	}
	return n
}

// minMessage is the shortest valid form of the message about to be read.
func (hs *Handshake) minMessage() int {
	tag := hs.pat.suite.aead.TagLen()
	keyed := hs.ss.cs.AEAD != nil
	n := 0
	for _, t := range hs.pat.msgs[hs.idx] {
		switch t {
		case tokE:
			n += hs.pat.suite.kex.publicLen()
			keyed = keyed || hs.pat.hasPSK()
		case tokS, tokE1, tokEKEM1:
			switch t {
			case tokS:
				n += hs.pat.suite.kex.publicLen()
			case tokE1:
				n += hs.pat.suite.kem.encapKeyLen()
			default:
				n += hs.pat.suite.kem.ciphertextLen()
			}
			if keyed {
				n += tag
			}
			keyed = keyed || t == tokEKEM1
		case tokPSK, tokEE, tokES, tokSE, tokSS:
			keyed = true
		}
	}
	if keyed {
		n += tag
	}
	return n
}

func (hs *Handshake) mixDH(t token) error {
	var priv any
	var pub []byte
	switch t {
	case tokEE:
		priv, pub = hs.e.priv, hs.re
	case tokSS:
		priv, pub = hs.s.priv, hs.rs
	case tokES:
		if hs.initiator {
			priv, pub = hs.e.priv, hs.rs
		} else {
			priv, pub = hs.s.priv, hs.re
		}
	case tokSE:
		if hs.initiator {
			priv, pub = hs.s.priv, hs.re
		} else {
			priv, pub = hs.e.priv, hs.rs
		}
	case tokE, tokS, tokPSK, tokE1, tokEKEM1:
		panic("seal/proto: mixDH of a token that is not a DH")
	}
	out, err := hs.pat.suite.kex.exchange(priv, pub)
	if err != nil {
		return err
	}
	hs.ss.mixKey(out)
	return nil
}

// Profile is the profile of the handshake.
func (hs *Handshake) Profile() Profile { return hs.cfg.Profile }

// PeerStatic is the static key the peer has shown so far, nil before that.
// The handshake does not judge it: the caller verifies it before writing the
// next message.
func (hs *Handshake) PeerStatic() []byte { return hs.rs }

// Finished reports whether all messages have passed.
func (hs *Handshake) Finished() bool {
	return hs.err == nil && hs.idx == len(hs.pat.msgs) && !hs.finished
}

// Result is what a finished handshake leaves.
type Result struct {
	Profile       Profile
	Mode          Mode
	PeerStatic    []byte // nil when the profile has none
	PeerEphemeral []byte
	HandshakeHash []byte
	// Sealer and Opener protect the records of a Sealed carrier; they are
	// nil for the other modes, whose split keys are discarded.
	Sealer *Sealer
	Opener *Opener

	suite          suite
	exporterSecret []byte
}

// Finish ends the handshake and gives up its state; it can succeed once.
func (hs *Handshake) Finish() (*Result, error) {
	if !hs.Finished() {
		if hs.err != nil {
			return nil, hs.err
		}
		return nil, ErrState
	}
	hs.finished = true
	c1, c2 := hs.ss.split()
	r := &Result{
		Profile:        hs.cfg.Profile,
		Mode:           hs.cfg.Mode,
		PeerStatic:     hs.rs,
		PeerEphemeral:  hs.re,
		HandshakeHash:  hs.ss.h,
		suite:          hs.pat.suite,
		exporterSecret: exporterSecret(hs.pat.suite, hs.ss.ck, hs.ss.h),
	}
	if hs.cfg.Mode == Sealed {
		send, recv := c1, c2
		if !hs.initiator {
			send, recv = c2, c1
		}
		r.Sealer, r.Opener = join.NewSealer(hs.pat.suite.aead, send), join.NewOpener(hs.pat.suite.aead, recv)
	}
	hs.ss = symmetricState{}
	hs.e, hs.s = keypair{}, keypair{}
	hs.dropKEM()
	hs.cfg.PSK = nil
	return r, nil
}

// ExportKeyingMaterial derives keying material from this handshake, for
// every mode. The carrier's own exporter is never passed through.
func (r *Result) ExportKeyingMaterial(label string, context []byte, length int) ([]byte, error) {
	return join.Export(r.suite.hash, r.exporterSecret, label, context, length)
}
