package join

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/zmuxio/zmux-go/internal/wire"
	"github.com/zmuxio/zmux-go/varint"
)

// Mode says who protects the records of a carrier. It is fixed by the
// handshake.
type Mode uint8

const (
	// Sealed protects records with the keys of the handshake.
	Sealed Mode = 0
	// Bound leaves records to the carrier and binds its exporter in.
	Bound Mode = 1
	// Trusted leaves records to the carrier by explicit configuration.
	Trusted Mode = 2

	// ModeReserved and above are invalid.
	ModeReserved Mode = 3
)

func (m Mode) String() string {
	switch m {
	case Sealed:
		return "sealed"
	case Bound:
		return "bound"
	case Trusted:
		return "trusted"
	}
	return "invalid"
}

// Role is an endpoint's role in the session.
type Role uint8

const (
	RoleInitiator Role = iota
	RoleResponder
)

// ExporterLabel is the carrier exporter label of a Bound Join, read without a
// context in BindingLen bytes.
const ExporterLabel = "EXPORTER-zmux-join"

// Payload item types of Join; the registry is shared with Establish. Type 4
// is reserved: unknown in every message, so skipped (low bit 0).
const (
	tlvPadding  = 0
	tlvPipe     = 5
	tlvFrames   = 6
	tlvDecision = 7
	tlvKeyShare = 9
)

// ModeByte bits beside the Mode: bit 7 is the key phase, bits 2 to 6 are zero.
const (
	modeMask  = 0x03
	phaseBit  = 0x80
	modeSpare = 0x7c
)

const (
	lenPrefix   = 2
	nonceLabel  = "zmux join nonce"
	confirmText = "confirm"
)

// SessionCrypto is session_crypto, the declared Suites and Groups: the value of the
// session's initiator then that of its responder. Each must be Valid.
func SessionCrypto(initiator, responder wire.SessionCrypto) ([]byte, error) {
	if !initiator.Valid() || !responder.Valid() {
		return nil, fmt.Errorf("%w: zmux_session_crypto sets", ErrInvalidConfig)
	}
	return responder.Append(initiator.Append(nil)), nil
}

// MinEpochSecret is the length of every epoch secret, the session secret
// included: SHA-256 is the only hash of the session layer.
const MinEpochSecret = sha256.Size

// PSK derives join_psk[e], the pre-shared key of a Join, of the joiner
// role r from the secret of epoch e: SHA-256 whatever the Suite, with the
// Suite's hash output length. sessionCrypto is SessionCrypto of the session, so
// tampering with the parameter makes every Join fail.
func PSK(s *Suite, epochSecret []byte, r Role, sessionCrypto []byte) ([]byte, error) {
	return PSKLen(s.ID, epochSecret, r, sessionCrypto, s.Hash.Size())
}

// PSKLen is PSK of Suite id with an output of n bytes, for vectors of
// Suites whose hash output is not 32 bytes.
func PSKLen(id uint64, epochSecret []byte, r Role, sessionCrypto []byte, n int) ([]byte, error) {
	if len(epochSecret) < MinEpochSecret || len(sessionCrypto) == 0 || n < 1 {
		return nil, fmt.Errorf("%w: epoch secret or session_crypto", ErrInvalidConfig)
	}
	l := "zmux join i"
	if r == RoleResponder {
		l = "zmux join r"
	}
	d := sha256.Sum256(sessionCrypto)
	info := varint.AppendVarint(append([]byte(l), d[:]...), id)
	return Expand(sha256.New, epochSecret, string(info), n), nil
}

// Entropy is what R takes besides its message.
type Entropy struct {
	Attempt uint64
	Random  []byte
}

// NonceR computes R, bound to the message it goes in: header is the bytes before
// R and payload the plaintext.
func NonceR(s *Suite, psk []byte, e Entropy, header, payload []byte) []byte {
	d := s.Hash.New()
	d.Write(header)
	d.Write(payload)
	m := hmac.New(s.Hash.New, psk)
	m.Write([]byte(nonceLabel))
	m.Write(binary.BigEndian.AppendUint64(nil, e.Attempt))
	m.Write(e.Random)
	m.Write(d.Sum(nil))
	return m.Sum(nil)[:s.NonceLen]
}

var attempts atomic.Uint64

func freshEntropy(s *Suite) (Entropy, error) {
	random := make([]byte, s.NonceLen)
	if _, err := rand.Read(random); err != nil {
		return Entropy{}, fmt.Errorf("%w: %w", ErrRandomFailed, err) // no fallback
	}
	return Entropy{attempts.Add(1), random}, nil
}

// KeyShare is a key share item: a Group and the share of the sender's role.
type KeyShare struct {
	Group uint64
	Share []byte
}

// appendKeyShare appends the item; a zero Group, which no sender uses, omits it
// for tests of a missing share.
func appendKeyShare(b []byte, k KeyShare) []byte {
	if k.Group == 0 {
		return b
	}
	v := varint.AppendVarint(nil, k.Group)
	return appendItem(b, tlvKeyShare, append(v, k.Share...))
}

// decodeKeyShare checks a key share value: a shortest Group among usable, and
// a share of the length of that Group and of the sender's role.
func decodeKeyShare(v []byte, usable func(uint64) bool, initiator bool) (KeyShare, error) {
	id, n, err := ConsumeMinimal(v)
	if err != nil {
		return KeyShare{}, err
	}
	g, ok := LookupGroup(id)
	if !ok || !usable(id) {
		return KeyShare{}, ErrBadField
	}
	want := g.ShareR
	if initiator {
		want = g.ShareI
	}
	if len(v)-n != want {
		return KeyShare{}, ErrBadField
	}
	return KeyShare{Group: id, Share: v[n:]}, nil
}

// Pipe is the required content of a Hello.
type Pipe struct {
	ID        uint64
	FateGroup uint64
	Priority  uint64
	Direction uint8 // 0 up, 1 down, 2 bidi
	Standby   bool
}

// HelloPayload is the content of a Hello.
type HelloPayload struct {
	Pipe Pipe
	// FirstBatch is the encoded frames of the first batch, opaque here.
	FirstBatch []byte
	// KeyShare is the joiner's share; the Joiner writes its own.
	KeyShare KeyShare
	// Padding is the length of an ignorable padding item; 0 omits it unless
	// PadEmpty asks for an item with no value.
	Padding  int
	PadEmpty bool
}

// Decision is the acceptor's read-only prediction of the cap check.
type Decision uint8

const (
	DecisionAccept Decision = 0
	DecisionRefuse Decision = 1
)

// ReplyPayload is the content of a Reply.
type ReplyPayload struct {
	// Frames are return frames, opaque here.
	Frames []byte
	// KeyShare is the acceptor's share; the Acceptor writes its own.
	KeyShare KeyShare
	Padding  int
	Decision Decision
	// PadEmpty asks for a padding item with no value.
	PadEmpty bool
}

func appendItem(b []byte, typ uint64, val []byte) []byte {
	return append(AppendItemHeader(b, typ, len(val)), val...)
}

// EncodePipe returns the value of the pipe item.
func EncodePipe(p Pipe) ([]byte, error) {
	if p.Direction > 2 || p.ID > varint.MaxVarint || p.FateGroup > varint.MaxVarint || p.Priority > varint.MaxVarint {
		return nil, fmt.Errorf("%w: pipe fields", ErrInvalidConfig)
	}
	v := varint.AppendVarint(nil, p.ID)
	v = append(v, p.Direction)
	v = varint.AppendVarint(v, p.FateGroup)
	v = varint.AppendVarint(v, p.Priority)
	if p.Standby {
		return append(v, 1), nil
	}
	return append(v, 0), nil
}

func decodePipe(v []byte) (Pipe, error) {
	var p Pipe
	var n int
	var err error
	if p.ID, n, err = ConsumeMinimal(v); err != nil {
		return Pipe{}, err
	}
	v = v[n:]
	if len(v) < 1 {
		return Pipe{}, ErrTLVTruncated
	}
	if v[0] > 2 {
		return Pipe{}, ErrBadField
	}
	p.Direction = v[0]
	v = v[1:]
	if p.FateGroup, n, err = ConsumeMinimal(v); err != nil {
		return Pipe{}, err
	}
	v = v[n:]
	if p.Priority, n, err = ConsumeMinimal(v); err != nil {
		return Pipe{}, err
	}
	v = v[n:]
	if len(v) != 1 {
		return Pipe{}, ErrTLVTruncated
	}
	if v[0] > 1 {
		return Pipe{}, ErrBadField
	}
	p.Standby = v[0] == 1
	return p, nil
}

// AppendHelloPayload encodes p in the order of its types. Padding bytes are
// zero.
func AppendHelloPayload(b []byte, p HelloPayload) ([]byte, error) {
	pipe, err := EncodePipe(p.Pipe)
	if err != nil {
		return nil, err
	}
	b = p.padItem(b)
	b = appendItem(b, tlvPipe, pipe)
	if len(p.FirstBatch) > 0 {
		b = appendItem(b, tlvFrames, p.FirstBatch)
	}
	return appendKeyShare(b, p.KeyShare), nil
}

// AppendReplyPayload encodes p in the order of its types.
func AppendReplyPayload(b []byte, p ReplyPayload) ([]byte, error) {
	if p.Decision > DecisionRefuse {
		return nil, fmt.Errorf("%w: decision", ErrInvalidConfig)
	}
	if p.Padding > 0 || p.PadEmpty {
		b = appendItem(b, tlvPadding, make([]byte, p.Padding))
	}
	if len(p.Frames) > 0 {
		b = appendItem(b, tlvFrames, p.Frames)
	}
	b = appendItem(b, tlvDecision, []byte{byte(p.Decision)})
	return appendKeyShare(b, p.KeyShare), nil
}

// parseHelloPayload parses a Hello payload whose key share must have a Group
// that usable accepts.
func parseHelloPayload(b []byte, usable func(uint64) bool) (HelloPayload, error) {
	var p HelloPayload
	seen, share := false, false
	err := ParseItems(b, func(typ uint64, val []byte) (bool, error) {
		var err error
		switch typ {
		case tlvPadding:
			p.Padding = len(val)
		case tlvPipe:
			p.Pipe, err = decodePipe(val)
			seen = true
		case tlvFrames:
			p.FirstBatch = val
		case tlvKeyShare:
			p.KeyShare, err = decodeKeyShare(val, usable, true)
			share = true
		default:
			return false, nil
		}
		return true, err
	})
	if err != nil {
		return HelloPayload{}, err
	}
	if !seen || !share {
		return HelloPayload{}, ErrMissingField
	}
	return p, nil
}

// ParseReplyPayload parses a Reply payload as a joiner whose Hello used group.
func ParseReplyPayload(b []byte, group uint64) (ReplyPayload, error) {
	return parseReplyPayload(b, group)
}

// parseReplyPayload parses a Reply payload whose key share must be of group.
func parseReplyPayload(b []byte, group uint64) (ReplyPayload, error) {
	var p ReplyPayload
	seen, share := false, false
	same := func(id uint64) bool { return id == group }
	err := ParseItems(b, func(typ uint64, val []byte) (bool, error) {
		switch typ {
		case tlvPadding:
			p.Padding = len(val)
		case tlvFrames:
			p.Frames = val
		case tlvDecision:
			if len(val) != 1 || val[0] > byte(DecisionRefuse) {
				return true, ErrBadField
			}
			p.Decision, seen = Decision(val[0]), true
		case tlvKeyShare:
			var err error
			p.KeyShare, err = decodeKeyShare(val, same, false)
			share = true
			return true, err
		default:
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		return ReplyPayload{}, err
	}
	if !seen || !share {
		return ReplyPayload{}, ErrMissingField
	}
	return p, nil
}

// Hello is a parsed Join Hello. Its fields alias the parsed bytes.
type Hello struct {
	Suite *Suite
	Mode  Mode
	// Phase is the key phase, e mod 2 of the joiner's epoch.
	Phase      uint8
	Locator    []byte
	R          []byte
	Ciphertext []byte
	clear      []byte
}

// ParseHello parses the Join Hello at the start of b and returns the bytes it
// took. With ErrIncomplete, n is the smallest total length that can make
// progress. On a Message carrier b is the whole unit. Later bytes in b are left
// to the caller; ParseHelloExact refuses them. A suite outside the session's
// negotiated set is for the caller to refuse before going further.
func ParseHello(b []byte, f Framing) (h Hello, n int, err error) {
	h, n, err = ParseHelloHead(b, f)
	if err != nil {
		return Hello{}, n, err
	}
	short := func(need int) (Hello, int, error) {
		if f == Message {
			return Hello{}, 0, ErrMalformed
		}
		return Hello{}, need, ErrIncomplete
	}
	if f == Message {
		if len(b)-n > MaxMessage {
			return Hello{}, 0, ErrTooLarge
		}
		h.Ciphertext = b[n:]
		return h, len(b), nil
	}
	if len(b) < n+lenPrefix {
		return short(n + lenPrefix)
	}
	cl := int(binary.BigEndian.Uint16(b[n:]))
	n += lenPrefix
	if len(b) < n+cl {
		return short(n + cl)
	}
	h.Ciphertext = b[n : n+cl]
	return h, n + cl, nil
}

// ParseHelloHead parses the clear head of a Join Hello at the start of b,
// up to and including R, and returns the bytes it took; Ciphertext is
// not set. With ErrIncomplete, n is the smallest total length that can make
// progress (the length of the head once its Locator length is known).
func ParseHelloHead(b []byte, f Framing) (h Hello, n int, err error) {
	short := func(need int) (Hello, int, error) {
		if f == Message {
			return Hello{}, 0, ErrMalformed
		}
		return Hello{}, need, ErrIncomplete
	}
	if len(b) == 0 {
		return short(1)
	}
	if b[0] != ProfileJoin {
		return Hello{}, 0, ErrUnknownProfile
	}
	if len(b) < 2 {
		return short(2)
	}
	id, m := varint.ConsumeVarint(b[1:])
	if m == 0 {
		return short(1 + 1<<(b[1]>>6))
	}
	if m != varint.SizeVarint(id) {
		return Hello{}, 0, ErrMalformed
	}
	s, ok := LookupSuite(id)
	if !ok {
		return Hello{}, 0, ErrUnknownSuite
	}
	h.Suite = s
	n = 1 + m
	if len(b) < n+1 {
		return short(n + 1)
	}
	if b[n]&modeSpare != 0 || b[n]&modeMask >= byte(ModeReserved) {
		return Hello{}, 0, ErrBadFlags
	}
	h.Mode, h.Phase = Mode(b[n]&modeMask), (b[n]&phaseBit)>>7
	n++
	if len(b) < n+1 {
		return short(n + 1)
	}
	l := int(b[n])
	n++
	if l == 0 {
		return Hello{}, 0, ErrBadHandle
	}
	if len(b) < n+l+s.NonceLen {
		return short(n + l + s.NonceLen)
	}
	h.Locator = b[n : n+l]
	n += l
	h.R = b[n : n+s.NonceLen]
	n += s.NonceLen
	h.clear = b[:n]
	return h, n, nil
}

// ParseHelloExact is ParseHello for an acceptor's buffer: a joiner sends nothing
// after a Hello until it has the Reply.
func ParseHelloExact(b []byte, f Framing) (Hello, int, error) {
	h, n, err := ParseHello(b, f)
	if err == nil && n < len(b) {
		return Hello{}, n, ErrTrailing
	}
	return h, n, err
}

// Secrets are the values of the key schedule, for vectors and for a custom
// record layer.
type Secrets struct {
	RI, RR       []byte
	TH, TH2, TH3 []byte
	K0, K1, HS   []byte
	// SS are the shared secrets of the key exchange, in order.
	SS       [][]byte
	I2R, R2I []byte
	MAC, Exp []byte
	Confirm  []byte
}

type schedule struct {
	suite *Suite
	mode  Mode
	f     Framing
	psk   []byte
	sec   Secrets
}

func (c *schedule) h(parts ...[]byte) []byte {
	d := c.suite.Hash.New()
	for _, p := range parts {
		d.Write(p)
	}
	return d.Sum(nil)
}

func (c *schedule) key(prk []byte, label string, th []byte, n int) []byte {
	return Expand(c.suite.Hash.New, prk, label+string(th), n)
}

func (c *schedule) begin(clear, binding []byte) {
	c.sec.TH = c.h(clear, binding)
	c.sec.K0 = c.key(c.psk, "hello", c.sec.TH, c.suite.AEAD.KeyLen())
}

func (c *schedule) afterHello(helloCT, rr []byte) {
	c.sec.TH2 = c.h(c.sec.TH, helloCT, rr)
	c.sec.K1 = c.key(c.psk, "reply", c.sec.TH2, c.suite.AEAD.KeyLen())
}

// afterReply finishes the schedule with the shared secrets of the key exchange.
func (c *schedule) afterReply(replyCT []byte, ss [][]byte) {
	s := &c.sec
	s.TH3 = c.h(s.TH2, replyCT)
	s.HS = Combine(c.suite.Hash.New, c.psk, ss...)
	kl := c.suite.AEAD.KeyLen()
	s.I2R = c.key(s.HS, "i2r", s.TH3, kl)
	s.R2I = c.key(s.HS, "r2i", s.TH3, kl)
	hl := c.suite.Hash.Size()
	s.MAC = c.key(s.HS, "mac", s.TH3, hl)
	s.Exp = c.key(s.HS, "exporter", s.TH3, hl)
	m := hmac.New(c.suite.Hash.New, s.MAC)
	m.Write([]byte(confirmText))
	m.Write(s.TH3)
	s.Confirm = m.Sum(nil)
}

func (c *schedule) seal1(key, plaintext []byte) ([]byte, error) {
	a, err := c.suite.AEAD.New(key)
	if err != nil {
		return nil, err
	}
	return a.Seal(nil, c.suite.AEAD.Nonce(0), plaintext, nil), nil
}

func (c *schedule) open1(key, ciphertext []byte) ([]byte, error) {
	a, err := c.suite.AEAD.New(key)
	if err != nil {
		return nil, err
	}
	if len(ciphertext) < a.Overhead() {
		return nil, ErrMalformed
	}
	out, err := a.Open(nil, c.suite.AEAD.Nonce(0), ciphertext, nil)
	if err != nil {
		return nil, ErrAuth
	}
	return out, nil
}

func checkParams(s *Suite, m Mode, psk []byte) error {
	if m >= ModeReserved {
		return fmt.Errorf("%w: mode", ErrInvalidConfig)
	}
	if len(psk) < s.Hash.Size() {
		return fmt.Errorf("%w: join key", ErrInvalidConfig)
	}
	return nil
}

func checkBinding(m Mode, binding []byte) error {
	if (m == Bound && len(binding) != BindingLen) || (m != Bound && len(binding) != 0) {
		return fmt.Errorf("%w: binding", ErrInvalidConfig)
	}
	return nil
}

// Result is what a finished Join leaves. Sealer and Opener are nil unless the
// Mode is Sealed. The Sealed confirmation has already used n = 0 of the
// joiner's direction.
type Result struct {
	Mode   Mode
	Sealer *Sealer
	Opener *Opener

	suite *Suite
	exp   []byte
}

// Export derives keying material from this Join, in every Mode.
func (r *Result) Export(label string, context []byte, length int) ([]byte, error) {
	return Export(r.suite.Hash, r.exp, label, context, length)
}

func (c *schedule) result(joiner bool) *Result {
	r := &Result{Mode: c.mode, suite: c.suite, exp: c.sec.Exp}
	if c.mode == Sealed {
		send, recv := c.sec.I2R, c.sec.R2I
		if !joiner {
			send, recv = recv, send
		}
		r.Sealer, r.Opener = NewSealer(c.suite.AEAD, send), NewOpener(c.suite.AEAD, recv)
	}
	return r
}

// JoinerConfig is a joiner's side of a Join.
type JoinerConfig struct {
	Suite *Suite
	// Group is the key exchange, one usable in the session.
	Group *Group
	Mode  Mode
	// Phase is the key phase of the epoch whose PSK this is: 0 or 1.
	Phase   uint8
	Framing Framing
	Locator []byte
	// PSK is the join PSK of the local role in the session.
	PSK []byte
	// Binding is present exactly in Bound.
	Binding []byte
}

type joinerState uint8

const (
	joinerNew joinerState = iota
	joinerHelloSent
	joinerProven
	joinerDone
	joinerFailed
)

// Joiner is the joiner's state machine: Pending, HelloSent, Proven, Active. It
// is not safe for concurrent use.
type Joiner struct {
	c       schedule
	cfg     JoinerConfig
	entropy *Entropy // nil takes fresh entropy at the write
	offer   *Offer
	helloCT []byte
	state   joinerState
	err     error
	result  *Result
}

// NewJoiner starts a Join; its R is made fresh when the Hello is written.
func NewJoiner(cfg JoinerConfig) (*Joiner, error) {
	return newJoiner(cfg, nil)
}

func newJoiner(cfg JoinerConfig, r *GroupRand) (*Joiner, error) {
	if cfg.Suite == nil || cfg.Group == nil || cfg.Phase > 1 {
		return nil, fmt.Errorf("%w: suite, group or phase", ErrInvalidConfig)
	}
	if err := checkParams(cfg.Suite, cfg.Mode, cfg.PSK); err != nil {
		return nil, err
	}
	if err := checkBinding(cfg.Mode, cfg.Binding); err != nil {
		return nil, err
	}
	if len(cfg.Locator) == 0 || len(cfg.Locator) > 255 {
		return nil, fmt.Errorf("%w: locator", ErrInvalidConfig)
	}
	// The private values are made here, new for every Join.
	o, err := cfg.Group.NewOffer(r)
	if err != nil {
		return nil, err
	}
	return &Joiner{c: schedule{suite: cfg.Suite, mode: cfg.Mode, f: cfg.Framing, psk: cfg.PSK}, cfg: cfg, offer: o}, nil
}

// KeyShare is the share this Joiner sends in its Hello.
func (j *Joiner) KeyShare() KeyShare { return KeyShare{Group: j.cfg.Group.ID, Share: j.offer.Share} }

// NewJoinerFixed is NewJoiner with given entropy and private values, for vectors.
func NewJoinerFixed(cfg JoinerConfig, e Entropy, r *GroupRand) (*Joiner, error) {
	j, err := newJoiner(cfg, r)
	if err != nil {
		return nil, err
	}
	if len(e.Random) != cfg.Suite.NonceLen {
		return nil, fmt.Errorf("%w: nonce", ErrInvalidConfig)
	}
	j.entropy = &e
	return j, nil
}

func (j *Joiner) fail(err error) error {
	j.err, j.state, j.offer = err, joinerFailed, nil
	return err
}

// WriteHello appends the Hello.
func (j *Joiner) WriteHello(dst []byte, p HelloPayload) ([]byte, error) {
	p.KeyShare = j.KeyShare()
	plain, err := AppendHelloPayload(nil, p)
	if err != nil {
		return nil, err
	}
	return j.WriteHelloRaw(dst, plain)
}

// WriteHelloRaw takes the encoded payload as it is, for vectors.
func (j *Joiner) WriteHelloRaw(dst, plain []byte) ([]byte, error) {
	if j.state != joinerNew {
		return nil, ErrState
	}
	head := append([]byte{ProfileJoin}, varint.AppendVarint(nil, j.cfg.Suite.ID)...)
	head = append(head, byte(j.cfg.Mode)|j.cfg.Phase<<7, byte(len(j.cfg.Locator)))
	head = append(head, j.cfg.Locator...)
	e := j.entropy
	if e == nil {
		fresh, err := freshEntropy(j.cfg.Suite)
		if err != nil {
			return nil, j.fail(err)
		}
		e = &fresh
	}
	j.c.sec.RI = NonceR(j.cfg.Suite, j.cfg.PSK, *e, head, plain)
	head = append(head, j.c.sec.RI...)
	j.c.begin(head, j.cfg.Binding)
	ct, err := j.c.seal1(j.c.sec.K0, plain)
	if err != nil {
		return nil, j.fail(err)
	}
	if len(ct) > MaxMessage {
		return nil, j.fail(ErrTooLarge)
	}
	dst = append(dst, head...)
	if j.cfg.Framing == Stream {
		dst = binary.BigEndian.AppendUint16(dst, uint16(len(ct)))
	}
	j.helloCT = ct
	j.state = joinerHelloSent
	return append(dst, ct...), nil
}

// ReadReply takes the bytes that begin with the Reply and returns its payload
// and the bytes it took. ErrIncomplete leaves the state as it was, with n the
// smallest total length that can make progress; every other failure is
// permanent. A refusing Decision is not a failure of the handshake.
func (j *Joiner) ReadReply(b []byte) (p ReplyPayload, n int, err error) {
	if j.state != joinerHelloSent {
		return ReplyPayload{}, 0, ErrState
	}
	rr, ct, n, err := CutReply(b, j.cfg.Framing, j.cfg.Suite)
	if errors.Is(err, ErrIncomplete) {
		return ReplyPayload{}, n, err
	}
	if err != nil {
		return ReplyPayload{}, 0, j.fail(err)
	}
	j.c.sec.RR = rr
	j.c.afterHello(j.helloCT, rr)
	plain, err := j.c.open1(j.c.sec.K1, ct)
	if err != nil {
		return ReplyPayload{}, 0, j.fail(err)
	}
	if p, err = parseReplyPayload(plain, j.cfg.Group.ID); err != nil {
		return ReplyPayload{}, 0, j.fail(err)
	}
	// The key exchange only after the Reply's AEAD and payload passed.
	ss, err := j.offer.Finish(p.KeyShare.Share)
	j.offer = nil
	if err != nil {
		return ReplyPayload{}, 0, j.fail(err)
	}
	j.c.sec.SS = ss
	j.c.afterReply(ct, ss)
	j.result = j.c.result(true)
	j.state = joinerProven
	return p, n, nil
}

// CutReply parses a Reply's framing. With ErrIncomplete, n is the smallest total
// length that can make progress.
func CutReply(b []byte, f Framing, s *Suite) (rr, ct []byte, n int, err error) {
	short := func(need int) ([]byte, []byte, int, error) {
		if f == Message {
			return nil, nil, 0, ErrMalformed
		}
		return nil, nil, need, ErrIncomplete
	}
	if len(b) == 0 {
		return short(1)
	}
	if b[0] != ReplyType {
		return nil, nil, 0, ErrMalformed
	}
	n = 1 + s.NonceLen
	if len(b) < n {
		return short(n)
	}
	rr = b[1:n]
	if f == Message {
		if len(b)-n > MaxMessage {
			return nil, nil, 0, ErrTooLarge
		}
		return rr, b[n:], len(b), nil
	}
	if len(b) < n+lenPrefix {
		return short(n + lenPrefix)
	}
	cl := int(binary.BigEndian.Uint16(b[n:]))
	n += lenPrefix
	if len(b) < n+cl {
		return short(n + cl)
	}
	return rr, b[n : n+cl], n + cl, nil
}

// WriteConfirm appends the confirmation with zeros zero bytes and ends the
// handshake: a sealed record of them in Sealed, the tag and them in Trusted,
// nothing in Bound.
func (j *Joiner) WriteConfirm(dst []byte, zeros int) ([]byte, error) {
	if j.state != joinerProven {
		return nil, ErrState
	}
	j.state = joinerDone
	switch j.cfg.Mode {
	case Sealed:
		ct, err := j.result.Sealer.Seal(nil, make([]byte, zeros))
		if err != nil {
			return nil, j.fail(err)
		}
		if j.cfg.Framing == Stream {
			dst = binary.BigEndian.AppendUint16(dst, uint16(len(ct)))
		}
		return append(dst, ct...), nil
	case Trusted:
		if j.cfg.Framing == Stream {
			dst = binary.BigEndian.AppendUint16(dst, uint16(len(j.c.sec.Confirm)+zeros))
		}
		dst = append(dst, j.c.sec.Confirm...)
		return append(dst, make([]byte, zeros)...), nil
	}
	return dst, nil
}

// Finish returns the result after WriteConfirm.
func (j *Joiner) Finish() (*Result, error) {
	if j.state != joinerDone {
		if j.err != nil {
			return nil, j.err
		}
		return nil, ErrState
	}
	return j.result, nil
}

// Secrets returns the key schedule so far.
func (j *Joiner) Secrets() Secrets { return j.c.sec }

type acceptorState uint8

const (
	acceptorOpened acceptorState = iota
	acceptorUnconfirmed
	acceptorConfirmed
	acceptorFailed
)

// Acceptor is the acceptor's state machine: Unconfirmed, then Confirmed. It is
// not safe for concurrent use.
type Acceptor struct {
	c       schedule
	group   *Group
	share   []byte // the joiner's, until Exchange
	reply   []byte // this end's share, after Exchange
	ss      [][]byte
	helloCT []byte
	state   acceptorState
	err     error
	result  *Result
}

// Accept opens a parsed Hello with the join PSK of the joiner's role and the
// local carrier's Binding, if any, and parses its payload; usable reports the
// Groups usable in the session. A Hello in Bound without a Binding is ErrAuth,
// unopened; otherwise any disagreement fails as the AEAD does. Exchange then does
// this end's side of the key exchange. In Bound, passing here is already
// Confirmed. The payload aliases nothing of the input.
func Accept(h Hello, f Framing, psk, binding []byte, usable func(group uint64) bool) (*Acceptor, HelloPayload, error) {
	if h.Suite == nil {
		return nil, HelloPayload{}, fmt.Errorf("%w: suite", ErrInvalidConfig)
	}
	if err := checkParams(h.Suite, h.Mode, psk); err != nil {
		return nil, HelloPayload{}, err
	}
	if h.Mode == Bound && len(binding) == 0 {
		return nil, HelloPayload{}, ErrAuth
	}
	if h.Mode == Bound && len(binding) != BindingLen {
		return nil, HelloPayload{}, fmt.Errorf("%w: binding", ErrInvalidConfig)
	}
	a := &Acceptor{c: schedule{suite: h.Suite, mode: h.Mode, f: f, psk: psk}, helloCT: bytes.Clone(h.Ciphertext)}
	a.c.begin(h.clear, binding)
	plain, err := a.c.open1(a.c.sec.K0, h.Ciphertext)
	if err != nil {
		return nil, HelloPayload{}, err
	}
	p, err := parseHelloPayload(plain, usable)
	if err != nil {
		return nil, HelloPayload{}, err
	}
	a.group, _ = LookupGroup(p.KeyShare.Group)
	a.share = p.KeyShare.Share
	return a, p, nil
}

// Exchange is the acceptor's side of the key exchange, after the Pipe ID's bit is
// set: ErrKex for a share the Group refuses. r fixes the private values, for
// vectors only. The private values are dropped once the shared secrets are known.
func (a *Acceptor) Exchange(r *GroupRand) error {
	if a.state != acceptorOpened || a.share == nil {
		return ErrState
	}
	share := a.share
	a.share = nil
	out, ss, err := a.group.Respond(share, r)
	if err != nil {
		a.state, a.err = acceptorFailed, err
		return err
	}
	a.reply, a.ss = out, ss
	return nil
}

// KeyShare is the share this Acceptor sends in its Reply, after Exchange.
func (a *Acceptor) KeyShare() KeyShare { return KeyShare{Group: a.group.ID, Share: a.reply} }

// WriteReply appends the Reply with a fresh R_r.
func (a *Acceptor) WriteReply(dst []byte, p ReplyPayload) ([]byte, error) {
	if a.reply == nil {
		return nil, ErrState
	}
	p.KeyShare = a.KeyShare()
	plain, err := AppendReplyPayload(nil, p)
	if err != nil {
		return nil, err
	}
	e, err := freshEntropy(a.c.suite)
	if err != nil {
		a.state, a.err = acceptorFailed, err
		return nil, err
	}
	return a.WriteReplyRaw(dst, plain, e)
}

// WriteReplyRaw takes the encoded payload and entropy as they are, for vectors.
func (a *Acceptor) WriteReplyRaw(dst, plain []byte, e Entropy) ([]byte, error) {
	if a.state != acceptorOpened || a.reply == nil {
		return nil, ErrState
	}
	if len(e.Random) != a.c.suite.NonceLen {
		return nil, fmt.Errorf("%w: nonce", ErrInvalidConfig)
	}
	rr := NonceR(a.c.suite, a.c.psk, e, []byte{ReplyType}, plain)
	a.c.sec.RR = rr
	a.c.afterHello(a.helloCT, rr)
	ct, err := a.c.seal1(a.c.sec.K1, plain)
	if err != nil {
		a.state, a.err = acceptorFailed, err
		return nil, err
	}
	if len(ct) > MaxMessage {
		a.state, a.err = acceptorFailed, ErrTooLarge
		return nil, ErrTooLarge
	}
	a.c.sec.SS = a.ss
	a.c.afterReply(ct, a.ss)
	a.ss = nil
	a.result = a.c.result(false)
	a.state = acceptorUnconfirmed
	dst = append(dst, ReplyType)
	dst = append(dst, rr...)
	if a.c.f == Stream {
		dst = binary.BigEndian.AppendUint16(dst, uint16(len(ct)))
	}
	return append(dst, ct...), nil
}

// ReadConfirm takes the bytes that begin with the confirmation and returns the
// bytes it took. ErrIncomplete leaves the state as it was, with n the smallest
// total length that can make progress; every other failure is permanent.
func (a *Acceptor) ReadConfirm(b []byte) (n int, err error) {
	if a.state != acceptorUnconfirmed || a.c.mode == Bound {
		return 0, ErrState
	}
	fail := func(err error) (int, error) {
		a.state, a.err = acceptorFailed, err
		return 0, err
	}
	ct := b
	n = len(b)
	if a.c.f == Stream {
		if len(b) < lenPrefix {
			return lenPrefix, ErrIncomplete
		}
		n = lenPrefix + int(binary.BigEndian.Uint16(b))
		if a.c.mode == Trusted && n-lenPrefix < a.c.suite.Hash.Size() {
			return fail(ErrMalformed)
		}
		if len(b) < n {
			return n, ErrIncomplete
		}
		ct = b[lenPrefix:n]
	}
	var plain []byte
	if a.c.mode == Trusted {
		l := a.c.suite.Hash.Size()
		if len(ct) < l {
			return fail(ErrMalformed)
		}
		if !hmac.Equal(ct[:l], a.c.sec.Confirm) {
			return fail(ErrAuth)
		}
		plain = ct[l:]
	} else {
		var err error
		if plain, err = a.result.Opener.Open(nil, ct); err != nil {
			return fail(err)
		}
	}
	for _, x := range plain {
		if x != 0 {
			return fail(ErrBadConfirm)
		}
	}
	a.state = acceptorConfirmed
	return n, nil
}

// Confirmed reports whether the joiner has proven the Reply was read. Only then
// does the session count the pipe, evict, or leave a suspended state.
func (a *Acceptor) Confirmed() bool {
	return a.state == acceptorConfirmed || (a.c.mode == Bound && a.state != acceptorFailed)
}

// Finish returns the result once Confirmed and the Reply is written.
func (a *Acceptor) Finish() (*Result, error) {
	if !a.Confirmed() || a.result == nil {
		if a.err != nil {
			return nil, a.err
		}
		return nil, ErrState
	}
	return a.result, nil
}

// Secrets returns the key schedule so far.
func (a *Acceptor) Secrets() Secrets { return a.c.sec }
