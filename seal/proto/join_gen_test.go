package proto

import (
	"bytes"
	"crypto/mlkem"
	"crypto/mlkem/mlkemtest"
	"fmt"
	"slices"
	"testing"

	"github.com/zmuxio/zmux-go/internal/join"
	"github.com/zmuxio/zmux-go/internal/wire"
	"github.com/zmuxio/zmux-go/varint"
)

// cryptoSet is a zmux_session_crypto value of a vector.
type cryptoSet struct {
	Suites []uint64 `json:"suites"`
	Groups []uint64 `json:"groups"`
}

func (c cryptoSet) wire() wire.SessionCrypto {
	return wire.SessionCrypto{Suites: c.Suites, Groups: c.Groups}
}

// fullCrypto is the value of an end with every Suite and Group.
var fullCrypto = cryptoSet{Suites: []uint64{1, 2}, Groups: []uint64{1, 2}}

func jpsk(s *join.Suite, secret []byte, r join.Role) []byte {
	return jpskFor(s.ID, secret, r, fullCrypto, fullCrypto, s.Hash.Size())
}

func jpskFor(id uint64, secret []byte, r join.Role, ci, cr cryptoSet, n int) []byte {
	enc, err := join.SessionCrypto(ci.wire(), cr.wire())
	if err != nil {
		panic(err)
	}
	k, err := join.PSKLen(id, secret, r, enc, n)
	if err != nil {
		panic(err)
	}
	return k
}

// joinCase holds the inputs of one Join.
type joinCase struct {
	Suite       uint64    `json:"suite"`
	Mode        uint8     `json:"mode"`
	ModeByte    uint8     `json:"mode_byte"`
	Role        string    `json:"role"`
	EpochSecret hexb      `json:"epoch_secret"`
	CryptoI     cryptoSet `json:"crypto_i"`
	CryptoR     cryptoSet `json:"crypto_r"`
	JoinPSK     hexb      `json:"join_psk"`
	Locator     hexb      `json:"locator"`
	Binding     hexb      `json:"binding"`
	// Attempt makes r_i and AttemptR makes r_r.
	Attempt  uint64 `json:"attempt"`
	AttemptR uint64 `json:"attempt_r"`
	RandomI  hexb   `json:"random_i"`
	RI       hexb   `json:"r_i"`
	RandomR  hexb   `json:"random_r"`
	RR       hexb   `json:"r_r"`
	// Group and the private values of both shares: X25519 scalars, and for
	// Group 2 the joiner's ML-KEM seed d ‖ z and the acceptor's randomness m.
	Group   uint64 `json:"group"`
	XI      hexb   `json:"x_i"`
	XR      hexb   `json:"x_r"`
	KEMSeed hexb   `json:"kem_seed,omitempty"`
	EncapsM hexb   `json:"encaps_m,omitempty"`
	// AcceptedPhases are the key phases the acceptor has an epoch for; absent:
	// the phase of the Hello. A Hello of another phase is opened with a key of
	// the acceptor's own and fails as auth.
	AcceptedPhases []int `json:"accepted_phases,omitempty"`
	HelloPayload   hexb  `json:"hello_payload"`
	ConfirmPad     int   `json:"confirm_padding,omitempty"`
	ReplyPayload   hexb  `json:"reply_payload"`
}

func (c joinCase) group(t testing.TB) *join.Group {
	g, ok := join.LookupGroup(c.Group)
	if !ok {
		t.Fatal("no group", c.Group)
	}
	return g
}

func (c joinCase) randI() *join.GroupRand {
	r := &join.GroupRand{X25519: c.XI}
	if len(c.KEMSeed) > 0 {
		r.KEMSeed = c.KEMSeed
	}
	return r
}

func (c joinCase) randR() *join.GroupRand {
	m := c.EncapsM
	return &join.GroupRand{X25519: c.XR, Encaps: func(ek *mlkem.EncapsulationKey768) ([]byte, []byte, error) {
		return mlkemtest.Encapsulate768(ek, m)
	}}
}

// usable reports the Groups of both sets.
func (c joinCase) usable(g uint64) bool {
	return slices.Contains(c.CryptoI.Groups, g) && slices.Contains(c.CryptoR.Groups, g)
}

func (c joinCase) entropyR() join.Entropy {
	return join.Entropy{Attempt: c.AttemptR, Random: c.RandomR}
}

// acceptorPSK is the key the acceptor opens a Hello of phase with.
func (c joinCase) acceptorPSK(phase uint8) []byte {
	if c.AcceptedPhases != nil && !slices.Contains(c.AcceptedPhases, int(phase)) {
		return fixed("acceptor key of no epoch")
	}
	return c.JoinPSK
}

// pipeView is a join.Pipe in the field order of the vector files, which the
// production struct does not keep (it is ordered for size).
//
//goland:noinspection ALL
type pipeView struct {
	ID        uint64
	Direction uint8
	FateGroup uint64
	Priority  uint64
	Standby   bool
}

type joinVector struct {
	Name string `json:"name"`
	joinCase
	Pipe        pipeView `json:"pipe"`
	FirstBatch  hexb     `json:"first_batch"`
	ReturnBatch hexb     `json:"return_batch"`

	ShareI hexb   `json:"share_i"`
	ShareR hexb   `json:"share_r"`
	SS     []hexb `json:"ss"`
	TH     hexb   `json:"th"`
	TH2    hexb   `json:"th2"`
	TH3    hexb   `json:"th3"`
	K0     hexb   `json:"k0"`
	K1     hexb   `json:"k1"`
	HS     hexb   `json:"hs"`
	KeyI2R hexb   `json:"key_i2r"`
	KeyR2I hexb   `json:"key_r2i"`
	MACKey hexb   `json:"mac_key"`
	ExpSec hexb   `json:"exporter_secret"`
	Tag    hexb   `json:"confirm_tag"`

	HelloStream    hexb `json:"hello_stream"`
	HelloMessage   hexb `json:"hello_message"`
	ReplyStream    hexb `json:"reply_stream"`
	ReplyMessage   hexb `json:"reply_message"`
	ConfirmStream  hexb `json:"confirm_stream"`
	ConfirmMessage hexb `json:"confirm_message"`

	Export exportVector `json:"export"`
}

func joinSuite(t testing.TB, id uint64) *join.Suite {
	t.Helper()
	s, ok := join.LookupSuite(id)
	if !ok {
		t.Fatal("no suite", id)
	}
	return s
}

func joinRole(role string) join.Role {
	if role == "r" {
		return join.RoleResponder
	}
	return join.RoleInitiator
}

var joinPipe = join.Pipe{ID: 6, Direction: 2, FateGroup: 1, Priority: 3}

// joinDefaults is an honest Join of a Suite, Group, Mode, joiner role and phase,
// with an empty first batch.
func joinDefaults(id uint64, m Mode, role string, group uint64, phase uint8) joinCase {
	su, _ := join.LookupSuite(id)
	c := joinCase{
		Suite: id, Mode: uint8(m), ModeByte: uint8(m) | phase<<7, Role: role, EpochSecret: fixed("session secret"),
		Locator: fixed("locator")[:16], Attempt: 7, RandomI: fixed("join random i")[:su.NonceLen], RandomR: fixed("join random r")[:su.NonceLen],
		Group: group, XI: fixed("join x i"), XR: fixed("join x r"), CryptoI: fullCrypto, CryptoR: fullCrypto,
	}
	if phase == 1 {
		c.EpochSecret = fixed("epoch secret 1")
	}
	if group == join.GroupX25519MLKEM768 {
		c.KEMSeed, c.EncapsM = append(fixed("join kem d"), fixed("join kem z")...), fixed("join kem m")
	}
	if m == Bound {
		c.Binding = fixed("binding")
	}
	c.JoinPSK = jpsk(su, c.EpochSecret, joinRole(role))
	c.AttemptR = c.Attempt + 1
	return c.withPayloads(join.HelloPayload{Pipe: joinPipe}, join.ReplyPayload{})
}

// shares computes both shares of the case's fixed values.
func (c joinCase) shares() (shareI, shareR []byte, ss [][]byte) {
	g, _ := join.LookupGroup(c.Group)
	o, err := g.NewOffer(c.randI())
	if err != nil {
		panic(err)
	}
	shareR, ss, err = g.Respond(o.Share, c.randR())
	if err != nil {
		panic(err)
	}
	return o.Share, shareR, ss
}

// withPayloads encodes the payloads with the case's own shares.
func (c joinCase) withPayloads(h join.HelloPayload, r join.ReplyPayload) joinCase {
	si, sr, _ := c.shares()
	h.KeyShare = join.KeyShare{Group: c.Group, Share: si}
	r.KeyShare = join.KeyShare{Group: c.Group, Share: sr}
	var err error
	if c.HelloPayload, err = join.AppendHelloPayload(nil, h); err != nil {
		panic(err)
	}
	if c.ReplyPayload, err = join.AppendReplyPayload(nil, r); err != nil {
		panic(err)
	}
	return c
}

func (c joinCase) joiner(t testing.TB, f Framing) (*join.Joiner, error) {
	return join.NewJoinerFixed(join.JoinerConfig{
		Suite: joinSuite(t, c.Suite), Group: c.group(t), Phase: c.ModeByte >> 7, Mode: Mode(c.Mode), Framing: f, Locator: c.Locator, PSK: c.JoinPSK, Binding: c.Binding,
	}, join.Entropy{Attempt: c.Attempt, Random: c.RandomI}, c.randI())
}

// accept parses a Hello and opens it as the acceptor of this case.
func (c joinCase) accept(b []byte, f Framing) (*join.Acceptor, join.HelloPayload, error) {
	h, _, err := join.ParseHelloExact(b, f)
	if err != nil {
		return nil, join.HelloPayload{}, err
	}
	return join.Accept(h, f, c.acceptorPSK(h.Phase), c.Binding, c.usable)
}

// run does one honest exchange and returns the wires and both schedules.
func (c joinCase) run(t testing.TB, f Framing) (hello, reply, confirm []byte, s join.Secrets) {
	t.Helper()
	j, err := c.joiner(t, f)
	if err != nil {
		t.Fatal(err)
	}
	hello, err = j.WriteHelloRaw(nil, c.HelloPayload)
	if err != nil {
		t.Fatal(err)
	}
	a, _, err := c.accept(hello, f)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Exchange(c.randR()); err != nil {
		t.Fatal(err)
	}
	reply, err = a.WriteReplyRaw(nil, c.ReplyPayload, c.entropyR())
	if err != nil {
		t.Fatal(err)
	}
	var n int
	if _, n, err = j.ReadReply(reply); err != nil || n != len(reply) {
		t.Fatal(n, err)
	}
	confirm, err = j.WriteConfirm(nil, c.ConfirmPad)
	if err != nil {
		t.Fatal(err)
	}
	if Mode(c.Mode) != Bound {
		if a.Confirmed() {
			t.Fatal("confirmed early")
		}
		if n, err = a.ReadConfirm(confirm); err != nil || n != len(confirm) {
			t.Fatal(n, err)
		}
	}
	if !a.Confirmed() {
		t.Fatal("not confirmed")
	}
	if !bytes.Equal(a.Secrets().TH3, j.Secrets().TH3) || !bytes.Equal(a.Secrets().HS, j.Secrets().HS) {
		t.Fatal("schedules differ")
	}
	return hello, reply, confirm, j.Secrets()
}

func (c joinCase) vector(t testing.TB, name string) joinVector {
	t.Helper()
	v := joinVector{Name: name, joinCase: c}
	var s join.Secrets
	v.HelloStream, v.ReplyStream, v.ConfirmStream, s = c.run(t, Stream)
	v.HelloMessage, v.ReplyMessage, v.ConfirmMessage, _ = c.run(t, Message)
	v.RI, v.RR = s.RI, s.RR
	v.TH, v.TH2, v.TH3, v.K0, v.K1, v.HS = s.TH, s.TH2, s.TH3, s.K0, s.K1, s.HS
	v.KeyI2R, v.KeyR2I, v.MACKey, v.ExpSec, v.Tag = s.I2R, s.R2I, s.MAC, s.Exp, s.Confirm
	for _, x := range s.SS {
		v.SS = append(v.SS, x)
	}
	v.ShareI, v.ShareR, _ = c.shares()
	if !bytes.Equal(join.Combine(joinSuite(t, c.Suite).Hash.New, c.JoinPSK, s.SS...), s.HS) {
		t.Fatal("hs is not Combine")
	}
	_, p, err := c.accept(v.HelloMessage, Message)
	if err != nil {
		t.Fatal(err)
	}
	v.Pipe = pipeView{p.Pipe.ID, p.Pipe.Direction, p.Pipe.FateGroup, p.Pipe.Priority, p.Pipe.Standby}
	v.FirstBatch = p.FirstBatch
	if rp, err := join.ParseReplyPayload(c.ReplyPayload, c.Group); err == nil {
		v.ReturnBatch = rp.Frames
	}
	ex, err := join.Export(joinSuite(t, c.Suite).Hash, s.Exp, "EXPORTER-zmux-vectors", []byte("ctx"), 32)
	if err != nil {
		t.Fatal(err)
	}
	v.Export = exportVector{Label: "EXPORTER-zmux-vectors", Context: []byte("ctx"), Length: 32, Output: ex}
	return v
}

var (
	joinModes = []struct {
		name string
		m    Mode
	}{{"sealed", Sealed}, {"bound", Bound}, {"trusted", Trusted}}
	firstBatch  = []byte("first batch frames")
	returnBatch = []byte("return frames")
)

// genJoins builds join.json: each Suite, Group, Mode, joiner role and phase, with
// and without a batch, and variants.
func genJoins(t testing.TB) []joinVector {
	var out []joinVector
	for _, id := range []uint64{join.SuiteAES, join.SuiteChaCha} {
		for _, g := range []uint64{join.GroupX25519, join.GroupX25519MLKEM768} {
			for _, md := range joinModes {
				for _, role := range []string{"i", "r"} {
					for _, phase := range []uint8{0, 1} {
						for _, batch := range []bool{false, true} {
							c := joinDefaults(id, md.m, role, g, phase)
							name := fmt.Sprintf("suite%d-group%d-%s-%s-phase%d", id, g, md.name, role, phase)
							if batch {
								c = c.withPayloads(join.HelloPayload{Pipe: joinPipe, FirstBatch: firstBatch}, join.ReplyPayload{Frames: returnBatch})
								name += "-batch"
							}
							out = append(out, c.vector(t, name))
						}
					}
				}
			}
		}
	}
	for _, md := range []struct {
		name string
		m    Mode
	}{{"sealed", Sealed}, {"trusted", Trusted}} {
		for _, n := range []int{1, 40} {
			c := joinDefaults(join.SuiteAES, md.m, "i", join.GroupX25519, 0)
			c.ConfirmPad = n
			out = append(out, c.vector(t, fmt.Sprintf("suite1-%s-i-confirm-padding-%d", md.name, n)))
		}
	}
	c := joinDefaults(join.SuiteAES, Sealed, "i", join.GroupX25519, 0)
	c = c.withPayloads(join.HelloPayload{Pipe: joinPipe, Padding: 17}, join.ReplyPayload{Padding: 40})
	out = append(out, c.vector(t, "padding"))
	c = joinDefaults(join.SuiteAES, Sealed, "i", join.GroupX25519, 0)
	c = c.withPayloads(join.HelloPayload{Pipe: joinPipe}, join.ReplyPayload{Decision: join.DecisionRefuse})
	out = append(out, c.vector(t, "refused"))
	c = joinDefaults(join.SuiteAES, Sealed, "i", join.GroupX25519, 0)
	c.Locator = bytes.Repeat(fixed("locator"), 8)[:255]
	c = c.withPayloads(join.HelloPayload{Pipe: joinPipe}, join.ReplyPayload{})
	out = append(out, c.vector(t, "longest-locator"))
	c = joinDefaults(join.SuiteAES, Sealed, "i", join.GroupX25519MLKEM768, 0)
	c.CryptoI = cryptoSet{Suites: []uint64{1}, Groups: []uint64{2}}
	c.JoinPSK = jpskFor(1, c.EpochSecret, join.RoleInitiator, c.CryptoI, c.CryptoR, 32)
	c = c.withPayloads(join.HelloPayload{Pipe: joinPipe}, join.ReplyPayload{})
	out = append(out, c.vector(t, "minimal-session-crypto"))
	return out
}

// TestJoinVectorsFromFile replays both ends from the fixed inputs.
func TestJoinVectorsFromFile(t *testing.T) {
	for _, v := range readVectors[[]joinVector](t, "join.json") {
		if !bytes.Equal(jpskFor(v.Suite, v.EpochSecret, joinRole(v.Role), v.CryptoI, v.CryptoR, joinSuite(t, v.Suite).Hash.Size()), v.JoinPSK) {
			t.Errorf("%s: join_psk", v.Name)
		}
		for _, f := range []Framing{Stream, Message} {
			hello, reply, confirm, s := v.run(t, f)
			wantH, wantR, wantC := v.HelloStream, v.ReplyStream, v.ConfirmStream
			if f == Message {
				wantH, wantR, wantC = v.HelloMessage, v.ReplyMessage, v.ConfirmMessage
			}
			if !bytes.Equal(hello, wantH) || !bytes.Equal(reply, wantR) || !bytes.Equal(confirm, wantC) {
				t.Errorf("%s: wire differs", v.Name)
			}
			if !bytes.Equal(s.HS, v.HS) || !bytes.Equal(s.I2R, v.KeyI2R) || !bytes.Equal(s.Confirm, v.Tag) {
				t.Errorf("%s: schedule differs", v.Name)
			}
		}
	}
}

// Negative Join vectors.

type joinStep struct {
	// Op is hello (ParseHelloExact then Accept), write_hello, reply
	// (ReadReply, the joiner's key exchange included), write_reply (the
	// acceptor's key exchange, then the Reply) or confirm (ReadConfirm).
	// write_hello and write_reply put a padding item of Padding bytes, when not
	// zero, before the payload given.
	Op      string `json:"op"`
	Wire    hexb   `json:"wire,omitempty"`
	Padding int    `json:"padding,omitempty"`
	Need    int    `json:"need,omitempty"`
}

type joinNegative struct {
	Name string `json:"name"`
	joinCase
	Side    string     `json:"side"`
	Framing string     `json:"framing"`
	Steps   []joinStep `json:"steps"`
	Error   string     `json:"error"`
}

func padded(n int, payload []byte) []byte {
	if n == 0 {
		return payload
	}
	return append(item(0, n, make([]byte, n)...), payload...)
}

func runJoinNegative(t testing.TB, v joinNegative) (string, int) {
	t.Helper()
	f := framingOf(v.Framing)
	var j *join.Joiner
	var a *join.Acceptor
	if v.Side == "joiner" {
		var err error
		if j, err = v.joiner(t, f); err != nil {
			t.Fatal(v.Name, err)
		}
	}
	for i, s := range v.Steps {
		var err error
		switch s.Op {
		case "hello":
			a, _, err = v.accept(s.Wire, f)
		case "write_hello":
			if j == nil {
				t.Fatalf("%s: step %d: no joiner", v.Name, i)
			}
			_, err = j.WriteHelloRaw(nil, padded(s.Padding, v.HelloPayload))
		case "reply":
			if j == nil {
				t.Fatalf("%s: step %d: no joiner", v.Name, i)
			}
			_, _, err = j.ReadReply(s.Wire)
		case "write_reply":
			if a == nil {
				t.Fatalf("%s: step %d: no acceptor", v.Name, i)
			}
			if err = a.Exchange(v.randR()); err == nil {
				_, err = a.WriteReplyRaw(nil, padded(s.Padding, v.ReplyPayload), v.entropyR())
			}
		case "confirm":
			if a == nil {
				t.Fatalf("%s: step %d: no acceptor", v.Name, i)
			}
			_, err = a.ReadConfirm(s.Wire)
		default:
			t.Fatalf("%s: op %q", v.Name, s.Op)
		}
		if err != nil {
			return errorClass(err), i
		}
	}
	return "", len(v.Steps)
}

func TestJoinNegativeVectorsFromFile(t *testing.T) {
	for _, v := range readVectors[[]joinNegative](t, "join_negative.json") {
		if got, at := runJoinNegative(t, v); got != v.Error || at != len(v.Steps)-1 {
			t.Errorf("%s: got %q at step %d, want %q", v.Name, got, at, v.Error)
		}
	}
}

// helloOf writes the Hello of c with payload on f.
func (c joinCase) helloOf(t testing.TB, f Framing, payload []byte) []byte {
	t.Helper()
	j, err := c.joiner(t, f)
	if err != nil {
		t.Fatal(err)
	}
	w, err := j.WriteHelloRaw(nil, payload)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

// replyOf writes the Reply of c's acceptor with payload on f, after an honest
// Hello of c.
func (c joinCase) replyOf(t testing.TB, f Framing, payload []byte) []byte {
	t.Helper()
	a, _, err := c.accept(c.helloOf(t, f, c.HelloPayload), f)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Exchange(c.randR()); err != nil {
		t.Fatal(err)
	}
	w, err := a.WriteReplyRaw(nil, payload, c.entropyR())
	if err != nil {
		t.Fatal(err)
	}
	return w
}

// honestConfirm is the Confirm of an honest Join of c on f.
func (c joinCase) honestConfirm(t testing.TB, f Framing) []byte {
	_, _, confirm, _ := c.run(t, f)
	return confirm
}

func setByte(b []byte, at int, v byte) []byte {
	b = bytes.Clone(b)
	b[at] = v
	return b
}

// genJoinNegatives builds join_negative.json.
func genJoinNegatives(t testing.TB) []joinNegative {
	var out []joinNegative
	add := func(name string, c joinCase, side, framing, want string, steps ...joinStep) {
		v := joinNegative{Name: name, joinCase: c, Side: side, Framing: framing, Steps: steps, Error: want}
		if got, at := runJoinNegative(t, v); got != want || at != len(steps)-1 {
			t.Errorf("%s: got %q at step %d, want %q", name, got, at, want)
		}
		out = append(out, v)
	}
	hello := func(w []byte) joinStep { return joinStep{Op: "hello", Wire: w} }
	reply := func(w []byte) joinStep { return joinStep{Op: "reply", Wire: w} }
	confirm := func(w []byte) joinStep { return joinStep{Op: "confirm", Wire: w} }
	wh, wr := joinStep{Op: "write_hello"}, joinStep{Op: "write_reply"}

	c := joinDefaults(join.SuiteAES, Sealed, "i", join.GroupX25519, 0)
	c2 := joinDefaults(join.SuiteAES, Sealed, "i", join.GroupX25519MLKEM768, 0)
	h := c.helloOf(t, Stream, c.HelloPayload)
	locEnd := 1 + 1 + 1 + 1 + len(c.Locator) // 02, Suite, ModeByte, LocatorLen, Locator
	ctAt := locEnd + 32 + 2

	// The Hello header.
	add("hello-first-byte", c, "acceptor", "stream", "unknown_profile", hello(setByte(h, 0, 1)))
	add("hello-suite-zero", c, "acceptor", "stream", "unknown_suite", hello(setByte(h, 1, 0)))
	add("hello-suite-unassigned", c, "acceptor", "stream", "unknown_suite", hello(setByte(h, 1, 9)))
	add("hello-suite-private", c, "acceptor", "stream", "unknown_suite", hello(append([]byte{2, 0xc0, 0, 0, 0, 0x40, 0, 0, 0}, h[2:]...)))
	add("hello-suite-non-minimal", c, "acceptor", "stream", "malformed", hello(append([]byte{2, 0x40, 1}, h[2:]...)))
	for _, b := range []byte{0x03, 0x04, 0x40, 0x7f, 0x83} {
		add(fmt.Sprintf("hello-mode-byte-%02x", b), c, "acceptor", "stream", "bad_flags", hello(setByte(h, 2, b)))
	}
	add("hello-locator-empty", c, "acceptor", "stream", "bad_handle", hello(setByte(h, 3, 0)))
	add("hello-cut", c, "acceptor", "stream", "incomplete", hello(h[:len(h)-1]))
	add("hello-header-cut", c, "acceptor", "stream", "incomplete", hello(h[:locEnd+5]))
	add("hello-trailing", c, "acceptor", "stream", "trailing_bytes", hello(append(bytes.Clone(h), 0)))
	add("hello-ciphertext-under-tag", c, "acceptor", "stream", "malformed", hello(append(append(bytes.Clone(h[:ctAt-2]), 0, 15), make([]byte, 15)...)))
	add("hello-tag-flip", c, "acceptor", "stream", "auth", hello(flip(h, len(h)-1)))
	add("hello-mode-changed", c, "acceptor", "stream", "auth", hello(setByte(h, 2, 2)))
	add("hello-phase-changed", c, "acceptor", "stream", "auth", hello(setByte(h, 2, 0x80)))
	add("hello-suite-changed", c, "acceptor", "stream", "auth", hello(setByte(h, 1, 2)))
	add("hello-locator-flip", c, "acceptor", "stream", "auth", hello(flip(h, 5)))
	add("hello-r-flip", c, "acceptor", "stream", "auth", hello(flip(h, locEnd+3)))
	other := c
	other.JoinPSK = jpsk(joinSuite(t, 1), c.EpochSecret, join.RoleResponder)
	add("hello-reflected-other-role", other, "acceptor", "stream", "auth", hello(h))
	other = c
	other.JoinPSK = jpsk(joinSuite(t, 1), fixed("other session"), join.RoleInitiator)
	add("hello-other-session", other, "acceptor", "stream", "auth", hello(h))
	other = c
	other.JoinPSK = jpskFor(1, c.EpochSecret, join.RoleInitiator, cryptoSet{Suites: []uint64{1}, Groups: []uint64{1, 2}}, fullCrypto, 32)
	add("hello-session-crypto-tampered", other, "acceptor", "stream", "auth", hello(h))
	// The joiner uses epoch 1, the acceptor accepts epoch 0 only.
	p1 := joinDefaults(join.SuiteAES, Sealed, "i", join.GroupX25519, 1)
	p1.AcceptedPhases = []int{0}
	add("hello-phase-not-accepted", p1, "acceptor", "stream", "auth", hello(p1.helloOf(t, Stream, p1.HelloPayload)))
	b := joinDefaults(join.SuiteAES, Bound, "i", join.GroupX25519, 0)
	hb := b.helloOf(t, Stream, b.HelloPayload)
	other = b
	other.Binding = fixed("other binding")
	add("hello-binding-differs", other, "acceptor", "stream", "auth", hello(hb))
	other = c
	other.Binding = fixed("binding")
	add("hello-binding-surplus-at-acceptor", other, "acceptor", "stream", "auth", hello(h))
	other = b
	other.Binding = nil
	add("hello-binding-missing-at-acceptor", other, "acceptor", "stream", "auth", hello(hb))

	// Too large.
	big := c
	big.HelloPayload = padded(65535, c.HelloPayload)
	add("write-hello-too-large", big, "joiner", "stream", "too_large", wh)
	big = c
	big.ReplyPayload = padded(65535, c.ReplyPayload)
	add("write-reply-too-large", big, "acceptor", "stream", "too_large", hello(h), wr)

	// The Hello payload.
	pipe, _ := join.EncodePipe(joinPipe)
	si, sr, _ := c.shares()
	ks := func(g uint64, share []byte) []byte {
		v := append(varint.AppendVarint(nil, g), share...)
		return item(9, len(v), v...)
	}
	share := ks(1, si)
	pipeItem := item(5, len(pipe), pipe...)
	payload := func(name, want string, pl []byte) {
		add(name, c, "acceptor", "stream", want, hello(c.helloOf(t, Stream, pl)))
	}
	payload("payload-pipe-missing", "missing_field", share)
	payload("payload-only-padding", "missing_field", item(0, 3, 0, 0, 0))
	payload("payload-key-share-missing", "missing_field", pipeItem)
	payload("payload-pipe-direction", "bad_field", append(item(5, len(pipe), setByte(pipe, 1, 3)...), share...))
	payload("payload-pipe-standby", "bad_field", append(item(5, len(pipe), setByte(pipe, len(pipe)-1, 2)...), share...))
	payload("payload-pipe-excess", "tlv_truncated", append(item(5, len(pipe)+1, append(bytes.Clone(pipe), 0)...), share...))
	payload("payload-pipe-short", "tlv_truncated", append(item(5, len(pipe)-1, pipe[:len(pipe)-1]...), share...))
	payload("payload-pipe-non-minimal", "tlv_non_minimal", append(item(5, len(pipe)+1, append([]byte{0x40}, pipe...)...), share...))
	payload("payload-credentials-misplaced", "tlv_unknown_critical", append(item(1, 1, 0), append(pipeItem, share...)...))
	payload("payload-decision-misplaced", "tlv_unknown_critical", append(append(pipeItem, item(7, 1, 0)...), share...))
	payload("payload-first-bytes-misplaced", "tlv_unknown_critical", append(item(3, 1, 0), append(pipeItem, share...)...))
	payload("payload-order", "tlv_order", append(bytes.Clone(share), pipeItem...))
	payload("payload-duplicate-pipe", "tlv_order", append(append(bytes.Clone(pipeItem), pipeItem...), share...))
	payload("payload-trailing", "tlv_truncated", append(append(bytes.Clone(pipeItem), share...), 9))
	payload("payload-key-share-length", "bad_field", append(bytes.Clone(pipeItem), ks(1, si[:31])...))
	payload("payload-key-share-group-unknown", "bad_field", append(bytes.Clone(pipeItem), ks(3, si)...))
	payload("payload-key-share-group-private", "bad_field", append(bytes.Clone(pipeItem), ks(1<<30, si)...))
	payload("payload-key-share-group-zero", "bad_field", append(bytes.Clone(pipeItem), ks(0, si)...))
	nm := append([]byte{0x40, 1}, si...)
	payload("payload-key-share-group-non-minimal", "tlv_non_minimal", append(bytes.Clone(pipeItem), item(9, len(nm), nm...)...))
	only2 := c
	only2.CryptoR = cryptoSet{Suites: []uint64{1, 2}, Groups: []uint64{2}}
	only2.JoinPSK = jpskFor(1, c.EpochSecret, join.RoleInitiator, only2.CryptoI, only2.CryptoR, 32)
	add("payload-key-share-group-not-usable", only2, "acceptor", "stream", "bad_field", hello(only2.helloOf(t, Stream, only2.HelloPayload)))

	// The key exchange at the acceptor, after the payload passed.
	low := append(bytes.Clone(pipeItem), ks(1, make([]byte, 32))...)
	lc := c
	lc.HelloPayload = low
	add("kex-acceptor-x25519-low-order", lc, "acceptor", "stream", "kex", hello(c.helloOf(t, Stream, low)), wr)
	si2, _, _ := c2.shares()
	for _, coef := range []uint16{3329, 4095} {
		bad := append(bytes.Clone(si2[:32]), setCoefficient(si2[32:], 5, coef)...)
		pl := append(bytes.Clone(pipeItem), ks(2, bad)...)
		v := c2
		v.HelloPayload = pl
		add(fmt.Sprintf("kex-acceptor-ek-coefficient-%d", coef), v, "acceptor", "stream", "kex", hello(c2.helloOf(t, Stream, pl)), wr)
	}

	// The Reply.
	r := c.replyOf(t, Stream, c.ReplyPayload)
	add("reply-type", c, "joiner", "stream", "malformed", wh, reply(setByte(r, 0, 4)))
	add("reply-cut", c, "joiner", "stream", "incomplete", wh, reply(r[:len(r)-1]))
	add("reply-under-tag", c, "joiner", "stream", "malformed", wh, reply(append(append(bytes.Clone(r[:33]), 0, 15), make([]byte, 15)...)))
	add("reply-tag-flip", c, "joiner", "stream", "auth", wh, reply(flip(r, len(r)-1)))
	add("reply-r-flip", c, "joiner", "stream", "auth", wh, reply(flip(r, 2)))
	rshare := ks(1, sr)
	rpl := func(name, want string, pl []byte) {
		add(name, c, "joiner", "stream", want, wh, reply(c.replyOf(t, Stream, pl)))
	}
	rpl("reply-decision-missing", "missing_field", rshare)
	rpl("reply-decision-value", "bad_field", append(item(7, 1, 2), rshare...))
	rpl("reply-decision-length", "bad_field", append(item(7, 2, 0, 0), rshare...))
	rpl("reply-pipe-misplaced", "tlv_unknown_critical", append(append(bytes.Clone(pipeItem), item(7, 1, 0)...), rshare...))
	rpl("reply-key-share-missing", "missing_field", item(7, 1, 0))
	rpl("reply-key-share-other-group", "bad_field", append(item(7, 1, 0), ks(2, make([]byte, 1120))...))
	rpl("reply-key-share-length", "bad_field", append(item(7, 1, 0), ks(1, sr[:31])...))
	// The joiner's key exchange, after the Reply's AEAD passed.
	rpl("kex-joiner-x25519-low-order", "kex", append(item(7, 1, 0), ks(1, make([]byte, 32))...))
	// A changed ct decapsulates to another secret: the Confirm then fails.
	_, sr2, _ := c2.shares()
	badCT := append(item(7, 1, 0), ks(2, flip(sr2, 100))...)
	tampered := c2
	tampered.ReplyPayload = badCT
	jt, err := tampered.joiner(t, Stream)
	if err != nil {
		t.Fatal(err)
	}
	ht, _ := jt.WriteHelloRaw(nil, tampered.HelloPayload)
	at, _, err := tampered.accept(ht, Stream)
	if err != nil {
		t.Fatal(err)
	}
	if err := at.Exchange(tampered.randR()); err != nil {
		t.Fatal(err)
	}
	rt, err := at.WriteReplyRaw(nil, tampered.ReplyPayload, tampered.entropyR())
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := jt.ReadReply(rt); err != nil {
		t.Fatal(err)
	}
	ct, _ := jt.WriteConfirm(nil, 0)
	add("kex-ciphertext-tampered-confirm", c2, "acceptor", "stream", "auth", hello(ht), wr, confirm(ct))

	// The Confirm.
	cf := c.honestConfirm(t, Stream)
	add("confirm-sealed-flip", c, "acceptor", "stream", "auth", hello(h), wr, confirm(flip(cf, len(cf)-1)))
	add("confirm-sealed-cut", c, "acceptor", "stream", "incomplete", hello(h), wr, joinStep{Op: "confirm", Wire: cf[:len(cf)-1], Need: len(cf)})
	add("confirm-sealed-under-tag", c, "acceptor", "stream", "auth", hello(h), wr, confirm([]byte{0, 15, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}))
	add("confirm-sealed-nonzero", c, "acceptor", "stream", "bad_confirm", hello(h), wr, confirm(sealedConfirm(t, c, []byte{0, 1, 0})))
	add("confirm-sealed-under-reply-direction", c, "acceptor", "stream", "auth", hello(h), wr, confirm(sealedReverse(t, c)))
	tr := joinDefaults(join.SuiteAES, Trusted, "i", join.GroupX25519, 0)
	htr := tr.helloOf(t, Stream, tr.HelloPayload)
	tc := tr.honestConfirm(t, Stream)
	add("confirm-trusted-flip", tr, "acceptor", "stream", "auth", hello(htr), wr, confirm(flip(tc, 5)))
	tail := append(setByte(tc, 1, byte(len(tc)-2+2)), 0, 1)
	add("confirm-trusted-tail-not-zero", tr, "acceptor", "stream", "bad_confirm", hello(htr), wr, confirm(tail))
	add("confirm-trusted-cut", tr, "acceptor", "stream", "incomplete", hello(htr), wr, joinStep{Op: "confirm", Wire: tc[:10], Need: len(tc)})
	add("confirm-trusted-empty", tr, "acceptor", "stream", "incomplete", hello(htr), wr, joinStep{Op: "confirm", Need: 2})
	add("confirm-trusted-len-under-tag", tr, "acceptor", "stream", "malformed", hello(htr), wr, confirm(append([]byte{0, 31}, tc[2:33]...)))
	add("confirm-trusted-len-zero", tr, "acceptor", "stream", "malformed", hello(htr), wr, confirm([]byte{0, 0}))

	// On a message carrier.
	hm := c.helloOf(t, Message, c.HelloPayload)
	add("message-hello-empty", c, "acceptor", "message", "malformed", hello(nil))
	add("message-hello-first-byte", c, "acceptor", "message", "unknown_profile", hello(setByte(hm, 0, 9)))
	add("message-hello-header-cut", c, "acceptor", "message", "malformed", hello(hm[:locEnd+5]))
	add("message-hello-mode", c, "acceptor", "message", "bad_flags", hello(setByte(hm, 2, 0x04)))
	add("message-hello-no-ciphertext", c, "acceptor", "message", "malformed", hello(hm[:locEnd+32]))
	tm := tr.honestConfirm(t, Message)
	htm := tr.helloOf(t, Message, tr.HelloPayload)
	add("message-confirm-trusted-short", tr, "acceptor", "message", "malformed", hello(htm), wr, confirm(tm[:31]))
	add("message-confirm-trusted-tail-not-zero", tr, "acceptor", "message", "bad_confirm", hello(htm), wr, confirm(append(bytes.Clone(tm), 1)))
	add("message-confirm-sealed-empty", c, "acceptor", "message", "auth", hello(hm), wr, confirm(nil))
	return out
}

// sealedConfirm seals plain as the joiner's first record of an honest Join of c.
func sealedConfirm(t testing.TB, c joinCase, plain []byte) []byte {
	t.Helper()
	j, _ := c.joiner(t, Stream)
	w, _ := j.WriteHelloRaw(nil, c.HelloPayload)
	a, _, _ := c.accept(w, Stream)
	_ = a.Exchange(c.randR())
	r, _ := a.WriteReplyRaw(nil, c.ReplyPayload, c.entropyR())
	if _, _, err := j.ReadReply(r); err != nil {
		t.Fatal(err)
	}
	ct, err := join.NewSealer(joinSuite(t, c.Suite).AEAD, j.Secrets().I2R).Seal(nil, plain)
	if err != nil {
		t.Fatal(err)
	}
	return append([]byte{byte(len(ct) >> 8), byte(len(ct))}, ct...)
}

// sealedReverse is an empty Confirm sealed under the acceptor's direction.
func sealedReverse(t testing.TB, c joinCase) []byte {
	t.Helper()
	j, _ := c.joiner(t, Stream)
	w, _ := j.WriteHelloRaw(nil, c.HelloPayload)
	a, _, _ := c.accept(w, Stream)
	_ = a.Exchange(c.randR())
	r, _ := a.WriteReplyRaw(nil, c.ReplyPayload, c.entropyR())
	if _, _, err := j.ReadReply(r); err != nil {
		t.Fatal(err)
	}
	ct, err := join.NewSealer(joinSuite(t, c.Suite).AEAD, j.Secrets().R2I).Seal(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	return append([]byte{byte(len(ct) >> 8), byte(len(ct))}, ct...)
}
