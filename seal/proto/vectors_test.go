package proto

import (
	"bytes"
	"crypto/ecdh"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/flynn/noise"
	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/internal/join"
	"github.com/zmuxio/zmux-go/internal/wire"
	"github.com/zmuxio/zmux-go/varint"
)

// go test -run TestVectors -update rewrites testdata/vectors. The copy in a
// specification checkout is compared, and with -update rewritten, only when
// ZMUX_SPEC_DIR names that checkout; nothing outside this repository is
// touched otherwise.
var update = flag.Bool("update", false, "rewrite the vector files")

const localVectors = "testdata/vectors"

// specVectors returns the vector directory of the specification checkout
// named by ZMUX_SPEC_DIR, or "" when it is not set.
func specVectors(t testing.TB) string {
	root := os.Getenv("ZMUX_SPEC_DIR")
	if root == "" {
		return ""
	}
	dir := filepath.Join(root, "secure-carrier", "vectors")
	if fi, err := os.Stat(dir); err != nil || !fi.IsDir() {
		t.Fatalf("ZMUX_SPEC_DIR: %s is not a directory", dir)
	}
	return dir
}

type hexb []byte

// MarshalJSON encodes h as a hex string; UnmarshalJSON, beside it, needs a
// pointer receiver to set h.
//
//goland:noinspection GoMixedReceiverTypes
func (h hexb) MarshalJSON() ([]byte, error) { return json.Marshal(hex.EncodeToString(h)) }

// UnmarshalJSON decodes the hex string of MarshalJSON.
//
//goland:noinspection GoMixedReceiverTypes
func (h *hexb) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	d, err := hex.DecodeString(s)
	*h = d
	return err
}

func fixed(label string) []byte {
	d := sha256.Sum256([]byte("zmux-seal vectors " + label))
	return d[:]
}

func x25519(t testing.TB, seed []byte) *ecdh.PrivateKey {
	t.Helper()
	k, err := ecdh.X25519().NewPrivateKey(seed)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func fixedKeygen(t testing.TB, seed, pub []byte) func() (keypair, error) {
	return func() (keypair, error) {
		k := x25519(t, seed)
		kp := keypair{priv: k, pub: k.PublicKey().Bytes()}
		if pub != nil {
			kp.pub = pub
		}
		return kp, nil
	}
}

// parties holds the inputs of one handshake.
type parties struct {
	Profile            uint64 `json:"profile"`
	Mode               uint8  `json:"mode"`
	Handle             hexb   `json:"handle"`
	PSK                hexb   `json:"psk"`
	Binding            hexb   `json:"binding"`
	InitiatorStatic    hexb   `json:"initiator_static"`
	ResponderStatic    hexb   `json:"responder_static"`
	InitiatorEphemeral hexb   `json:"initiator_ephemeral"`
	ResponderEphemeral hexb   `json:"responder_ephemeral"`
	// InitiatorKEMSeed is the 64-byte seed (d, z) of the initiator's ML-KEM key
	// pair and ResponderKEMRandom the 32-byte m of its encapsulation, in
	// profiles with a KEM.
	InitiatorKEMSeed   hexb `json:"initiator_kem_seed,omitempty"`
	ResponderKEMRandom hexb `json:"responder_encaps_m,omitempty"`
	// MaxAppData is the receivers' local bound on application data; absent:
	// the default of 256 bytes.
	MaxAppData int `json:"max_app_data,omitempty"`
}

func defaults(p Profile, m Mode) parties {
	v := parties{
		Profile:            uint64(p),
		Mode:               uint8(m),
		InitiatorEphemeral: fixed("initiator ephemeral"),
		ResponderEphemeral: fixed("responder ephemeral"),
	}
	pat := profiles[p]
	if pat.hasHandle {
		v.Handle = fixed("handle")[:16]
	}
	if pat.hasPSK() {
		v.PSK = fixed("psk")
	}
	if m == Bound {
		v.Binding = fixed("binding")
	}
	if pat.usesStatic(true) {
		v.InitiatorStatic = fixed("initiator static")
	}
	if pat.usesStatic(false) {
		v.ResponderStatic = fixed("responder static")
	}
	v.InitiatorKEMSeed = append(fixed("initiator kem d"), fixed("initiator kem z")...)
	v.ResponderKEMRandom = fixed("responder kem m")
	return v
}

func (v parties) config(t testing.TB, static []byte) Config {
	c := Config{Profile: Profile(v.Profile), Mode: Mode(v.Mode), Handle: v.Handle, PSK: v.PSK, Binding: v.Binding, MaxAppData: v.MaxAppData}
	if len(static) > 0 {
		c.Static = x25519(t, static)
	}
	return c
}

func (v parties) side(t testing.TB, initiator bool, ephPub, staticPub []byte) *Handshake {
	t.Helper()
	static, eph := v.ResponderStatic, v.ResponderEphemeral
	if initiator {
		static, eph = v.InitiatorStatic, v.InitiatorEphemeral
	}
	hs, err := newHandshake(v.config(t, static), initiator, profiles[Profile(v.Profile)], fixedKeygen(t, eph, ephPub))
	if err != nil {
		t.Fatal(err)
	}
	if staticPub != nil {
		hs.s.pub = staticPub
	}
	hs.genKEM, hs.encaps = fixedKEM(t, v.InitiatorKEMSeed), fixedEncaps(v.ResponderKEMRandom)
	return hs
}

type msgVector struct {
	Sender      string      `json:"sender"`
	Payload     hexb        `json:"payload"`
	Credential  *credVector `json:"credential,omitempty"`
	AppData     hexb        `json:"app_data,omitempty"`
	Noise       hexb        `json:"noise"`
	WireStream  hexb        `json:"wire_stream"`
	WireMessage hexb        `json:"wire_message"`
	H           hexb        `json:"h"`
	CK          hexb        `json:"ck"`
	// With a proof: h_p, the M it signs, the ML-DSA-44 seed of the signing key
	// and the value of the proof item.
	HP           hexb `json:"h_p,omitempty"`
	ProofMessage hexb `json:"proof_message,omitempty"`
	ProofSeed    hexb `json:"proof_seed,omitempty"`
	Proof        hexb `json:"proof,omitempty"`
}

type credVector struct {
	Type uint64 `json:"type"`
	Data hexb   `json:"data"`
}

type exportVector struct {
	Label string `json:"label"`
	// LabelFill, when non-zero, stands for a label of that many 'l' bytes.
	LabelFill int  `json:"label_fill,omitempty"`
	Context   hexb `json:"context"`
	// ContextFill, when non-zero, stands for a context of that many zero bytes.
	ContextFill  int    `json:"context_fill,omitempty"`
	Length       int    `json:"length"`
	Output       hexb   `json:"output,omitempty"`
	OutputSHA256 hexb   `json:"output_sha256,omitempty"`
	Error        string `json:"error,omitempty"`
}

type handshakeVector struct {
	Name string `json:"name"`
	parties
	// PQConfidential is the Profile's PQ confidentiality.
	PQConfidential bool `json:"pq_confidential"`
	// VerifyServer is the client's verification of message 2, VerifyClient
	// the server's of message 3, when the vector has one.
	VerifyServer    *verifyVector `json:"verify_server,omitempty"`
	VerifyClient    *verifyVector `json:"verify_client,omitempty"`
	NoiseName       string        `json:"noise_name"`
	Prologue        hexb          `json:"prologue"`
	Messages        []msgVector   `json:"messages"`
	HandshakeHash   hexb          `json:"handshake_hash"`
	ChainingKey     hexb          `json:"chaining_key"`
	SplitC1         hexb          `json:"split_c1"`
	SplitC2         hexb          `json:"split_c2"`
	RecordsUseSplit bool          `json:"records_use_split"`
	ExporterSecret  hexb          `json:"exporter_secret"`
	Exporter        exportVector  `json:"exporter"`
}

func item(typ, n int, val ...byte) []byte {
	b := appendItemHeader(nil, uint64(typ), n)
	return append(b, val...)
}

func genHandshake(t testing.TB, name string, v parties, raw [][]byte) handshakeVector {
	t.Helper()
	out, _ := genHandshakeFn(t, name, v, func(idx int, _ []byte) []byte {
		if idx < len(raw) {
			return raw[idx]
		}
		return nil
	})
	return out
}

// genHandshakeFn makes each payload from h_p of its message and returns the
// payloads as read.
func genHandshakeFn(t testing.TB, name string, v parties, build func(idx int, hp []byte) []byte) (handshakeVector, []Payload) {
	t.Helper()
	var read []Payload
	pat := profiles[Profile(v.Profile)]
	out := handshakeVector{
		Name: name, parties: v, NoiseName: pat.noiseName(),
		Prologue:        prologue(Profile(v.Profile), Mode(v.Mode), v.Handle, v.Binding),
		RecordsUseSplit: Mode(v.Mode) == Sealed,
	}
	i, r := v.side(t, true, nil, nil), v.side(t, false, nil, nil)
	for idx := range pat.msgs {
		w, rd, sender := i, r, "initiator"
		if idx%2 == 1 {
			w, rd, sender = r, i, "responder"
		}
		var plain, hp []byte
		noiseMsg, err := w.writeRawFunc(nil, func(h []byte) ([]byte, error) {
			hp, plain = h, build(idx, h)
			return plain, nil
		})
		if err != nil {
			t.Fatal(name, err)
		}
		got, err := rd.ReadMessage(noiseMsg)
		if err != nil {
			t.Fatal(name, err)
		}
		read = append(read, got)
		m := msgVector{Sender: sender, Payload: append(hexb{}, plain...), Noise: noiseMsg, H: bytes.Clone(w.ss.h), CK: bytes.Clone(w.ss.ck)}
		if got.Proof != nil {
			m.HP, m.ProofMessage = hp, got.Proof.Message
		}
		if c := got.Credential; c != nil {
			m.Credential = &credVector{c.Type, c.Data}
		}
		m.AppData = got.AppData
		if idx == 0 {
			h := Hello{Profile: Profile(v.Profile), Mode: Mode(v.Mode), Handle: v.Handle, Msg1: noiseMsg}
			m.WireStream, _ = AppendHello(nil, Stream, h)
			m.WireMessage, _ = AppendHello(nil, Message, h)
		} else {
			m.WireStream, _ = AppendFrame(nil, Stream, noiseMsg)
			m.WireMessage, _ = AppendFrame(nil, Message, noiseMsg)
		}
		out.Messages = append(out.Messages, m)
	}
	c1, c2 := i.ss.split()
	out.HandshakeHash, out.ChainingKey, out.SplitC1, out.SplitC2 = bytes.Clone(i.ss.h), bytes.Clone(i.ss.ck), c1, c2
	out.ExporterSecret = exporterSecret(pat.suite, i.ss.ck, i.ss.h)
	ri, err := i.Finish()
	if err != nil {
		t.Fatal(err)
	}
	rr, err := r.Finish()
	if err != nil {
		t.Fatal(err)
	}
	a, err := ri.ExportKeyingMaterial("EXPORTER-zmux-pipe", nil, 32)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := rr.ExportKeyingMaterial("EXPORTER-zmux-pipe", nil, 32)
	if !bytes.Equal(a, b) {
		t.Fatal(name, "exporters differ")
	}
	out.Exporter = exportVector{Label: "EXPORTER-zmux-pipe", Context: hexb{}, Length: 32, Output: a}
	return out, read
}

var allProfiles = []Profile{ProfileEstablishPSK, ProfileEstablishPSKAES, ProfileEstablish, ProfileEstablishAES}

func genHandshakes(t testing.TB) []handshakeVector {
	var out []handshakeVector
	for _, p := range allProfiles {
		for _, m := range []Mode{Sealed, Bound, Trusted} {
			out = append(out, genHandshake(t, fmt.Sprintf("profile%d-%s-empty", p, m), defaults(p, m), nil))
		}
	}
	cred := testPayload(Payload{Padding: 5, Credential: &Credential{Type: 77, Data: []byte("opaque credential")}})
	firstBytes := func(p Profile) handshakeVector {
		return genHandshake(t, fmt.Sprintf("profile%d-sealed-first-bytes", p), defaults(p, Sealed), [][]byte{nil,
			testPayload(Payload{Credential: &Credential{Type: 1}, FirstBytes: []byte("server first record")}),
			testPayload(Payload{FirstBytes: []byte("client first record")})})
	}
	out = append(out,
		genHandshake(t, "profile1-sealed-credentials", defaults(ProfileEstablishPSK, Sealed),
			[][]byte{testPayload(Payload{Padding: 32}), cred, testPayload(Payload{Credential: &Credential{Type: 1}})}),
		genHandshake(t, "profile4-sealed-credentials", defaults(ProfileEstablish, Sealed), [][]byte{nil, cred}),
		firstBytes(ProfileEstablishPSK),
		firstBytes(ProfileEstablishPSKAES),
		genHandshake(t, "profile1-sealed-app-data", defaults(ProfileEstablishPSK, Sealed), [][]byte{
			testPayload(Payload{AppData: []byte("client message 1")}),
			testPayload(Payload{Credential: &Credential{Type: 1}, AppData: []byte("server message 2")}),
			testPayload(Payload{AppData: []byte("client message 3")})}),
		genHandshake(t, "profile3-sealed-app-data-msg1", defaults(ProfileEstablishPSKAES, Sealed), [][]byte{
			testPayload(Payload{AppData: bytes.Repeat([]byte{0xa5}, DefaultMaxAppData)})}),
		genHandshake(t, "profile4-sealed-app-data", defaults(ProfileEstablish, Sealed), [][]byte{nil,
			testPayload(Payload{AppData: []byte("server message 2")}),
			testPayload(Payload{Credential: &Credential{Type: 7, Data: []byte("c")}, AppData: []byte("client message 3")})}),
		genHandshake(t, "profile1-sealed-app-data-empty", defaults(ProfileEstablishPSK, Sealed), [][]byte{nil, item(2, 0), nil}),
		fullAppData(t),
		genHandshake(t, "profile4-trusted-client-first-bytes", defaults(ProfileEstablish, Trusted), [][]byte{nil, nil, testPayload(Payload{FirstBytes: []byte("client first record")})}),
		genHandshake(t, "profile3-sealed-credentials", defaults(ProfileEstablishPSKAES, Sealed),
			[][]byte{testPayload(Payload{Padding: 32}), cred, nil}),
	)
	for i := range out {
		out[i].PQConfidential = Profile(out[i].Profile).PQConfidential()
	}
	return append(out, genCredHandshakes(t)...)
}

// fullAppData is a message 3 whose application data fills the message, with
// the receivers' bound raised to that length.
func fullAppData(t testing.TB) handshakeVector {
	v := defaults(ProfileEstablishPSK, Sealed)
	n := maxMessage - 64 // message 3 with an empty payload is 64 bytes
	n -= 1 + varint.SizeVarint(uint64(n))
	v.MaxAppData = n
	out := genHandshake(t, "profile1-sealed-app-data-fills-msg3", v, [][]byte{nil, nil, testPayload(Payload{AppData: bytes.Repeat([]byte{0x5a}, n)})})
	if len(out.Messages[2].Noise) != maxMessage {
		t.Fatalf("message 3 of %d bytes", len(out.Messages[2].Noise))
	}
	return out
}

// Negative vectors drive one end with fixed inputs through steps; the last
// step fails with the named error class.

type stepVector struct {
	// Op is hello (ParseHelloExact then ReadMessage), read (CutFrame then
	// ReadMessage), write (WriteMessage with an empty payload), or verify (the
	// core's check of the proof of the message last read against the
	// verdict's ProofKey, none when absent, and RequirePQAuth). force_n sets
	// n of the current handshake cipher to 2^64-1. write takes a padding item
	// of Padding bytes and application data of AppDataLen zero bytes when
	// those are not zero.
	Op            string `json:"op"`
	Wire          hexb   `json:"wire,omitempty"`
	Padding       int    `json:"padding,omitempty"`
	AppDataLen    int    `json:"app_data_len,omitempty"`
	ProofKey      hexb   `json:"proof_key,omitempty"`
	RequirePQAuth bool   `json:"require_pq_auth,omitempty"`
}

type negativeVector struct {
	Name string `json:"name"`
	parties
	Side    string       `json:"side"`
	Framing string       `json:"framing"`
	Steps   []stepVector `json:"steps"`
	Error   string       `json:"error"`
}

func framingOf(name string) Framing {
	if name == "message" {
		return Message
	}
	return Stream
}

func errorClass(err error) string {
	for _, c := range []struct {
		e error
		n string
	}{
		{ErrIncomplete, "incomplete"}, {ErrMalformed, "malformed"}, {ErrTrailing, "trailing_bytes"},
		{ErrUnknownProfile, "unknown_profile"}, {join.ErrUnknownSuite, "unknown_suite"},
		{ErrPeerIsSelf, "peer_is_self"}, {ErrRandomFailed, "random_failed"}, {ErrInvalidConfig, "invalid_config"},
		{join.ErrMissingField, "missing_field"}, {join.ErrBadField, "bad_field"}, {join.ErrBadConfirm, "bad_confirm"},
		{ErrBadFlags, "bad_flags"}, {ErrBadHandle, "bad_handle"}, {ErrAuth, "auth"}, {ErrKex, "kex"},
		{ErrTooLarge, "too_large"}, {ErrExhausted, "exhausted"}, {ErrTLVTruncated, "tlv_truncated"},
		{ErrTLVNonMinimal, "tlv_non_minimal"}, {ErrTLVOrder, "tlv_order"},
		{ErrTLVUnknownCritical, "tlv_unknown_critical"}, {ErrTLVCredentials, "tlv_credentials"},
		{ErrCredentialsFailed, "credentials_failed"},
	} {
		if errors.Is(err, c.e) {
			return c.n
		}
	}
	return fmt.Sprintf("other: %v", err)
}

// runNegative returns the class of the first failure, and at which step.
func runNegative(t testing.TB, v negativeVector) (string, int) {
	t.Helper()
	var hs *Handshake
	f := framingOf(v.Framing)
	if v.Side == "initiator" {
		hs = v.side(t, true, nil, nil)
	}
	var last Payload
	for i, s := range v.Steps {
		var err error
		switch s.Op {
		case "hello":
			var h Hello
			if h, _, err = ParseHelloExact(s.Wire, f); err == nil {
				// The end takes the parsed fields and its own secrets.
				vv := v.parties
				vv.Profile, vv.Mode, vv.Handle = uint64(h.Profile), uint8(h.Mode), h.Handle
				hs = vv.side(t, false, nil, nil)
				_, err = hs.ReadMessage(h.Msg1)
			}
		case "read":
			var u []byte
			if hs == nil {
				t.Fatalf("%s: step %d: no handshake", v.Name, i)
			}
			if u, _, err = CutFrame(s.Wire, f); err == nil {
				last, err = hs.ReadMessage(u)
			}
		case "verify":
			// The verdict of the message just read: the core checks its proof.
			var key *KeyID
			if s.ProofKey != nil {
				k := KeyID(s.ProofKey)
				key = &k
			}
			_, err = CheckProof(key, last.Proof, s.RequirePQAuth)
		case "write":
			if hs == nil {
				t.Fatalf("%s: step %d: no handshake", v.Name, i)
			}
			_, err = hs.WriteMessage(nil, Payload{Padding: s.Padding, AppData: make([]byte, s.AppDataLen)})
		case "force_n":
			if hs == nil {
				t.Fatalf("%s: step %d: no handshake", v.Name, i)
			}
			hs.ss.cs.N = math.MaxUint64
		default:
			t.Fatalf("%s: op %q", v.Name, s.Op)
		}
		if err != nil {
			return errorClass(err), i
		}
	}
	return "", len(v.Steps)
}

// transcript runs an honest exchange with the given overrides and returns
// the wire units that were produced before anything failed.
type crafted struct {
	v                            parties
	raw                          [][]byte
	iEph, rEph, iStatic, rStatic []byte
	// iStaticMsg3 replaces the initiator's static key only as it is sent.
	iStaticMsg3 []byte
	// iEK replaces the initiator's KEM encapsulation key as it is sent; rCT
	// changes the responder's KEM ciphertext before it is sent.
	iEK []byte
	rCT func(ct []byte) []byte
}

func (c crafted) run(t testing.TB) [][]byte {
	i := c.v.side(t, true, c.iEph, c.iStatic)
	r := c.v.side(t, false, c.rEph, c.rStatic)
	if c.iEK != nil {
		gen := i.genKEM
		i.genKEM = func() (keypair, error) {
			kp, err := gen()
			kp.pub = c.iEK
			return kp, err
		}
	}
	if c.rCT != nil {
		enc := r.encaps
		r.encaps = func(ek []byte) ([]byte, []byte, error) {
			ct, ss, err := enc(ek)
			return c.rCT(ct), ss, err
		}
	}
	pat := profiles[Profile(c.v.Profile)]
	var out [][]byte
	for idx := range pat.msgs {
		w, rd := i, r
		if idx%2 == 1 {
			w, rd = r, i
		}
		var plain []byte
		if idx < len(c.raw) {
			plain = c.raw[idx]
		}
		if idx == 2 && c.iStaticMsg3 != nil {
			i.s.pub = c.iStaticMsg3
		}
		m, err := w.writeRaw(nil, plain)
		if err != nil {
			return out
		}
		var u []byte
		if idx == 0 {
			u, _ = AppendHello(nil, Stream, Hello{Profile: Profile(c.v.Profile), Mode: Mode(c.v.Mode), Handle: c.v.Handle, Msg1: m})
		} else {
			u, _ = AppendFrame(nil, Stream, m)
		}
		out = append(out, u)
		if _, err := rd.readRaw(m); err != nil {
			return out
		}
	}
	return out
}

func flip(b []byte, at int) []byte {
	c := bytes.Clone(b)
	if at < 0 {
		at += len(c)
	}
	c[at] ^= 0x01
	return c
}

// lowOrder is a curve point of order 8 (the second entry of the usual list).
var lowOrder = func() []byte {
	b, _ := hex.DecodeString("e0eb7a7c3b41b8ae1656e3faf19fc46ada098deb9c32b1fd866205165f49b800")
	return b
}()

func genNegatives(t testing.TB) []negativeVector {
	p1, p4 := defaults(ProfileEstablishPSK, Sealed), defaults(ProfileEstablish, Sealed)
	var out []negativeVector
	add := func(name string, v parties, side, class string, steps ...stepVector) {
		out = append(out, negativeVector{Name: name, parties: v, Side: side, Framing: "stream", Steps: steps, Error: class})
	}
	addMsg := func(name string, v parties, side, class string, steps ...stepVector) {
		out = append(out, negativeVector{Name: name, parties: v, Side: side, Framing: "message", Steps: steps, Error: class})
	}
	hello := func(w []byte) stepVector { return stepVector{Op: "hello", Wire: w} }
	read := func(w []byte) stepVector { return stepVector{Op: "read", Wire: w} }
	write := stepVector{Op: "write"}
	frame := func(n int) []byte { b, _ := AppendFrame(nil, Stream, make([]byte, n)); return b }

	// Hello layer.
	h1, h4 := crafted{v: p1}.run(t), crafted{v: p4}.run(t)
	hlen := 1 + 1 + HandleLen // Profile, Flags, Handle
	head4 := HelloHeaderLen(Stream, ProfileEstablish) - lenPrefix
	for _, b := range []byte{0, 2, 6, 7, 9, 0xff} {
		add(fmt.Sprintf("unknown-profile-%d", b), p1, "responder", "unknown_profile", hello(append([]byte{b}, h1[0][1:]...)))
	}
	add("unknown-profile-escape-extended", p1, "responder", "unknown_profile", hello(append([]byte{8, 0x40, 0xff}, h1[0][1:]...)))
	add("unassigned-escape-profile-10", p1, "responder", "unknown_profile", hello(append([]byte{8, 10}, h1[0][1:]...)))
	add("escape-names-a-small-profile", p1, "responder", "unknown_profile", hello(append([]byte{8, 1}, h1[0][1:]...)))
	add("escape-non-minimal", p1, "responder", "malformed", hello(append([]byte{8, 0x40, 0x01}, h1[0][1:]...)))
	add("flags-reserved-mode", p1, "responder", "bad_flags", hello(func() []byte { b := bytes.Clone(h1[0]); b[1] = 3; return b }()))
	add("flags-high-bit", p1, "responder", "bad_flags", hello(func() []byte { b := bytes.Clone(h1[0]); b[1] = 0x80; return b }()))
	add("flags-bit2", p1, "responder", "bad_flags", hello(func() []byte { b := bytes.Clone(h1[0]); b[1] = 0x04; return b }()))
	add("hello-incomplete", p1, "responder", "incomplete", hello(h1[0][:len(h1[0])-1]))
	add("bytes-after-hello", p1, "responder", "trailing_bytes", hello(append(bytes.Clone(h1[0]), 0)))

	// Short messages are malformed whatever their bytes are: one byte under the
	// shortest message 1 (1248 with a pre-shared key, 1216 without) and 2 (1200).
	add("msg1-empty", p1, "responder", "malformed", hello(append(bytes.Clone(h1[0][:hlen]), 0, 0)))
	add("msg1-short-psk", p1, "responder", "malformed", hello(append(bytes.Clone(h1[0][:hlen]), frame(1247)...)))
	add("msg1-short-no-psk", p4, "responder", "malformed", hello(append(bytes.Clone(h4[0][:head4]), frame(1215)...)))
	add("msg2-short-psk", p1, "initiator", "malformed", write, read(frame(1199)))
	add("msg2-short-no-psk", p4, "initiator", "malformed", write, read(frame(1199)))
	add("msg2-short-before-low-order-ephemeral", p1, "initiator", "malformed", write, read(append([]byte{0, 32}, lowOrder...)))
	add("msg3-short", p1, "responder", "malformed", hello(h1[0]), write, read(frame(63)))
	add("msg2-too-short", p1, "initiator", "malformed", write, read([]byte{0, 10, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}))

	// Prologue and key agreement.
	add("flags-changed-on-the-path", p1, "responder", "auth", hello(func() []byte { b := bytes.Clone(h1[0]); b[1] = byte(Trusted); return b }()))
	add("handle-changed-on-the-path", p1, "responder", "auth", hello(flip(h1[0], 3)))
	add("msg1-bit-flip-ephemeral", p1, "responder", "auth", hello(flip(h1[0], hlen+2)))
	add("msg1-bit-flip-encapsulation-key", p1, "responder", "auth", hello(flip(h1[0], hlen+lenPrefix+40)))
	add("msg1-bit-flip-tag", p1, "responder", "auth", hello(flip(h1[0], -1)))
	add("msg2-bit-flip-tag", p1, "initiator", "auth", write, read(flip(h1[1], -1)))
	add("msg2-bit-flip-ciphertext-tag", p1, "initiator", "auth", write, read(flip(h1[1], lenPrefix+32+1088+3)))
	add("msg3-bit-flip", p1, "responder", "auth", hello(h1[0]), write, read(flip(h1[2], 10)))

	// A wrong pre-shared key fails at message 1 whatever the encapsulation key
	// holds, before anything is encapsulated.
	ek := kemPub(t, p1.InitiatorKEMSeed)
	badEK := setCoefficient(ek, 0, 3329)
	wrongPSK := p1
	wrongPSK.PSK = fixed("another psk")
	add("wrong-psk", wrongPSK, "responder", "auth", hello(h1[0]))
	add("wrong-psk-invalid-encapsulation-key", wrongPSK, "responder", "auth", hello(crafted{v: p1, iEK: badEK}.run(t)[0]))
	// Without a pre-shared key a changed key surfaces at the initiator.
	add("msg1-encapsulation-key-changed-no-psk", p4, "initiator", "auth", write,
		read(crafted{v: p4, iEK: setCoefficient(ek, 5, 7)}.run(t)[1]))

	// An invalid encapsulation key (a coefficient not below 3329) fails when the
	// responder writes message 2, having sent nothing; under a pre-shared key it
	// first arrives under a valid tag.
	const last = 3*256 - 1
	for _, c := range []struct {
		name string
		i    int
		v    uint16
	}{{"first-3329", 0, 3329}, {"first-4095", 0, 4095}, {"second-3329", 1, 3329}, {"last-3329", last, 3329}, {"last-4095", last, 4095}} {
		bad := setCoefficient(ek, c.i, c.v)
		add("encapsulation-key-coefficient-"+c.name+"-psk", p1, "responder", "kex", hello(crafted{v: p1, iEK: bad}.run(t)[0]), write)
		add("encapsulation-key-coefficient-"+c.name+"-no-psk", p4, "responder", "kex", hello(crafted{v: p4, iEK: bad}.run(t)[0]), write)
	}
	// The largest coefficient that is valid.
	add("encapsulation-key-coefficient-3328-valid", p4, "responder", "malformed", hello(crafted{v: p4, iEK: setCoefficient(ek, 0, 3328)}.run(t)[0]), write, read(frame(63)))

	// A changed ciphertext decapsulates to another secret: auth, never kex.
	for _, c := range []struct {
		name string
		f    func([]byte) []byte
	}{
		{"bit-flip", func(ct []byte) []byte { return flip(ct, 100) }},
		{"all-zero", func(ct []byte) []byte { return make([]byte, len(ct)) }},
		{"all-ff", func(ct []byte) []byte { return bytes.Repeat([]byte{0xff}, len(ct)) }},
	} {
		add("ciphertext-"+c.name+"-psk", p1, "initiator", "auth", write, read(crafted{v: p1, rCT: c.f}.run(t)[1]))
		add("ciphertext-"+c.name+"-no-psk", p4, "initiator", "auth", write, read(crafted{v: p4, rCT: c.f}.run(t)[1]))
	}

	bound1, bound4 := defaults(ProfileEstablishPSK, Bound), defaults(ProfileEstablish, Bound)
	otherBinding := func(v parties) parties { v.Binding = fixed("another binding"); return v }
	add("binding-differs-psk", otherBinding(bound1), "responder", "auth", hello(crafted{v: bound1}.run(t)[0]))
	// Without a pre-shared key the responder cannot tell; the initiator can.
	bc := crafted{v: otherBinding(bound4)}.run(t)
	add("binding-differs-no-psk-seen-by-initiator", bound4, "initiator", "auth", write, read(bc[1]))

	// Low-order keys surface at the DH that uses them; ee comes before ekem1,
	// so a low-order ephemeral key is kex even beside an invalid
	// encapsulation key.
	add("low-order-initiator-ephemeral-psk", p1, "responder", "kex", hello(crafted{v: p1, iEph: lowOrder}.run(t)[0]), write)
	add("low-order-initiator-ephemeral-no-psk", p4, "responder", "kex", hello(crafted{v: p4, iEph: lowOrder}.run(t)[0]), write)
	add("low-order-initiator-ephemeral-before-encapsulation-key", p1, "responder", "kex", hello(crafted{v: p1, iEph: lowOrder, iEK: badEK}.run(t)[0]), write)
	add("low-order-responder-ephemeral", p1, "initiator", "kex", write, read(crafted{v: p1, rEph: lowOrder}.run(t)[1]))
	add("low-order-responder-static", p4, "initiator", "kex", write, read(crafted{v: p4, rStatic: lowOrder}.run(t)[1]))
	lc := crafted{v: p1, iStatic: lowOrder}.run(t)
	add("low-order-initiator-static", p1, "responder", "kex", hello(lc[0]), write, read(lc[2]))

	allZero := make([]byte, 32)
	add("all-zero-dh-initiator-ephemeral", p4, "responder", "kex", hello(crafted{v: p4, iEph: allZero}.run(t)[0]), write)
	add("all-zero-dh-responder-ephemeral", p1, "initiator", "kex", write, read(crafted{v: p1, rEph: allZero}.run(t)[1]))
	zc := crafted{v: p4, iStatic: allZero}.run(t)
	add("all-zero-dh-initiator-static-no-psk", p4, "responder", "kex", hello(zc[0]), write, read(zc[2]))
	zc = crafted{v: p4, iStatic: lowOrder}.run(t)
	add("low-order-initiator-static-no-psk", p4, "responder", "kex", hello(zc[0]), write, read(zc[2]))

	// A peer showing the local static key.
	pubOf := func(seed []byte) []byte { return x25519(t, seed).PublicKey().Bytes() }
	add("peer-is-self-initiator", p1, "initiator", "peer_is_self", write, read(crafted{v: p1, rStatic: pubOf(p1.InitiatorStatic)}.run(t)[1]))
	sc := crafted{v: p1, iStaticMsg3: pubOf(p1.ResponderStatic)}.run(t)
	add("peer-is-self-responder", p1, "responder", "peer_is_self", hello(sc[0]), write, read(sc[2]))

	// First bytes of a carrier only where the peer is authenticated.
	fb := item(3, 2, 1, 2)
	add("first-bytes-in-no-psk-msg2", p4, "initiator", "tlv_unknown_critical", write, read(crafted{v: p4, raw: [][]byte{nil, fb}}.run(t)[1]))
	add("first-bytes-in-msg1", p1, "responder", "tlv_unknown_critical", hello(crafted{v: p1, raw: [][]byte{fb}}.run(t)[0]))
	// Join's required item is a critical unknown here.
	add("join-pipe-item-in-msg2", p4, "initiator", "tlv_unknown_critical", write, read(crafted{v: p4, raw: [][]byte{nil, item(5, 0)}}.run(t)[1]))
	// A key share is Join's: unknown in every Establish message.
	ks := item(9, 33, append([]byte{1}, make([]byte, 32)...)...)
	add("key-share-in-msg1", p1, "responder", "tlv_unknown_critical", hello(crafted{v: p1, raw: [][]byte{ks}}.run(t)[0]))
	add("key-share-in-msg2", p1, "initiator", "tlv_unknown_critical", write, read(crafted{v: p1, raw: [][]byte{nil, ks}}.run(t)[1]))
	m3ks := crafted{v: p1, raw: [][]byte{nil, nil, ks}}.run(t)
	add("key-share-in-msg3", p1, "responder", "tlv_unknown_critical", hello(m3ks[0]), write, read(m3ks[2]))

	// Payload rules, on the second message of two profiles.
	nonMinLen := []byte{0x00, 0x40, 0x01, 0xaa}
	nonMinType := []byte{0x40, 0x00, 0x00}
	for _, c := range []struct {
		name  string
		raw   []byte
		class string
	}{
		{"unknown-critical", item(5, 0), "tlv_unknown_critical"},
		{"order-descending", append(item(14, 0), item(6, 0)...), "tlv_order"},
		{"duplicate-ignorable", append(item(6, 0), item(6, 0)...), "tlv_order"},
		{"duplicate-padding", append(item(0, 1, 0), item(0, 1, 0)...), "tlv_order"},
		{"padding-after-higher-type", append(item(6, 0), item(0, 0)...), "tlv_order"},
		{"non-minimal-length", nonMinLen, "tlv_non_minimal"},
		{"non-minimal-type", nonMinType, "tlv_non_minimal"},
		{"length-beyond-end", item(0, 5, 1, 2), "tlv_truncated"},
		{"trailing-byte", append(item(0, 0), 0x02), "tlv_truncated"},
		// Priority: the first violation in byte order; within an item syntax,
		// then order, then the value and whether the type is known.
		{"priority-order-before-critical", append(item(6, 0), item(5, 0)...), "tlv_order"},
		{"priority-truncated-before-order", append(item(6, 0), 0x00, 0x05), "tlv_truncated"},
		{"priority-critical-before-later-truncation", append(item(5, 0), 0x40), "tlv_unknown_critical"},
		{"priority-non-minimal-before-order", append(item(6, 0), 0x40, 0x00, 0x00), "tlv_non_minimal"},
	} {
		raw := [][]byte{nil, c.raw}
		add("payload-psk-"+c.name, p1, "initiator", c.class, write, read(crafted{v: p1, raw: raw}.run(t)[1]))
		add("payload-no-psk-"+c.name, p4, "initiator", c.class, write, read(crafted{v: p4, raw: raw}.run(t)[1]))
	}
	for _, c := range []struct {
		name  string
		raw   []byte
		class string
	}{
		{"empty", item(1, 0), "tlv_credentials"},
		{"credtype-0", item(1, 2, 0, 9), "tlv_credentials"},
		{"credtype-0-empty-credential", item(1, 1, 0), "tlv_credentials"},
		{"non-minimal-type", item(1, 3, 0x40, 0x01, 0x00), "tlv_non_minimal"},
		{"type-truncated", item(1, 1, 0x40), "tlv_truncated"},
		{"two-items", append(item(1, 2, 1, 0), item(1, 2, 1, 0)...), "tlv_order"},
		{"empty-before-later-items", append(item(1, 0), item(0, 0)...), "tlv_credentials"},
	} {
		add("credential-"+c.name, p4, "initiator", c.class, write, read(crafted{v: p4, raw: [][]byte{nil, c.raw}}.run(t)[1]))
	}
	add("credential-in-msg1", p1, "responder", "tlv_unknown_critical", hello(crafted{v: p1, raw: [][]byte{item(1, 2, 1, 0)}}.run(t)[0]))

	// Too large, and the nonce space, in a handshake.
	add("write-msg1-too-large", p1, "initiator", "too_large", stepVector{Op: "write", Padding: 65535})
	add("write-msg2-too-large", p1, "responder", "too_large", hello(h1[0]), stepVector{Op: "write", Padding: 65535})
	add("write-msg3-too-large", p1, "initiator", "too_large", write, read(h1[1]), stepVector{Op: "write", Padding: 65535})
	forceN := stepVector{Op: "force_n"}
	add("write-exhausted", p1, "initiator", "exhausted", write, read(h1[1]), forceN, write)
	add("read-exhausted", p1, "responder", "exhausted", hello(h1[0]), write, forceN, read(h1[2]))

	// Application data: the local bound, one item per message.
	over := make([]byte, DefaultMaxAppData+1)
	add("app-data-over-the-bound-msg1", p1, "responder", "too_large", hello(crafted{v: p1, raw: [][]byte{item(2, len(over), over...)}}.run(t)[0]))
	add("app-data-over-the-bound-msg2", p4, "initiator", "too_large", write, read(crafted{v: p4, raw: [][]byte{nil, item(2, len(over), over...)}}.run(t)[1]))
	am3 := crafted{v: p1, raw: [][]byte{nil, nil, item(2, len(over), over...)}}.run(t)
	add("app-data-over-the-bound-msg3", p1, "responder", "too_large", hello(am3[0]), write, read(am3[2]))
	add("app-data-in-msg1-no-psk", p4, "responder", "malformed", hello(crafted{v: p4, raw: [][]byte{item(2, 1, 0)}}.run(t)[0]))
	add("app-data-in-msg1-no-psk-empty", p4, "responder", "malformed", hello(crafted{v: p4, raw: [][]byte{item(2, 0)}}.run(t)[0]))
	add("app-data-write-beyond-the-message", p1, "initiator", "too_large", stepVector{Op: "write", AppDataLen: 65536})
	add("app-data-second-item", p1, "initiator", "tlv_order", write, read(crafted{v: p1, raw: [][]byte{nil, append(item(2, 1, 1), item(2, 1, 2)...)}}.run(t)[1]))

	// A verdict that names a proof key: the message's proof must be there,
	// under that key, of a proof key's KeyAlg, and verify.
	pk := newVKey(t, KeyAlgMLDSA44, "server proof key")
	verify := func(key []byte, pq bool) stepVector {
		return stepVector{Op: "verify", ProofKey: key, RequirePQAuth: pq}
	}
	withProof := func(f func(hp []byte) *Proof) []byte {
		i, r := p1.side(t, true, nil, nil), p1.side(t, false, nil, nil)
		m1, _ := i.writeRaw(nil, nil)
		if _, err := r.ReadMessage(m1); err != nil {
			t.Fatal(err)
		}
		m2, err := r.writeRawFunc(nil, func(hp []byte) ([]byte, error) { return testPayload(Payload{Proof: f(hp)}), nil })
		if err != nil {
			t.Fatal(err)
		}
		w, _ := AppendFrame(nil, Stream, m2)
		return w
	}
	good := withProof(func(hp []byte) *Proof { return proofOf(t, pk, 1, hp) })
	if got, _ := runNegative(t, negativeVector{parties: p1, Side: "initiator", Framing: "stream", Steps: []stepVector{write, read(good), verify(pk.ID().slice(), true)}}); got != "" {
		t.Fatalf("the valid proof fails: %s", got)
	}
	other := newVKey(t, KeyAlgMLDSA44, "client proof key")
	add("proof-key-named-no-proof", p1, "initiator", "credentials_failed", write, read(h1[1]), verify(pk.ID().slice(), false))
	add("proof-key-id-mismatch", p1, "initiator", "credentials_failed", write, read(good), verify(other.ID().slice(), false))
	add("proof-keyalg-not-3", p1, "initiator", "credentials_failed", write, read(withProof(func(hp []byte) *Proof {
		p := proofOf(t, pk, 1, hp)
		p.Alg = 4
		return p
	})), verify(pk.ID().slice(), false))
	add("proof-bad-signature", p1, "initiator", "credentials_failed", write, read(withProof(func(hp []byte) *Proof {
		p := proofOf(t, pk, 1, hp)
		p.Sig = flip(p.Sig, 10)
		return p
	})), verify(pk.ID().slice(), false))
	add("pq-auth-required-no-proof-key", p1, "initiator", "credentials_failed", write, read(good), verify(nil, true))

	// A proof: the core checks only its varints and that Pub is within it.
	for _, c := range []struct {
		name, class string
		v           []byte
	}{
		{"proof-keyalg-non-minimal", "tlv_non_minimal", []byte{0x40, 0x03, 0x01, 0xaa, 0xbb}},
		{"proof-publen-non-minimal", "tlv_non_minimal", []byte{0x03, 0x40, 0x01, 0xaa, 0xbb}},
		{"proof-pub-beyond-value", "tlv_truncated", []byte{0x03, 0x05, 0xaa, 0xbb}},
		{"proof-empty", "tlv_truncated", []byte{}},
	} {
		add(c.name, p1, "initiator", c.class, write, read(crafted{v: p1, raw: [][]byte{nil, item(8, len(c.v), c.v...)}}.run(t)[1]))
	}
	pm3 := crafted{v: p4, raw: [][]byte{nil, nil, item(8, 3, 0x03, 0x05, 0xaa)}}.run(t)
	add("proof-in-msg3-pub-beyond-value", p4, "responder", "tlv_truncated", hello(pm3[0]), write, read(pm3[2]))

	// A Bound first message with no Binding here is refused unread, also in a Profile
	// whose message 1 has no tag; any other disagreement is auth too.
	surplus := p1
	surplus.Binding = fixed("binding")
	add("responder-binding-surplus", surplus, "responder", "auth", hello(h1[0]))
	missing := bound1
	missing.Binding = nil
	add("responder-binding-missing", missing, "responder", "auth", hello(crafted{v: bound1}.run(t)[0]))
	missing4 := bound4
	missing4.Binding = nil
	add("responder-binding-missing-no-tag", missing4, "responder", "auth", hello(crafted{v: bound4}.run(t)[0]))

	// The AES-256-GCM profiles share the rules above; their own cases.
	a3, a5 := defaults(ProfileEstablishPSKAES, Sealed), defaults(ProfileEstablishAES, Sealed)
	w3 := crafted{v: a3}.run(t)
	wrongAES := a3
	wrongAES.PSK = fixed("another psk")
	add("aes-wrong-psk", wrongAES, "responder", "auth", hello(w3[0]))
	add("aes-profile-changed-on-the-path", p1, "responder", "auth", hello(append([]byte{1}, w3[0][1:]...)))
	add("aes-msg2-bit-flip", a3, "initiator", "auth", write, read(flip(w3[1], -1)))
	add("aes-msg3-bit-flip", a3, "responder", "auth", hello(w3[0]), write, read(flip(w3[2], 10)))
	add("aes-low-order-ephemeral", a5, "responder", "kex", hello(crafted{v: a5, iEph: lowOrder}.run(t)[0]), write)
	add("aes-unknown-critical", a3, "initiator", "tlv_unknown_critical", write, read(crafted{v: a3, raw: [][]byte{nil, item(5, 0)}}.run(t)[1]))
	add("aes-encapsulation-key-invalid-psk", a3, "responder", "kex", hello(crafted{v: a3, iEK: badEK}.run(t)[0]), write)
	add("aes-encapsulation-key-invalid-no-psk", a5, "responder", "kex", hello(crafted{v: a5, iEK: badEK}.run(t)[0]), write)
	add("aes-ciphertext-bit-flip", a3, "initiator", "auth", write, read(crafted{v: a3, rCT: func(ct []byte) []byte { return flip(ct, 0) }}.run(t)[1]))

	// Message carrier: no length prefix, a unit is its own end.
	unit := func(w []byte) []byte { return w[2:] } // a stream frame without Len
	mh := append(bytes.Clone(h1[0][:hlen]), h1[0][hlen+2:]...)
	addMsg("message-empty-hello", p1, "responder", "malformed", hello(nil))
	addMsg("message-hello-header-cut", p1, "responder", "malformed", hello(h1[0][:3]))
	addMsg("message-unknown-profile", p1, "responder", "unknown_profile", hello(append([]byte{2}, mh[1:]...)))
	addMsg("message-flags-reserved", p1, "responder", "bad_flags", hello(func() []byte { b := bytes.Clone(mh); b[1] = 3; return b }()))
	addMsg("message-stream-length-left-in", p1, "responder", "auth", hello(h1[0]))
	addMsg("message-msg1-short", p1, "responder", "malformed", hello(mh[:hlen+1247]))
	addMsg("message-msg1-short-no-psk", p4, "responder", "malformed", hello(append(bytes.Clone(h4[0][:head4]), make([]byte, 1215)...)))
	addMsg("message-msg1-tag-flip", p1, "responder", "auth", hello(flip(mh, -1)))
	addMsg("message-empty-reply", p1, "initiator", "malformed", write, read(nil))
	addMsg("message-reply-flip", p1, "initiator", "auth", write, read(flip(unit(h1[1]), -1)))
	addMsg("message-reply-short", p1, "initiator", "malformed", write, read(unit(h1[1])[:1199]))
	addMsg("message-finish-flip", p1, "responder", "auth", hello(mh), write, read(flip(unit(h1[2]), 10)))
	addMsg("message-low-order", p1, "initiator", "kex", write, read(unit(crafted{v: p1, rEph: lowOrder}.run(t)[1])))
	for _, v := range out {
		if got, at := runNegative(t, v); got != v.Error || at != len(v.Steps)-1 {
			t.Errorf("%s: got %q at step %d of %d, want %q", v.Name, got, at, len(v.Steps), v.Error)
		}
	}
	return out
}

type recordVector struct {
	Cipher        string `json:"cipher"`
	Name          string `json:"name"`
	Key           hexb   `json:"key"`
	N             uint64 `json:"n"`
	Plaintext     hexb   `json:"plaintext,omitempty"`
	PlaintextFill *struct {
		Byte   byte `json:"byte"`
		Length int  `json:"length"`
	} `json:"plaintext_fill,omitempty"`
	Ciphertext       hexb   `json:"ciphertext,omitempty"`
	CiphertextSHA256 hexb   `json:"ciphertext_sha256,omitempty"`
	KeyAfter         hexb   `json:"key_after,omitempty"`
	NAfter           uint64 `json:"n_after,omitempty"`
	Error            string `json:"error,omitempty"`
}

func withN(s *join.Sealer, n uint64) *join.Sealer { s.CS.N = n; return s }

func genRecords(t testing.TB) []recordVector {
	out := append(genRecordsFor(t, chachaPolyAEAD{}), genRecordsFor(t, join.AESGCM{})...)
	return append(out, genSplitRecords(t)...)
}

func genRecordsFor(t testing.TB, kind join.AEADFunc) []recordVector {
	key := fixed("record key")
	p := []byte("a zmux record")
	const (
		rekeyInterval = join.DefaultRekeyInterval
		tagLen        = 16
		maxRecord     = 65535 - tagLen
	)
	var out []recordVector
	for _, c := range []struct {
		name string
		n    uint64
	}{
		{"first", 0}, {"before-first-rekey", rekeyInterval - 1}, {"first-rekey", rekeyInterval},
		{"after-first-rekey", rekeyInterval + 1}, {"second-rekey", 2 * rekeyInterval},
		{"last-nonce", math.MaxUint64 - 1}, {"reserved-nonce", math.MaxUint64},
	} {
		s := withN(join.NewSealer(kind, key), c.n)
		ct, err := s.Seal(nil, p)
		v := recordVector{Cipher: kind.Name(), Name: c.name, Key: key, N: c.n, Plaintext: p}
		if err != nil {
			v.Error = errorClass(err)
		} else {
			v.Ciphertext, v.KeyAfter, v.NAfter = ct, bytes.Clone(s.CS.K), s.CS.N
			o := join.NewOpener(kind, key)
			o.CS.N = c.n
			if got, err := o.Open(nil, ct); err != nil || !bytes.Equal(got, p) {
				t.Fatal(c.name, err)
			}
		}
		out = append(out, v)
	}
	fill := func(n int) *struct {
		Byte   byte `json:"byte"`
		Length int  `json:"length"`
	} {
		return &struct {
			Byte   byte `json:"byte"`
			Length int  `json:"length"`
		}{0x5a, n}
	}
	s := join.NewSealer(kind, key)
	ct, err := s.Seal(nil, bytes.Repeat([]byte{0x5a}, maxRecord))
	if err != nil || len(ct) != 65535 {
		t.Fatal(err, len(ct))
	}
	d := sha256.Sum256(ct)
	out = append(out,
		recordVector{Cipher: kind.Name(), Name: "largest", Key: key, PlaintextFill: fill(maxRecord), CiphertextSHA256: d[:], KeyAfter: key, NAfter: 1},
		recordVector{Cipher: kind.Name(), Name: "too-large", Key: key, PlaintextFill: fill(maxRecord + 1), Error: "too_large"},
		recordVector{Cipher: kind.Name(), Name: "empty", Key: key, Plaintext: hexb{}, Ciphertext: func() []byte { c, _ := join.NewSealer(kind, key).Seal(nil, nil); return c }(), KeyAfter: key, NAfter: 1},
	)
	good, _ := join.NewSealer(kind, key).Seal(nil, p)
	out = append(out,
		recordVector{Cipher: kind.Name(), Name: "open-bit-flip", Key: key, Ciphertext: flip(good, 0), Error: "auth"},
		recordVector{Cipher: kind.Name(), Name: "open-too-short", Key: key, Ciphertext: good[:tagLen-1], Error: "auth"},
		recordVector{Cipher: kind.Name(), Name: "open-reserved-nonce", Key: key, N: math.MaxUint64, Ciphertext: good, Error: "exhausted"},
	)
	return out
}

type exporterVector struct {
	Name           string `json:"name"`
	ExporterSecret hexb   `json:"exporter_secret"`
	exportVector
}

func (v exporterVector) fillLabel() string {
	if v.LabelFill != 0 {
		return string(bytes.Repeat([]byte{'l'}, v.LabelFill))
	}
	return v.Label
}

func (v exporterVector) fillContext() []byte {
	if v.ContextFill != 0 {
		return make([]byte, v.ContextFill)
	}
	return v.Context
}

func genExporters(t testing.TB) []exporterVector {
	sec := fixed("exporter secret")
	var out []exporterVector
	for _, c := range []struct {
		name, label string
		ctx         []byte
		n           int
	}{
		{"session", "EXPORTER-zmux-session", nil, 32}, {"pipe", "EXPORTER-zmux-pipe", nil, 32},
		{"context", "EXPORTER-host", []byte("context"), 32}, {"short", "EXPORTER-host", nil, 16},
		{"long", "EXPORTER-host", nil, 64}, {"empty-label", "", nil, 1},
	} {
		o, err := join.Export(join.SHA256{}, sec, c.label, c.ctx, c.n)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, exporterVector{c.name, sec, exportVector{Label: c.label, Context: append(hexb{}, c.ctx...), Length: c.n, Output: o}})
	}
	longest, err := join.Export(join.SHA256{}, sec, "EXPORTER-host", nil, 8160)
	if err != nil {
		t.Fatal(err)
	}
	d := sha256.Sum256(longest)
	out = append(out, exporterVector{"longest", sec, exportVector{Label: "EXPORTER-host", Context: hexb{}, Length: 8160, OutputSHA256: d[:]}})
	withFill, err := join.Export(join.SHA256{}, sec, string(bytes.Repeat([]byte{'l'}, 255)), make([]byte, 65535), 32)
	if err != nil {
		t.Fatal(err)
	}
	out = append(out, exporterVector{"longest-label-and-context", sec, exportVector{LabelFill: 255, Context: hexb{}, ContextFill: 65535, Length: 32, Output: withFill}})
	longSec := append(bytes.Clone(sec), fixed("longer")[:16]...)
	lo, err := join.Export(join.SHA256{}, longSec, "EXPORTER-host", nil, 32)
	if err != nil {
		t.Fatal(err)
	}
	out = append(out, exporterVector{"secret-longer-than-hash", longSec, exportVector{Label: "EXPORTER-host", Context: hexb{}, Length: 32, Output: lo}})
	for _, c := range []exporterVector{
		{"secret-shorter-than-hash", sec[:31], exportVector{Label: "EXPORTER-host", Context: hexb{}, Length: 32, Error: "invalid_config"}},
		{"length-zero", sec, exportVector{Label: "EXPORTER-host", Context: hexb{}, Length: 0, Error: "invalid_config"}},
		{"length-over-limit", sec, exportVector{Label: "EXPORTER-host", Context: hexb{}, Length: 8161, Error: "invalid_config"}},
		{"label-too-long", sec, exportVector{LabelFill: 256, Context: hexb{}, Length: 32, Error: "invalid_config"}},
		{"context-too-long", sec, exportVector{Label: "EXPORTER-host", Context: hexb{}, ContextFill: 65536, Length: 32, Error: "invalid_config"}},
	} {
		if _, err := join.Export(join.SHA256{}, c.ExporterSecret, c.fillLabel(), c.fillContext(), c.Length); errorClass(err) != c.Error {
			t.Fatal(c.Name, err)
		}
		out = append(out, c)
	}
	return out
}

type deriveVectors struct {
	JoinPSK        []joinPSKVector    `json:"join_psk"`
	SessionCrypto  []sessionCryptoVec `json:"session_crypto"`
	KeyID          []keyIDVector      `json:"key_id"`
	Combine        []combineVector    `json:"combine"`
	Epoch          []epochVector      `json:"epoch"`
	ExporterSecret struct {
		CK     hexb `json:"ck"`
		H      hexb `json:"h"`
		Output hexb `json:"exporter_secret"`
	} `json:"exporter_secret"`
	BoundExporter []boundVector `json:"bound_exporter"`
	HandlePSK     []tokenVector `json:"handle_psk"`
	// Session follows Establishes to the Join keys of their session.
	Session []sessionVector `json:"session"`
}

// sessionCryptoVec encodes session_crypto from the two values, or, with value,
// parses one value as configured locally.
type sessionCryptoVec struct {
	Name     string     `json:"name"`
	I        *cryptoSet `json:"crypto_i,omitempty"`
	R        *cryptoSet `json:"crypto_r,omitempty"`
	Value    hexb       `json:"value,omitempty"`
	Encoding hexb       `json:"encoding,omitempty"`
	Error    string     `json:"error,omitempty"`
}

type joinPSKVector struct {
	Name        string    `json:"name,omitempty"`
	EpochSecret hexb      `json:"epoch_secret"`
	Suite       uint64    `json:"suite"`
	HashLen     int       `json:"hash_len"`
	CryptoI     cryptoSet `json:"crypto_i"`
	CryptoR     cryptoSet `json:"crypto_r"`
	I           hexb      `json:"join_psk_i,omitempty"`
	R           hexb      `json:"join_psk_r,omitempty"`
	Error       string    `json:"error,omitempty"`
}

// combineVector is Combine of a Join key share.
type combineVector struct {
	Name   string `json:"name"`
	Group  uint64 `json:"group"`
	Hash   string `json:"hash"`
	Salt   hexb   `json:"salt"`
	SS     []hexb `json:"ss"`
	Output hexb   `json:"output"`
}

// epochVector is epoch_secret[e+1] of a session epoch exchange; offer_body and
// answer_body are the frames from Phase through Locator.
type epochVector struct {
	Name            string    `json:"name"`
	EpochSecret     hexb      `json:"epoch_secret"`
	Epoch           uint64    `json:"epoch"`
	CryptoI         cryptoSet `json:"crypto_i"`
	CryptoR         cryptoSet `json:"crypto_r"`
	OfferBody       hexb      `json:"offer_body"`
	AnswerBody      hexb      `json:"answer_body"`
	SS              []hexb    `json:"ss"`
	PRK             hexb      `json:"prk"`
	NextEpochSecret hexb      `json:"next_epoch_secret"`
}

type keyIDVector struct {
	KeyAlg uint64 `json:"key_alg"`
	Key    hexb   `json:"key"`
	KeyID  hexb   `json:"key_id"`
}

type tokenVector struct {
	Name   string `json:"name"`
	Secret hexb   `json:"secret"`
	Domain hexb   `json:"domain"`
	Salt   hexb   `json:"salt,omitempty"`
	Handle hexb   `json:"handle,omitempty"`
	PSK    hexb   `json:"psk,omitempty"`
	Error  string `json:"error,omitempty"`
}

// dispatchVector is what a host concludes from the first bytes of a carrier.
type dispatchVector struct {
	Framing string `json:"framing"`
	Bytes   hexb   `json:"bytes"`
	Hello   bool   `json:"hello"`
	Record  bool   `json:"record"`
	// Class is the result of carrier dispatch: establish, join, reserved, record,
	// other or incomplete. Need is the total length wanted when incomplete.
	Class string `json:"class"`
	Need  int    `json:"need,omitempty"`
}

// dispatchClass is the carrier dispatch algorithm: the Hello test, then the first
// record test.
func dispatchClass(b []byte, f Framing) (string, int) {
	switch Dispatch(b, f) {
	case join.ClassIncomplete:
		return "incomplete", 1
	case join.ClassEstablish:
		return "establish", 0
	case join.ClassJoin:
		return "join", 0
	case join.ClassReserved:
		return "reserved", 0
	case join.ClassNotHello:
		// Not a Hello: the first-record test below decides.
	}
	if len(b) == 0 {
		return "other", 0 // a message carrier: the empty message is nothing
	}
	zf := zmux.FramingByteStream
	if f == Message {
		zf = zmux.FramingRecords
	}
	switch c, need := zmux.ClassifyFirstBytes(b, zf); c {
	case zmux.FirstBytesRecord:
		return "record", 0
	case zmux.FirstBytesNeedMore:
		return "incomplete", need
	case zmux.FirstBytesOther:
	}
	return "other", 0
}

func genDispatch() []dispatchVector {
	var out []dispatchVector
	for b := 1; b <= 8; b++ {
		// The rest would make a record if the first byte allowed it.
		out = append(out,
			dispatchVector{Framing: "stream", Bytes: hexb{byte(b), 0xff, 0, 0, 0}, Hello: true},
			dispatchVector{Framing: "message", Bytes: hexb{byte(b)}, Hello: true})
	}
	for _, lead := range []byte{0xff, 0x9a, 0xba} {
		out = append(out,
			dispatchVector{Framing: "message", Bytes: hexb{lead}, Record: lead != 0x9a},
			dispatchVector{Framing: "stream", Bytes: hexb{9, lead}, Record: lead != 0x9a},
			dispatchVector{Framing: "stream", Bytes: hexb{0x40, 0x40, lead}, Record: lead != 0x9a})
	}
	out = append(out,
		dispatchVector{Framing: "stream", Bytes: hexb{0, 0xff}},
		dispatchVector{Framing: "message", Bytes: hexb{0}},
		dispatchVector{Framing: "stream", Bytes: hexb{9, 0x03}},
		dispatchVector{Framing: "message", Bytes: hexb{0x09}},
		dispatchVector{Framing: "stream", Bytes: hexb{}},
		dispatchVector{Framing: "message", Bytes: hexb{}},
		dispatchVector{Framing: "stream", Bytes: hexb{9}},
		dispatchVector{Framing: "stream", Bytes: hexb{0x40}},
		dispatchVector{Framing: "stream", Bytes: hexb{0x40, 0x40}},
		dispatchVector{Framing: "stream", Bytes: hexb{0x80, 0}},
		dispatchVector{Framing: "stream", Bytes: hexb{0x16, 0x03, 0x01}})
	for i := range out {
		out[i].Class, out[i].Need = dispatchClass(out[i].Bytes, framingOf(out[i].Framing))
	}
	return out
}

// TestDispatchVectors checks the vectors against both sides of the rule: the
// Hello test here and the first record test of zmux.
func TestDispatchVectors(t *testing.T) {
	for _, v := range readVectors[[]dispatchVector](t, "dispatch.json") {
		f := zmux.FramingByteStream
		if v.Framing == "message" {
			f = zmux.FramingRecords
		}
		c, _ := zmux.ClassifyFirstBytes(v.Bytes, f)
		if (c == zmux.FirstBytesRecord) != v.Record || (v.Hello && c != zmux.FirstBytesOther) {
			t.Errorf("% x on %s: classified %v", []byte(v.Bytes), v.Framing, c)
		}
		if len(v.Bytes) > 0 && IsHelloFirstByte(v.Bytes[0]) != v.Hello {
			t.Errorf("% x on %s: Hello test", []byte(v.Bytes), v.Framing)
		}
		if class, need := dispatchClass(v.Bytes, framingOf(v.Framing)); class != v.Class || need != v.Need {
			t.Errorf("% x on %s: %s %d, want %s %d", []byte(v.Bytes), v.Framing, class, need, v.Class, v.Need)
		}
	}
}

type boundVector struct {
	Name   string `json:"name"`
	Up     hexb   `json:"up"`
	Down   hexb   `json:"down"`
	Output hexb   `json:"output,omitempty"`
	Error  string `json:"error,omitempty"`
}

// bodyOf is the body of an encoded SESSION_EPOCH: the bytes after its type.
func bodyOf(f []byte) []byte {
	_, n := varint.ConsumeVarint(f)
	return f[n:]
}

func genDerive(t testing.TB) deriveVectors {
	var d deriveVectors
	one := cryptoSet{Suites: []uint64{1}, Groups: []uint64{2}}
	e1 := fixed("epoch secret 1")
	for _, id := range []uint64{join.SuiteAES, join.SuiteChaCha} {
		for _, sec := range []struct {
			name   string
			secret []byte
		}{{"e0", fixed("session secret")}, {"e1", e1}} {
			for _, cs := range []struct {
				name string
				i, r cryptoSet
			}{{"minimal", one, cryptoSet{Suites: []uint64{1, 2}, Groups: []uint64{2}}}, {"full", fullCrypto, fullCrypto}} {
				d.JoinPSK = append(d.JoinPSK, joinPSKVector{Name: fmt.Sprintf("suite-%d-%s-%s", id, sec.name, cs.name), EpochSecret: sec.secret, Suite: id, HashLen: 32,
					CryptoI: cs.i, CryptoR: cs.r, I: jpskFor(id, sec.secret, join.RoleInitiator, cs.i, cs.r, 32), R: jpskFor(id, sec.secret, join.RoleResponder, cs.i, cs.r, 32)})
			}
		}
	}
	d.JoinPSK = append(d.JoinPSK, joinPSKVector{Name: "hash-len-48", EpochSecret: fixed("session secret"), Suite: 1, HashLen: 48, CryptoI: fullCrypto, CryptoR: fullCrypto,
		I: jpskFor(1, fixed("session secret"), join.RoleInitiator, fullCrypto, fullCrypto, 48), R: jpskFor(1, fixed("session secret"), join.RoleResponder, fullCrypto, fullCrypto, 48)})
	for _, c := range []struct {
		name string
		i, r cryptoSet
	}{
		{"minimal", one, one},
		{"full", fullCrypto, one},
		{"long-identifiers", cryptoSet{Suites: []uint64{1, 16384}, Groups: []uint64{2, 1 << 30}}, one},
		{"no-suite-1", cryptoSet{Suites: []uint64{2}, Groups: []uint64{2}}, one},
		{"no-group-2", cryptoSet{Suites: []uint64{1}, Groups: []uint64{1}}, one},
		{"zero", cryptoSet{Suites: []uint64{0, 1}, Groups: []uint64{2}}, one},
		{"duplicate", cryptoSet{Suites: []uint64{1, 1}, Groups: []uint64{2}}, one},
		{"descending", cryptoSet{Suites: []uint64{2, 1}, Groups: []uint64{2}}, one},
	} {
		v := sessionCryptoVec{Name: c.name, I: &c.i, R: &c.r}
		e, err := join.SessionCrypto(c.i.wire(), c.r.wire())
		if err != nil {
			v.Error = errorClass(err)
		}
		v.Encoding = e
		d.SessionCrypto = append(d.SessionCrypto, v)
	}
	for _, c := range []struct {
		name  string
		value []byte
	}{
		{"value-non-shortest", []byte{0x40, 1, 1, 1, 1, 2}},
		{"value-without-group-set", []byte{1, 1}},
		{"value-trailing-bytes", []byte{1, 1, 1, 2, 0}},
	} {
		v := sessionCryptoVec{Name: c.name, Value: c.value}
		if _, ok := wire.DecodeSessionCrypto(c.value); !ok {
			v.Error = "invalid_config"
		}
		d.SessionCrypto = append(d.SessionCrypto, v)
	}
	for _, g := range []uint64{join.GroupX25519, join.GroupX25519MLKEM768} {
		c := joinDefaults(join.SuiteAES, Sealed, "i", g, 0)
		_, _, ss := c.shares()
		v := combineVector{Name: fmt.Sprintf("group-%d", g), Group: g, Hash: "SHA256", Salt: fixed("combine salt")}
		for _, x := range ss {
			v.SS = append(v.SS, x)
		}
		v.Output = join.Combine(sha256.New, v.Salt, ss...)
		d.Combine = append(d.Combine, v)
		// The hs of a Join of Suite 1: Combine(join_psk, ss...).
		hs := combineVector{Name: fmt.Sprintf("join-suite-1-group-%d", g), Group: g, Hash: "SHA256", Salt: c.JoinPSK, SS: v.SS,
			Output: join.Combine(sha256.New, c.JoinPSK, ss...)}
		d.Combine = append(d.Combine, hs)
	}
	for _, c := range []struct {
		name  string
		epoch uint64
		loc   bool
	}{{"epoch-1-keep-locators", 1, false}, {"epoch-1-new-locators", 1, true}, {"epoch-64", 64, true}} {
		jc := joinDefaults(join.SuiteAES, Sealed, "i", join.GroupX25519MLKEM768, 0)
		si, sr, ss := jc.shares()
		var lo, la []byte
		if c.loc {
			lo, la = []byte("offerer-locator"), []byte("answerer-locator")
		}
		ob := bodyOf(wire.AppendSessionEpoch(nil, wire.EpochOffer, c.epoch, si, lo))
		ab := bodyOf(wire.AppendSessionEpoch(nil, wire.EpochAnswer, c.epoch, sr, la))
		sc, err := join.SessionCrypto(fullCrypto.wire(), fullCrypto.wire())
		if err != nil {
			t.Fatal(err)
		}
		secret := fixed(fmt.Sprintf("epoch secret %d", c.epoch-1))
		v := epochVector{Name: c.name, EpochSecret: secret, Epoch: c.epoch, CryptoI: fullCrypto, CryptoR: fullCrypto,
			OfferBody: ob, AnswerBody: ab, PRK: join.EpochPRK(secret, c.epoch, sc, ob, ab),
			NextEpochSecret: join.NextEpochSecret(secret, c.epoch, sc, ob, ab, ss...)}
		for _, x := range ss {
			v.SS = append(v.SS, x)
		}
		d.Epoch = append(d.Epoch, v)
	}
	for _, a := range []KeyAlg{KeyAlgX25519, KeyAlgMLDSA44} {
		key := bytes.Repeat(fixed(fmt.Sprintf("key %d", a)), (a.KeyLen()+31)/32)[:a.KeyLen()]
		id, err := KeyIDOf(a, key)
		if err != nil {
			t.Fatal(err)
		}
		d.KeyID = append(d.KeyID, keyIDVector{KeyAlg: uint64(a), Key: key, KeyID: id[:]})
	}
	hv := genHandshake(t, "", defaults(ProfileEstablishPSK, Sealed), nil)
	d.ExporterSecret.CK, d.ExporterSecret.H, d.ExporterSecret.Output = hv.ChainingKey, hv.HandshakeHash, hv.ExporterSecret
	if !bytes.Equal(exporterSecret(suiteChaCha, hv.ChainingKey, hv.HandshakeHash), hv.ExporterSecret) {
		t.Fatal("exporter secret")
	}
	up, down := fixed("uplink exporter"), fixed("downlink exporter")
	same, err := BoundExporter(up, up)
	if err != nil {
		t.Fatal(err)
	}
	diff, err := BoundExporter(up, down)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := BoundExporter(up, nil); !errors.Is(err, ErrInvalidConfig) {
		t.Fatal(err)
	}
	d.BoundExporter = []boundVector{
		{Name: "one-connection", Up: up, Down: up, Output: same},
		{Name: "two-connections", Up: up, Down: down, Output: diff},
		{Name: "half-without-exporter", Up: up, Down: hexb{}, Error: "invalid_config"},
	}
	for _, c := range []struct {
		name                 string
		secret, domain, salt []byte
	}{
		{"shortest-secret", fixed("token secret")[:16], []byte("example.test"), nil},
		{"default-secret", fixed("token secret"), []byte("example.test"), nil},
		{"other-domain", fixed("token secret"), []byte("other.test"), nil},
		{"empty-domain", fixed("token secret"), []byte{}, nil},
		{"deployment-salt", fixed("token secret"), []byte("example.test"), []byte("deployment")},
	} {
		h, p, err := DeriveHandlePSK(c.secret, c.domain, c.salt)
		if err != nil {
			t.Fatal(err)
		}
		d.HandlePSK = append(d.HandlePSK, tokenVector{Name: c.name, Secret: c.secret, Domain: c.domain, Salt: c.salt, Handle: h, PSK: p})
	}
	d.HandlePSK = append(d.HandlePSK, tokenVector{Name: "secret-too-short", Secret: fixed("token secret")[:15], Domain: []byte("example.test"), Error: "invalid_config"})
	d.Session = genSessions(t)
	return d
}

func encode(t testing.TB, v any) []byte {
	t.Helper()
	b, err := json.MarshalIndent(v, "", " ")
	if err != nil {
		t.Fatal(err)
	}
	return append(b, '\n')
}

func generated(t testing.TB) map[string][]byte {
	return map[string][]byte{
		"handshake.json":     encode(t, genHandshakes(t)),
		"negative.json":      encode(t, genNegatives(t)),
		"record.json":        encode(t, genRecords(t)),
		"exporter.json":      encode(t, genExporters(t)),
		"derive.json":        encode(t, genDerive(t)),
		"dispatch.json":      encode(t, genDispatch()),
		"receiver.json":      encode(t, genReceivers(t)),
		"hello.json":         encode(t, genHelloBounds()),
		"config.json":        encode(t, genConfigs(t)),
		"join.json":          encode(t, genJoins(t)),
		"join_negative.json": encode(t, genJoinNegatives(t)),
	}
}

// base64Value is a JSON string that looks like base64 padding, which a []byte
// field without hexb would produce.
var base64Value = regexp.MustCompile(`"[A-Za-z0-9+/]+={1,2}"`)

func TestVectors(t *testing.T) {
	files := generated(t)
	for name, b := range files {
		if m := base64Value.Find(b); m != nil {
			t.Errorf("%s: base64 value %s; byte fields are hexb", name, m)
		}
	}
	spec := specVectors(t)
	if *update {
		dirs := []string{localVectors}
		if spec != "" {
			dirs = append(dirs, spec)
		}
		for _, dir := range dirs {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				t.Fatal(err)
			}
			for name, b := range files {
				if err := os.WriteFile(filepath.Join(dir, name), b, 0o644); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	for name, b := range files {
		got, err := os.ReadFile(filepath.Join(localVectors, name))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, b) {
			t.Errorf("%s differs from what the code produces", name)
		}
		if spec == "" {
			continue
		}
		if want, err := os.ReadFile(filepath.Join(spec, name)); err != nil {
			t.Error(err)
		} else if !bytes.Equal(want, got) {
			t.Errorf("%s differs from the specification's copy", name)
		}
	}
}

func readVectors[T any](t *testing.T, name string) T {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(localVectors, name))
	if err != nil {
		t.Fatal(err)
	}
	var v T
	if err := json.Unmarshal(b, &v); err != nil {
		t.Fatal(err)
	}
	return v
}

func TestNegativeVectorsFromFile(t *testing.T) {
	for _, v := range readVectors[[]negativeVector](t, "negative.json") {
		if got, at := runNegative(t, v); got != v.Error || at != len(v.Steps)-1 {
			t.Errorf("%s: got %q at step %d, want %q", v.Name, got, at, v.Error)
		}
	}
}

// TestRecordVectorsAgainstNoise recomputes each record on an independent
// ChaChaPoly and REKEY.
func TestRecordVectorsAgainstNoise(t *testing.T) {
	for _, v := range readVectors[[]recordVector](t, "record.json") {
		var kind join.AEADFunc = chachaPolyAEAD{}
		fc := noise.CipherChaChaPoly
		if v.Cipher == "AESGCM" {
			kind, fc = join.AESGCM{}, noise.CipherAESGCM
		}
		if v.Error != "" {
			o := withN(join.NewSealer(kind, v.Key), v.N)
			plain := []byte(v.Plaintext)
			if v.PlaintextFill != nil {
				plain = bytes.Repeat([]byte{v.PlaintextFill.Byte}, v.PlaintextFill.Length)
			}
			if v.Ciphertext != nil { // an Open failure
				op := join.NewOpener(kind, v.Key)
				op.CS.N = v.N
				if _, err := op.Open(nil, v.Ciphertext); errorClass(err) != v.Error {
					t.Errorf("%s: %v", v.Name, err)
				}
				if _, err := op.Open(nil, v.Ciphertext); err == nil {
					t.Errorf("%s: failure is not permanent", v.Name)
				}
				continue
			}
			if _, err := o.Seal(nil, plain); errorClass(err) != v.Error {
				t.Errorf("%s: %v", v.Name, err)
			}
			continue
		}
		k := bytes.Clone(v.Key)
		if v.N != 0 && v.N%join.DefaultRekeyInterval == 0 {
			var c [32]byte
			copy(c[:], k)
			out := fc.Cipher(c).Encrypt(nil, math.MaxUint64, nil, make([]byte, 32))
			k = out[:32]
		}
		if !bytes.Equal(k, v.KeyAfter) {
			t.Errorf("%s: key after differs", v.Name)
		}
		var c [32]byte
		copy(c[:], k)
		plain := []byte(v.Plaintext)
		if v.PlaintextFill != nil {
			plain = bytes.Repeat([]byte{v.PlaintextFill.Byte}, v.PlaintextFill.Length)
		}
		ct := fc.Cipher(c).Encrypt(nil, v.N, nil, plain)
		if v.CiphertextSHA256 != nil {
			d := sha256.Sum256(ct)
			if !bytes.Equal(d[:], v.CiphertextSHA256) {
				t.Errorf("%s: ciphertext differs", v.Name)
			}
		} else if !bytes.Equal(ct, v.Ciphertext) {
			t.Errorf("%s: ciphertext differs", v.Name)
		}
	}
}

func testPayload(p Payload) []byte {
	b, err := appendPayload(nil, p, false)
	if err != nil {
		panic(err)
	}
	return b
}
