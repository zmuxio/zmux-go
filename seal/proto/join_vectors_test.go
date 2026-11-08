package proto

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/zmuxio/zmux-go/internal/join"
	"github.com/zmuxio/zmux-go/varint"
)

// Hello bounds: a header, then fill bytes of 0x5a.

type helloBound struct {
	Name     string `json:"name"`
	Kind     string `json:"kind"`
	Framing  string `json:"framing"`
	Header   hexb   `json:"header"`
	Fill     int    `json:"fill"`
	Consumed int    `json:"consumed,omitempty"`
	Need     int    `json:"need,omitempty"`
	Error    string `json:"error,omitempty"`
}

func (v helloBound) parse() (int, error) {
	b := append(bytes.Clone(v.Header), bytes.Repeat([]byte{0x5a}, v.Fill)...)
	f := framingOf(v.Framing)
	switch v.Kind {
	case "join":
		_, n, err := join.ParseHello(b, f)
		return n, err
	case "reply":
		su, _ := join.LookupSuite(join.SuiteAES)
		_, _, n, err := join.CutReply(b, f, su)
		return n, err
	case "frame":
		_, n, err := CutFrame(b, f)
		return n, err
	}
	_, n, err := ParseHello(b, f)
	return n, err
}

func genHelloBounds() []helloBound {
	est := func(p byte) []byte {
		return append([]byte{p, 0}, bytes.Repeat([]byte{0x48}, HandleLen)...)
	}
	jn := func(loc int) []byte {
		h := []byte{2, 1, 0, byte(loc)}
		h = append(h, bytes.Repeat([]byte{0x4c}, loc)...)
		return append(h, bytes.Repeat([]byte{0x52}, 32)...)
	}
	l := func(b []byte, n int) []byte { return append(bytes.Clone(b), byte(n>>8), byte(n)) }
	const maxLen = 65535
	out := []helloBound{
		{Name: "establish-profile1-handle", Kind: "establish", Framing: "stream", Header: l(est(1), 1248), Fill: 1248},
		{Name: "establish-profile3-handle", Kind: "establish", Framing: "stream", Header: l(est(3), 1248), Fill: 1248},
		{Name: "establish-handle-cut", Kind: "establish", Framing: "stream", Header: est(1)[:10]},
		{Name: "establish-handle-cut-before-length", Kind: "establish", Framing: "stream", Header: est(1)},
		{Name: "establish-empty-msg1-parses", Kind: "establish", Framing: "stream", Header: l(est(1), 0)},
		{Name: "establish-longest-msg1", Kind: "establish", Framing: "stream", Header: l(est(1), maxLen), Fill: maxLen},
		{Name: "establish-msg1-cut", Kind: "establish", Framing: "stream", Header: l(est(1), maxLen), Fill: maxLen - 1},
		{Name: "establish-byte-only", Kind: "establish", Framing: "stream", Header: []byte{1}},
		{Name: "establish-empty", Kind: "establish", Framing: "stream", Header: []byte{}},
		{Name: "establish-escape-cut", Kind: "establish", Framing: "stream", Header: []byte{8, 0x40}},
		{Name: "establish-profile4-no-handle", Kind: "establish", Framing: "stream", Header: l([]byte{4, 0}, 1216), Fill: 1216},
		{Name: "establish-profile5-no-handle", Kind: "establish", Framing: "stream", Header: l([]byte{5, 0}, 1216), Fill: 1216},
		{Name: "establish-profile5-no-handle-cut", Kind: "establish", Framing: "stream", Header: l([]byte{5, 0}, 1216), Fill: 1215},
		{Name: "establish-unassigned-profile6", Kind: "establish", Framing: "stream", Header: l([]byte{6, 0}, 1216), Fill: 1216},
		{Name: "establish-escape-unassigned-profile9", Kind: "establish", Framing: "stream", Header: l(append([]byte{8, 9, 0}, bytes.Repeat([]byte{0x48}, HandleLen)...), 1248), Fill: 1248},
		{Name: "establish-escape-private-profile", Kind: "establish", Framing: "stream", Header: l(append([]byte{8, 0xc0, 0, 0, 0, 0x40, 0, 0, 0, 0}, bytes.Repeat([]byte{0x48}, HandleLen)...), 1248), Fill: 1248},
		{Name: "establish-message-longest", Kind: "establish", Framing: "message", Header: est(1), Fill: maxLen},
		{Name: "establish-message-over", Kind: "establish", Framing: "message", Header: est(1), Fill: maxLen + 1},
		{Name: "establish-message-empty-msg1-parses", Kind: "establish", Framing: "message", Header: est(1)},
		{Name: "establish-message-handle-cut", Kind: "establish", Framing: "message", Header: est(3)[:17]},
		{Name: "establish-message-profile4", Kind: "establish", Framing: "message", Header: []byte{4, 0}, Fill: 1216},
		{Name: "frame-longest", Kind: "frame", Framing: "stream", Header: l(nil, maxLen), Fill: maxLen},
		{Name: "frame-cut", Kind: "frame", Framing: "stream", Header: l(nil, maxLen), Fill: maxLen - 1},
		{Name: "frame-message-longest", Kind: "frame", Framing: "message", Fill: maxLen},
		{Name: "frame-message-over", Kind: "frame", Framing: "message", Fill: maxLen + 1},
		{Name: "reply-longest", Kind: "reply", Framing: "stream", Header: l(append([]byte{3}, bytes.Repeat([]byte{0x52}, 32)...), maxLen), Fill: maxLen},
		{Name: "reply-cut", Kind: "reply", Framing: "stream", Header: l(append([]byte{3}, bytes.Repeat([]byte{0x52}, 32)...), maxLen), Fill: maxLen - 1},
		{Name: "reply-empty", Kind: "reply", Framing: "stream", Header: []byte{}},
		{Name: "reply-type", Kind: "reply", Framing: "stream", Header: []byte{4}},
		{Name: "reply-message-longest", Kind: "reply", Framing: "message", Header: append([]byte{3}, bytes.Repeat([]byte{0x52}, 32)...), Fill: maxLen},
		{Name: "reply-message-over", Kind: "reply", Framing: "message", Header: append([]byte{3}, bytes.Repeat([]byte{0x52}, 32)...), Fill: maxLen + 1},
		{Name: "join-shortest-locator", Kind: "join", Framing: "stream", Header: l(jn(1), 16), Fill: 16},
		{Name: "join-longest-locator", Kind: "join", Framing: "stream", Header: l(jn(255), 16), Fill: 16},
		{Name: "join-locator-zero", Kind: "join", Framing: "stream", Header: l(jn(0), 16), Fill: 16},
		{Name: "join-longest-ciphertext", Kind: "join", Framing: "stream", Header: l(jn(16), maxLen), Fill: maxLen},
		{Name: "join-ciphertext-cut", Kind: "join", Framing: "stream", Header: l(jn(16), maxLen), Fill: maxLen - 1},
		{Name: "join-suite-cut", Kind: "join", Framing: "stream", Header: []byte{2, 0x40}},
		{Name: "join-empty", Kind: "join", Framing: "stream", Header: []byte{}},
		{Name: "join-r-cut", Kind: "join", Framing: "stream", Header: jn(4)[:20]},
		{Name: "join-suite2", Kind: "join", Framing: "stream", Header: l(append([]byte{2, 2, 0, 1, 0x4c}, bytes.Repeat([]byte{0x52}, 32)...), 16), Fill: 16},
		{Name: "join-message-longest", Kind: "join", Framing: "message", Header: jn(16), Fill: maxLen},
		{Name: "join-message-over", Kind: "join", Framing: "message", Header: jn(16), Fill: maxLen + 1},
		{Name: "join-message-no-ciphertext-parses", Kind: "join", Framing: "message", Header: jn(1)},
	}
	for _, mb := range []byte{0x00, 0x01, 0x02, 0x80, 0x81, 0x82, 0x03, 0x04, 0x40, 0x7f, 0x83} {
		h := setByte(jn(1), 2, mb)
		out = append(out, helloBound{Name: fmt.Sprintf("join-mode-byte-%02x", mb), Kind: "join", Framing: "stream", Header: l(h, 16), Fill: 16})
	}
	for i := range out {
		n, err := out[i].parse()
		switch {
		case err == nil:
			out[i].Consumed = n
		case errors.Is(err, ErrIncomplete):
			out[i].Error, out[i].Need = "incomplete", n
		default:
			out[i].Error = errorClass(err)
		}
	}
	return out
}

func TestHelloBoundsFromFile(t *testing.T) {
	for _, v := range readVectors[[]helloBound](t, "hello.json") {
		n, err := v.parse()
		if v.Error != "" {
			if errorClass(err) != v.Error || (v.Error == "incomplete" && n != v.Need) {
				t.Errorf("%s: %d %v", v.Name, n, err)
			}
		} else if err != nil || n != v.Consumed {
			t.Errorf("%s: %d %v", v.Name, n, err)
		}
	}
}

// Local configuration failures: nothing is sent.

// configVector keeps the field order of the vector files.
//
//goland:noinspection ALL
type configVector struct {
	Name    string `json:"name"`
	Kind    string `json:"kind"`
	Profile uint64 `json:"profile,omitempty"`
	Suite   uint64 `json:"suite,omitempty"`
	Mode    uint8  `json:"mode"`
	Handle  hexb   `json:"handle,omitempty"`
	Static  hexb   `json:"static,omitempty"`
	PSK     hexb   `json:"psk,omitempty"`
	Binding hexb   `json:"binding,omitempty"`
	// WriteType, when not zero, is a payload type put in the first message of an
	// otherwise valid configuration, which does not know it.
	WriteType int `json:"write_type,omitempty"`
	// AppDataLen, when not zero, is application data of that many zero bytes
	// in the first message.
	AppDataLen int `json:"app_data_len,omitempty"`
	// CredType0 makes the credential of WriteType 1 one of CredType 0.
	CredType0 bool `json:"cred_type_0,omitempty"`
	// RequirePQAuth, for kind pin, requires PQ authentication of a pin that
	// names no proof key.
	RequirePQAuth bool `json:"require_pq_auth,omitempty"`
	// Crypto, for kind session_crypto, is a locally configured value of
	// zmux_session_crypto.
	Crypto *cryptoSet `json:"crypto,omitempty"`
	Error  string     `json:"error"`
}

func (v configVector) start(t testing.TB) error {
	if v.Kind == "session_crypto" {
		_, err := join.SessionCrypto(v.Crypto.wire(), fullCrypto.wire())
		return err
	}
	if v.Kind == "pin" {
		return CheckPinPQAuth(v.RequirePQAuth, false)
	}
	if v.Kind == "join" {
		s, ok := join.LookupSuite(v.Suite)
		if !ok {
			return join.ErrUnknownSuite
		}
		g, _ := join.LookupGroup(join.GroupX25519MLKEM768)
		_, err := join.NewJoiner(join.JoinerConfig{Suite: s, Group: g, Mode: Mode(v.Mode), Locator: v.Handle, PSK: v.PSK, Binding: v.Binding})
		return err
	}
	c := Config{Profile: Profile(v.Profile), Mode: Mode(v.Mode), Handle: v.Handle, PSK: v.PSK, Binding: v.Binding}
	if len(v.Static) > 0 {
		c.Static = x25519(t, v.Static)
	}
	hs, err := NewInitiator(c)
	if err != nil {
		return err
	}
	p := Payload{AppData: make([]byte, v.AppDataLen)}
	switch v.WriteType {
	case tlvCredential:
		p.Credential = &Credential{Type: 1}
		if v.CredType0 {
			p.Credential.Type = 0
		}
	case tlvProof:
		p.Proof = &Proof{Alg: KeyAlgMLDSA44}
	case tlvFirstBytes:
		p.FirstBytes = []byte{0}
	}
	_, err = hs.WriteMessage(nil, p)
	return err
}

func genConfigs(t testing.TB) []configVector {
	psk, h, st, b := fixed("psk"), fixed("handle")[:16], fixed("initiator static"), fixed("binding")
	out := []configVector{
		{Name: "establish-binding-surplus-sealed", Kind: "establish", Profile: 1, Handle: h, Static: st, PSK: psk, Binding: b},
		{Name: "establish-binding-surplus-trusted", Kind: "establish", Profile: 1, Mode: 2, Handle: h, Static: st, PSK: psk, Binding: b},
		{Name: "establish-binding-short-surplus", Kind: "establish", Profile: 4, Static: st, Binding: b[:5]},
		{Name: "establish-bound-no-binding", Kind: "establish", Profile: 1, Mode: 1, Handle: h, Static: st, PSK: psk},
		{Name: "establish-bound-short-binding", Kind: "establish", Profile: 1, Mode: 1, Handle: h, Static: st, PSK: psk, Binding: b[:31]},
		{Name: "establish-psk-missing", Kind: "establish", Profile: 1, Handle: h, Static: st},
		{Name: "establish-psk-short", Kind: "establish", Profile: 1, Handle: h, Static: st, PSK: psk[:31]},
		{Name: "establish-psk-surplus", Kind: "establish", Profile: 4, Static: st, PSK: psk},
		{Name: "establish-mode-reserved", Kind: "establish", Profile: 4, Mode: 3, Static: st},
		{Name: "establish-handle-missing", Kind: "establish", Profile: 1, Static: st, PSK: psk},
		{Name: "establish-handle-surplus", Kind: "establish", Profile: 4, Handle: h, Static: st},
		{Name: "establish-handle-surplus-profile-5", Kind: "establish", Profile: 5, Handle: h, Static: st},
		{Name: "establish-handle-empty", Kind: "establish", Profile: 1, Handle: hexb{}, Static: st, PSK: psk},
		{Name: "establish-handle-15", Kind: "establish", Profile: 1, Handle: h[:15], Static: st, PSK: psk},
		{Name: "establish-handle-17", Kind: "establish", Profile: 3, Handle: fixed("handle")[:17], Static: st, PSK: psk},
		{Name: "session-crypto-no-suite-1", Kind: "session_crypto", Crypto: &cryptoSet{Suites: []uint64{2}, Groups: []uint64{1, 2}}},
		{Name: "session-crypto-no-group-2", Kind: "session_crypto", Crypto: &cryptoSet{Suites: []uint64{1, 2}, Groups: []uint64{1}}},
		{Name: "session-crypto-zero", Kind: "session_crypto", Crypto: &cryptoSet{Suites: []uint64{0, 1}, Groups: []uint64{2}}},
		{Name: "session-crypto-descending", Kind: "session_crypto", Crypto: &cryptoSet{Suites: []uint64{2, 1}, Groups: []uint64{2}}},
		{Name: "session-crypto-duplicate", Kind: "session_crypto", Crypto: &cryptoSet{Suites: []uint64{1}, Groups: []uint64{2, 2}}},
		{Name: "sender-credentials-in-msg1", Kind: "establish", Profile: 1, Handle: h, Static: st, PSK: psk, WriteType: 1},
		{Name: "sender-app-data-in-msg1-no-psk", Kind: "establish", Profile: 4, Static: st, AppDataLen: 1},
		{Name: "sender-credential-type-0", Kind: "establish", Profile: 1, Handle: h, Static: st, PSK: psk, WriteType: 1, CredType0: true},
		{Name: "require-pq-auth-pin-without-proof-id", Kind: "pin", RequirePQAuth: true},
		{Name: "sender-proof-in-msg1", Kind: "establish", Profile: 1, Handle: h, Static: st, PSK: psk, WriteType: 8},
		{Name: "sender-first-bytes-in-msg1", Kind: "establish", Profile: 4, Static: st, WriteType: 3},
		{Name: "join-binding-surplus-sealed", Kind: "join", Suite: 1, Handle: h, PSK: psk, Binding: b},
		{Name: "join-binding-surplus-trusted", Kind: "join", Suite: 1, Mode: 2, Handle: h, PSK: psk, Binding: b},
		{Name: "join-bound-no-binding", Kind: "join", Suite: 1, Mode: 1, Handle: h, PSK: psk},
		{Name: "join-psk-short", Kind: "join", Suite: 1, Handle: h, PSK: psk[:31]},
		{Name: "join-mode-reserved", Kind: "join", Suite: 1, Mode: 3, Handle: h, PSK: psk},
		{Name: "join-locator-empty", Kind: "join", Suite: 1, PSK: psk},
		{Name: "join-locator-over", Kind: "join", Suite: 1, Handle: make([]byte, 256), PSK: psk},
	}
	for i := range out {
		out[i].Error = errorClass(out[i].start(t))
		if out[i].Error != "invalid_config" {
			t.Errorf("%s: %s", out[i].Name, out[i].Error)
		}
	}
	return out
}

func TestConfigVectorsFromFile(t *testing.T) {
	for _, v := range readVectors[[]configVector](t, "config.json") {
		if c := errorClass(v.start(t)); c != v.Error {
			t.Errorf("%s: %s", v.Name, c)
		}
	}
}

// Node credentials.

// Received types a message does not know, even ones: the receiver skips them. A
// sender could not have written these through its API.

type receiverVectors struct {
	Establish []negativeVector `json:"establish"`
	Join      []joinNegative   `json:"join"`
}

func genReceivers(t testing.TB) receiverVectors {
	var out receiverVectors
	p1 := defaults(ProfileEstablishPSK, Sealed)
	hello := func(w []byte) stepVector { return stepVector{Op: "hello", Wire: w} }
	read := func(w []byte) stepVector { return stepVector{Op: "read", Wire: w} }
	write := stepVector{Op: "write"}
	add := func(name, side string, steps ...stepVector) {
		out.Establish = append(out.Establish, negativeVector{Name: name, parties: p1, Side: side, Framing: "stream", Steps: steps})
	}
	ignorable := append(item(6, 3, 1, 2, 3), item(70, 0)...)
	add("msg1-even-type-of-another-message", "responder", hello(crafted{v: p1, raw: [][]byte{item(6, 1, 0)}}.run(t)[0]))
	add("msg1-ignorable-types", "responder", hello(crafted{v: p1, raw: [][]byte{ignorable}}.run(t)[0]))
	add("msg2-ignorable-types", "initiator", write, read(crafted{v: p1, raw: [][]byte{nil, append(item(0, 1, 9), item(6, 0)...)}}.run(t)[1]))
	// Unassigned even types are skipped whatever their values.
	add("msg2-unassigned-even-types", "initiator", write, read(crafted{v: p1, raw: [][]byte{nil, slices.Concat(item(4, 3, 1, 2, 3), item(10, 1, 0))}}.run(t)[1]))
	m3 := crafted{v: p1, raw: [][]byte{nil, nil, slices.Concat(item(4, 0), item(10, 1, 0), item(12, 1, 0))}}.run(t)
	add("msg3-even-types-of-other-messages", "responder", hello(m3[0]), write, read(m3[2]))
	add("msg1-unassigned-even-types", "responder", hello(crafted{v: p1, raw: [][]byte{append(item(4, 2, 1, 2), item(10, 1, 0)...)}}.run(t)[0]))
	add("msg2-unassigned-type-12", "initiator", write, read(crafted{v: p1, raw: [][]byte{nil, item(12, 1, 0)}}.run(t)[1]))
	add("msg2-app-data-empty", "initiator", write, read(crafted{v: p1, raw: [][]byte{nil, item(2, 0)}}.run(t)[1]))
	add("msg1-app-data-at-the-bound", "responder", hello(crafted{v: p1, raw: [][]byte{item(2, DefaultMaxAppData, make([]byte, DefaultMaxAppData)...)}}.run(t)[0]))
	// A proof is unknown in message 1, and 12 is unassigned: both skipped.
	add("msg1-proof-and-type-12", "responder", hello(crafted{v: p1, raw: [][]byte{append(item(8, 1, 0), item(12, 31, make([]byte, 31)...)...)}}.run(t)[0]))
	add("msg2-type-12", "initiator", write, read(crafted{v: p1, raw: [][]byte{nil, item(12, 0)}}.run(t)[1]))
	p4 := defaults(ProfileEstablish, Sealed)
	out.Establish = append(out.Establish, negativeVector{Name: "msg1-no-psk-type-12", parties: p4, Side: "responder", Framing: "stream",
		Steps: []stepVector{hello(crafted{v: p4, raw: [][]byte{item(12, 1, 0)}}.run(t)[0])}})

	for _, g := range []uint64{join.GroupX25519, join.GroupX25519MLKEM768} {
		c := joinDefaults(join.SuiteAES, Sealed, "i", g, 0)
		si, sr, _ := c.shares()
		pipe, _ := join.EncodePipe(joinPipe)
		share := func(s []byte) []byte {
			v := append(varint.AppendVarint(nil, g), s...)
			return item(9, len(v), v...)
		}
		// Type 8 is known in Establish messages only; 2, 4, 10 and 12 are unassigned.
		v := c
		v.HelloPayload = slices.Concat(item(2, 1, 0), item(4, 2, 1, 2), item(5, len(pipe), pipe...), item(8, 3, 1, 2, 3), share(si), item(10, 0), item(12, 32, make([]byte, 32)...))
		w := v.helloOf(t, Stream, v.HelloPayload)
		out.Join = append(out.Join, joinNegative{Name: fmt.Sprintf("hello-establish-types-group%d", g), joinCase: v, Side: "acceptor", Framing: "stream", Steps: []joinStep{{Op: "hello", Wire: w}, {Op: "write_reply"}}})
		v = c
		v.ReplyPayload = slices.Concat(item(2, 1, 0), item(4, 0), item(7, 1, 0), item(8, 1, 0), share(sr), item(10, 53, make([]byte, 53)...), item(12, 0))
		out.Join = append(out.Join, joinNegative{Name: fmt.Sprintf("reply-establish-types-group%d", g), joinCase: v, Side: "joiner", Framing: "stream",
			Steps: []joinStep{{Op: "write_hello"}, {Op: "reply", Wire: v.replyOf(t, Stream, v.ReplyPayload)}}})
	}
	checkReceivers(t, out)
	return out
}

func checkReceivers(t testing.TB, r receiverVectors) {
	t.Helper()
	for _, v := range r.Establish {
		if got, _ := runNegative(t, v); got != "" {
			t.Errorf("%s: %s", v.Name, got)
		}
	}
	for _, v := range r.Join {
		if got, _ := runJoinNegative(t, v); got != "" {
			t.Errorf("%s: %s", v.Name, got)
		}
	}
}

func TestReceiverVectorsFromFile(t *testing.T) {
	checkReceivers(t, readVectors[receiverVectors](t, "receiver.json"))
}
