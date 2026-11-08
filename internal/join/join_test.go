package join

import (
	"bytes"
	"crypto/mlkem"
	"crypto/mlkem/mlkemtest"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash"
	"os"
	"testing"

	"github.com/zmuxio/zmux-go/internal/wire"
)

// testSuites is SessionCrypto of {1, 2}, {1, 2} at both ends.
var testSuites = []byte{2, 1, 2, 2, 1, 2, 2, 1, 2, 2, 1, 2}

var testGroup, _ = LookupGroup(GroupX25519)

func anyGroup(uint64) bool { return true }

// cryptoOf reads a {suites, groups} object of a vector.
func cryptoOf(v any) wire.SessionCrypto {
	m, _ := v.(map[string]any)
	var c wire.SessionCrypto
	for _, k := range []string{"suites", "groups"} {
		ids, _ := m[k].([]any)
		for _, id := range ids {
			if k == "suites" {
				c.Suites = append(c.Suites, uint64(id.(float64)))
			} else {
				c.Groups = append(c.Groups, uint64(id.(float64)))
			}
		}
	}
	return c
}

// A suite with a longer hash, to show no HashLen is assumed.
func init() {
	Register(Suite{ID: 99, AEAD: AESGCM{}, Hash: sha384Hash{}, NonceLen: 48})
}

type sha384Hash struct{}

func (sha384Hash) Name() string   { return "SHA384" }
func (sha384Hash) Size() int      { return sha512.Size384 }
func (sha384Hash) New() hash.Hash { return sha512.New384() }

func TestOtherHashLength(t *testing.T) {
	su, _ := LookupSuite(99)
	if _, err := PSK(su, make([]byte, 31), RoleInitiator, testSuites); !errors.Is(err, ErrInvalidConfig) {
		t.Fatal(err)
	}
	psk, err := PSK(su, make([]byte, 32), RoleInitiator, testSuites)
	if err != nil || len(psk) != 48 {
		t.Fatal(err)
	}
	for _, m := range []Mode{Sealed, Trusted} {
		j, _ := NewJoiner(JoinerConfig{Suite: su, Group: testGroup, Mode: m, Locator: []byte("l"), PSK: psk})
		w, err := j.WriteHello(nil, HelloPayload{})
		if err != nil {
			t.Fatal(err)
		}
		h, _, err := ParseHelloExact(w, Stream)
		if err != nil {
			t.Fatal(err)
		}
		a, _, err := Accept(h, Stream, psk, nil, anyGroup)
		if err != nil {
			t.Fatal(err)
		}
		if err := a.Exchange(nil); err != nil {
			t.Fatal(err)
		}
		r, err := a.WriteReply(nil, ReplyPayload{})
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := j.ReadReply(r); err != nil {
			t.Fatal(err)
		}
		c, _ := j.WriteConfirm(nil, 0)
		if _, err := a.ReadConfirm(c); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSessionCrypto(t *testing.T) {
	c := wire.SessionCrypto{Suites: []uint64{1, 2}, Groups: []uint64{1, 2}}
	got, err := SessionCrypto(c, wire.SessionCrypto{Suites: []uint64{1}, Groups: []uint64{2}})
	if err != nil || !bytes.Equal(got, []byte{2, 1, 2, 2, 1, 2, 1, 1, 1, 2}) {
		t.Fatal(got, err)
	}
	for _, bad := range []wire.SessionCrypto{{}, {Suites: []uint64{2}, Groups: []uint64{2}}, {Suites: []uint64{1}, Groups: []uint64{1}},
		{Suites: []uint64{0, 1}, Groups: []uint64{2}}, {Suites: []uint64{1, 1}, Groups: []uint64{2}}, {Suites: []uint64{2, 1}, Groups: []uint64{2}}} {
		if _, err := SessionCrypto(bad, c); !errors.Is(err, ErrInvalidConfig) {
			t.Fatal(bad, err)
		}
	}
}

// The vectors are generated with both suites in seal/proto; this module replays
// those of the suite it has, and must not have the other.
func TestSuite1Vectors(t *testing.T) {
	data, err := os.ReadFile("../../seal/proto/testdata/vectors/join.json")
	if err != nil {
		t.Fatal(err)
	}
	var vectors []map[string]any
	if err := json.Unmarshal(data, &vectors); err != nil {
		t.Fatal(err)
	}
	hx := func(v map[string]any, k string) []byte {
		s, _ := v[k].(string)
		b, err := hex.DecodeString(s)
		if err != nil {
			t.Fatal(k, err)
		}
		return b
	}
	seen := 0
	for _, v := range vectors {
		if v["suite"].(float64) != SuiteAES {
			if _, _, err := ParseHello(hx(v, "hello_stream"), Stream); !errors.Is(err, ErrUnknownSuite) {
				t.Fatalf("%v: %v", v["name"], err)
			}
			continue
		}
		seen++
		su, _ := LookupSuite(SuiteAES)
		m := Mode(v["mode"].(float64))
		role := RoleInitiator
		if v["role"] == "r" {
			role = RoleResponder
		}
		psk := hx(v, "join_psk")
		if _, ok := v["epoch_secret"]; ok {
			sc, err := SessionCrypto(cryptoOf(v["crypto_i"]), cryptoOf(v["crypto_r"]))
			if err != nil {
				t.Fatal(err)
			}
			if got, err := PSK(su, hx(v, "epoch_secret"), role, sc); err != nil || !bytes.Equal(got, psk) {
				t.Fatal(v["name"], "join_psk", err)
			}
		}
		binding := hx(v, "binding")
		for _, f := range []Framing{Stream, Message} {
			hk, rk, ck := "hello_stream", "reply_stream", "confirm_stream"
			if f == Message {
				hk, rk, ck = "hello_message", "reply_message", "confirm_message"
			}
			group, _ := LookupGroup(uint64(v["group"].(float64)))
			ri, rr := vectorRand(t, v, true), vectorRand(t, v, false)
			phase := uint8(v["mode_byte"].(float64)) >> 7
			j, err := NewJoinerFixed(JoinerConfig{Suite: su, Group: group, Phase: phase, Mode: m, Framing: f, Locator: hx(v, "locator"), PSK: psk, Binding: binding}, Entropy{uint64(v["attempt"].(float64)), hx(v, "random_i")}, ri)
			if err != nil {
				t.Fatal(err)
			}
			hello, err := j.WriteHelloRaw(nil, hx(v, "hello_payload"))
			if err != nil || !bytes.Equal(hello, hx(v, hk)) {
				t.Fatal(v["name"], "hello", err)
			}
			h, _, err := ParseHelloExact(hello, f)
			if err != nil {
				t.Fatal(err)
			}
			a, _, err := Accept(h, f, psk, binding, anyGroup)
			if err != nil {
				t.Fatal(err)
			}
			if err := a.Exchange(rr); err != nil {
				t.Fatal(err)
			}
			reply, err := a.WriteReplyRaw(nil, hx(v, "reply_payload"), Entropy{uint64(v["attempt_r"].(float64)), hx(v, "random_r")})
			if err != nil || !bytes.Equal(reply, hx(v, rk)) {
				t.Fatal(v["name"], "reply", err)
			}
			if _, _, err := j.ReadReply(reply); err != nil {
				t.Fatal(err)
			}
			zeros := 0
			if z, ok := v["confirm_padding"].(float64); ok {
				zeros = int(z)
			}
			confirm, err := j.WriteConfirm(nil, zeros)
			if err != nil || !bytes.Equal(confirm, hx(v, ck)) {
				t.Fatal(v["name"], "confirm", err)
			}
			if m != Bound {
				if _, err := a.ReadConfirm(confirm); err != nil {
					t.Fatal(err)
				}
			}
			ra, err := a.Finish()
			if err != nil {
				t.Fatal(err)
			}
			rj, err := j.Finish()
			if err != nil {
				t.Fatal(err)
			}
			if (ra.Sealer != nil) != (m == Sealed) {
				t.Fatal("record keys")
			}
			if m == Sealed {
				ct, _ := ra.Sealer.Seal(nil, []byte("x"))
				if got, err := rj.Opener.Open(nil, ct); err != nil || string(got) != "x" {
					t.Fatal(err)
				}
				ct, _ = rj.Sealer.Seal(nil, []byte("y")) // n = 1 after the confirmation
				if got, err := ra.Opener.Open(nil, ct); err != nil || string(got) != "y" {
					t.Fatal(err)
				}
			}
		}
	}
	if seen == 0 {
		t.Fatal("no vectors")
	}
}

// vectorRand reads the fixed private values of one end of a join vector.
func vectorRand(t *testing.T, v map[string]any, joiner bool) *GroupRand {
	hx := func(k string) []byte {
		s, _ := v[k].(string)
		b, err := hex.DecodeString(s)
		if err != nil {
			t.Fatal(k, err)
		}
		return b
	}
	if joiner {
		r := &GroupRand{X25519: hx("x_i")}
		if seed := hx("kem_seed"); len(seed) > 0 {
			r.KEMSeed = seed
		}
		return r
	}
	m := hx("encaps_m")
	return &GroupRand{X25519: hx("x_r"), Encaps: func(ek *mlkem.EncapsulationKey768) ([]byte, []byte, error) {
		return mlkemtest.Encapsulate768(ek, m)
	}}
}

func TestStateMisuseAndIncomplete(t *testing.T) {
	su, _ := LookupSuite(SuiteAES)
	psk, _ := PSK(su, bytes.Repeat([]byte{1}, 32), RoleInitiator, testSuites)
	j, err := NewJoiner(JoinerConfig{Suite: su, Group: testGroup, Mode: Trusted, Locator: []byte("loc"), PSK: psk})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := j.ReadReply(nil); !errors.Is(err, ErrState) {
		t.Fatal(err)
	}
	hello, err := j.WriteHello(nil, HelloPayload{Pipe: Pipe{ID: 2}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := j.WriteHello(nil, HelloPayload{}); !errors.Is(err, ErrState) {
		t.Fatal(err)
	}
	h, _, err := ParseHelloExact(hello, Stream)
	if err != nil {
		t.Fatal(err)
	}
	a, p, err := Accept(h, Stream, psk, nil, anyGroup)
	if err != nil || p.Pipe.ID != 2 {
		t.Fatal(err)
	}
	if _, err := a.WriteReply(nil, ReplyPayload{}); !errors.Is(err, ErrState) {
		t.Fatal("a Reply before the key exchange:", err)
	}
	if err := a.Exchange(nil); err != nil {
		t.Fatal(err)
	}
	if _, err := a.ReadConfirm(nil); !errors.Is(err, ErrState) {
		t.Fatal(err)
	}
	reply, err := a.WriteReply(nil, ReplyPayload{})
	if err != nil {
		t.Fatal(err)
	}
	for i := range reply { // Incomplete leaves the state as it was.
		if _, n, err := j.ReadReply(reply[:i]); !errors.Is(err, ErrIncomplete) || n <= i || n > len(reply) {
			t.Fatal(i, n, err)
		}
	}
	if _, _, err := j.ReadReply(reply); err != nil {
		t.Fatal(err)
	}
	confirm, _ := j.WriteConfirm(nil, 0)
	if _, err := a.Finish(); !errors.Is(err, ErrState) {
		t.Fatal(err)
	}
	if n, err := a.ReadConfirm(confirm[:5]); !errors.Is(err, ErrIncomplete) || n != len(confirm) {
		t.Fatal(n, err)
	}
	bad := bytes.Clone(confirm)
	bad[len(bad)-1] ^= 1
	if _, err := a.ReadConfirm(bad); !errors.Is(err, ErrAuth) {
		t.Fatal(err)
	}
	if _, err := a.ReadConfirm(confirm); !errors.Is(err, ErrState) {
		t.Fatal("a failure must be permanent:", err)
	}
	if _, err := a.Finish(); !errors.Is(err, ErrAuth) {
		t.Fatal(err)
	}
}

func TestBoundIsConfirmedAtAccept(t *testing.T) {
	su, _ := LookupSuite(SuiteAES)
	psk, _ := PSK(su, bytes.Repeat([]byte{1}, 32), RoleResponder, testSuites)
	b := bytes.Repeat([]byte{7}, BindingLen)
	j, _ := NewJoiner(JoinerConfig{Suite: su, Group: testGroup, Mode: Bound, Framing: Message, Locator: []byte("l"), PSK: psk, Binding: b})
	w, err := j.WriteHello(nil, HelloPayload{})
	if err != nil {
		t.Fatal(err)
	}
	h, _, _ := ParseHello(w, Message)
	a, _, err := Accept(h, Message, psk, b, anyGroup)
	if err != nil || !a.Confirmed() {
		t.Fatal(err)
	}
	if err := a.Exchange(nil); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Finish(); !errors.Is(err, ErrState) {
		t.Fatal("no result before the Reply:", err)
	}
	if _, err := a.WriteReply(nil, ReplyPayload{}); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Finish(); err != nil {
		t.Fatal(err)
	}
	if _, err := a.ReadConfirm(nil); !errors.Is(err, ErrState) {
		t.Fatal(err)
	}
}

func TestDispatchAndReply(t *testing.T) {
	if Dispatch(nil, Stream) != ClassIncomplete || Dispatch(nil, Message) != ClassNotHello {
		t.Fatal("empty")
	}
	for b, want := range map[byte]Class{0: ClassNotHello, 1: ClassEstablish, 2: ClassJoin, 3: ClassEstablish, 4: ClassEstablish, 5: ClassEstablish, 6: ClassReserved, 7: ClassReserved, 8: ClassEstablish, 9: ClassNotHello, 0xff: ClassNotHello} {
		if Dispatch([]byte{b}, Stream) != want {
			t.Fatal(b)
		}
	}
}

func TestReservedTypeFourSkipped(t *testing.T) {
	pipe, _ := EncodePipe(Pipe{ID: 2})
	b := appendItem(appendItem(nil, 4, []byte{2, 2, 0x40, 0xff}), tlvPipe, pipe)
	b = appendKeyShare(b, KeyShare{Group: GroupX25519, Share: make([]byte, 32)})
	p, err := parseHelloPayload(b, anyGroup)
	if err != nil || p.Pipe.ID != 2 {
		t.Fatal(p, err)
	}
	r := appendItem(appendItem(nil, 4, []byte{1}), tlvDecision, []byte{0})
	r = appendKeyShare(r, KeyShare{Group: GroupX25519, Share: make([]byte, 32)})
	if rp, err := parseReplyPayload(r, GroupX25519); err != nil || rp.Decision != DecisionAccept {
		t.Fatal(rp, err)
	}
}
