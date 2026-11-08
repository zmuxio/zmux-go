package proto

import (
	"bytes"
	"crypto"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/rand"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/binary"
	"errors"
	"hash"
	"testing"

	"github.com/zmuxio/zmux-go/internal/join"
)

func newKey(t *testing.T) *ecdh.PrivateKey {
	t.Helper()
	k, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

// TestEstablishThroughPublicAPI runs the whole path a host runs, on both
// framings: bytes in, trust decision after msg2, records after the handshake.
func TestEstablishThroughPublicAPI(t *testing.T) {
	for _, f := range []Framing{Stream, Message} {
		psk := fixed("psk")
		cc := Config{Profile: ProfileEstablishPSK, Mode: Sealed, Handle: make([]byte, HandleLen), PSK: psk, Static: newKey(t)}
		serverStatic := newKey(t)
		c, err := NewInitiator(cc)
		if err != nil {
			t.Fatal(err)
		}
		msg1, err := c.WriteMessage(nil, Payload{Padding: 10})
		if err != nil {
			t.Fatal(err)
		}
		wire, err := AppendHello(nil, f, Hello{Profile: cc.Profile, Mode: cc.Mode, Handle: cc.Handle, Msg1: msg1})
		if err != nil {
			t.Fatal(err)
		}
		h, n, err := ParseHello(wire, f)
		if err != nil || n != len(wire) {
			t.Fatal(err, n)
		}
		s, err := NewResponder(Config{Profile: h.Profile, Mode: h.Mode, Handle: h.Handle, PSK: psk, Static: serverStatic})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.ReadMessage(h.Msg1); err != nil {
			t.Fatal(err)
		}
		cred := Credential{Type: 1, Data: []byte("node")}
		msg2, err := s.WriteMessage(nil, Payload{Credential: &cred})
		if err != nil {
			t.Fatal(err)
		}
		p, err := c.ReadMessage(msg2)
		if err != nil {
			t.Fatal(err)
		}
		if p.Credential == nil || !bytes.Equal(p.Credential.Data, cred.Data) {
			t.Fatalf("credential %+v", p.Credential)
		}
		if !bytes.Equal(c.PeerStatic(), serverStatic.PublicKey().Bytes()) {
			t.Fatal("peer static")
		}
		msg3, err := c.WriteMessage(nil, Payload{})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.ReadMessage(msg3); err != nil {
			t.Fatal(err)
		}
		cr, err := c.Finish()
		if err != nil {
			t.Fatal(err)
		}
		sr, err := s.Finish()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.Finish(); !errors.Is(err, ErrState) {
			t.Fatal(err)
		}
		if !bytes.Equal(sr.PeerStatic, cc.Static.(*ecdh.PrivateKey).PublicKey().Bytes()) || !bytes.Equal(cr.HandshakeHash, sr.HandshakeHash) {
			t.Fatal("results differ")
		}
		ct, err := cr.Sealer.Seal(nil, []byte("up"))
		if err != nil {
			t.Fatal(err)
		}
		if got, err := sr.Opener.Open(nil, ct); err != nil || string(got) != "up" {
			t.Fatal(err)
		}
		ct, _ = sr.Sealer.Seal(nil, []byte("down"))
		if got, err := cr.Opener.Open(nil, ct); err != nil || string(got) != "down" {
			t.Fatal(err)
		}
	}
}

func TestNonSealedModesHaveNoRecordKeys(t *testing.T) {
	for _, m := range []Mode{Bound, Trusted} {
		v := defaults(ProfileEstablishPSK, m)
		i, r := v.side(t, true, nil, nil), v.side(t, false, nil, nil)
		a, _ := i.WriteMessage(nil, Payload{})
		if _, err := r.ReadMessage(a); err != nil {
			t.Fatal(err)
		}
		b, _ := r.WriteMessage(nil, Payload{})
		if _, err := i.ReadMessage(b); err != nil {
			t.Fatal(err)
		}
		c, _ := i.WriteMessage(nil, Payload{})
		if _, err := r.ReadMessage(c); err != nil {
			t.Fatal(err)
		}
		res, err := i.Finish()
		if err != nil {
			t.Fatal(err)
		}
		if res.Sealer != nil || res.Opener != nil {
			t.Fatal(m, "record keys")
		}
	}
}

func TestStateMachineMisuseAndStickyFailure(t *testing.T) {
	v := defaults(ProfileEstablishPSK, Sealed)
	i := v.side(t, true, nil, nil)
	if _, err := i.ReadMessage(nil); !errors.Is(err, ErrState) {
		t.Fatal(err)
	}
	if _, err := i.WriteMessage(nil, Payload{Credential: &Credential{Type: 1}}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatal(err)
	}
	if _, err := i.WriteMessage(nil, Payload{}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatal("a failed handshake must stay failed:", err)
	}
	r := v.side(t, false, nil, nil)
	if _, err := r.ReadMessage(make([]byte, 1248)); !errors.Is(err, ErrAuth) {
		t.Fatal(err)
	}
	if _, err := r.WriteMessage(nil, Payload{}); !errors.Is(err, ErrAuth) {
		t.Fatal(err)
	}
	if _, err := r.Finish(); !errors.Is(err, ErrAuth) {
		t.Fatal(err)
	}
}

func TestConfigChecks(t *testing.T) {
	psk := fixed("psk")
	s := newKey(t)
	for name, c := range map[string]Config{
		"without psk":         {Profile: ProfileEstablishPSK, Handle: make([]byte, HandleLen), Static: s},
		"no handle":           {Profile: ProfileEstablishPSK, PSK: psk, Static: s},
		"long handle":         {Profile: ProfileEstablishPSK, PSK: psk, Static: s, Handle: make([]byte, HandleLen+1)},
		"short handle":        {Profile: ProfileEstablishPSK, PSK: psk, Static: s, Handle: make([]byte, HandleLen-1)},
		"wrong static type":   {Profile: ProfileEstablish, Static: "key"},
		"no-psk handle":       {Profile: ProfileEstablish, Handle: make([]byte, HandleLen), Static: s},
		"no-psk psk":          {Profile: ProfileEstablish, PSK: psk, Static: s},
		"no-psk no static":    {Profile: ProfileEstablish},
		"bound no binding":    {Profile: ProfileEstablish, Mode: Bound, Static: s},
		"sealed with binding": {Profile: ProfileEstablish, Binding: psk, Static: s},
		"reserved mode":       {Profile: ProfileEstablish, Mode: modeReserved, Static: s},
	} {
		if _, err := NewInitiator(c); !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if _, err := NewInitiator(Config{Profile: 6}); !errors.Is(err, ErrUnknownProfile) {
		t.Error(err)
	}
}

// TestParseHelloNeedsExactlyWhatItSays feeds every prefix of a Hello.
func TestParseHelloNeedsExactlyWhatItSays(t *testing.T) {
	wire, err := AppendHello(nil, Stream, Hello{Profile: ProfileEstablishPSK, Handle: []byte("0123456789abcdef"), Msg1: make([]byte, 48)})
	if err != nil {
		t.Fatal(err)
	}
	for i := range wire {
		_, n, err := ParseHello(wire[:i], Stream)
		if !errors.Is(err, ErrIncomplete) || n <= i || n > len(wire) {
			t.Fatalf("prefix %d: need %d, %v", i, n, err)
		}
		if _, _, err := ParseHello(wire[:i], Message); i < 6 && !errors.Is(err, ErrMalformed) {
			t.Fatalf("message prefix %d: %v", i, err)
		}
	}
	if _, n, err := ParseHello(append(bytes.Clone(wire), 1, 2), Stream); err != nil || n != len(wire) {
		t.Fatal(n, err)
	}
}

func TestExtendedProfileRoundTrip(t *testing.T) {
	const ext Profile = 1 << 20
	profiles[ext] = profiles[ProfileEstablish]
	defer delete(profiles, ext)
	wire, err := AppendHello(nil, Stream, Hello{Profile: ext, Msg1: make([]byte, 32)})
	if err != nil {
		t.Fatal(err)
	}
	if wire[0] != profileEscape {
		t.Fatalf("% x", wire[:2])
	}
	h, _, err := ParseHello(wire, Stream)
	if err != nil {
		t.Fatal(err)
	}
	if h.Profile != ext {
		t.Fatal(h.Profile)
	}
}

func TestRekeyWalk(t *testing.T) {
	if testing.Short() {
		t.Skip("walks a key interval")
	}
	k := fixed("walk")
	s, o := join.NewSealer(chachaPolyAEAD{}, k), join.NewOpener(chachaPolyAEAD{}, k)
	var ct []byte
	for n := range join.DefaultRekeyInterval + 2 {
		var err error
		if ct, err = s.Seal(ct[:0], nil); err != nil {
			t.Fatal(n, err)
		}
		if _, err = o.Open(nil, ct); err != nil {
			t.Fatal(n, err)
		}
	}
	if bytes.Equal(s.CS.K, k) || s.CS.N != join.DefaultRekeyInterval+2 {
		t.Fatal("no rekey")
	}
}

func TestSealTooLargeDoesNotBreak(t *testing.T) {
	s := join.NewSealer(chachaPolyAEAD{}, fixed("k"))
	if _, err := s.Seal(nil, make([]byte, s.MaxPlaintext()+1)); !errors.Is(err, ErrTooLarge) {
		t.Fatal(err)
	}
	if _, err := s.Seal(nil, []byte("x")); err != nil {
		t.Fatal(err)
	}
}

func FuzzParse(f *testing.F) {
	f.Add([]byte{2, 0, 1, 'h', 0, 0})
	f.Add([]byte{8, 0x40, 9})
	f.Add(append(item(1, 2, 1, 0), item(8, 2, 3, 0)...))
	f.Fuzz(func(t *testing.T, b []byte) {
		_, _, _ = ParseHello(b, Stream)
		_, _, _ = ParseHello(b, Message)
		_, _ = parsePayload(b, knownCredential|knownAppData|knownFirstBytes|knownProof, DefaultMaxAppData, false)
	})
}

func TestAESVolumeLimitHolds(t *testing.T) {
	for p, pat := range profiles {
		lim := pat.suite.aead.MaxBytesPerKey()
		if lim != 0 && pat.suite.aead.RekeyInterval()*maxMessage > lim {
			t.Errorf("%v: rekey interval exceeds the data volume limit", p)
		}
	}
}

// A suite with other sizes everywhere, to show that no size is assumed.

type fakeKex struct{ x25519Kex }

const fakeSuffix = "sixteen-byte-tail"

func (fakeKex) name() string   { return "FakeDH" }
func (fakeKex) publicLen() int { return 32 + len(fakeSuffix) }
func (f fakeKex) generate() (keypair, error) {
	kp, err := f.x25519Kex.generate()
	if err != nil {
		return keypair{}, err
	}
	kp.pub = append(kp.pub, fakeSuffix...)
	return kp, nil
}
func (f fakeKex) importStatic(k crypto.PrivateKey) (keypair, error) {
	kp, err := f.x25519Kex.importStatic(k)
	if err != nil {
		return keypair{}, err
	}
	kp.pub = append(kp.pub, fakeSuffix...)
	return kp, nil
}
func (f fakeKex) exchange(priv any, pub []byte) ([]byte, error) {
	if !bytes.HasSuffix(pub, []byte(fakeSuffix)) {
		return nil, ErrKex
	}
	return f.x25519Kex.exchange(priv, pub[:32])
}

// fakeAEAD has a 24-byte key, a 25-byte tag and a 20-byte nonce, big-endian
// at the front.
type fakeAEAD struct{ inner cipher.AEAD }

func (fakeAEAD) NonceSize() int { return 20 }
func (fakeAEAD) Overhead() int  { return 25 }
func (f fakeAEAD) Seal(dst, nonce, p, ad []byte) []byte {
	out := f.inner.Seal(dst, nonce[:12], p, ad)
	tail := sha256.Sum256(out[len(out)-16:])
	return append(out, tail[:9]...)
}
func (f fakeAEAD) Open(dst, nonce, c, ad []byte) ([]byte, error) {
	if len(c) < 25 {
		return nil, ErrAuth
	}
	body := c[:len(c)-9]
	tail := sha256.Sum256(body[len(body)-16:])
	if !bytes.Equal(tail[:9], c[len(c)-9:]) {
		return nil, ErrAuth
	}
	return f.inner.Open(dst, nonce[:12], body, ad)
}

type fakeAEADFunc struct{}

func (fakeAEADFunc) Name() string           { return "FakeAEAD" }
func (fakeAEADFunc) KeyLen() int            { return 24 }
func (fakeAEADFunc) TagLen() int            { return 25 }
func (fakeAEADFunc) RekeyInterval() uint64  { return 3 }
func (fakeAEADFunc) MaxBytesPerKey() uint64 { return 0 }
func (fakeAEADFunc) Nonce(n uint64) []byte {
	return append(binary.BigEndian.AppendUint64(nil, n), make([]byte, 12)...)
}
func (fakeAEADFunc) New(k []byte) (cipher.AEAD, error) {
	d := sha256.Sum256(k)
	inner, err := join.AESGCM{}.New(d[:])
	return fakeAEAD{inner}, err
}

type sha512Hash struct{}

func (sha512Hash) Name() string   { return "SHA512" }
func (sha512Hash) Size() int      { return sha512.Size }
func (sha512Hash) New() hash.Hash { return sha512.New() }

func TestStateMachineAssumesNoSizes(t *testing.T) {
	s := suite{kex: fakeKex{}, kem: mlkem768{}, aead: fakeAEADFunc{}, hash: sha512Hash{}, pskLen: 40}
	for _, sh := range []shape{shapeXXhfsPSK0, shapeXXhfs} {
		pat := &pattern{shape: sh, suite: s}
		cfg := func(static bool) Config {
			c := Config{Profile: 1 << 30, Mode: Sealed}
			if sh.hasHandle {
				c.Handle = make([]byte, HandleLen)
				c.PSK = make([]byte, 40)
			}
			if static {
				c.Static = newKey(t)
			}
			return c
		}
		gen := pat.suite.kex.generate
		i, err := newHandshake(cfg(pat.usesStatic(true)), true, pat, gen)
		if err != nil {
			t.Fatal(err)
		}
		r, err := newHandshake(cfg(pat.usesStatic(false)), false, pat, gen)
		if err != nil {
			t.Fatal(err)
		}
		for idx := range pat.msgs {
			w, rd := i, r
			if idx%2 == 1 {
				w, rd = r, i
			}
			m, err := w.WriteMessage(nil, Payload{Padding: 7})
			if err != nil {
				t.Fatal(sh.name, idx, err)
			}
			got, err := rd.ReadMessage(m)
			if err != nil || got.Padding != 7 {
				t.Fatal(sh.name, idx, err)
			}
		}
		ri, err := i.Finish()
		if err != nil {
			t.Fatal(err)
		}
		rr, err := r.Finish()
		if err != nil {
			t.Fatal(err)
		}
		if len(ri.HandshakeHash) != sha512.Size || !bytes.Equal(ri.HandshakeHash, rr.HandshakeHash) {
			t.Fatal("hash")
		}
		if ri.Sealer.MaxPlaintext() != 65535-25 || ri.Sealer.Overhead() != 25 {
			t.Fatal(ri.Sealer.MaxPlaintext())
		}
		if _, err := ri.Sealer.Seal(nil, make([]byte, 65535-25+1)); !errors.Is(err, ErrTooLarge) {
			t.Fatal(err)
		}
		// Across two turns of the key, with the largest record once.
		for n := range 8 {
			p := []byte{byte(n)}
			if n == 4 {
				p = make([]byte, ri.Sealer.MaxPlaintext())
			}
			ct, err := ri.Sealer.Seal(nil, p)
			if err != nil {
				t.Fatal(n, err)
			}
			got, err := rr.Opener.Open(nil, ct)
			if err != nil || !bytes.Equal(got, p) {
				t.Fatal(n, err)
			}
		}
		if len(ri.Sealer.CS.K) != 24 {
			t.Fatal("key length")
		}
		a, err := ri.ExportKeyingMaterial("l", nil, 100)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := rr.ExportKeyingMaterial("l", nil, 100)
		if !bytes.Equal(a, b) {
			t.Fatal("exporter")
		}
	}
}
