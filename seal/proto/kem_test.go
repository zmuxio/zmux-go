package proto

import (
	"bytes"
	"crypto/ecdh"
	"crypto/mlkem"
	"crypto/rand"
	"errors"
	"fmt"
	"testing"

	"github.com/zmuxio/zmux-go/internal/join"
)

// handshakeEnds are the two ends of one handshake through the public API.
type handshakeEnds struct {
	cfgI, cfgR Config
}

func newEnds(tb testing.TB, p Profile, m Mode, iStatic, rStatic *ecdh.PrivateKey) handshakeEnds {
	tb.Helper()
	e := handshakeEnds{
		cfgI: Config{Profile: p, Mode: m, Static: iStatic},
		cfgR: Config{Profile: p, Mode: m, Static: rStatic},
	}
	if p.HasPSK() {
		psk := fixed("psk")
		e.cfgI.Handle, e.cfgI.PSK = fixed("handle")[:HandleLen], psk
		e.cfgR.Handle, e.cfgR.PSK = fixed("handle")[:HandleLen], psk
	}
	if m == Bound {
		e.cfgI.Binding, e.cfgR.Binding = fixed("binding"), fixed("binding")
	}
	return e
}

// run makes a whole handshake and returns the three messages.
func (e handshakeEnds) run(tb testing.TB) (msgs [3][]byte, ri, rr *Result) {
	i, err := NewInitiator(e.cfgI)
	if err != nil {
		tb.Fatal(err)
	}
	r, err := NewResponder(e.cfgR)
	if err != nil {
		tb.Fatal(err)
	}
	for idx := range 3 {
		w, rd := i, r
		if idx == 1 {
			w, rd = r, i
		}
		want := w.EmptyMessageLen()
		m, err := w.WriteMessage(nil, Payload{})
		if err != nil {
			tb.Fatal(idx, err)
		}
		if len(m) != want {
			tb.Fatalf("message %d: %d bytes, EmptyMessageLen %d", idx, len(m), want)
		}
		if _, err := rd.ReadMessage(m); err != nil {
			tb.Fatal(idx, err)
		}
		msgs[idx] = m
	}
	if ri, err = i.Finish(); err != nil {
		tb.Fatal(err)
	}
	if rr, err = r.Finish(); err != nil {
		tb.Fatal(err)
	}
	return msgs, ri, rr
}

func TestProfiles(t *testing.T) {
	iStatic, rStatic := newKey(t), newKey(t)
	for _, p := range allProfiles {
		if !p.PQConfidential() {
			t.Fatalf("%v: not PQ-confidential", p)
		}
		msg1 := 1216
		if p.HasPSK() {
			msg1 = 1248
		}
		for _, m := range []Mode{Sealed, Bound, Trusted} {
			msgs, ri, rr := newEnds(t, p, m, iStatic, rStatic).run(t)
			if len(msgs[0]) != msg1 || len(msgs[1]) != 1200 || len(msgs[2]) != 64 {
				t.Fatalf("%v %v: lengths %d %d %d", p, m, len(msgs[0]), len(msgs[1]), len(msgs[2]))
			}
			if !bytes.Equal(ri.HandshakeHash, rr.HandshakeHash) {
				t.Fatal("hash")
			}
			a, _ := ri.ExportKeyingMaterial("l", nil, 32)
			b, _ := rr.ExportKeyingMaterial("l", nil, 32)
			if !bytes.Equal(a, b) {
				t.Fatal("exporter")
			}
			if m == Sealed {
				ct, _ := ri.Sealer.Seal(nil, []byte("up"))
				if got, err := rr.Opener.Open(nil, ct); err != nil || string(got) != "up" {
					t.Fatal(err)
				}
			}
		}
	}
	if Profile(6).Supported() || Profile(6).HasPSK() {
		t.Fatal("unknown profile")
	}
}

// The table is consistent: every row has both KEM tokens and is PQ-confidential.
func TestProfileTable(t *testing.T) {
	for p, pat := range profiles {
		e1, ekem1 := false, false
		for _, m := range pat.msgs {
			e1, ekem1 = e1 || hasToken(m, tokE1), ekem1 || hasToken(m, tokEKEM1)
		}
		if !e1 || !ekem1 || !pat.pqConf {
			t.Errorf("%v: inconsistent row", p)
		}
	}
}

// Every handshake makes its own KEM key pair, and gives it up after use.
func TestFreshKEMKeys(t *testing.T) {
	seen := map[string]bool{}
	for range 3 {
		i, err := NewInitiator(Config{Profile: ProfileEstablish, Mode: Sealed, Static: newKey(t)})
		if err != nil {
			t.Fatal(err)
		}
		m, err := i.WriteMessage(nil, Payload{})
		if err != nil {
			t.Fatal(err)
		}
		ek := string(m[32 : 32+1184])
		if seen[ek] {
			t.Fatal("encapsulation key reused")
		}
		seen[ek] = true
		if i.e1.priv == nil {
			t.Fatal("no key pair kept for message 2")
		}
		r, _ := NewResponder(Config{Profile: ProfileEstablish, Mode: Sealed, Static: newKey(t)})
		if _, err := r.ReadMessage(m); err != nil {
			t.Fatal(err)
		}
		m2, err := r.WriteMessage(nil, Payload{})
		if err != nil {
			t.Fatal(err)
		}
		if r.re1 != nil {
			t.Fatal("responder keeps the encapsulation key")
		}
		if _, err := i.ReadMessage(m2); err != nil {
			t.Fatal(err)
		}
		if i.e1.priv != nil || i.e1.pub != nil {
			t.Fatal("decapsulation key kept after use")
		}
	}
	// A failure gives it up too.
	i, _ := NewInitiator(Config{Profile: ProfileEstablish, Mode: Sealed, Static: newKey(t)})
	if _, err := i.WriteMessage(nil, Payload{}); err != nil {
		t.Fatal(err)
	}
	if _, err := i.ReadMessage(make([]byte, 1200)); err == nil || i.e1.priv != nil {
		t.Fatalf("%v, key kept %v", err, i.e1.priv != nil)
	}
}

// An invalid encapsulation key fails at message 2, which is not written; under a
// pre-shared key nothing is encapsulated before message 1 authenticates.
func TestEncapsulationOrder(t *testing.T) {
	i, _ := NewInitiator(Config{Profile: ProfileEstablish, Mode: Sealed, Static: newKey(t)})
	m, _ := i.WriteMessage(nil, Payload{})
	bad := bytes.Clone(m)
	copy(bad[32:], setCoefficient(bad[32:32+1184], 0, 4095)[:3])
	r, _ := NewResponder(Config{Profile: ProfileEstablish, Mode: Sealed, Static: newKey(t)})
	if _, err := r.ReadMessage(bad); err != nil {
		t.Fatal(err)
	}
	if out, err := r.WriteMessage([]byte("kept"), Payload{}); !errors.Is(err, ErrKex) || out != nil {
		t.Fatalf("%v %q", err, out)
	}
	if _, err := r.WriteMessage(nil, Payload{}); !errors.Is(err, ErrKex) {
		t.Fatal("failure not permanent")
	}

	e := newEnds(t, ProfileEstablishPSK, Sealed, newKey(t), newKey(t))
	ci, _ := NewInitiator(e.cfgI)
	m, _ = ci.WriteMessage(nil, Payload{})
	e.cfgR.PSK = fixed("another psk")
	cr, _ := NewResponder(e.cfgR)
	calls := 0
	enc := cr.encaps
	cr.encaps = func(ek []byte) ([]byte, []byte, error) { calls++; return enc(ek) }
	if _, err := cr.ReadMessage(m); !errors.Is(err, ErrAuth) {
		t.Fatal(err)
	}
	if _, err := cr.WriteMessage(nil, Payload{}); err == nil || calls != 0 {
		t.Fatalf("%v, %d encapsulations", err, calls)
	}
}

// Handshakes run concurrently share nothing.
func TestConcurrentHandshakes(t *testing.T) {
	iStatic, rStatic := newKey(t), newKey(t)
	t.Run("group", func(t *testing.T) {
		for _, p := range allProfiles {
			for n := range 4 {
				t.Run(fmt.Sprintf("%d-%d", p, n), func(t *testing.T) {
					t.Parallel()
					newEnds(t, p, Sealed, iStatic, rStatic).run(t)
				})
			}
		}
	})
}

// BenchmarkHandshake is one whole Establish, both ends, in memory, with the
// bytes of its three messages on a byte stream.
func BenchmarkHandshake(b *testing.B) {
	iStatic, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		b.Fatal(err)
	}
	rStatic, _ := ecdh.X25519().GenerateKey(rand.Reader)
	for _, p := range allProfiles {
		e := newEnds(b, p, Sealed, iStatic, rStatic)
		b.Run(fmt.Sprintf("profile=%d", p), func(b *testing.B) {
			b.ReportAllocs()
			var wire int
			for b.Loop() {
				msgs, _, _ := e.run(b)
				wire = HelloHeaderLen(Stream, p) + len(msgs[0]) + 2*lenPrefix + len(msgs[1]) + len(msgs[2])
			}
			b.ReportMetric(float64(wire), "wire-B/op")
		})
	}
}

// BenchmarkKEM is the share of each end's KEM work.
func BenchmarkKEM(b *testing.B) {
	k := mlkem768{}
	b.Run("initiator", func(b *testing.B) {
		kp, _ := k.generate()
		ct, _, _ := k.encapsulate(kp.pub)
		b.ReportAllocs()
		for b.Loop() {
			kp, err := k.generate()
			if err != nil {
				b.Fatal(err)
			}
			if _, err := k.decapsulate(kp.priv, ct); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("responder", func(b *testing.B) {
		kp, _ := k.generate()
		b.ReportAllocs()
		for b.Loop() {
			if _, _, err := k.encapsulate(kp.pub); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// The explicit modulus check agrees with crypto/mlkem: a valid key passes, a
// coefficient of q or more (either of a pair, the first or the last) and a
// wrong length fail.
func TestCheckEncapsulationKey(t *testing.T) {
	dk, err := mlkem.GenerateKey768()
	if err != nil {
		t.Fatal(err)
	}
	ek := dk.EncapsulationKey().Bytes()
	if !join.CheckEncapsulationKey(ek) {
		t.Fatal("a valid key refused")
	}
	set := func(b []byte, i int, v uint16) { // coefficient i
		j := i / 2 * 3
		if i%2 == 0 {
			b[j], b[j+1] = byte(v), b[j+1]&0xf0|byte(v>>8)
		} else {
			b[j+1], b[j+2] = b[j+1]&0x0f|byte(v<<4), byte(v>>4)
		}
	}
	for _, c := range []struct {
		i int
		v uint16
	}{{0, 3329}, {1, 3329}, {0, 4095}, {767, 3329}, {767, 4095}} {
		b := bytes.Clone(ek)
		set(b, c.i, c.v)
		if join.CheckEncapsulationKey(b) {
			t.Errorf("coefficient %d = %d accepted", c.i, c.v)
		}
		if _, err := mlkem.NewEncapsulationKey768(b); err == nil {
			t.Errorf("coefficient %d = %d: crypto/mlkem disagrees", c.i, c.v)
		}
		set(b, c.i, 3328)
		if !join.CheckEncapsulationKey(b) {
			t.Errorf("coefficient %d = 3328 refused", c.i)
		}
	}
	if join.CheckEncapsulationKey(ek[:len(ek)-1]) {
		t.Error("a short key accepted")
	}
}
