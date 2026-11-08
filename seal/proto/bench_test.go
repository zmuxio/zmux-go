package proto

import (
	"crypto/ecdh"
	"crypto/rand"
	"testing"
)

func benchKey(b *testing.B, alg KeyAlg) *SigningKey {
	seed := make([]byte, 32)
	_, _ = rand.Read(seed)
	k, err := NewSigningKey(alg, seed)
	if err != nil {
		b.Fatal(err)
	}
	return k
}

func BenchmarkMLDSA44Sign(b *testing.B) {
	k := benchKey(b, KeyAlgMLDSA44)
	m := ProofMessage(1, make([]byte, 32))
	for b.Loop() {
		if _, err := k.Sign(m); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMLDSA44Verify(b *testing.B) {
	k := benchKey(b, KeyAlgMLDSA44)
	m := ProofMessage(1, make([]byte, 32))
	sig, _ := k.Sign(m)
	for b.Loop() {
		if !VerifySignature(KeyAlgMLDSA44, k.Public(), m, sig) {
			b.Fatal("verify")
		}
	}
}

// BenchmarkServerMessage2 is the server's message 2 of Profile 1 (key
// exchange, encapsulation, encryption), without and with a hedged ML-DSA-44
// proof.
func BenchmarkServerMessage2(b *testing.B) {
	for _, proof := range []bool{false, true} {
		name := "no-proof"
		if proof {
			name = "proof"
		}
		b.Run(name, func(b *testing.B) {
			pk := benchKey(b, KeyAlgMLDSA44)
			cs, _ := ecdh.X25519().GenerateKey(rand.Reader)
			ss, _ := ecdh.X25519().GenerateKey(rand.Reader)
			cfg := Config{Profile: ProfileEstablishPSK, Handle: make([]byte, HandleLen), PSK: make([]byte, 32)}
			for b.Loop() {
				b.StopTimer()
				ci, si := cfg, cfg
				ci.Static, si.Static = cs, ss
				i, _ := NewInitiator(ci)
				r, _ := NewResponder(si)
				m1, _ := i.WriteMessage(nil, Payload{})
				if _, err := r.ReadMessage(m1); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				var err error
				if proof {
					_, err = r.WriteMessageProof(nil, Payload{Proof: &Proof{Alg: pk.Alg(), Pub: pk.Public()}}, pk.Sign)
				} else {
					_, err = r.WriteMessage(nil, Payload{})
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
