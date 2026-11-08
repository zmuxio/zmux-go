package proto

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/hmac"
	"crypto/mlkem"
	"crypto/mlkem/mlkemtest"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/zmuxio/zmux-go/internal/join"
	"github.com/zmuxio/zmux-go/internal/wire"
	"golang.org/x/crypto/chacha20poly1305"
)

// fixedKEM makes the ML-KEM-768 key pair of a 64-byte seed (d, z).
func fixedKEM(t testing.TB, seed []byte) func() (keypair, error) {
	return func() (keypair, error) {
		dk, err := mlkem.NewDecapsulationKey768(seed)
		if err != nil {
			t.Fatal(err)
		}
		return keypair{priv: dk, pub: dk.EncapsulationKey().Bytes()}, nil
	}
}

// fixedEncaps encapsulates with the 32-byte randomness m.
func fixedEncaps(m []byte) func(ek []byte) ([]byte, []byte, error) {
	return func(ek []byte) ([]byte, []byte, error) {
		k, err := mlkem.NewEncapsulationKey768(ek)
		if err != nil {
			return nil, nil, ErrKex
		}
		ss, ct, err := mlkemtest.Encapsulate768(k, m)
		return ct, ss, err
	}
}

// setCoefficient sets the 12-bit coefficient i of an ML-KEM-768 encapsulation
// key (ByteEncode12 of FIPS 203) to v.
func setCoefficient(ek []byte, i int, v uint16) []byte {
	b := bytes.Clone(ek)
	o := i / 2 * 3
	if i%2 == 0 {
		b[o] = byte(v)
		b[o+1] = b[o+1]&0xf0 | byte(v>>8)&0x0f
	} else {
		b[o+1] = b[o+1]&0x0f | byte(v<<4)
		b[o+2] = byte(v >> 4)
	}
	return b
}

// kemPub returns the encapsulation key of a KEM seed.
func kemPub(t testing.TB, seed []byte) []byte {
	kp, _ := fixedKEM(t, seed)()
	return kp.pub
}

// genSplitRecords seals with the split keys of handshakes, which checks the
// chain from the handshake to the records end to end.
func genSplitRecords(t testing.TB) []recordVector {
	var out []recordVector
	p := []byte("a zmux record")
	for _, pr := range []Profile{ProfileEstablishPSK, ProfileEstablishPSKAES} {
		v := genHandshake(t, "", defaults(pr, Sealed), nil)
		kind := profiles[pr].suite.aead
		for _, k := range []struct {
			name string
			key  []byte
		}{{"c1", v.SplitC1}, {"c2", v.SplitC2}} {
			s := join.NewSealer(kind, k.key)
			ct, err := s.Seal(nil, p)
			if err != nil {
				t.Fatal(err)
			}
			out = append(out, recordVector{Cipher: kind.Name(), Name: fmt.Sprintf("profile%d-split-%s", pr, k.name), Key: k.key, Plaintext: p, Ciphertext: ct, KeyAfter: bytes.Clone(s.CS.K), NAfter: s.CS.N})
		}
	}
	return out
}

// sessionVector follows an Establish to the session secret and the Join keys
// derived from it.
type sessionVector struct {
	Profile        uint64          `json:"profile"`
	ExporterSecret hexb            `json:"exporter_secret"`
	SessionExport  hexb            `json:"session_export"`
	InitNonce      hexb            `json:"init_nonce"`
	RespNonce      hexb            `json:"resp_nonce"`
	SessionSecret  hexb            `json:"session_secret"`
	JoinPSK        []joinPSKVector `json:"join_psk"`
}

func genSessions(t testing.TB) []sessionVector {
	var out []sessionVector
	both := []uint64{join.SuiteAES, join.SuiteChaCha}
	for _, p := range []Profile{ProfileEstablishPSK, ProfileEstablishAES} {
		v := genHandshake(t, "", defaults(p, Sealed), nil)
		exp, err := join.Export(profiles[p].suite.hash, v.ExporterSecret, wire.ExporterSession, nil, 32)
		if err != nil {
			t.Fatal(err)
		}
		in, re := fixed("init nonce")[:16], fixed("resp nonce")[:16]
		s := sessionVector{Profile: uint64(p), ExporterSecret: v.ExporterSecret, SessionExport: exp, InitNonce: in, RespNonce: re,
			SessionSecret: wire.SessionSecret(in, re, exp, nil)}
		for _, id := range both {
			su, _ := join.LookupSuite(id)
			s.JoinPSK = append(s.JoinPSK, joinPSKVector{Suite: id, EpochSecret: s.SessionSecret, HashLen: su.Hash.Size(), CryptoI: fullCrypto, CryptoR: fullCrypto,
				I: jpsk(su, s.SessionSecret, join.RoleInitiator), R: jpsk(su, s.SessionSecret, join.RoleResponder)})
		}
		out = append(out, s)
	}
	return out
}

// refNoise is an independent, plain transcription of Noise with the HFS
// tokens, used to check the handshake vectors.
type refNoise struct {
	aes   bool
	ck, h []byte
	k     []byte
	n     uint64
}

func refHMAC(key []byte, parts ...[]byte) []byte {
	m := hmac.New(sha256.New, key)
	for _, p := range parts {
		m.Write(p)
	}
	return m.Sum(nil)
}

func (r *refNoise) hkdf(ikm []byte) (a, b, c []byte) {
	tk := refHMAC(r.ck, ikm)
	a = refHMAC(tk, []byte{1})
	b = refHMAC(tk, a, []byte{2})
	c = refHMAC(tk, b, []byte{3})
	return
}

func (r *refNoise) mixHash(b []byte) { r.h = sha256Of(r.h, b) }

func sha256Of(h, b []byte) []byte {
	d := sha256.New()
	d.Write(h)
	d.Write(b)
	return d.Sum(nil)
}

func (r *refNoise) mixKey(ikm []byte) { r.ck, r.k, _ = r.hkdf(ikm); r.n = 0 }

func (r *refNoise) mixKeyAndHash(ikm []byte) {
	var th []byte
	r.ck, th, r.k = r.hkdf(ikm)
	r.mixHash(th)
	r.n = 0
}

func (r *refNoise) aead() (cipher.AEAD, []byte) {
	nonce := make([]byte, 12)
	if r.aes {
		binary.BigEndian.PutUint64(nonce[4:], r.n)
		b, _ := aes.NewCipher(r.k)
		a, _ := cipher.NewGCM(b)
		return a, nonce
	}
	binary.LittleEndian.PutUint64(nonce[4:], r.n)
	a, _ := chacha20poly1305.New(r.k)
	return a, nonce
}

func (r *refNoise) encryptAndHash(dst, p []byte) []byte {
	out := p
	if r.k != nil {
		a, nonce := r.aead()
		out = a.Seal(nil, nonce, p, r.h)
		r.n++
	}
	r.mixHash(out)
	return append(dst, out...)
}

// TestHandshakeVectorsIndependent replays the vector files on refNoise.
func TestHandshakeVectorsIndependent(t *testing.T) {
	for _, v := range readVectors[[]handshakeVector](t, "handshake.json") {
		pat := profiles[Profile(v.Profile)]
		r := &refNoise{aes: pat.suite.aead.Name() == "AESGCM"}
		name := "Noise_" + pat.name + "_25519+MLKEM768_" + pat.suite.aead.Name() + "_SHA256"
		if name != v.NoiseName {
			t.Fatalf("%s: name %s", v.Name, v.NoiseName)
		}
		d := sha256.Sum256([]byte(name))
		r.h, r.ck = d[:], d[:]
		r.mixHash(v.Prologue)
		priv := func(seed []byte) *ecdh.PrivateKey { return x25519(t, seed) }
		dh := func(a []byte, b *ecdh.PrivateKey) []byte {
			out, err := priv(a).ECDH(b.PublicKey())
			if err != nil {
				t.Fatal(err)
			}
			return out
		}
		ie, re := priv(v.InitiatorEphemeral), priv(v.ResponderEphemeral)
		psk := pat.hasPSK()
		dk, err := mlkem.NewDecapsulationKey768(v.InitiatorKEMSeed)
		if err != nil {
			t.Fatal(err)
		}
		ss, ct, err := mlkemtest.Encapsulate768(dk.EncapsulationKey(), v.ResponderKEMRandom)
		if err != nil {
			t.Fatal(err)
		}
		var msgs [3][]byte
		if psk {
			r.mixKeyAndHash(v.PSK)
		}
		m := append([]byte{}, ie.PublicKey().Bytes()...)
		r.mixHash(ie.PublicKey().Bytes())
		if psk {
			r.mixKey(ie.PublicKey().Bytes())
		}
		m = r.encryptAndHash(m, dk.EncapsulationKey().Bytes())
		msgs[0] = r.encryptAndHash(m, v.Messages[0].Payload)
		check := func(i int) {
			if !bytes.Equal(msgs[i], v.Messages[i].Noise) || !bytes.Equal(r.h, v.Messages[i].H) || !bytes.Equal(r.ck, v.Messages[i].CK) {
				t.Errorf("%s: message %d differs from the reference", v.Name, i)
			}
		}
		check(0)
		m = append([]byte{}, re.PublicKey().Bytes()...)
		r.mixHash(re.PublicKey().Bytes())
		if psk {
			r.mixKey(re.PublicKey().Bytes())
		}
		r.mixKey(dh(v.InitiatorEphemeral, re))
		m = r.encryptAndHash(m, ct)
		r.mixKey(ss)
		rs := priv(v.ResponderStatic)
		m = r.encryptAndHash(m, rs.PublicKey().Bytes())
		r.mixKey(dh(v.InitiatorEphemeral, rs))
		msgs[1] = r.encryptAndHash(m, v.Messages[1].Payload)
		check(1)
		is := priv(v.InitiatorStatic)
		m = r.encryptAndHash(nil, is.PublicKey().Bytes())
		r.mixKey(dh(v.InitiatorStatic, re))
		msgs[2] = r.encryptAndHash(m, v.Messages[2].Payload)
		check(2)
		c1, c2, _ := r.hkdf(nil)
		if !bytes.Equal(c1, v.SplitC1) || !bytes.Equal(c2, v.SplitC2) || !bytes.Equal(r.h, v.HandshakeHash) {
			t.Errorf("%s: split differs from the reference", v.Name)
		}
	}
}
