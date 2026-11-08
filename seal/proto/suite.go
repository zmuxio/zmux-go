package proto

import (
	"crypto"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/mlkem"
	"crypto/rand"
	"encoding/binary"
	"fmt"

	"github.com/zmuxio/zmux-go/internal/join"
	"golang.org/x/crypto/chacha20poly1305"
)

// A suite is what a profile needs from cryptography. Nothing outside these
// interfaces knows a key, tag, hash or nonce size. The set is closed: a new
// algorithm is a new table row in this package.

// keyExchange is the Noise DH function.
type keyExchange interface {
	name() string
	publicLen() int
	generate() (keypair, error)
	// importStatic takes the Config.Static of a profile.
	importStatic(k crypto.PrivateKey) (keypair, error)
	// exchange must fail on a key whose shared secret is all zero.
	exchange(priv any, pub []byte) ([]byte, error)
}

type keypair struct {
	priv any
	pub  []byte
}

// kem is the KEM function of a suite (Noise HFS): the initiator sends
// an encapsulation key, the responder a ciphertext, and both mix the shared
// secret. A KEM is not contributory: the encapsulating end alone decides the
// secret.
type kem interface {
	name() string
	encapKeyLen() int
	ciphertextLen() int
	// generate makes a key pair for one handshake; its private part
	// decapsulates once.
	generate() (keypair, error)
	// encapsulate fails with ErrKex on an invalid encapsulation key.
	encapsulate(ek []byte) (ct, ss []byte, err error)
	decapsulate(priv any, ct []byte) ([]byte, error)
}

type suite struct {
	kex keyExchange
	// kem is the KEM of the HFS tokens.
	kem  kem
	aead join.AEADFunc
	hash join.HashFunc
	// pskLen is the length of a pre-shared key.
	pskLen int
}

func (s suite) noiseName(pattern string) string {
	return "Noise_" + pattern + "_" + s.kex.name() + "+" + s.kem.name() + "_" + s.aead.Name() + "_" + s.hash.Name()
}

type x25519Kex struct{}

func (x25519Kex) name() string   { return "25519" }
func (x25519Kex) publicLen() int { return 32 }

func (x25519Kex) generate() (keypair, error) {
	k, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return keypair{}, fmt.Errorf("%w: %w", ErrRandomFailed, err)
	}
	return keypair{priv: k, pub: k.PublicKey().Bytes()}, nil
}

func (x25519Kex) importStatic(k crypto.PrivateKey) (keypair, error) {
	p, ok := k.(*ecdh.PrivateKey)
	if !ok || p == nil || p.Curve() != ecdh.X25519() {
		return keypair{}, fmt.Errorf("%w: static key is not X25519", ErrInvalidConfig)
	}
	return keypair{priv: p, pub: p.PublicKey().Bytes()}, nil
}

// exchange relies on crypto/ecdh, which fails on an all-zero output and keeps
// the precomputation of the private key object.
func (x25519Kex) exchange(priv any, pub []byte) ([]byte, error) {
	p, err := ecdh.X25519().NewPublicKey(pub)
	if err != nil {
		return nil, ErrKex
	}
	out, err := priv.(*ecdh.PrivateKey).ECDH(p)
	if err != nil {
		return nil, ErrKex
	}
	return out, nil
}

// mlkem768 is ML-KEM-768 of FIPS 203.
type mlkem768 struct{}

func (mlkem768) name() string       { return "MLKEM768" }
func (mlkem768) encapKeyLen() int   { return mlkem.EncapsulationKeySize768 }
func (mlkem768) ciphertextLen() int { return mlkem.CiphertextSize768 }

func (mlkem768) generate() (keypair, error) {
	dk, err := mlkem.GenerateKey768()
	if err != nil {
		return keypair{}, fmt.Errorf("%w: %w", ErrRandomFailed, err)
	}
	return keypair{priv: dk, pub: dk.EncapsulationKey().Bytes()}, nil
}

// encapsulate checks the key itself (join.CheckEncapsulationKey) before
// crypto/mlkem, which applies the same check, so that the rule does not
// rest on the library alone.
func (mlkem768) encapsulate(ek []byte) (ct, ss []byte, err error) {
	if !join.CheckEncapsulationKey(ek) {
		return nil, nil, ErrKex
	}
	k, err := mlkem.NewEncapsulationKey768(ek)
	if err != nil {
		return nil, nil, ErrKex
	}
	ss, ct = k.Encapsulate()
	return ct, ss, nil
}

// decapsulate has no failure of its own: a wrong ciphertext gives a
// pseudorandom secret (implicit rejection), and the AEAD that follows fails.
func (mlkem768) decapsulate(priv any, ct []byte) ([]byte, error) {
	ss, err := priv.(*mlkem.DecapsulationKey768).Decapsulate(ct)
	if err != nil {
		return nil, ErrMalformed
	}
	return ss, nil
}

// chachaPolyAEAD is ChaCha20-Poly1305, which the root module does not have.
type chachaPolyAEAD struct{}

func (chachaPolyAEAD) Name() string                      { return "ChaChaPoly" }
func (chachaPolyAEAD) TagLen() int                       { return chacha20poly1305.Overhead }
func (chachaPolyAEAD) KeyLen() int                       { return chacha20poly1305.KeySize }
func (chachaPolyAEAD) New(k []byte) (cipher.AEAD, error) { return chacha20poly1305.New(k) }
func (chachaPolyAEAD) RekeyInterval() uint64             { return join.DefaultRekeyInterval }
func (chachaPolyAEAD) MaxBytesPerKey() uint64            { return 0 }
func (chachaPolyAEAD) Nonce(n uint64) []byte {
	return binary.LittleEndian.AppendUint64(make([]byte, 4, 12), n)
}

func init() {
	join.Register(join.Suite{ID: join.SuiteChaCha, AEAD: chachaPolyAEAD{}, Hash: join.SHA256{}, NonceLen: 32})
}

var (
	suiteChaCha = suite{kex: x25519Kex{}, kem: mlkem768{}, aead: chachaPolyAEAD{}, hash: join.SHA256{}, pskLen: 32}
	suiteAES    = suite{kex: x25519Kex{}, kem: mlkem768{}, aead: join.AESGCM{}, hash: join.SHA256{}, pskLen: 32}
)
