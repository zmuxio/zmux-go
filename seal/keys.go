package seal

import (
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"crypto/sha512"
	"errors"
	"math/big"
)

// PrivateKey is an X25519 private key (RFC 7748), clamped.
type PrivateKey [32]byte

// PublicKey is an X25519 public key (RFC 7748, a little-endian u-coordinate).
type PublicKey [32]byte

// GenerateKey returns a new private key from a secure random source.
func GenerateKey() (PrivateKey, error) {
	var k PrivateKey
	if _, err := rand.Read(k[:]); err != nil {
		return PrivateKey{}, err
	}
	clamp(&k)
	return k, nil
}

func clamp(k *PrivateKey) {
	k[0] &= 248
	k[31] &= 127
	k[31] |= 64
}

// Public returns the public key of k.
func (k PrivateKey) Public() PublicKey {
	var p PublicKey
	copy(p[:], k.ecdh().PublicKey().Bytes())
	return p
}

func (k PrivateKey) ecdh() *ecdh.PrivateKey {
	pk, err := ecdh.X25519().NewPrivateKey(k[:])
	if err != nil {
		panic(err) // every 32-byte string is an X25519 private key
	}
	return pk
}

// KeyFromSecret derives a private key from secret and label:
// HKDF-SHA256 with salt "zmux sc static", secret as the input keying
// material and label as the info, 32 bytes, clamped. Both ends that know
// secret compute the same key, such as a key for a client that has not
// registered a key of its own yet. secret should hold at least 128 bits
// of uniform randomness.
func KeyFromSecret(secret []byte, label string) PrivateKey {
	b, err := hkdf.Key(sha256.New, secret, []byte("zmux sc static"), label, 32)
	if err != nil {
		panic(err) // 32 bytes of SHA-256 never fail
	}
	var k PrivateKey
	copy(k[:], b)
	clamp(&k)
	return k
}

// X25519FromEd25519Private returns the X25519 private key of an Ed25519
// private key: the first 32 bytes of SHA-512 of its seed, clamped (RFC
// 8032 Section 5.1.5). Its public key is X25519FromEd25519Public of the
// Ed25519 public key.
func X25519FromEd25519Private(k ed25519.PrivateKey) PrivateKey {
	h := sha512.Sum512(k.Seed())
	var x PrivateKey
	copy(x[:], h[:32])
	clamp(&x)
	return x
}

var (
	curveP = func() *big.Int {
		p := new(big.Int).Lsh(big.NewInt(1), 255)
		return p.Sub(p, big.NewInt(19))
	}()
	// curveD is the d of edwards25519: -121665/121666 mod p.
	curveD = func() *big.Int {
		n := new(big.Int).Neg(big.NewInt(121665))
		d := new(big.Int).ModInverse(big.NewInt(121666), curveP)
		return n.Mul(n, d).Mod(n, curveP)
	}()
)

// ErrInvalidKey is returned for an Ed25519 public key that is not a point
// of the curve or maps to no X25519 key.
var ErrInvalidKey = errors.New("seal: invalid public key")

// X25519FromEd25519Public returns the X25519 public key of an Ed25519
// public key, by the birational map u = (1+y)/(1-y) of RFC 7748 Section
// 4.1. It is meant for registration and lookups, not for a hot path.
func X25519FromEd25519Public(k ed25519.PublicKey) (PublicKey, error) {
	if len(k) != ed25519.PublicKeySize {
		return PublicKey{}, ErrInvalidKey
	}
	var le [32]byte
	copy(le[:], k)
	le[31] &= 0x7f // the sign of x
	be := make([]byte, 32)
	for i := range le {
		be[i] = le[31-i]
	}
	p := curveP
	y := new(big.Int).SetBytes(be)
	if y.Cmp(p) >= 0 {
		return PublicKey{}, ErrInvalidKey
	}
	// On the curve: x^2 = (y^2 - 1) / (d y^2 + 1) has a root.
	y2 := new(big.Int).Mul(y, y)
	y2.Mod(y2, p)
	num := new(big.Int).Sub(y2, big.NewInt(1))
	num.Mod(num, p)
	den := new(big.Int).Mul(curveD, y2)
	den.Add(den, big.NewInt(1)).Mod(den, p)
	inv := new(big.Int).ModInverse(den, p)
	if inv == nil {
		return PublicKey{}, ErrInvalidKey
	}
	x2 := num.Mul(num, inv).Mod(num, p)
	if x2.Sign() != 0 {
		e := new(big.Int).Rsh(new(big.Int).Sub(p, big.NewInt(1)), 1)
		if new(big.Int).Exp(x2, e, p).Cmp(big.NewInt(1)) != 0 {
			return PublicKey{}, ErrInvalidKey
		}
	}
	one := big.NewInt(1)
	d := new(big.Int).Sub(one, y)
	d.Mod(d, p)
	di := new(big.Int).ModInverse(d, p)
	if di == nil { // y = 1: the identity
		return PublicKey{}, ErrInvalidKey
	}
	u := new(big.Int).Add(one, y)
	u.Mul(u, di).Mod(u, p)
	var out PublicKey
	ub := u.FillBytes(make([]byte, 32))
	for i := range ub {
		out[i] = ub[31-i]
	}
	return out, nil
}
