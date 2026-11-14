package proto

import (
	"crypto/sha256"
	"fmt"

	"github.com/zmuxio/zmux-go/varint"
)

// KeyAlg is the algorithm of a long-term public key. A key on the wire is KeyAlg(varint) ‖ key. The table holds the
// X25519 static key, which a pin names, and the signature algorithms of a
// transcript signature; a new algorithm is a new row.
type KeyAlg uint64

// Key algorithms.
const (
	KeyAlgX25519  KeyAlg = 1
	KeyAlgMLDSA44 KeyAlg = 3
	mldsa44KeyLen        = 1312
	mldsa44SigLen        = 2420
	keyIDLabel           = "zmux-seal key"
	keyIDLen             = sha256.Size
)

// keyAlgInfo is one row of the KeyAlg table.
type keyAlgInfo struct {
	verify         func(key, message, sig []byte) bool
	keyLen, sigLen int // sigLen 0: the algorithm does not sign
	pq             bool
}

var keyAlgs = map[KeyAlg]keyAlgInfo{
	KeyAlgX25519:  {keyLen: 32},
	KeyAlgMLDSA44: {keyLen: mldsa44KeyLen, sigLen: mldsa44SigLen, pq: true, verify: verifyMLDSA44},
}

// KeyLen is the length of a key of a, 0 for an unknown KeyAlg.
func (a KeyAlg) KeyLen() int { return keyAlgs[a].keyLen }

// SigLen is the length of a signature of a, 0 when a does not sign.
func (a KeyAlg) SigLen() int { return keyAlgs[a].sigLen }

// PQ reports whether a resists a quantum adversary (the PQ column).
func (a KeyAlg) PQ() bool { return keyAlgs[a].pq }

// Known reports a KeyAlg of the table; a private or unassigned value is unknown.
func (a KeyAlg) Known() bool { _, ok := keyAlgs[a]; return ok }

// KeyID is the name of a key: SHA-256("zmux-seal key" ‖ varint(KeyAlg) ‖ key).
type KeyID [keyIDLen]byte

// KeyIDOf computes the KeyId of a key of a. A key of another length than the
// table's, or of an unknown KeyAlg, is ErrInvalidConfig.
func KeyIDOf(a KeyAlg, key []byte) (KeyID, error) {
	if !a.Known() || len(key) != a.KeyLen() {
		return KeyID{}, fmt.Errorf("%w: key of KeyAlg %d", ErrInvalidConfig, uint64(a))
	}
	return keyID(a, key), nil
}

func keyID(a KeyAlg, key []byte) KeyID {
	d := sha256.New()
	d.Write([]byte(keyIDLabel))
	d.Write(varint.AppendVarint(nil, uint64(a)))
	d.Write(key)
	return KeyID(d.Sum(nil))
}

// AppendKey appends KeyAlg(varint) ‖ key.
func AppendKey(dst []byte, a KeyAlg, key []byte) []byte {
	return append(varint.AppendVarint(dst, uint64(a)), key...)
}
