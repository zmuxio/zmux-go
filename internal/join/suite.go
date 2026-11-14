package join

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash"
	"slices"

	"github.com/zmuxio/zmux-go/internal/wire"
)

// AEADFunc is a cipher function and its record limits.
type AEADFunc interface {
	Name() string
	KeyLen() int
	TagLen() int
	New(key []byte) (cipher.AEAD, error)
	// Nonce encodes n in the nonce format of the cipher.
	Nonce(n uint64) []byte
	// RekeyInterval is the number of records under one key of a direction.
	RekeyInterval() uint64
	// MaxBytesPerKey is the data volume limit of one key; 0 means none.
	MaxBytesPerKey() uint64
}

// HashFunc is a hash function.
type HashFunc interface {
	Name() string
	Size() int
	New() hash.Hash
}

// DefaultRekeyInterval is the rekey interval of every current cipher.
const DefaultRekeyInterval = 1 << 20

// AESGCM is AES-256-GCM.
type AESGCM struct{}

func (AESGCM) Name() string { return "AESGCM" }
func (AESGCM) KeyLen() int  { return 32 }
func (AESGCM) TagLen() int  { return 16 }
func (AESGCM) New(k []byte) (cipher.AEAD, error) {
	b, err := aes.NewCipher(k)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(b)
}
func (AESGCM) RekeyInterval() uint64  { return DefaultRekeyInterval }
func (AESGCM) MaxBytesPerKey() uint64 { return 1 << 56 } // Noise Section 14
func (AESGCM) Nonce(n uint64) []byte {
	return binary.BigEndian.AppendUint64(make([]byte, 4, 12), n)
}

// SHA256 is SHA-256.
type SHA256 struct{}

func (SHA256) Name() string   { return "SHA256" }
func (SHA256) Size() int      { return sha256.Size }
func (SHA256) New() hash.Hash { return sha256.New() }

// Suite is one row of the Join suite table.
type Suite struct {
	AEAD AEADFunc
	Hash HashFunc
	ID   uint64
	// NonceLen is N, the length of R on the wire: at least 16, at most the
	// output of Hash.
	NonceLen int
}

// Suite identifiers.
const (
	SuiteAES    = 1 // mandatory, always present
	SuiteChaCha = 2 // present when the seal module is linked
)

var suites = map[uint64]*Suite{
	SuiteAES: {ID: SuiteAES, AEAD: AESGCM{}, Hash: SHA256{}, NonceLen: 32},
}

// PrivateFirst and PrivateLast bound the private values of the Profile, payload
// type, CredType, Suite, Group and KeyAlg spaces: never assigned and never
// interpreted, so each is an unknown value of its space.
const (
	PrivateFirst = 1 << 30
	PrivateLast  = 1<<31 - 1
)

// IsPrivate reports a value of the private range.
func IsPrivate(v uint64) bool { return v >= PrivateFirst && v <= PrivateLast }

// Register adds a suite. It must be called from an init function only: the
// table is read without locks afterwards.
func Register(s Suite) {
	if s.ID == 0 || IsPrivate(s.ID) || s.NonceLen < 16 || s.NonceLen > s.Hash.Size() {
		panic(fmt.Sprintf("zmux join: invalid suite %d", s.ID))
	}
	if _, dup := suites[s.ID]; dup {
		panic(fmt.Sprintf("zmux join: suite %d registered twice", s.ID))
	}
	suites[s.ID] = &s
}

// LookupSuite returns a suite this process has.
func LookupSuite(id uint64) (*Suite, bool) {
	s, ok := suites[id]
	return s, ok
}

// PreferChaCha is set at initialization where AES has no hardware support: such an
// end then does not choose Suite 1 where Suite 2 is usable.
var PreferChaCha bool

// Preferred chooses a Suite among the usable ones of a session. They always hold
// Suite 1.
func Preferred(usable []uint64) *Suite {
	if PreferChaCha && slices.Contains(usable, SuiteChaCha) {
		return suites[SuiteChaCha]
	}
	return suites[SuiteAES]
}

// LocalCrypto is what this build declares in zmux_session_crypto: every Suite
// and Group it implements, ascending.
func LocalCrypto() wire.SessionCrypto {
	c := wire.SessionCrypto{Suites: []uint64{SuiteAES}, Groups: []uint64{GroupX25519, GroupX25519MLKEM768}}
	if _, ok := suites[SuiteChaCha]; ok {
		c.Suites = append(c.Suites, SuiteChaCha)
	}
	return c
}
