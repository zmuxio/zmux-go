package join

import (
	"crypto/ecdh"
	"crypto/hkdf"
	"crypto/mlkem"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"hash"
	"slices"

	"github.com/zmuxio/zmux-go/varint"
)

// Group identifiers: the key exchanges of a Join key share.
const (
	GroupX25519         = 1
	GroupX25519MLKEM768 = 2 // always usable: every set holds it
)

const x25519Len = 32

// Group is one row of the key exchange table of Join and of session epochs.
type Group struct {
	Name string
	// ShareI and ShareR are the share lengths of the initiator (the joiner or
	// the offering end) and of the responder.
	ShareI, ShareR int
	ID             uint64
	// Priority is the order of the table, higher preferred.
	Priority int
	kem      bool
}

var groups = map[uint64]*Group{
	GroupX25519:         {ID: GroupX25519, Name: "X25519", Priority: 1, ShareI: x25519Len, ShareR: x25519Len},
	GroupX25519MLKEM768: {ID: GroupX25519MLKEM768, Name: "X25519+MLKEM768", Priority: 2, ShareI: x25519Len + mlkem.EncapsulationKeySize768, ShareR: x25519Len + mlkem.CiphertextSize768, kem: true},
}

// LookupGroup returns a Group this implementation has; a private or
// unassigned value is unknown.
func LookupGroup(id uint64) (*Group, bool) {
	g, ok := groups[id]
	return g, ok
}

// HighestGroup is the usable Group of highest priority, the Group of an epoch
// exchange. usable always holds Group 2.
func HighestGroup(usable []uint64) *Group {
	var best *Group
	for _, id := range usable {
		if g, ok := groups[id]; ok && (best == nil || g.Priority > best.Priority) {
			best = g
		}
	}
	return best
}

// DefaultJoinGroup is the Group a joiner uses unless told otherwise: Group 1
// when it is usable, otherwise Group 2.
func DefaultJoinGroup(usable []uint64) *Group {
	if slices.Contains(usable, GroupX25519) {
		return groups[GroupX25519]
	}
	return groups[GroupX25519MLKEM768]
}

// GroupRand fixes the private values of one share, for vectors only:
// production code leaves it nil and every value is fresh from the operating
// system. X25519 is the private scalar, KEMSeed the ML-KEM seed d ‖ z of an
// initiator, and Encaps an encapsulation with fixed randomness at a responder.
type GroupRand struct {
	Encaps  func(ek *mlkem.EncapsulationKey768) (ss, ct []byte, err error)
	X25519  []byte
	KEMSeed []byte
}

// Offer is the initiator's half of one exchange. Its private values serve one
// Finish; it is not safe for concurrent use.
type Offer struct {
	g     *Group
	x     *ecdh.PrivateKey
	dk    *mlkem.DecapsulationKey768
	Share []byte
}

// NewOffer makes the initiator share with fresh private values (r nil) or
// with the fixed values of r.
func (g *Group) NewOffer(r *GroupRand) (*Offer, error) {
	o := &Offer{g: g}
	var err error
	if o.x, err = newX25519(r); err != nil {
		return nil, err
	}
	o.Share = o.x.PublicKey().Bytes()
	if g.kem {
		if r != nil && r.KEMSeed != nil {
			o.dk, err = mlkem.NewDecapsulationKey768(r.KEMSeed)
		} else {
			o.dk, err = mlkem.GenerateKey768()
		}
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrRandomFailed, err)
		}
		o.Share = append(o.Share, o.dk.EncapsulationKey().Bytes()...)
	}
	return o, nil
}

func newX25519(r *GroupRand) (*ecdh.PrivateKey, error) {
	if r != nil && r.X25519 != nil {
		return ecdh.X25519().NewPrivateKey(r.X25519)
	}
	k, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrRandomFailed, err)
	}
	return k, nil
}

// Finish takes the responder share, whose length the caller has checked, and
// returns the shared secrets in the order of the table. An all-zero X25519
// output is ErrKex. The private values are dropped whatever the outcome.
func (o *Offer) Finish(share []byte) ([][]byte, error) {
	x, dk := o.x, o.dk
	o.x, o.dk = nil, nil
	if x == nil || len(share) != o.g.ShareR {
		return nil, ErrKex
	}
	ss1, err := exchange(x, share[:x25519Len])
	if err != nil {
		return nil, err
	}
	if !o.g.kem {
		return [][]byte{ss1}, nil
	}
	// ct is checked by its length only; a wrong one decapsulates to an
	// unrelated secret (implicit rejection).
	ss2, err := dk.Decapsulate(share[x25519Len:])
	if err != nil {
		return nil, ErrKex
	}
	return [][]byte{ss1, ss2}, nil
}

// Respond takes the initiator share, whose length the caller has checked, and
// returns the responder share and the shared secrets. An all-zero X25519
// output and an encapsulation key that fails the check of FIPS 203 Section
// 7.2 are ErrKex.
func (g *Group) Respond(share []byte, r *GroupRand) (out []byte, ss [][]byte, err error) {
	if len(share) != g.ShareI {
		return nil, nil, ErrKex
	}
	var ek *mlkem.EncapsulationKey768
	if g.kem {
		// The one check of ek, before any key is made.
		if !CheckEncapsulationKey(share[x25519Len:]) {
			return nil, nil, ErrKex
		}
		if ek, err = mlkem.NewEncapsulationKey768(share[x25519Len:]); err != nil {
			return nil, nil, ErrKex
		}
	}
	x, err := newX25519(r)
	if err != nil {
		return nil, nil, err
	}
	ss1, err := exchange(x, share[:x25519Len])
	if err != nil {
		return nil, nil, err
	}
	out = x.PublicKey().Bytes()
	if !g.kem {
		return out, [][]byte{ss1}, nil
	}
	var ss2, ct []byte
	if r != nil && r.Encaps != nil {
		if ss2, ct, err = r.Encaps(ek); err != nil {
			return nil, nil, err
		}
	} else {
		ss2, ct = ek.Encapsulate()
	}
	return append(out, ct...), [][]byte{ss1, ss2}, nil
}

// exchange relies on crypto/ecdh, which refuses an all-zero output.
func exchange(x *ecdh.PrivateKey, pub []byte) ([]byte, error) {
	p, err := ecdh.X25519().NewPublicKey(pub)
	if err != nil {
		return nil, ErrKex
	}
	out, err := x.ECDH(p)
	if err != nil {
		return nil, ErrKex
	}
	return out, nil
}

// Combine is the chain of HKDF-Extract that mixes the shared secrets of a key share into a key:
// each shared secret is the input keying material of one Extract, the previous
// output its salt.
func Combine(newHash func() hash.Hash, salt []byte, ss ...[]byte) []byte {
	out := salt
	for _, s := range ss {
		var err error
		if out, err = hkdf.Extract(newHash, s, out); err != nil {
			panic(err)
		}
	}
	return out
}

// CheckEncapsulationKey is the modulus check of FIPS 203 Section 7.2 on an
// ML-KEM-768 encapsulation key: its length, and every 12-bit coefficient of
// ByteDecode12 of its first 1152 bytes below q = 3329. The key is public, so the
// check need not take constant time.
func CheckEncapsulationKey(ek []byte) bool {
	if len(ek) != mlkem.EncapsulationKeySize768 {
		return false
	}
	for i := 0; i < 3*384; i += 3 {
		a := uint16(ek[i]) | uint16(ek[i+1]&0x0f)<<8
		b := uint16(ek[i+1]>>4) | uint16(ek[i+2])<<4
		if a >= 3329 || b >= 3329 {
			return false
		}
	}
	return true
}

// NextEpochSecret is epoch_secret[e+1], the secret of the next session epoch, SHA-256:
// prk from the secret of epoch e, the target epoch next = e+1, session_crypto and
// the two frame bodies, then Combine with the shared secrets.
func NextEpochSecret(secret []byte, next uint64, sessionCrypto, offerBody, answerBody []byte, ss ...[]byte) []byte {
	return Combine(sha256.New, EpochPRK(secret, next, sessionCrypto, offerBody, answerBody), ss...)
}

// EpochPRK is prk[e+1], the key of the next session epoch before its shared secrets.
func EpochPRK(secret []byte, next uint64, sessionCrypto, offerBody, answerBody []byte) []byte {
	sc := sha256.Sum256(sessionCrypto)
	d := sha256.New()
	d.Write(offerBody)
	d.Write(answerBody)
	info := varint.AppendVarint([]byte("zmux epoch"), next)
	info = append(append(info, sc[:]...), d.Sum(nil)...)
	return Expand(sha256.New, secret, string(info), sha256.Size)
}
