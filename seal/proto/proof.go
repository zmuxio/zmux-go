package proto

import (
	"crypto/mldsa"
	"errors"
	"fmt"
)

// ErrCredentialsFailed is the failure of the peer's verification: the
// verifier failed it, a proof its verdict requires failed, or PQ
// authentication is required and does not hold.
var ErrCredentialsFailed = errors.New("seal/proto: credentials failed")

// Reasons of a failed proof check, each ErrCredentialsFailed.
var (
	ErrProof          = fmt.Errorf("%w: proof missing or does not verify", ErrCredentialsFailed)
	ErrPQAuthRequired = fmt.Errorf("%w: post-quantum authentication required", ErrCredentialsFailed)
)

// CheckPinPQAuth is the rule on an end that requires PQ authentication: a pin
// of an X25519 key alone, without a proof key, is ErrInvalidConfig.
func CheckPinPQAuth(requirePQ, hasProofKey bool) error {
	if requirePQ && !hasProofKey {
		return fmt.Errorf("%w: PQ authentication required of a pin without a proof key", ErrInvalidConfig)
	}
	return nil
}

// CheckProof is the core's check of a message's proof against the verdict's
// proof key, nil when it names none: the proof is present, its key has that
// KeyId and the KeyAlg of a proof key, and it verifies over M. pq reports PQ
// authentication; with requirePQ its absence fails too.
func CheckProof(proofKey *KeyID, p *Proof, requirePQ bool) (pq bool, err error) {
	if proofKey != nil {
		if !VerifyProof(p, *proofKey) {
			return false, ErrProof
		}
		pq = p.Alg.PQ()
	}
	if requirePQ && !pq {
		return false, ErrPQAuthRequired
	}
	return pq, nil
}

// Proof is the transcript signature of a handshake message: Sig by the key Pub of KeyAlg Alg over Message, which is M.
type Proof struct {
	Pub     []byte
	Sig     []byte
	Message []byte
	Alg     KeyAlg
}

// VerifyProof checks a proof against the proof key named by want: the KeyId of
// its key is want, and the signature verifies over M.
func VerifyProof(p *Proof, want KeyID) bool {
	if p == nil {
		return false
	}
	id, err := KeyIDOf(p.Alg, p.Pub)
	return err == nil && id == want && VerifySignature(p.Alg, p.Pub, p.Message, p.Sig)
}

// VerifySignature verifies sig over message with a key of KeyAlg alg.
func VerifySignature(alg KeyAlg, key, message, sig []byte) bool {
	info, ok := keyAlgs[alg]
	if !ok || info.verify == nil || len(key) != info.keyLen || len(sig) != info.sigLen {
		return false
	}
	return info.verify(key, message, sig)
}

func verifyMLDSA44(pub, message, sig []byte) bool {
	k, err := mldsa.NewPublicKey(mldsa.MLDSA44(), pub)
	if err != nil {
		return false
	}
	return mldsa.Verify(k, message, sig, nil) == nil
}

// SigningKey is a key of a transcript signature.
type SigningKey struct {
	ml  *mldsa.PrivateKey
	pub []byte
	alg KeyAlg
	// deterministic signs with rnd of zero bytes, for vectors only.
	deterministic bool
}

// NewSigningKey makes the key of alg from its seed: 32 bytes for ML-DSA-44.
func NewSigningKey(alg KeyAlg, seed []byte) (*SigningKey, error) {
	if alg != KeyAlgMLDSA44 {
		return nil, fmt.Errorf("%w: KeyAlg %d does not sign", ErrInvalidConfig, alg)
	}
	if len(seed) != mldsa.PrivateKeySize {
		return nil, fmt.Errorf("%w: a seed of %d bytes for KeyAlg %d", ErrInvalidConfig, mldsa.PrivateKeySize, alg)
	}
	ml, err := mldsa.NewPrivateKey(mldsa.MLDSA44(), seed)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidConfig, err)
	}
	return &SigningKey{alg: alg, ml: ml, pub: ml.PublicKey().Bytes()}, nil
}

// Alg is the key's KeyAlg.
func (k *SigningKey) Alg() KeyAlg { return k.alg }

// Public is the canonical encoding of the public key.
func (k *SigningKey) Public() []byte { return k.pub }

// ID is the KeyId of the key.
func (k *SigningKey) ID() KeyID { return keyID(k.alg, k.pub) }

// Sign signs message, hedged.
func (k *SigningKey) Sign(message []byte) ([]byte, error) {
	var s []byte
	var err error
	if k.deterministic {
		s, err = k.ml.SignDeterministic(message, nil)
	} else {
		s, err = k.ml.Sign(nil, message, nil)
	}
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrRandomFailed, err)
	}
	return s, nil
}
