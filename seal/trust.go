package seal

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"

	"github.com/zmuxio/zmux-go/seal/proto"
)

// Credential is an opaque credential: Type selects the format, Data is the
// credential. A message carries at most one.
type Credential = proto.Credential

// Proof is a transcript signature with the message M it signs.
type Proof = proto.Proof

// Principal is whom a verdict names. It does not change when the node's key does.
type Principal struct {
	Type  uint16
	Bytes []byte
}

// PrincipalNodeKey is a node named by its pinned static key's KeyId.
const PrincipalNodeKey uint16 = 1

// VerifyInput is everything an Anchor judges a peer by.
type VerifyInput struct {
	// Role is the role expected of the peer: RoleServer or RoleClient.
	Role Role
	// Profile is the handshake's.
	Profile proto.Profile
	// PeerStatic is the peer's static key.
	PeerStatic []byte
	// PSKIdentity is the Handle of the handshake's pre-shared key, nil in a
	// Profile without one.
	PSKIdentity []byte
	// Credential is the peer's credential, nil when its message had none.
	Credential *Credential
	// PeerAppData is the application data the peer has sent so far, in
	// message order; each piece states its message and protection.
	PeerAppData []AppData
	// LocalContext is the host's label of the configuration that runs the
	// handshake (ClientConfig.LocalContext, ServerConfig.LocalContext).
	LocalContext any
}

// AppData is application handshake data of one message: opaque bytes, with
// the message that carried them and what that message protects.
type AppData struct {
	// Message is 1, 2 or 3.
	Message int
	Data    []byte
	// ForwardSecret: the data is encrypted under keys with forward secrecy.
	// Message 1 (Profiles 1 and 3 only) is under the pre-shared key alone: an
	// eavesdropper who later learns the key reads it, and it can be replayed
	// like 0-RTT data.
	ForwardSecret bool
	// Authenticated: the sender passed this end's verification, so the data is
	// that of the peer the verdict names. It is false in every VerifyInput,
	// since the verification is still running, and true in Peer.AppData for
	// the data of messages 2 and 3 once the peer passed. Message 3 data is not
	// to be acted on with side effects before the handshake completes: the
	// server has not yet admitted the client.
	Authenticated bool
}

// appData is the AppData of message msg, nil without data.
func appData(msg int, b []byte) []AppData {
	if len(b) == 0 {
		return nil
	}
	return []AppData{{Message: msg, Data: bytes.Clone(b), ForwardSecret: msg > 1}}
}

// authenticated marks the data of messages 2 and 3 as the verified peer's;
// message 1 data authenticates no more than knowledge of the pre-shared key.
func authenticated(d []AppData) {
	for i := range d {
		d[i].Authenticated = d[i].Message > 1
	}
}

// AppDataRequest is what an AppDataSource knows when asked for the data of a
// message: Message (1, 2 or 3) and, for messages 2 and 3, Peer, what is known
// of the peer by then (for message 3, the VerifyInput the Anchor judged).
// Verdict is the Anchor's verdict of the server, for message 3 only, so that
// a client chooses its data by who the server is; it is nil for messages 1
// and 2, and for message 3 under AllowPSKOnly, where no Anchor judges.
type AppDataRequest struct {
	Message int
	Peer    *VerifyInput
	Verdict *Verdict
}

// AppDataSource supplies the application data of the messages an end sends: a
// client's messages 1 (Profiles 1 and 3) and 3, a server's message 2. It runs
// under the handshake's context, and an error ends the handshake; nil or empty
// data sends nothing.
type AppDataSource func(ctx context.Context, r AppDataRequest) ([]byte, error)

// Role is the role of a peer.
type Role uint8

// Roles of a peer.
const (
	RoleServer Role = 1
	RoleClient Role = 2
)

// Verdict is the judgement of a peer that passed.
type Verdict struct {
	Principal Principal
	// ProofKey, when set, is the KeyId of the key whose transcript signature
	// the message must carry: the handshake fails without a signature that
	// verifies with it.
	ProofKey *proto.KeyID
	// Value is the Anchor's own result, for the host.
	Value any
}

// Anchor judges the peer in one call. It does not see the transcript: the
// handshake checks the transcript signature against Verdict.ProofKey. An error
// that wraps ErrVerifyUnavailable is temporary; any other error, or a panic,
// rejects the peer.
type Anchor interface {
	Verify(ctx context.Context, in VerifyInput) (Verdict, error)
}

// Reasons of a rejected peer; they stay local.
var (
	ErrNotPinned      = errors.New("seal: key not pinned")
	ErrProof          = proto.ErrProof
	ErrPQAuthRequired = proto.ErrPQAuthRequired
)

// ErrCredentialsFailed is the error class of a peer that failed verification.
var ErrCredentialsFailed = proto.ErrCredentialsFailed

// ErrVerifyRejected is a peer the Anchor or the transcript signature
// rejected; it matches ErrCredentialsFailed. An Anchor may return it, or any
// other error but ErrVerifyUnavailable, to reject. On a server a rejection
// goes to the reject policy.
var ErrVerifyRejected = fmt.Errorf("%w: peer rejected", ErrCredentialsFailed)

// ErrVerifyUnavailable is an Anchor that could not judge, such as a
// verification service that did not answer: it wraps its cause as
// fmt.Errorf("%w: ...", ErrVerifyUnavailable). The carrier is closed, and a
// server does not count it as a refusal.
var ErrVerifyUnavailable = errors.New("seal: verifier unavailable")

// rejected is err as a rejection: it matches ErrVerifyRejected.
func rejected(err error) error {
	if errors.Is(err, ErrVerifyRejected) {
		return err
	}
	return fmt.Errorf("%w: %w", ErrVerifyRejected, err)
}

// KeyID is the KeyId of a key, the name a pin and a verdict use for it.
func KeyID(alg proto.KeyAlg, key []byte) (proto.KeyID, error) { return proto.KeyIDOf(alg, key) }

// NodeKeyID is the KeyId of an X25519 node key, for PinAnchor.
func NodeKeyID(pub []byte) proto.KeyID {
	id, err := proto.KeyIDOf(proto.KeyAlgX25519, pub)
	if err != nil {
		return proto.KeyID{}
	}
	return id
}

// Pin is the anchor of PinAnchor.
type Pin struct {
	node  proto.KeyID
	proof *proto.KeyID
}

// PinAnchor trusts the one X25519 key with this KeyId, the key itself with no
// credential; a credential the peer sends is not inspected. With proof, the
// KeyId of a proof key, the peer must also sign the transcript with that key.
func PinAnchor(node proto.KeyID, proof ...proto.KeyID) *Pin {
	a := &Pin{node: node}
	if len(proof) > 0 {
		a.proof = &proof[0]
	}
	return a
}

// Verify judges the peer by its key.
func (a *Pin) Verify(_ context.Context, in VerifyInput) (Verdict, error) {
	id, err := proto.KeyIDOf(proto.KeyAlgX25519, in.PeerStatic)
	if err != nil || id != a.node {
		return Verdict{}, ErrNotPinned
	}
	return Verdict{Principal: Principal{Type: PrincipalNodeKey, Bytes: id[:]}, ProofKey: a.proof}, nil
}

// checkPQAuth is the configuration rule of RequirePQAuth: a pin without a
// proof key can never satisfy it.
func checkPQAuth(require bool, a Anchor) error {
	if p, ok := a.(*Pin); ok {
		return proto.CheckPinPQAuth(require, p.proof != nil)
	}
	return nil
}

// judge runs an Anchor and checks the transcript signature its verdict asks
// for. It returns whether the peer is authenticated by a post-quantum
// signature.
func judge(ctx context.Context, a Anchor, in VerifyInput, proof *proto.Proof, requirePQ bool) (v Verdict, pq bool, err error) {
	func() {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("the Anchor's verifier panicked (%T)", r) // not the value: it may hold secrets
			}
		}()
		v, err = a.Verify(ctx, in)
	}()
	if err != nil {
		if ctx.Err() != nil {
			return Verdict{}, false, ctx.Err()
		}
		if errors.Is(err, ErrVerifyUnavailable) {
			return Verdict{}, false, err
		}
		return Verdict{}, false, rejected(err)
	}
	if pq, err = proto.CheckProof(v.ProofKey, proof, requirePQ); err != nil {
		return Verdict{}, false, rejected(err)
	}
	return v, pq, nil
}

// Signer makes the transcript signature of a handshake once per handshake; it may live in an HSM or another process.
// Signatures are hedged.
type Signer interface {
	// ProofKey is the KeyAlg and key of the signing key.
	ProofKey() (proto.KeyAlg, []byte)
	// SignProof signs m.
	SignProof(ctx context.Context, m []byte) ([]byte, error)
}

// KeySigner is a Signer of a key in this process.
func KeySigner(k *proto.SigningKey) Signer { return keySigner{k} }

type keySigner struct{ k *proto.SigningKey }

func (s keySigner) ProofKey() (proto.KeyAlg, []byte) { return s.k.Alg(), s.k.Public() }

func (s keySigner) SignProof(_ context.Context, m []byte) ([]byte, error) { return s.k.Sign(m) }

// GenerateSigningKey makes a new signing key of alg (ML-DSA-44) from the
// operating system's entropy.
func GenerateSigningKey(alg proto.KeyAlg) (*proto.SigningKey, error) {
	seed := make([]byte, 32)
	if _, err := rand.Read(seed); err != nil {
		return nil, fmt.Errorf("%w: %w", proto.ErrRandomFailed, err)
	}
	defer clear(seed)
	return proto.NewSigningKey(alg, seed)
}

// DeriveHandlePSK derives a Handle and a pre-shared key from a uniformly random secret
// of at least 16 bytes (32 by default), within a domain; salt is an optional
// per-deployment value. It is not for passwords: a Handle lets anyone test guesses.
func DeriveHandlePSK(secret, domain, salt []byte) (handle, psk []byte, err error) {
	return proto.DeriveHandlePSK(secret, domain, salt)
}
