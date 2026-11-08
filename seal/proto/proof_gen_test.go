package proto

import (
	"fmt"
	"slices"
	"testing"
)

// vkey is a signing key of the vectors, with its seed; it signs
// deterministically.
type vkey struct {
	*SigningKey
	seed []byte
}

func newVKey(t testing.TB, alg KeyAlg, label string) vkey {
	t.Helper()
	seed := fixed(label)
	k, err := NewSigningKey(alg, seed)
	if err != nil {
		t.Fatal(err)
	}
	k.deterministic = true
	return vkey{k, seed}
}

// proofOf signs M of message idx with k.
func proofOf(t testing.TB, k vkey, idx int, hp []byte) *Proof {
	sig, err := k.Sign(ProofMessage(idx, hp))
	if err != nil {
		t.Fatal(err)
	}
	return &Proof{Alg: k.Alg(), Pub: k.Public(), Sig: sig, Message: ProofMessage(idx, hp)}
}

func (k KeyID) slice() []byte { return slices.Clone(k[:]) }

// verifyVector is an end's verification of the peer's message: a pin (PinID,
// with PinProofID optionally), or a verdict naming ProofKey or none; with
// RequirePQAuth. PQAuthenticated is the outcome.
type verifyVector struct {
	PinID           hexb `json:"pin_id,omitempty"`
	PinProofID      hexb `json:"pin_proof_id,omitempty"`
	ProofKey        hexb `json:"proof_key,omitempty"`
	RequirePQAuth   bool `json:"require_pq_auth,omitempty"`
	PQAuthenticated bool `json:"pq_authenticated"`
}

// run checks the verification as the core does, given the peer's static key
// and the payload read.
func (v *verifyVector) run(t testing.TB, name string, static []byte, pl Payload) {
	t.Helper()
	key := v.ProofKey
	if v.PinID != nil {
		id, err := KeyIDOf(KeyAlgX25519, static)
		if err != nil || !slices.Equal(id[:], v.PinID) {
			t.Fatal(name, "not pinned")
		}
		key = v.PinProofID
	}
	var k *KeyID
	if key != nil {
		x := KeyID(key)
		k = &x
	}
	pq, err := CheckProof(k, pl.Proof, v.RequirePQAuth)
	if err != nil {
		t.Fatal(name, err)
	}
	v.PQAuthenticated = pq
}

// privateCredType is a CredType of the private range.
const privateCredType = 1<<30 + 7

// sideOpts are the options of one end of a credential handshake: its
// credential, whether it signs, and how its peer verifies it.
type sideOpts struct {
	cred  bool
	sign  bool
	pin   bool // the peer pins the static key (with the proof key when sign)
	reqPQ bool
}

// credHandshake is an Establish whose message 2 carries the server's side of
// srv and message 3 the client's side of cli (nil: an empty message 3).
func credHandshake(t testing.TB, name string, p Profile, srv sideOpts, cli *sideOpts) handshakeVector {
	v := defaults(p, Sealed)
	keys := [3]vkey{1: newVKey(t, KeyAlgMLDSA44, "server proof key"), 2: newVKey(t, KeyAlgMLDSA44, "client proof key")}
	sides := [3]*sideOpts{1: &srv, 2: cli}
	creds := [3]*Credential{1: {Type: privateCredType, Data: []byte("server credential")}, 2: {Type: privateCredType, Data: []byte("client credential")}}
	var proofs [3]*Proof
	out, read := genHandshakeFn(t, name, v, func(idx int, hp []byte) []byte {
		if idx == 0 || sides[idx] == nil {
			return nil
		}
		o := sides[idx]
		var pl Payload
		if o.cred {
			pl.Credential = creds[idx]
		}
		if o.sign {
			proofs[idx] = proofOf(t, keys[idx], idx, hp)
			pl.Proof = proofs[idx]
		}
		return testPayload(pl)
	})
	out.PQConfidential = p.PQConfidential()
	statics := [3][]byte{1: x25519(t, v.ResponderStatic).PublicKey().Bytes(), 2: x25519(t, v.InitiatorStatic).PublicKey().Bytes()}
	for idx := 1; idx <= 2; idx++ {
		o := sides[idx]
		if o == nil {
			continue
		}
		if proofs[idx] != nil {
			m := &out.Messages[idx]
			m.ProofSeed, m.Proof = keys[idx].seed, appendProof(nil, proofs[idx])
		}
		vv := &verifyVector{RequirePQAuth: o.reqPQ}
		switch {
		case o.pin:
			id := keyID(KeyAlgX25519, statics[idx])
			vv.PinID = id.slice()
			if o.sign {
				vv.PinProofID = keys[idx].ID().slice()
			}
		case o.sign:
			vv.ProofKey = keys[idx].ID().slice()
		}
		vv.run(t, name, statics[idx], read[idx])
		if idx == 1 {
			out.VerifyServer = vv
		} else {
			out.VerifyClient = vv
		}
	}
	return out
}

func genCredHandshakes(t testing.TB) []handshakeVector {
	var out []handshakeVector
	for _, p := range allProfiles {
		out = append(out, credHandshake(t, fmt.Sprintf("profile%d-sealed-opaque-credential", p), p, sideOpts{cred: true}, nil))
	}
	return append(out,
		credHandshake(t, "profile1-sealed-pin", ProfileEstablishPSK, sideOpts{pin: true}, nil),
		credHandshake(t, "profile1-sealed-pin-with-proof", ProfileEstablishPSK, sideOpts{pin: true, sign: true}, nil),
		credHandshake(t, "profile4-sealed-proof", ProfileEstablish, sideOpts{cred: true, sign: true}, nil),
		credHandshake(t, "profile3-sealed-pq-auth-required", ProfileEstablishPSKAES, sideOpts{cred: true, sign: true, reqPQ: true}, nil),
		credHandshake(t, "profile1-sealed-mutual-proof", ProfileEstablishPSK, sideOpts{cred: true, sign: true}, &sideOpts{cred: true, sign: true}),
		credHandshake(t, "profile5-sealed-mutual-proof", ProfileEstablishAES, sideOpts{cred: true, sign: true}, &sideOpts{cred: true, sign: true}))
}
