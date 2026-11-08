package proto

import (
	"slices"

	"github.com/zmuxio/zmux-go/internal/join"
)

// Profile selects one complete handshake and cipher combination. The client
// chooses; there is no negotiation.
type Profile uint64

const (
	// ProfileEstablishPSK is XXhfs+psk0: the handle names a pre-shared key,
	// and ML-KEM-768 runs beside X25519.
	ProfileEstablishPSK Profile = 1
	// ProfileEstablishPSKAES is ProfileEstablishPSK with AES-256-GCM.
	ProfileEstablishPSKAES Profile = 3
	// ProfileEstablish is XXhfs, without a pre-shared key.
	ProfileEstablish Profile = 4
	// ProfileEstablishAES is ProfileEstablish with AES-256-GCM.
	ProfileEstablishAES Profile = 5

	profileEscape = 8 // first wire byte announcing a varint extended profile
)

// HandshakeEstablish is the name of the handshake of every Establish
// profile, as a handshake shaper sees it.
const HandshakeEstablish = "establish"

// Supported reports an Establish Profile of this implementation.
func (p Profile) Supported() bool { _, ok := profiles[p]; return ok }

// PQConfidential is the PQ confidentiality column of the Profile table.
func (p Profile) PQConfidential() bool { pat, ok := profiles[p]; return ok && pat.pqConf }

// HasPSK reports whether p takes a Handle and a pre-shared key.
func (p Profile) HasPSK() bool {
	pat, ok := profiles[p]
	return ok && pat.hasPSK()
}

// Mode says who protects the records of a carrier. It is fixed by the
// handshake.
type Mode = join.Mode

const (
	// Sealed protects records with the keys of the handshake.
	Sealed = join.Sealed
	// Bound leaves records to the carrier and binds the carrier's exporter
	// into the handshake.
	Bound = join.Bound
	// Trusted leaves records to the carrier by explicit configuration of
	// both ends.
	Trusted = join.Trusted

	modeReserved = join.ModeReserved
	flagsMode    = 0x03
)

type token uint8

const (
	tokE token = iota
	tokS
	tokEE
	tokES
	tokSE
	tokSS
	tokPSK
	// tokE1 sends a fresh KEM encapsulation key; tokEKEM1 encapsulates to the
	// peer's and mixes the shared secret (Noise HFS).
	tokE1
	tokEKEM1
)

// payloadSet is the set of known payload types of one message.
type payloadSet uint8

const (
	knownCredential payloadSet = 1 << iota
	knownAppData
	knownFirstBytes
	knownProof
)

// shape is a handshake pattern with what each of its messages can carry.
type shape struct {
	name      string
	hasHandle bool
	msgs      [][]token
	known     []payloadSet
}

// pattern is one row of the profile table. The state machine is driven by
// it alone: a new profile is a new row.
type pattern struct {
	shape
	suite  suite
	pqConf bool
}

func (p *pattern) noiseName() string { return p.suite.noiseName(p.name) }

func (p *pattern) hasPSK() bool {
	for _, m := range p.msgs {
		if slices.Contains(m, tokPSK) {
			return true
		}
	}
	return false
}

// sendsStatic reports whether the role transmits its static key.
func (p *pattern) sendsStatic(initiator bool) bool {
	for i, m := range p.msgs {
		if (i%2 == 0) == initiator && hasToken(m, tokS) {
			return true
		}
	}
	return false
}

func hasToken(m []token, want token) bool {
	return slices.Contains(m, want)
}

// usesStatic reports whether the role needs a static private key.
func (p *pattern) usesStatic(initiator bool) bool {
	return p.sendsStatic(initiator)
}

var (
	knownPSK = []payloadSet{knownAppData, knownCredential | knownAppData | knownFirstBytes | knownProof, knownCredential | knownAppData | knownFirstBytes | knownProof}
	// The msg2 of XX does not authenticate the client, so it takes no first
	// bytes of a carrier.
	knownNoPSK = []payloadSet{0, knownCredential | knownAppData | knownProof, knownCredential | knownAppData | knownFirstBytes | knownProof}

	// The KEM ciphertext follows ee, so the responder's static key, its
	// credential and everything after them are under keys that hold the KEM
	// secret.
	shapeXXhfsPSK0 = shape{
		name:      "XXhfs+psk0",
		hasHandle: true,
		msgs:      [][]token{{tokPSK, tokE, tokE1}, {tokE, tokEE, tokEKEM1, tokS, tokES}, {tokS, tokSE}},
		known:     knownPSK,
	}
	shapeXXhfs = shape{
		name:  "XXhfs",
		msgs:  [][]token{{tokE, tokE1}, {tokE, tokEE, tokEKEM1, tokS, tokES}, {tokS, tokSE}},
		known: knownNoPSK,
	}
)

// profiles is the profile table. Numbers, shapes and Levels are assigned here
// only.
var profiles = map[Profile]*pattern{
	ProfileEstablishPSK:    {shape: shapeXXhfsPSK0, suite: suiteChaCha, pqConf: true},
	ProfileEstablishPSKAES: {shape: shapeXXhfsPSK0, suite: suiteAES, pqConf: true},
	ProfileEstablish:       {shape: shapeXXhfs, suite: suiteChaCha, pqConf: true},
	ProfileEstablishAES:    {shape: shapeXXhfs, suite: suiteAES, pqConf: true},
}
