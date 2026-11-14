package proto

import (
	"crypto/rand"
	"fmt"

	"github.com/zmuxio/zmux-go/internal/join"
	"github.com/zmuxio/zmux-go/varint"
)

// Payload item types. The low bit set means the receiver must understand
// the type.
const (
	tlvPadding    = 0
	tlvCredential = 1
	tlvAppData    = 2
	tlvFirstBytes = 3
	tlvProof      = 8
)

// Credential is an opaque credential; its Type selects how it is verified.
type Credential struct {
	Data []byte
	Type uint64
}

// Payload is the content of a handshake message.
type Payload struct {
	// Credential is the sender's credential, at most one per message; only
	// messages that transmit a static key carry it.
	Credential *Credential
	// Proof is the transcript signature of the message (messages 2 and 3). When
	// read, its Message is the M it signs; when written, Message is not used.
	Proof *Proof
	// AppData is the application's handshake data (messages 2 and 3), opaque
	// here; nil or empty sends nothing.
	AppData []byte
	// FirstBytes is the start of the carrier's byte stream, handed to the
	// upper layer in place of its first record.
	FirstBytes []byte
	// Padding is the length of an ignorable padding item; 0 omits it unless
	// PadEmpty asks for an item with no value.
	Padding  int
	PadEmpty bool
}

var appendItemHeader = join.AppendItemHeader

// known returns the types p uses.
func (p Payload) known() (k payloadSet) {
	if p.Credential != nil {
		k |= knownCredential
	}
	if len(p.AppData) > 0 {
		k |= knownAppData
	}
	if p.FirstBytes != nil {
		k |= knownFirstBytes
	}
	if p.Proof != nil {
		k |= knownProof
	}
	return k
}

// appendPayload encodes p in the order of its types. Padding bytes are zero,
// or random when the payload goes without a key.
func appendPayload(b []byte, p Payload, random bool) ([]byte, error) {
	if p.Padding > 0 || p.PadEmpty {
		b = appendItemHeader(b, tlvPadding, p.Padding)
		v := make([]byte, p.Padding)
		if random {
			if _, err := rand.Read(v); err != nil {
				return nil, fmt.Errorf("%w: %w", ErrRandomFailed, err)
			}
		}
		b = append(b, v...)
	}
	if c := p.Credential; c != nil {
		b = appendItemHeader(b, tlvCredential, varint.SizeVarint(c.Type)+len(c.Data))
		b = append(varint.AppendVarint(b, c.Type), c.Data...)
	}
	if len(p.AppData) > 0 {
		b = append(appendItemHeader(b, tlvAppData, len(p.AppData)), p.AppData...)
	}
	if p.FirstBytes != nil {
		b = append(appendItemHeader(b, tlvFirstBytes, len(p.FirstBytes)), p.FirstBytes...)
	}
	if p.Proof != nil {
		v := appendProof(nil, p.Proof)
		b = append(appendItemHeader(b, tlvProof, len(v)), v...)
	}
	return b, nil
}

// PayloadLen is the plaintext length of p without padding.
func PayloadLen(p Payload) int {
	p.Padding, p.PadEmpty = 0, false
	b, _ := appendPayload(nil, p, false)
	return len(b)
}

// parsePayload validates a whole payload against the types known in its
// message. A known type is a type this message may carry; any other is
// unknown, whether or not another message defines it. The result aliases b.
func parsePayload(b []byte, known payloadSet, maxAppData int, refuseAppData bool) (Payload, error) {
	var p Payload
	err := join.ParseItems(b, func(typ uint64, val []byte) (bool, error) {
		var err error
		switch {
		case typ == tlvPadding:
			p.Padding = len(val)
		case typ == tlvCredential && known&knownCredential != 0:
			p.Credential, err = parseCredential(val)
		case typ == tlvAppData && refuseAppData:
			return true, fmt.Errorf("%w: application data in an unencrypted message 1", ErrMalformed)
		case typ == tlvAppData && known&knownAppData != 0:
			if len(val) > maxAppData {
				return true, fmt.Errorf("%w: application data of %d bytes, above %d", ErrTooLarge, len(val), maxAppData)
			}
			p.AppData = val
		case typ == tlvFirstBytes && known&knownFirstBytes != 0:
			p.FirstBytes = val
		case typ == tlvProof && known&knownProof != 0:
			p.Proof, err = parseProof(val)
		default:
			return false, nil
		}
		return true, err
	})
	if err != nil {
		return Payload{}, err
	}
	return p, nil
}

// parseCredential reads CredType(varint) ‖ Credential: one opaque credential.
// An empty value and CredType 0 are invalid; the credential may be empty.
func parseCredential(b []byte) (*Credential, error) {
	if len(b) == 0 {
		return nil, ErrTLVCredentials
	}
	typ, n, err := join.ConsumeMinimal(b)
	if err != nil {
		return nil, err
	}
	if typ == 0 {
		return nil, ErrTLVCredentials // CredType 0 is "no credential"
	}
	return &Credential{Type: typ, Data: b[n:]}, nil
}

// parseProof checks only the structure of a proof: shortest varints and Pub
// within the value. The verifier judges the rest.
func parseProof(v []byte) (*Proof, error) {
	alg, n, err := join.ConsumeMinimal(v)
	if err != nil {
		return nil, err
	}
	v = v[n:]
	l, n, err := join.ConsumeMinimal(v)
	if err != nil {
		return nil, err
	}
	if l > uint64(len(v)-n) {
		return nil, ErrTLVTruncated
	}
	return &Proof{Alg: KeyAlg(alg), Pub: v[n : n+int(l)], Sig: v[n+int(l):]}, nil
}

// appendProof appends the value of a proof item.
func appendProof(b []byte, p *Proof) []byte {
	b = AppendKey(b, p.Alg, nil)
	b = append(varint.AppendVarint(b, uint64(len(p.Pub))), p.Pub...)
	return append(b, p.Sig...)
}
