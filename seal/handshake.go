package seal

import (
	"crypto/ecdh"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"

	"github.com/zmuxio/zmux-go"
)

// Handshake message layout (SECURE-CARRIER binding):
//
//	IK msg1 = e(32) ‖ enc_s(48) ‖ Len(2) ‖ enc_payload(Len+16)
//	NX msg1 = e(32) ‖ tag(16)   ‖ Len(2) ‖ enc_payload(Len+16)
//	IK msg2 = e(32)             ‖ Len(2) ‖ enc_payload(Len+16)
//	NX msg2 = e(32) ‖ enc_s(48) ‖ Len(2) ‖ enc_payload(Len+16)
//
// Len, the payload's plaintext length, is mixed into the handshake hash
// before the payload is encrypted; the tag is mixed in after e.
const (
	dhLen    = 32
	encSLen  = dhLen + tagLen
	nxTagLen = 16
	lenLen   = 2

	ikMsg1Head = dhLen + encSLen + lenLen  // 82
	nxMsg1Head = dhLen + nxTagLen + lenLen // 50
	ikMsg2Head = dhLen + lenLen            // 34
	nxMsg2Head = dhLen + encSLen + lenLen  // 82

	maxPayload = 65535
	stampLen   = 8 // Unix milliseconds
	flagsLen   = 1

	// maxMsg1 is the longest msg1 (a client keeps the first flight within
	// it, so that msg1 fits a message as large as a record).
	maxMsg1 = 65535

	flagEarlyAccepted = 0x01
)

var errMalformed = errors.New("seal: malformed handshake")

// hsParams are what a handshake mixes in besides its tokens: the
// prologue, and whether Len (and the tag) belong to the messages. The
// carrier always uses defaultParams; plain Noise (empty prologue, no Len)
// checks the primitives against Noise's own test vectors.
type hsParams struct {
	prologue []byte
	framed   bool
}

var defaultParams = hsParams{prologue: prologue, framed: true}

// handshake is one end of an IKpsk1 or NXpsk0 handshake.
type handshake struct {
	p   hsParams
	nx  bool
	ss  symmetricState
	e   *ecdh.PrivateKey
	s   *ecdh.PrivateKey // own static: the client's (IK), the server's
	rs  [dhLen]byte      // the peer's static
	re  [dhLen]byte
	psk [keyLen]byte
	// h1 is the handshake hash after msg1: the replay key.
	h1 [hashLen]byte
}

// firstBytesOK reports whether an ephemeral public key may begin a
// carrier: its first bytes cannot begin a zmux first record with either
// framing, and are not 16 03.
func firstBytesOK(pub []byte) bool {
	if pub[0] == 0x16 && pub[1] == 0x03 {
		return false
	}
	for _, f := range [...]zmux.Framing{zmux.FramingByteStream, zmux.FramingRecords} {
		if c, _ := zmux.ClassifyFirstBytes(pub[:zmux.MaxFirstBytes], f); c != zmux.FirstBytesOther {
			return false
		}
	}
	return true
}

// genEphemeral draws an ephemeral key from r, again while its public key
// fails firstBytesOK.
func genEphemeral(r io.Reader) (*ecdh.PrivateKey, error) {
	if r == nil {
		r = rand.Reader
	}
	for {
		var b [32]byte
		if _, err := io.ReadFull(r, b[:]); err != nil {
			return nil, err
		}
		k, err := ecdh.X25519().NewPrivateKey(b[:])
		if err != nil {
			return nil, err
		}
		if firstBytesOK(k.PublicKey().Bytes()) {
			return k, nil
		}
	}
}

// compressPSK is the Noise psk of a host PSK: SHA-256 of it, or 32 zero
// bytes without one.
func compressPSK(psk []byte) (k [keyLen]byte) {
	if len(psk) == 0 {
		return k
	}
	return sha256.Sum256(psk)
}

func appendLen(out []byte, n int) []byte { return binary.BigEndian.AppendUint16(out, uint16(n)) }

// mixLen mixes the 2 Len bytes of a message into the hash.
func (h *handshake) mixLen(b []byte) {
	if h.p.framed {
		h.ss.mixHash(b)
	}
}

// writeMsg1 appends msg1 with payload to out (client). tag is the NX tag.
func (h *handshake) writeMsg1(out []byte, tag *[nxTagLen]byte, payload []byte) ([]byte, error) {
	if len(payload) > maxPayload {
		return nil, errMalformed
	}
	ePub := h.e.PublicKey().Bytes()
	if h.nx {
		h.ss.init(nameNX)
		h.ss.mixHash(h.p.prologue)
		h.ss.mixKeyAndHash(h.psk[:])
		out = append(out, ePub...)
		h.ss.mixHash(ePub)
		h.ss.mixKey(ePub)
		if h.p.framed {
			out = append(out, tag[:]...)
			h.ss.mixHash(tag[:])
		}
	} else {
		h.ss.init(nameIK)
		h.ss.mixHash(h.p.prologue)
		h.ss.mixHash(h.rs[:])
		out = append(out, ePub...)
		h.ss.mixHash(ePub)
		h.ss.mixKey(ePub)
		es, err := dh(h.e, h.rs[:])
		if err != nil {
			return nil, err
		}
		h.ss.mixKey(es)
		if out, err = h.ss.encryptAndHash(out, h.s.PublicKey().Bytes()); err != nil {
			return nil, err
		}
		ss, err := dh(h.s, h.rs[:])
		if err != nil {
			return nil, err
		}
		h.ss.mixKey(ss)
		h.ss.mixKeyAndHash(h.psk[:])
	}
	return h.sealPayload(out, payload)
}

// sealPayload appends Len (framed) and the encrypted payload.
func (h *handshake) sealPayload(out, payload []byte) ([]byte, error) {
	if h.p.framed {
		off := len(out)
		out = appendLen(out, len(payload))
		h.mixLen(out[off:])
	}
	return h.ss.encryptAndHash(out, payload)
}

// openPayload opens the rest of a message after its tokens: Len and the
// encrypted payload (framed), or the payload alone. It returns the
// plaintext and the message bytes consumed.
func (h *handshake) openPayload(rest []byte) (pt []byte, n int, err error) {
	ct := rest
	if h.p.framed {
		if len(rest) < lenLen {
			return nil, 0, errShortMsg
		}
		l := int(binary.BigEndian.Uint16(rest))
		if len(rest) < lenLen+l+tagLen {
			return nil, 0, errShortMsg
		}
		h.mixLen(rest[:lenLen])
		ct = rest[lenLen : lenLen+l+tagLen]
		n = lenLen
	}
	pt, err = h.ss.decryptAndHash(nil, ct)
	if err != nil {
		return nil, 0, err
	}
	return pt, n + len(ct), nil
}

// readIKStatic processes the tokens of an IK msg1 up to s with the server
// key s (msg holds at least e and enc_s): ok false when msg1 was not made
// for s. It sets the peer's static key.
func (h *handshake) readIKStatic(msg []byte) error {
	if len(msg) < dhLen+encSLen {
		return errShortMsg
	}
	h.ss.init(nameIK)
	h.ss.mixHash(h.p.prologue)
	h.ss.mixHash(h.s.PublicKey().Bytes())
	copy(h.re[:], msg[:dhLen])
	h.ss.mixHash(h.re[:])
	h.ss.mixKey(h.re[:])
	es, err := dh(h.s, h.re[:])
	if err != nil {
		return err
	}
	h.ss.mixKey(es)
	rs, err := h.ss.decryptAndHash(nil, msg[dhLen:dhLen+encSLen])
	if err != nil {
		return err
	}
	copy(h.rs[:], rs)
	return nil
}

// readIKRest processes the rest of an IK msg1 after readIKStatic, with
// the psk in h.psk; it returns the payload and msg1's length.
func (h *handshake) readIKRest(msg []byte) (payload []byte, n int, err error) {
	ss, err := dh(h.s, h.rs[:])
	if err != nil {
		return nil, 0, err
	}
	h.ss.mixKey(ss)
	h.ss.mixKeyAndHash(h.psk[:])
	payload, m, err := h.openPayload(msg[dhLen+encSLen:])
	if err != nil {
		return nil, 0, err
	}
	h.h1 = h.ss.h
	return payload, dhLen + encSLen + m, nil
}

// readNX processes an NX msg1 with the psk in h.psk.
func (h *handshake) readNX(msg []byte) (payload []byte, n int, err error) {
	head := dhLen
	if h.p.framed {
		head += nxTagLen
	}
	if len(msg) < head {
		return nil, 0, errShortMsg
	}
	h.ss.init(nameNX)
	h.ss.mixHash(h.p.prologue)
	h.ss.mixKeyAndHash(h.psk[:])
	copy(h.re[:], msg[:dhLen])
	h.ss.mixHash(h.re[:])
	h.ss.mixKey(h.re[:])
	if h.p.framed {
		h.ss.mixHash(msg[dhLen:head])
	}
	payload, m, err := h.openPayload(msg[head:])
	if err != nil {
		return nil, 0, err
	}
	h.h1 = h.ss.h
	return payload, head + m, nil
}

// writeMsg2 appends msg2 with payload (server) and returns the cipher
// states of the transport, the client's sending one first.
func (h *handshake) writeMsg2(out, payload []byte) ([]byte, error) {
	if len(payload) > maxPayload {
		return nil, errMalformed
	}
	ePub := h.e.PublicKey().Bytes()
	out = append(out, ePub...)
	h.ss.mixHash(ePub)
	h.ss.mixKey(ePub)
	ee, err := dh(h.e, h.re[:])
	if err != nil {
		return nil, err
	}
	h.ss.mixKey(ee)
	if h.nx {
		if out, err = h.ss.encryptAndHash(out, h.s.PublicKey().Bytes()); err != nil {
			return nil, err
		}
		es, err := dh(h.s, h.re[:])
		if err != nil {
			return nil, err
		}
		h.ss.mixKey(es)
	} else {
		se, err := dh(h.e, h.rs[:])
		if err != nil {
			return nil, err
		}
		h.ss.mixKey(se)
	}
	return h.sealPayload(out, payload)
}

// readMsg2 processes msg2 (client) and returns its payload and length.
func (h *handshake) readMsg2(msg []byte) (payload []byte, n int, err error) {
	head := dhLen
	if h.nx {
		head += encSLen
	}
	if len(msg) < head {
		return nil, 0, errShortMsg
	}
	copy(h.re[:], msg[:dhLen])
	h.ss.mixHash(h.re[:])
	h.ss.mixKey(h.re[:])
	ee, err := dh(h.e, h.re[:])
	if err != nil {
		return nil, 0, err
	}
	h.ss.mixKey(ee)
	if h.nx {
		rs, err := h.ss.decryptAndHash(nil, msg[dhLen:head])
		if err != nil {
			return nil, 0, err
		}
		copy(h.rs[:], rs)
		es, err := dh(h.e, h.rs[:])
		if err != nil {
			return nil, 0, err
		}
		h.ss.mixKey(es)
	} else {
		se, err := dh(h.s, h.re[:])
		if err != nil {
			return nil, 0, err
		}
		h.ss.mixKey(se)
	}
	payload, m, err := h.openPayload(msg[head:])
	if err != nil {
		return nil, 0, err
	}
	return payload, head + m, nil
}

// exporterSecret is the carrier's exporter secret after msg2:
// HKDF-Expand(ck, "zmux sc exporter" ‖ h, 32).
func (h *handshake) exporterSecret() [32]byte {
	info := append([]byte("zmux sc exporter"), h.ss.h[:]...)
	b, err := hkdf.Expand(sha256.New, h.ss.ck[:], string(info), 32)
	if err != nil {
		panic(err)
	}
	var s [32]byte
	copy(s[:], b)
	return s
}

var errExportLen = errors.New("seal: exporter length out of range")

// export is the exporter of a carrier with exporter secret sec:
// HKDF-Expand(HKDF-Extract(salt = sec, IKM = label), context, length).
func export(sec *[32]byte, label string, context []byte, length int) ([]byte, error) {
	if length < 0 || length > 255*hashLen {
		return nil, errExportLen
	}
	prk, err := hkdf.Extract(sha256.New, []byte(label), sec[:])
	if err != nil {
		return nil, err
	}
	return hkdf.Expand(sha256.New, prk, string(context), length)
}
