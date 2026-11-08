package join

import (
	"bytes"
	"crypto/hkdf"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash"
)

// Expand is HKDF-Expand; lengths are checked by the callers.
func Expand(newHash func() hash.Hash, prk []byte, info string, n int) []byte {
	out, err := hkdf.Expand(newHash, prk, info, n)
	if err != nil {
		panic(err)
	}
	return out
}

// BindingLen is the length of a carrier exporter and a Binding.
const BindingLen = 32

const labelBound = "zmux-seal bound"

// CombineBinding computes a Binding from the exporters of the uplink and
// downlink halves of a carrier, which are the same value on one connection.
// A half without an exporter is ErrInvalidConfig.
func CombineBinding(up, down []byte) ([]byte, error) {
	if len(up) != BindingLen || len(down) != BindingLen {
		return nil, fmt.Errorf("%w: a carrier half has no exporter", ErrInvalidConfig)
	}
	if bytes.Equal(up, down) {
		return bytes.Clone(up), nil
	}
	prk, err := hkdf.Extract(sha256.New, append(bytes.Clone(up), down...), nil)
	if err != nil {
		return nil, err
	}
	return Expand(sha256.New, prk, labelBound, BindingLen), nil
}

// Export derives keying material from an exporter secret. A length of 0 or
// above 255 hash outputs is ErrInvalidConfig, as is a label over 255 bytes, a
// context over 65535 or a secret shorter than the hash output.
func Export(h HashFunc, secret []byte, label string, context []byte, length int) ([]byte, error) {
	if len(secret) < h.Size() || len(label) > 255 || len(context) > 65535 || length < 1 || length > min(255*h.Size(), 65535) {
		return nil, fmt.Errorf("%w: exporter parameters", ErrInvalidConfig)
	}
	info := make([]byte, 0, 1+len(label)+2+len(context)+2)
	info = append(info, byte(len(label)))
	info = append(info, label...)
	info = binary.BigEndian.AppendUint16(info, uint16(len(context)))
	info = append(info, context...)
	info = binary.BigEndian.AppendUint16(info, uint16(length))
	return Expand(h.New, secret, string(info), length), nil
}
