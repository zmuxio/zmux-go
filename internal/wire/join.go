package wire

import (
	"crypto/hkdf"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
)

// TLS exporter labels (MULTIPATH.md 10.2, 10.5).
const (
	ExporterSession = "EXPORTER-zmux-session"
	ExporterPipe    = "EXPORTER-zmux-pipe"
)

// SessionSecret derives the session secret (MULTIPATH.md 10.3).
func SessionSecret(initNonce, respNonce []byte, exporter, psk []byte) []byte {
	salt := make([]byte, 0, 12+32)
	salt = append(salt, "zmux session"...)
	salt = append(salt, initNonce...)
	salt = append(salt, respNonce...)
	ikm := make([]byte, 0, len(exporter)+len(psk))
	ikm = append(append(ikm, exporter...), psk...)
	prk, err := hkdf.Extract(sha256.New, ikm, salt)
	if err != nil {
		panic(err)
	}
	return prk
}

// JoinKeys derives join_key_i and join_key_r (MULTIPATH.md 10.4).
func JoinKeys(secret []byte) (ki, kr []byte) {
	var err error
	if ki, err = hkdf.Expand(sha256.New, secret, "zmux join i", 32); err != nil {
		panic(err)
	}
	if kr, err = hkdf.Expand(sha256.New, secret, "zmux join r", 32); err != nil {
		panic(err)
	}
	return ki, kr
}

// JoinToken computes the 16-byte token over the MAC input and binding.
func JoinToken(key, macInput, binding []byte) []byte {
	m := hmac.New(sha256.New, key)
	m.Write(macInput)
	m.Write(binding)
	return m.Sum(nil)[:16]
}

// VerifyJoinToken compares in constant time.
func VerifyJoinToken(key, macInput, binding, token []byte) bool {
	return subtle.ConstantTimeCompare(JoinToken(key, macInput, binding), token) == 1
}
