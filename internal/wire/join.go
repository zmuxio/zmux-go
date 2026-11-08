package wire

import (
	"crypto/hkdf"
	"crypto/sha256"
)

// ExporterSession is the TLS exporter label of the session secret.
const ExporterSession = "EXPORTER-zmux-session"

// SessionSecret derives the session secret.
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

// SessionBinding is the public binding value of a session:
// HKDF-Extract with SHA-256 of the two Session Nonces under a
// fixed label, 32 bytes, the same at both ends.
func SessionBinding(initNonce, respNonce []byte) []byte {
	ikm := make([]byte, 0, len(initNonce)+len(respNonce))
	ikm = append(append(ikm, initNonce...), respNonce...)
	prk, err := hkdf.Extract(sha256.New, ikm, []byte("zmux session binding"))
	if err != nil {
		panic(err)
	}
	return prk
}
