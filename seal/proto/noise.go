package proto

import (
	"crypto/hmac"

	"github.com/zmuxio/zmux-go/internal/join"
)

// The SymmetricState of the Noise Protocol Framework (revision 34), over the
// functions of a suite.

type cipherState = join.CipherState

type symmetricState struct {
	suite suite
	ck    []byte
	h     []byte
	cs    cipherState
}

func (s *symmetricState) init(name string, su suite) {
	s.suite = su
	s.cs.Fn = su.aead
	n := su.hash.Size()
	if len(name) <= n {
		s.h = make([]byte, n)
		copy(s.h, name) // zero padded
	} else {
		d := su.hash.New()
		d.Write([]byte(name))
		s.h = d.Sum(nil)
	}
	s.ck = append([]byte{}, s.h...)
}

func (s *symmetricState) mixHash(data []byte) {
	d := s.suite.hash.New()
	d.Write(s.h)
	d.Write(data)
	s.h = d.Sum(s.h[:0])
}

// hkdf3 is HKDF of Noise Section 4.3 with three outputs.
func (s *symmetricState) hkdf3(ikm []byte) (a, b, c []byte) {
	mac := hmac.New(s.suite.hash.New, s.ck)
	mac.Write(ikm)
	temp := mac.Sum(nil)
	out := hmac.New(s.suite.hash.New, temp)
	out.Write([]byte{1})
	a = out.Sum(nil)
	out.Reset()
	out.Write(a)
	out.Write([]byte{2})
	b = out.Sum(nil)
	out.Reset()
	out.Write(b)
	out.Write([]byte{3})
	c = out.Sum(nil)
	return a, b, c
}

func (s *symmetricState) mixKey(ikm []byte) {
	ck, k, _ := s.hkdf3(ikm)
	s.ck = ck
	s.cs.Init(k[:s.suite.aead.KeyLen()])
}

func (s *symmetricState) mixKeyAndHash(ikm []byte) {
	ck, th, k := s.hkdf3(ikm)
	s.ck = ck
	s.mixHash(th)
	s.cs.Init(k[:s.suite.aead.KeyLen()])
}

func (s *symmetricState) encryptAndHash(dst, plaintext []byte) ([]byte, error) {
	start := len(dst)
	if s.cs.AEAD == nil {
		dst = append(dst, plaintext...)
	} else {
		var err error
		if dst, err = s.cs.Encrypt(dst, s.h, plaintext); err != nil {
			return nil, err
		}
	}
	s.mixHash(dst[start:])
	return dst, nil
}

func (s *symmetricState) decryptAndHash(ciphertext []byte) ([]byte, error) {
	var out []byte
	if s.cs.AEAD == nil {
		out = append([]byte{}, ciphertext...)
	} else {
		var err error
		if out, err = s.cs.Decrypt(nil, s.h, ciphertext); err != nil {
			return nil, err
		}
	}
	s.mixHash(ciphertext)
	return out, nil
}

func (s *symmetricState) split() (c1, c2 []byte) {
	a, b, _ := s.hkdf3(nil)
	n := s.suite.aead.KeyLen()
	return a[:n], b[:n]
}
