package join

import (
	"crypto/cipher"
	"math"
)

// CipherState is the CipherState of Noise revision 34 over a cipher function.
type CipherState struct {
	Fn   AEADFunc
	K    []byte
	N    uint64
	AEAD cipher.AEAD // nil until a key is set
}

// Init sets the key and resets n.
func (c *CipherState) Init(k []byte) {
	c.K = append(c.K[:0], k...)
	c.N = 0
	c.setAEAD()
}

func (c *CipherState) setAEAD() {
	a, err := c.Fn.New(c.K)
	if err != nil {
		panic(err) // the key has the length of the suite
	}
	c.AEAD = a
}

// Encrypt appends the sealed plaintext to dst. The nonce 2^64-1 is reserved.
func (c *CipherState) Encrypt(dst, ad, plaintext []byte) ([]byte, error) {
	if c.N == math.MaxUint64 {
		return nil, ErrExhausted
	}
	out := c.AEAD.Seal(dst, c.Fn.Nonce(c.N), plaintext, ad)
	c.N++
	return out, nil
}

// Decrypt appends the opened ciphertext to dst.
func (c *CipherState) Decrypt(dst, ad, ciphertext []byte) ([]byte, error) {
	if c.N == math.MaxUint64 {
		return nil, ErrExhausted
	}
	out, err := c.AEAD.Open(dst, c.Fn.Nonce(c.N), ciphertext, ad)
	if err != nil {
		return nil, ErrAuth
	}
	c.N++
	return out, nil
}

// Rekey is REKEY of Noise Section 11.3; n is unchanged.
func (c *CipherState) Rekey() {
	out := c.AEAD.Seal(nil, c.Fn.Nonce(math.MaxUint64), make([]byte, len(c.K)), nil)
	c.K = append(c.K[:0], out[:len(c.K)]...)
	c.setAEAD()
}

func (c *CipherState) stepRekey() {
	if c.N != 0 && c.N%c.Fn.RekeyInterval() == 0 {
		c.Rekey()
	}
}

// MaxMessage is what a length prefix allows: the largest handshake message
// and record ciphertext.
const MaxMessage = 65535

// Sealer protects the records of one direction, without framing. It is not
// safe for concurrent use.
type Sealer struct {
	CS  CipherState
	err error
}

// NewSealer returns a Sealer at n = 0.
func NewSealer(fn AEADFunc, k []byte) *Sealer {
	s := &Sealer{CS: CipherState{Fn: fn}}
	s.CS.Init(k)
	return s
}

// MaxPlaintext is the largest record plaintext: its ciphertext is the largest
// message.
func (s *Sealer) MaxPlaintext() int { return MaxMessage - s.CS.AEAD.Overhead() }

// Overhead is what the cipher adds to a record, before any framing.
func (s *Sealer) Overhead() int { return s.CS.AEAD.Overhead() }

// Seal appends the sealed record to dst. dst may be plaintext[:0] exactly.
func (s *Sealer) Seal(dst, plaintext []byte) ([]byte, error) {
	if s.err != nil {
		return nil, s.err
	}
	if len(plaintext) > s.MaxPlaintext() {
		return nil, ErrTooLarge // nothing was consumed
	}
	s.CS.stepRekey()
	out, err := s.CS.Encrypt(dst, nil, plaintext)
	if err != nil {
		s.err = err
		return nil, err
	}
	return out, nil
}

// Opener verifies the records of one direction. Any failure is permanent. It
// is not safe for concurrent use.
type Opener struct {
	CS  CipherState
	err error
}

// NewOpener returns an Opener at n = 0.
func NewOpener(fn AEADFunc, k []byte) *Opener {
	o := &Opener{CS: CipherState{Fn: fn}}
	o.CS.Init(k)
	return o
}

// Open appends the plaintext of a sealed record to dst. dst may be
// ciphertext[:0] exactly.
func (o *Opener) Open(dst, ciphertext []byte) ([]byte, error) {
	if o.err != nil {
		return nil, o.err
	}
	if len(ciphertext) < o.CS.AEAD.Overhead() || len(ciphertext) > MaxMessage {
		o.err = ErrAuth
		return nil, o.err
	}
	o.CS.stepRekey()
	out, err := o.CS.Decrypt(dst, nil, ciphertext)
	if err != nil {
		o.err = err
		return nil, err
	}
	return out, nil
}
