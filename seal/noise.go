package seal

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash"
	"math"
)

// The Noise Protocol Framework (revision 34) for the two patterns the
// carrier uses, with DH 25519, cipher AESGCM and hash SHA256: the cipher
// and symmetric states of its Section 5 and the REKEY of its Section 11.3.
// Only what IKpsk1 and NXpsk0 need is here.

const (
	nameIK = "Noise_IKpsk1_25519_AESGCM_SHA256"
	nameNX = "Noise_NXpsk0_25519_AESGCM_SHA256"

	keyLen  = 32
	hashLen = sha256.Size
	tagLen  = 16 // AES-GCM tag
)

// prologue is mixed into every handshake hash; it is never sent.
var prologue = []byte("zmux sc v1")

var (
	errAuth     = errors.New("seal: message authentication failed")
	errExhaust  = errors.New("seal: nonce space exhausted")
	errShortMsg = errors.New("seal: message too short")
)

// cipherState is the CipherState of Noise Section 5.1 for AESGCM: a key, a
// 64-bit nonce n (the 96-bit GCM nonce is 32 zero bits and big-endian n)
// and the AEAD built from the key.
type cipherState struct {
	k     [keyLen]byte
	has   bool
	n     uint64
	aead  cipher.AEAD
	nonce [12]byte
}

func newGCM(k []byte) cipher.AEAD {
	b, err := aes.NewCipher(k)
	if err != nil {
		panic(err) // a 32-byte key never fails
	}
	g, err := cipher.NewGCM(b)
	if err != nil {
		panic(err)
	}
	return g
}

// init is InitializeKey.
func (c *cipherState) init(k []byte) {
	copy(c.k[:], k)
	c.has = true
	c.n = 0
	c.aead = newGCM(c.k[:])
}

func (c *cipherState) setNonce(n uint64) {
	binary.BigEndian.PutUint64(c.nonce[4:], n)
}

// seal is EncryptWithAd: it appends the ciphertext of pt to dst (pt itself
// without a key). dst and pt may overlap exactly (in place).
func (c *cipherState) seal(dst, ad, pt []byte) ([]byte, error) {
	if !c.has {
		return append(dst, pt...), nil
	}
	if c.n == math.MaxUint64 {
		return nil, errExhaust
	}
	c.setNonce(c.n)
	c.n++
	return c.aead.Seal(dst, c.nonce[:], pt, ad), nil
}

// open is DecryptWithAd; dst and ct may overlap exactly. A failure does not
// advance the nonce.
func (c *cipherState) open(dst, ad, ct []byte) ([]byte, error) {
	if !c.has {
		return append(dst, ct...), nil
	}
	if c.n == math.MaxUint64 {
		return nil, errExhaust
	}
	c.setNonce(c.n)
	pt, err := c.aead.Open(dst, c.nonce[:], ct, ad)
	if err != nil {
		return nil, errAuth
	}
	c.n++
	return pt, nil
}

// rekey is Rekey with the default REKEY of Noise Section 4.2: the key
// becomes the first 32 bytes of ENCRYPT(k, 2^64-1, empty, 32 zero bytes);
// n is unchanged.
func (c *cipherState) rekey() {
	var zeros [keyLen]byte
	var nonce [12]byte
	binary.BigEndian.PutUint64(nonce[4:], math.MaxUint64)
	out := c.aead.Seal(make([]byte, 0, keyLen+tagLen), nonce[:], zeros[:], nil)
	n := c.n
	c.init(out[:keyLen])
	c.n = n
}

// symmetricState is the SymmetricState of Noise Section 5.2.
type symmetricState struct {
	cs cipherState
	ck [hashLen]byte
	h  [hashLen]byte
}

// init is InitializeSymmetric (protocol names of at most 32 bytes).
func (s *symmetricState) init(name string) {
	*s = symmetricState{}
	if len(name) <= hashLen {
		copy(s.h[:], name)
	} else {
		s.h = sha256.Sum256([]byte(name))
	}
	s.ck = s.h
}

func (s *symmetricState) mixHash(data []byte) {
	d := sha256.New()
	d.Write(s.h[:])
	d.Write(data)
	d.Sum(s.h[:0])
}

func (s *symmetricState) mixKey(ikm []byte) {
	var tk [hashLen]byte
	noiseHKDF(s.ck[:], ikm, s.ck[:], tk[:], nil)
	s.cs.init(tk[:])
}

func (s *symmetricState) mixKeyAndHash(ikm []byte) {
	var th, tk [hashLen]byte
	noiseHKDF(s.ck[:], ikm, s.ck[:], th[:], tk[:])
	s.mixHash(th[:])
	s.cs.init(tk[:])
}

// encryptAndHash appends the ciphertext of pt to dst.
func (s *symmetricState) encryptAndHash(dst, pt []byte) ([]byte, error) {
	off := len(dst)
	out, err := s.cs.seal(dst, s.h[:], pt)
	if err != nil {
		return nil, err
	}
	s.mixHash(out[off:])
	return out, nil
}

// decryptAndHash appends the plaintext of ct to dst.
func (s *symmetricState) decryptAndHash(dst, ct []byte) ([]byte, error) {
	ad := s.h
	s.mixHash(ct) // before an in-place open overwrites ct
	out, err := s.cs.open(dst, ad[:], ct)
	if err != nil {
		s.h = ad
		return nil, err
	}
	return out, nil
}

// split is Split: the initiator's sending and receiving cipher states.
func (s *symmetricState) split() (c1, c2 cipherState) {
	var k1, k2 [hashLen]byte
	noiseHKDF(s.ck[:], nil, k1[:], k2[:], nil)
	c1.init(k1[:])
	c2.init(k2[:])
	return c1, c2
}

// noiseHKDF is the HKDF of Noise Section 4.3 with HMAC-SHA256: it writes
// two outputs, or three when o3 is not nil. The outputs may alias ck.
func noiseHKDF(ck, ikm []byte, o1, o2, o3 []byte) {
	m := hmac.New(sha256.New, ck)
	m.Write(ikm)
	var temp [hashLen]byte
	m.Sum(temp[:0])
	var t1, t2 [hashLen]byte
	expandStep(temp[:], nil, 1, t1[:])
	expandStep(temp[:], t1[:], 2, t2[:])
	if o3 != nil {
		expandStep(temp[:], t2[:], 3, o3)
	}
	copy(o1, t1[:])
	copy(o2, t2[:])
}

func expandStep(key, prev []byte, i byte, out []byte) {
	m := hmac.New(sha256.New, key)
	m.Write(prev)
	m.Write([]byte{i})
	m.Sum(out[:0])
}

// dh is DH(priv, pub) of X25519; an all-zero result (a low-order public
// key) is an error.
func dh(priv *ecdh.PrivateKey, pub []byte) ([]byte, error) {
	pk, err := ecdh.X25519().NewPublicKey(pub)
	if err != nil {
		return nil, err
	}
	return priv.ECDH(pk)
}

// newHMAC is HMAC-SHA256 under key.
func newHMAC(key []byte) hash.Hash { return hmac.New(sha256.New, key) }
