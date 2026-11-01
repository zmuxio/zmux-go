package zmux

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"net"
	"net/http"
	"sync"
	"time"
)

// Prefix authentication tokens: a carrier
// starts with a token of PrefixTokenLen bytes, a random Nonce and a MAC
// over a time slot, an optional binding to the carrier's TLS handshake
// and the Nonce. Every byte of a token is random or a MAC output. A
// carrier decided on its HTTP request head (WebSocket, WebTransport)
// carries the token in a request header field instead, HeaderPrefixAuth
// by default, as base64url without padding.
const (
	PrefixTokenLen = prefixNonceLen + prefixTagLen
	prefixNonceLen = 16
	prefixTagLen   = 16
	// PrefixAuthExporterLabel is the TLS exporter label of the binding.
	PrefixAuthExporterLabel = "EXPORTER-zmux-prefix-auth"
	// HeaderPrefixAuth is the request field of the header form.
	HeaderPrefixAuth = "Zmux-Prefix-Auth"
)

// prefixHeaderLen is the length of a token in the header form.
var prefixHeaderLen = base64.RawURLEncoding.EncodedLen(PrefixTokenLen)

// Credential is what a client presents on a carrier it opens, for the
// server's Admission: the client side of an Admission. info describes
// the carrier as CarrierInfo does on the server: Remote is the server's
// address and TLS the carrier's completed handshake (nil without TLS);
// Head is set for Header. PrefixAuth is a Credential.
//
// The adapters present it: zmux.Client, Dial and DialTLS take
// Config.Credential, as the QUIC, WebSocket and WebTransport dialers of
// the sub-modules do; HTTPPipe and HTTPSplitPipe take one; PresentPipe
// presents it on any other byte-stream pipe. Methods are called
// concurrently.
type Credential interface {
	// Prefix returns the bytes a carrier starts with, before any zmux
	// record: byte streams, HTTP request bodies, a QUIC connection's
	// first stream.
	Prefix(info *CarrierInfo) ([]byte, error)
	// Header returns the request header field of a carrier decided on its
	// HTTP request head (WebSocket, WebTransport, a request without a
	// body); info.TLS is the state of the connection that carries the
	// request.
	Header(info *CarrierInfo) (name, value string, err error)
}

// PresentPipe writes c's prefix on p, a byte-stream pipe the client
// opened, before zmux writes on it; a nil c writes nothing. A TLS
// connection is first handshaken (HandshakeContext, bounded by ctx), so
// that its state can bind the prefix. A message pipe (FramingRecords) or
// one without a write half is an ErrInvalidConfig error: a carrier
// decided on its request head presents c in the request (Header).
func PresentPipe(ctx context.Context, p PipeIO, c Credential) error {
	if c == nil {
		return nil
	}
	if p.W == nil || p.Framing != FramingByteStream {
		return fmt.Errorf("%w: a credential prefix needs a byte-stream pipe with a write half", ErrInvalidConfig)
	}
	if h, ok := p.W.(interface{ HandshakeContext(context.Context) error }); ok {
		if err := h.HandshakeContext(ctx); err != nil {
			return err
		}
	}
	info := &CarrierInfo{}
	if cs, ok := carrierIsTLS(p); ok {
		info.TLS = &cs
	}
	if ra, ok := p.W.(interface{ RemoteAddr() net.Addr }); ok && ra.RemoteAddr() != nil {
		info.Peer = addrPortOf(ra.RemoteAddr())
	}
	b, err := c.Prefix(info)
	if err != nil {
		return err
	}
	if len(b) == 0 {
		return nil
	}
	_, err = p.W.Write(b)
	return err
}

var prefixLabel = []byte("zmux-prefix-auth/1")

// PrefixAuth configures prefix authentication tokens. Key, Slot, BindTLS
// and Header must be the same on both ends; Skew and CacheSize are the
// verifier's. As a Credential it presents a fresh token on every carrier.
type PrefixAuth struct {
	// Key is the pre-shared key (required).
	Key []byte
	// Slot is the length of a time slot (required, a whole number of
	// seconds).
	Slot time.Duration
	// BindTLS binds tokens to the carrier's TLS handshake through the
	// exporter (PrefixAuthBinding); tokens are then valid on that carrier
	// only.
	BindTLS bool
	// Skew is the number of slots on either side of the verifier's current
	// slot that it also accepts (0: only the current one).
	Skew int
	// CacheSize bounds the Nonces the verifier remembers while their slot
	// is acceptable (0: 65536, as an empty cache would reject every
	// token); a token that finds it full is rejected.
	CacheSize int
	// HeaderField is the request field of the header form (empty:
	// HeaderPrefixAuth), another name only where an intermediary strips
	// unknown fields.
	HeaderField string
}

// headerName is the request field of the header form.
func (a *PrefixAuth) headerName() string {
	if a.HeaderField == "" {
		return HeaderPrefixAuth
	}
	return http.CanonicalHeaderKey(a.HeaderField)
}

// Prefix returns a fresh token for the current time slot, bound to
// info.TLS with BindTLS (a carrier without TLS is then an error).
func (a *PrefixAuth) Prefix(info *CarrierInfo) ([]byte, error) {
	var binding []byte
	if a.BindTLS {
		if info == nil || info.TLS == nil {
			return nil, errors.New("zmux: PrefixAuth.BindTLS on a carrier without TLS")
		}
		b, err := PrefixAuthBinding(info.TLS)
		if err != nil {
			return nil, err
		}
		binding = b
	}
	return a.token(binding, time.Now())
}

// Header returns the header form of a fresh token: the field HeaderField
// (HeaderPrefixAuth) with the token in base64url without padding.
func (a *PrefixAuth) Header(info *CarrierInfo) (name, value string, err error) {
	tok, err := a.Prefix(info)
	if err != nil {
		return "", "", err
	}
	return a.headerName(), base64.RawURLEncoding.EncodeToString(tok), nil
}

// prefixKey derives the token key from a PrefixAuth.
func (a *PrefixAuth) prefixKey() ([]byte, int64, error) {
	if len(a.Key) == 0 || a.Slot < time.Second || a.Slot%time.Second != 0 || a.Skew < 0 || a.CacheSize < 0 {
		return nil, 0, fmt.Errorf("%w: PrefixAuth needs a Key, a whole-second Slot and non-negative bounds", ErrInvalidConfig)
	}
	m := hmac.New(sha256.New, a.Key)
	m.Write(prefixLabel)
	return m.Sum(nil), int64(a.Slot / time.Second), nil
}

// prefixTag computes a token's tag with a keyed MAC state m.
func prefixTag(m hash.Hash, slot uint64, binding, nonce []byte) (out [prefixTagLen]byte) {
	m.Reset()
	var hdr [9]byte
	binary.BigEndian.PutUint64(hdr[:8], slot)
	hdr[8] = byte(len(binding))
	m.Write(hdr[:])
	m.Write(binding)
	m.Write(nonce)
	var sum [sha256.Size]byte
	m.Sum(sum[:0])
	copy(out[:], sum[:])
	return out
}

func (a *PrefixAuth) token(binding []byte, now time.Time) ([]byte, error) {
	k, slot, err := a.prefixKey()
	if err != nil {
		return nil, err
	}
	if len(binding) > 255 {
		return nil, fmt.Errorf("%w: binding over 255 bytes", ErrInvalidConfig)
	}
	b := make([]byte, PrefixTokenLen)
	if _, err := rand.Read(b[:prefixNonceLen]); err != nil {
		return nil, err
	}
	tag := prefixTag(hmac.New(sha256.New, k), uint64(floorDiv(now.Unix(), slot)), binding, b[:prefixNonceLen])
	copy(b[prefixNonceLen:], tag[:])
	return b, nil
}

// PrefixAuthBinding returns the binding of a TLS carrier: 32 bytes of the
// exporter with PrefixAuthExporterLabel and an empty context.
func PrefixAuthBinding(cs *tls.ConnectionState) ([]byte, error) {
	return cs.ExportKeyingMaterial(PrefixAuthExporterLabel, []byte{}, 32)
}

// PrefixAuthVerifier verifies tokens and rejects each Nonce after its
// first use. It is safe for concurrent use.
type PrefixAuthVerifier struct {
	bind   bool
	header string
	slot   int64
	skew   int64
	pool   sync.Pool
	cache  *replayCache
	now    func() time.Time
}

// NewPrefixAuthVerifier returns a verifier for a; an invalid a is an
// ErrInvalidConfig error.
func NewPrefixAuthVerifier(a *PrefixAuth) (*PrefixAuthVerifier, error) {
	k, slot, err := a.prefixKey()
	if err != nil {
		return nil, err
	}
	size := a.CacheSize
	if size == 0 {
		size = 1 << 16
	}
	v := &PrefixAuthVerifier{bind: a.BindTLS, header: a.headerName(), slot: slot, skew: int64(a.Skew), now: time.Now,
		cache: newReplayCache(2*int64(a.Skew)+1, 1, size)}
	v.pool.New = func() any { return hmac.New(sha256.New, k) }
	return v, nil
}

// Verify reports whether token is valid for binding and its Nonce is new.
func (v *PrefixAuthVerifier) Verify(token, binding []byte) bool {
	if len(token) != PrefixTokenLen || len(binding) > 255 {
		return false
	}
	nonce, tag := token[:prefixNonceLen], token[prefixNonceLen:]
	cur := floorDiv(v.now().Unix(), v.slot)
	m := v.pool.Get().(hash.Hash)
	found, at := false, int64(0)
	for d := int64(0); d <= 2*v.skew && !found; d++ {
		// cur, cur-1, cur+1, cur-2, ...
		s := cur + (d+1)/2
		if d%2 == 1 {
			s = cur - (d+1)/2
		}
		want := prefixTag(m, uint64(s), binding, nonce)
		if hmac.Equal(want[:], tag) {
			found, at = true, s
		}
	}
	v.pool.Put(m)
	if !found {
		return false
	}
	var k replayKey
	copy(k[:], nonce)
	return v.cache.add(k, at, cur-v.skew)
}

// Decide is an Admission.Decide that accepts a carrier whose prefix is a
// valid token, without the token, and returns VerdictFallback otherwise.
// On a carrier decided on its request head (CarrierInfo.Head) the
// token is the request's only HeaderField field (HeaderPrefixAuth), 43
// characters of base64url without padding, and the decision has no
// prefix. With BindTLS, a carrier without TLS is not accepted. The
// Admission's Peek must be PrefixTokenLen; a carrier decided on its
// request head reads none.
func (v *PrefixAuthVerifier) Decide(_ context.Context, info *CarrierInfo, prefix []byte) Decision {
	no := Decision{Verdict: VerdictFallback}
	var binding []byte
	if v.bind {
		if info.TLS == nil {
			return no
		}
		b, err := PrefixAuthBinding(info.TLS)
		if err != nil {
			return no
		}
		binding = b
	}
	if !info.Head {
		if !v.Verify(prefix, binding) {
			return no
		}
		return Decision{Verdict: VerdictAccept, Skip: PrefixTokenLen}
	}
	if info.Request == nil {
		return no
	}
	vals := info.Request.Header.Values(v.header)
	if len(vals) != 1 || len(vals[0]) != prefixHeaderLen {
		return no
	}
	var tok [PrefixTokenLen]byte
	if n, err := base64.RawURLEncoding.Strict().Decode(tok[:], []byte(vals[0])); err != nil || n != PrefixTokenLen {
		return no
	}
	if !v.Verify(tok[:], binding) {
		return no
	}
	return Decision{Verdict: VerdictAccept}
}
