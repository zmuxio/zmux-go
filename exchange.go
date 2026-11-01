package zmux

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"
	"math/bits"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zmuxio/zmux-go/varint"
)

// HTTP exchange carriers: one bidirectional carrier made of a sequence of
// bounded HTTP POST exchanges, for paths where a long-lived request does
// not work (intermediaries that buffer whole bodies, HTTP/1.0 hops,
// request duration limits, caches). The client keeps DOWN requests
// outstanding that the server answers with downlink pieces and sends the
// uplink in UP requests; every request and response is numbered,
// idempotent and authenticated, so lost, repeated, reordered and cached
// exchanges are recovered.
//
// Trust: the exchange MACs bind every response to its request and
// authenticate both ends with the PSK, but they do not encrypt. When TLS
// ends at a proxy or CDN, that intermediary can read the carrier.

// HTTPExchangeConfig configures both ends of an HTTP exchange carrier. A
// PSK is required unless AllowInsecure is set; the zero value of every
// other field selects its default. PSK (or AllowInsecure) and Base64 must
// be the same on both ends; the other fields are local to one end. The
// server's HoldTime must stay below the response timeout of every
// intermediary on the path, and the client's RequestTimeout above the
// HoldTime.
//
// The MACs authenticate and bind responses to requests, but nothing is
// encrypted: when TLS is terminated by a proxy or CDN, that intermediary
// can read the carrier.
//
// Under the Settings rules of the package documentation, a field of 0
// selects its default; a negative RequestTimeout, Deadline, HoldTime or
// IdleTimeout is no timeout, and a negative Coalesce sends every piece at
// once; another negative value is invalid (ErrInvalidConfig).
type HTTPExchangeConfig struct {
	// PSK is the pre-shared key: the MACs authenticate both ends with it.
	PSK []byte
	// AllowInsecure runs the carrier without a PSK, for tests and
	// protected networks: the MACs then only bind responses to requests
	// (they are computed with the empty key), and the carrier ID inside
	// every body is a bearer secret. It must not be combined with PSK.
	AllowInsecure bool
	// Base64 sends every body base64-encoded (standard alphabet, padded),
	// for paths that do not pass binary bodies unchanged.
	Base64 bool

	// NoCacheBust leaves a URL without {nonce} unchanged (client). By
	// default such a URL gets the NonceParam query parameter with a random
	// value per request, so that no cache keyed on the target can answer
	// one request with the response to another. Set it for a server or
	// intermediary that refuses unknown query parameters, when nothing on
	// the path caches POST responses; a cached response would still fail
	// its MAC and be repeated, but not get through.
	NoCacheBust bool
	// testSkipResponseMAC disables the client's response MAC check (tests
	// only: shows that the check is what rejects replayed responses).
	testSkipResponseMAC bool
	// Header is added to every request (client).
	Header http.Header
	// DownURL is the target template of DOWN requests (client; empty: the
	// URL given to HTTPExchangePipe, which UP and CLOSE requests use). It
	// may differ in scheme, host, port and path and pass other
	// intermediaries, as long as every request of a carrier reaches the
	// server process that holds it; it takes the same placeholders.
	DownURL string
	// Instance replaces {instance} in the URL (client). The default is a
	// random token per carrier, the same in every request of the carrier,
	// so that a load balancer hashing the target keeps a carrier on one
	// server.
	Instance string
	// NonceParam names the query parameter that carries a random value per
	// request when the URL has no {nonce} placeholder ("n"), so that no
	// cache keyed on the target matches (client).
	NonceParam string
	// DownRequests is the number of DOWN requests the client keeps
	// outstanding (2); it is lowered while the server answers BUSY.
	DownRequests int
	// UpRequests bounds the UP requests outstanding at once (2).
	UpRequests int
	// UpPieceSize is the largest UP piece the client sends, and the server's
	// limit when above 1 MiB, the default limit of piece data a request may
	// carry. A request whose Content-Length exceeds that limit plus the
	// largest request header, after the body encoding, is not read: it goes
	// to the fallback (Next) as a request that is not an exchange request.
	// The default (DefaultExchangePieceSize) keeps every request body at
	// most 1,000,000 bytes after the body encoding.
	UpPieceSize int
	// RequestTimeout bounds one attempt of one request (25 s; negative: no timeout); it must
	// exceed the server's HoldTime.
	RequestTimeout time.Duration
	// BackoffMin and BackoffMax bound the randomized, doubling delay
	// before a request without a result is repeated (20 ms, 2 s).
	BackoffMin, BackoffMax time.Duration
	// Deadline fails the carrier when a request has had no result for this
	// long (30 s); zmux then recovers on its own.
	Deadline time.Duration

	// HoldTime bounds how long the server holds a DOWN request without
	// data (20 s).
	HoldTime time.Duration
	// Coalesce is how long a piece waits for more bytes after its first
	// byte while a request can take it. Zero is 2 ms; a negative value
	// sends every piece at once, as httputil.ReverseProxy.FlushInterval
	// does. The client coalesces UP pieces the same way.
	Coalesce time.Duration
	// DownPieceSize is the largest DOWN piece the server sends, and the
	// client's limit when above 1 MiB, the default limit of piece data a
	// response may carry; a larger piece fails the carrier. The default
	// (DefaultExchangePieceSize) keeps every response body at most
	// 1,000,000 bytes after the body encoding.
	DownPieceSize int
	// DownWindow is the number of downlink pieces the server assigns ahead
	// of the client's acknowledgement (32); requests beyond it get BUSY.
	DownWindow int
	// UpStoreLimit bounds the uplink bytes the server holds per carrier
	// (4 MiB; at least twice the largest UP piece accepted). Each piece
	// held out of order also counts a fixed 64 bytes, which bounds how
	// many are held.
	UpStoreLimit int
	// IdleTimeout fails a server carrier without requests (60 s; negative:
	// no timeout).
	IdleTimeout time.Duration
	// GoneMemory is how long the server remembers an ended carrier to
	// answer its late requests with GONE (60 s).
	GoneMemory time.Duration
	// MaxClockSkew is the largest difference between a request's Time and
	// the server's clock that the server accepts (60 s).
	MaxClockSkew time.Duration
	// NonceCacheSize bounds the (Carrier ID, Nonce) pairs the server
	// remembers within the time window (262144); a request that finds it
	// full is not authenticated.
	NonceCacheSize int
	// Next is the handler of the requests the server does not
	// authenticate, and of VerdictFallback (HTTPOptions.Next; nil:
	// http.NotFound). The listener's admission decides on the request
	// that creates a carrier, on its head (CarrierInfo.Head): Peek does
	// not apply.
	Next http.Handler
	// Trusted names the intermediaries whose forwarding header field gives
	// a request's source (HTTPOptions.Trusted).
	Trusted *TrustedProxies
}

// exBodyMax bounds every exchange body of the default piece size, after
// the body encoding: below the 1 MiB request body limit that reverse
// proxies commonly apply, header and encoding included.
const exBodyMax = 1_000_000

// exLimit is the default limit of piece data in one body.
const exLimit = 1 << 20

// DefaultExchangePieceSize returns the default UP and DOWN piece size: the
// largest whose bodies, with the longest header and after the body
// encoding (base64 or not), stay at most 1,000,000 bytes.
func DefaultExchangePieceSize(base64 bool) int {
	if base64 {
		return exBodyMax/4*3 - exReqHdrMax
	}
	return exBodyMax - exReqHdrMax
}

// exParams is a resolved HTTPExchangeConfig.
type exParams struct {
	psk                    []byte
	header                 http.Header
	instance               string
	downURL                string
	nonceParam             string
	downs, ups             int
	upPiece, downPiece     int
	upLimit, downLimit     int // the largest piece accepted
	reqTimeout             time.Duration
	backoffMin, backoffMax time.Duration
	deadline               time.Duration
	hold, coalesce         time.Duration
	window                 int
	upStore                int
	idle, goneFor          time.Duration
	skew                   time.Duration
	nonceCache             int
	http                   HTTPOptions
	b64                    bool
	cacheBust              bool
	skipRespMAC            bool
}

// resolve validates c (nil: the zero config) and fills in the defaults.
func (c *HTTPExchangeConfig) resolve() (exParams, error) {
	var x HTTPExchangeConfig
	if c != nil {
		x = *c
	}
	switch {
	case x.AllowInsecure && len(x.PSK) > 0:
		return exParams{}, fmt.Errorf("%w: HTTPExchangeConfig.AllowInsecure with a PSK", ErrInvalidConfig)
	case !x.AllowInsecure && len(x.PSK) == 0:
		return exParams{}, fmt.Errorf("%w: HTTPExchangeConfig needs a PSK or AllowInsecure", ErrInvalidConfig)
	}
	neg := false
	pos := func(v, d int) int {
		neg = neg || v < 0
		if v > 0 {
			return v
		}
		return d
	}
	dur := func(v, d time.Duration) time.Duration {
		neg = neg || v < 0
		if v > 0 {
			return v
		}
		return d
	}
	p := exParams{
		psk: x.PSK, b64: x.Base64, header: x.Header, instance: x.Instance,
		nonceParam: x.NonceParam, downURL: x.DownURL, cacheBust: !x.NoCacheBust,
		downs: pos(x.DownRequests, 2), ups: pos(x.UpRequests, 2),
		upPiece:    pos(x.UpPieceSize, DefaultExchangePieceSize(x.Base64)),
		downPiece:  pos(x.DownPieceSize, DefaultExchangePieceSize(x.Base64)),
		reqTimeout: timeout(x.RequestTimeout, 25*time.Second),
		backoffMin: dur(x.BackoffMin, 20*time.Millisecond), backoffMax: dur(x.BackoffMax, 2*time.Second),
		deadline: timeout(x.Deadline, 30*time.Second),
		hold:     timeout(x.HoldTime, 20*time.Second), coalesce: max(x.Coalesce, 0),
		window: pos(x.DownWindow, 32), upStore: pos(x.UpStoreLimit, 4<<20),
		idle: timeout(x.IdleTimeout, 60*time.Second), goneFor: dur(x.GoneMemory, 60*time.Second),
		skipRespMAC: x.testSkipResponseMAC,
		skew:        dur(x.MaxClockSkew, 60*time.Second), nonceCache: pos(x.NonceCacheSize, 1<<18),
		http: HTTPOptions{Next: x.Next, Trusted: x.Trusted},
	}
	if err := p.http.Validate(); err != nil {
		return exParams{}, err
	}
	if neg {
		return exParams{}, fmt.Errorf("%w: negative HTTPExchangeConfig field", ErrInvalidConfig)
	}
	if x.Coalesce == 0 {
		p.coalesce = 2 * time.Millisecond
	}
	if p.nonceParam == "" {
		p.nonceParam = "n"
	}
	p.backoffMax = max(p.backoffMax, p.backoffMin)
	p.upLimit, p.downLimit = max(p.upPiece, exLimit), max(p.downPiece, exLimit)
	p.upStore = max(p.upStore, 2*p.upLimit)
	return p, nil
}

// ---- wire format ----

const (
	exKindUp    = 0x01
	exKindDown  = 0x02
	exKindClose = 0x03

	exFlagFin  = 0x01 // request (UP) and response (DOWN)
	exFlagBusy = 0x02
	exFlagGone = 0x04

	exCIDLen    = 16
	exTimeLen   = 4
	exNonceLen  = 8
	exMACLen    = 16
	exReqHdrMax = exCIDLen + 2 + 8 + 8 + exTimeLen + exNonceLen + exMACLen // with the longest varints
	exRespHdr   = exMACLen + 1
	exReasonMax = 256
)

var (
	exLabel = []byte("zmux-http-exchange/1")
	exQ     = []byte{'Q'}
)

var (
	errExchangeGone     = errors.New("zmux: exchange carrier gone")
	errExchangeDeadline = errors.New("zmux: exchange carrier deadline exceeded")
	errExchangeProtocol = errors.New("zmux: exchange carrier protocol violation")
	errExchangeIdle     = errors.New("zmux: exchange carrier idle")
	errExchangeClosed   = errors.New("zmux: exchange carrier closed by the peer")
)

// exKey is a carrier key K with a pool of keyed MAC states.
type exKey struct {
	pool sync.Pool
	k    [32]byte
}

func newExKey(psk, cid []byte) *exKey {
	m := hmac.New(sha256.New, psk)
	m.Write(exLabel)
	m.Write(cid)
	k := &exKey{}
	m.Sum(k.k[:0])
	k.pool.New = func() any { return hmac.New(sha256.New, k.k[:]) }
	return k
}

// mac is the first 16 bytes of HMAC-SHA256(K, a || b || c).
func (k *exKey) mac(a, b, c []byte) (out [exMACLen]byte) {
	h := k.pool.Get().(hash.Hash)
	h.Reset()
	h.Write(a)
	h.Write(b)
	h.Write(c)
	var sum [sha256.Size]byte
	h.Sum(sum[:0])
	k.pool.Put(h)
	copy(out[:], sum[:])
	return out
}

// requestMAC covers "Q" || hdr || data, where hdr is the request from its
// Carrier ID through its Nonce, as sent.
func (k *exKey) requestMAC(hdr, data []byte) [exMACLen]byte { return k.mac(exQ, hdr, data) }

// responseMAC covers "R" || Carrier ID || Kind || Sequence (as sent) ||
// Nonce || Flags || Data.
func (k *exKey) responseMAC(cid []byte, kind byte, seqRaw, nonce []byte, flags byte, data []byte) [exMACLen]byte {
	var pre [1 + exCIDLen + 1 + 8 + exNonceLen + 1]byte
	b := append(pre[:0], 'R')
	b = append(b, cid...)
	b = append(b, kind)
	b = append(b, seqRaw...)
	b = append(b, nonce...)
	b = append(b, flags)
	return k.mac(b, data, nil)
}

// appendExReqHdr appends a request from its Carrier ID through its Nonce;
// ts is the Time field.
func appendExReqHdr(b []byte, cid *[exCIDLen]byte, kind, flags byte, seq, ack uint64, ts uint32, nonce *[exNonceLen]byte) []byte {
	b = append(b, cid[:]...)
	b = append(b, kind, flags)
	b = varint.AppendVarint(b, seq)
	b = varint.AppendVarint(b, ack)
	b = binary.BigEndian.AppendUint32(b, ts)
	return append(b, nonce[:]...)
}

// exReq is a parsed request; its slices alias the body.
type exReq struct {
	cid    [exCIDLen]byte
	kind   byte
	flags  byte
	valid  bool // Kind, Flags, Data and reason are well formed
	ts     uint32
	seq    uint64
	ack    uint64
	seqRaw []byte
	nonce  []byte
	hdr    []byte // Carrier ID through Nonce
	mac    []byte
	data   []byte
}

// parseExReq parses a binary request body; ok reports a complete header,
// q.valid whether its fields are well formed.
func parseExReq(b []byte) (q exReq, ok bool) {
	if len(b) < exCIDLen+2 {
		return q, false
	}
	copy(q.cid[:], b)
	q.kind, q.flags = b[exCIDLen], b[exCIDLen+1]
	off := exCIDLen + 2
	var n int
	if q.seq, n = varint.ConsumeVarint(b[off:]); n == 0 {
		return q, false
	}
	q.seqRaw = b[off : off+n]
	off += n
	if q.ack, n = varint.ConsumeVarint(b[off:]); n == 0 {
		return q, false
	}
	off += n
	if len(b) < off+exTimeLen+exNonceLen+exMACLen {
		return q, false
	}
	q.ts = binary.BigEndian.Uint32(b[off:])
	off += exTimeLen
	q.nonce = b[off : off+exNonceLen]
	off += exNonceLen
	q.hdr = b[:off]
	q.mac = b[off : off+exMACLen]
	q.data = b[off+exMACLen:]
	switch q.kind {
	case exKindUp:
		q.valid = q.flags&^exFlagFin == 0
	case exKindDown:
		q.valid = q.flags == 0 && len(q.data) == 0
	case exKindClose:
		q.valid = q.flags == 0 && q.seq == 0 && len(q.data) <= exReasonMax
	}
	return q, true
}

// exRespValid reports whether response flags and data are well formed for
// a request of kind; a malformed response fails the carrier.
func exRespValid(kind, flags byte, data []byte) bool {
	switch {
	case flags&^(exFlagFin|exFlagBusy|exFlagGone) != 0:
		return false
	case flags&(exFlagBusy|exFlagGone) != 0:
		return (flags == exFlagBusy || flags == exFlagGone) && len(data) == 0
	case kind != exKindDown:
		return flags == 0 && len(data) == 0
	}
	return true
}

// b64Len is the length of the padded base64 encoding of n bytes.
func b64Len(n int) int { return base64.StdEncoding.EncodedLen(n) }

// readExBody reads a body of exactly cl bytes (a complete HTTP body) into a
// pooled buffer and decodes it when b64; the result is the binary body.
func readExBody(r io.Reader, cl int, b64 bool) (*exBuf, []byte, error) {
	raw := getExBuf(cl)
	if _, err := io.ReadFull(r, raw.b[:cl]); err != nil {
		raw.unref()
		return nil, nil, err
	}
	if !b64 {
		return raw, raw.b[:cl], nil
	}
	dec := getExBuf(base64.StdEncoding.DecodedLen(cl))
	n, err := base64.StdEncoding.Strict().Decode(dec.b, raw.b[:cl])
	raw.unref()
	if err != nil {
		dec.unref()
		return nil, nil, err
	}
	return dec, dec.b[:n], nil
}

// ---- buffers ----

// exHeadroom lets a buffer of class k hold 2^k bytes of piece data plus a
// request or response header.
const exHeadroom = 64

// exPools holds free buffers by size class: class k holds 2^k + exHeadroom
// bytes. The garbage collector trims them, so idle processes keep none.
var exPools [64]sync.Pool

// exBuf is a pooled, reference-counted buffer: it returns to its pool when
// the last reference is dropped.
type exBuf struct {
	b    []byte
	refs atomic.Int32
	cls  uint8
}

func exClass(n int) uint8 {
	if n <= 1<<9+exHeadroom {
		return 9
	}
	return uint8(bits.Len(uint(n - exHeadroom - 1)))
}

// getExBuf returns a buffer of at least n bytes holding one reference.
func getExBuf(n int) *exBuf {
	k := exClass(n)
	b, _ := exPools[k].Get().(*exBuf)
	if b == nil {
		b = &exBuf{b: make([]byte, 1<<k+exHeadroom), cls: k}
	}
	b.refs.Store(1)
	return b
}

func (b *exBuf) ref() { b.refs.Add(1) }

func (b *exBuf) unref() {
	switch n := b.refs.Add(-1); {
	case n == 0:
		exPools[b.cls].Put(b)
	case n < 0:
		panic("zmux: exchange buffer released twice")
	}
}

// exGrow returns a buffer that holds need bytes and keeps the first used
// bytes of b (nil: none); it at least doubles, up to limit.
func exGrow(b *exBuf, used, need, limit int) *exBuf {
	if b != nil && need <= len(b.b) {
		return b
	}
	size := max(need, 4<<10)
	if b != nil {
		size = max(size, 2*(len(b.b)-exHeadroom))
	}
	nb := getExBuf(min(size, limit))
	if b != nil {
		copy(nb.b, b.b[:used])
		b.unref()
	}
	return nb
}
