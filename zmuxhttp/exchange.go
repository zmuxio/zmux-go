package zmuxhttp

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"
	"math"
	"math/bits"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/internal/setting"
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

// ExchangeConfig holds what both ends of an HTTP exchange carrier must
// agree on: Exchange (the client) and ExchangeOptions (the server) each
// have the rest. A PSK is required unless AllowInsecure is set; the zero
// value of every other field selects its default. PSK (or AllowInsecure)
// and Base64 must be the same on both ends, and a piece size is the largest
// piece its sender sends and, above 1 MiB, the receiver's limit.
//
// The MACs authenticate and bind responses to requests, but nothing is
// encrypted: when TLS is terminated by a proxy or CDN, that intermediary
// can read the carrier.
//
// Under the Settings rules of the package documentation, a field of 0
// selects its default and a negative value is invalid
// (zmux.ErrInvalidConfig).
type ExchangeConfig struct {
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
	// UpPieceSize is the largest UP piece the client sends, and the server's
	// limit when above 1 MiB, the default limit of piece data a request may
	// carry. A request whose Content-Length exceeds that limit plus the
	// largest request header, after the body encoding, is not read: it goes
	// to the fallback (Options.Next) as a request that is not an exchange request.
	// The default (DefaultExchangePieceSize) keeps every request body at
	// most 1,000,000 bytes after the body encoding.
	UpPieceSize int64
	// DownPieceSize is the largest DOWN piece the server sends, and the
	// client's limit when above 1 MiB, the default limit of piece data a
	// response may carry; a larger piece fails the carrier. The default
	// (DefaultExchangePieceSize) keeps every response body at most
	// 1,000,000 bytes after the body encoding.
	DownPieceSize int64
}

// Exchange is the client of an HTTP exchange carrier to the
// ExchangeHandler at URL: its methods are the operations on it. The zero
// value of every field but URL and Config selects its default. The server's
// HoldTime (ExchangeOptions) must stay below the response timeout of every
// intermediary on the path, and the client's RequestTimeout above the
// HoldTime. A negative RequestTimeout or Deadline is no timeout, a negative
// Coalesce sends every piece at once; another negative value is invalid
// (zmux.ErrInvalidConfig).
type Exchange struct {
	// Client sends the requests (nil: http.DefaultClient); keep its
	// transport's MaxIdleConnsPerHost at least DownRequests+UpRequests so
	// that connections are reused.
	Client *http.Client
	// URL is the target template of UP and CLOSE requests. It may contain
	// {instance} and {nonce} placeholders, anywhere in its path or query.
	URL string
	// Config is what both ends agree on.
	Config ExchangeConfig
	// Header is added to every request.
	Header http.Header
	// DownURL is the target template of DOWN requests (empty: URL). It
	// may differ in scheme, host, port and path and pass other
	// intermediaries, as long as every request of a carrier reaches the
	// server process that holds it; it takes the same placeholders.
	DownURL string
	// Instance replaces {instance} in the URL. The default is a
	// random token per carrier, the same in every request of the carrier,
	// so that a load balancer hashing the target keeps a carrier on one
	// server.
	Instance string
	// NonceParam names the query parameter that carries a random value per
	// request when the URL has no {nonce} placeholder ("n"), so that no
	// cache keyed on the target matches.
	NonceParam string
	// CacheBust gives a URL without {nonce} the NonceParam query parameter
	// with a random value per request. It is on by default; zmux.SwitchOff
	// leaves such a URL unchanged, for a server or intermediary that
	// refuses unknown query parameters, when nothing on the path caches
	// POST responses: a cached response would still fail its MAC and be
	// repeated, but not get through.
	CacheBust zmux.Switch
	// DownRequests is the number of DOWN requests the client keeps
	// outstanding (2); it is lowered while the server answers BUSY.
	DownRequests int
	// UpRequests bounds the UP requests outstanding at once (2).
	UpRequests int
	// RequestTimeout bounds one attempt of one request (25 s; negative: no
	// timeout); it must exceed the server's HoldTime.
	RequestTimeout time.Duration
	// Backoff is the randomized, doubling delay before a request without a
	// result is repeated: from Min (20 ms) up to Max (2 s), each delay
	// reduced by a random fraction of up to Jitter (0.5). A zero field is
	// its default; a negative Jitter is none.
	Backoff zmux.Backoff
	// Deadline fails the carrier when a request has had no result for this
	// long (30 s; negative: no deadline); zmux then recovers on its own.
	Deadline time.Duration
	// Coalesce is how long an UP piece waits for more bytes after its first
	// byte while a request can take it. Zero is 2 ms; a negative value
	// sends every piece at once, as httputil.ReverseProxy.FlushInterval
	// does.
	Coalesce time.Duration

	// testSkipResponseMAC disables the client's response MAC check (tests
	// only: shows that the check is what rejects replayed responses).
	testSkipResponseMAC bool
}

// ExchangeOptions configure the server end of exchange carriers
// (NewExchangeHandler): the HTTP options shared by every adapter and what
// only the server reads. The zero value of every other field selects its
// default; a negative IdleTimeout or HoldTime is no timeout, a negative
// Coalesce sends every piece at once, another negative value is invalid
// (zmux.ErrInvalidConfig).
type ExchangeOptions struct {
	// Options are the endpoint's HTTP options (Next, Trusted, PairField).
	Options
	// HoldTime bounds how long the server holds a DOWN request without
	// data (20 s). It must stay below the response timeout of every
	// intermediary on the path.
	HoldTime time.Duration
	// Coalesce is how long a DOWN piece waits for more bytes after its
	// first byte while a request can take it. Zero is 2 ms; a negative
	// value sends every piece at once.
	Coalesce time.Duration
	// DownWindow is the number of downlink pieces the server assigns ahead
	// of the client's acknowledgement (32); requests beyond it get BUSY.
	DownWindow int
	// UpStoreLimit bounds the uplink bytes the server holds per carrier
	// (4 MiB; at least twice the largest UP piece accepted). Each piece
	// held out of order also counts a fixed 64 bytes, which bounds how
	// many are held.
	UpStoreLimit int64
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
		return exBodyMax/4*3 - exReqOverhead
	}
	return exBodyMax - exReqOverhead
}

// exParams is a resolved ExchangeConfig.
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
	backoffJitter          float64
	deadline               time.Duration
	hold, coalesce         time.Duration
	window                 int
	upStore                int
	idle, goneFor          time.Duration
	skew                   time.Duration
	nonceCache             int
	b64                    bool
	cacheBust              bool
	skipRespMAC            bool
}

// resolve validates the configuration of one end (cl is the client's
// settings, sv the server's; nil: the defaults) and fills in the defaults.
func (c ExchangeConfig) resolve(cl *Exchange, sv *ExchangeOptions) (exParams, error) {
	switch {
	case c.AllowInsecure && len(c.PSK) > 0:
		return exParams{}, fmt.Errorf("%w: ExchangeConfig.AllowInsecure with a PSK", zmux.ErrInvalidConfig)
	case !c.AllowInsecure && len(c.PSK) == 0:
		return exParams{}, fmt.Errorf("%w: ExchangeConfig needs a PSK or AllowInsecure", zmux.ErrInvalidConfig)
	}
	var x Exchange
	if cl != nil {
		x = *cl
	}
	var o ExchangeOptions
	if sv != nil {
		o = *sv
	}
	neg := false
	pos := func(v, d int) int {
		neg = neg || v < 0
		if v > 0 {
			return v
		}
		return d
	}
	size := func(v int64, d int) int {
		neg = neg || v < 0
		if v > 0 {
			return int(min(v, math.MaxInt32))
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
	bo, err := x.Backoff.WithDefaults(zmux.Backoff{Min: 20 * time.Millisecond, Max: 2 * time.Second, Jitter: 0.5})
	if err != nil {
		return exParams{}, fmt.Errorf("Exchange.Backoff: %w", err)
	}
	p := exParams{
		psk: c.PSK, b64: c.Base64, header: x.Header, instance: x.Instance,
		nonceParam: x.NonceParam, downURL: x.DownURL, cacheBust: x.CacheBust.On(),
		downs: pos(x.DownRequests, 2), ups: pos(x.UpRequests, 2),
		upPiece:    size(c.UpPieceSize, DefaultExchangePieceSize(c.Base64)),
		downPiece:  size(c.DownPieceSize, DefaultExchangePieceSize(c.Base64)),
		reqTimeout: setting.Timeout(x.RequestTimeout, 25*time.Second),
		backoffMin: bo.Min, backoffMax: bo.Max, backoffJitter: bo.Jitter,
		deadline: setting.Timeout(x.Deadline, 30*time.Second),
		hold:     setting.Timeout(o.HoldTime, 20*time.Second),
		window:   pos(o.DownWindow, 32), upStore: size(o.UpStoreLimit, 4<<20),
		idle: setting.Timeout(o.IdleTimeout, 60*time.Second), goneFor: dur(o.GoneMemory, 60*time.Second),
		skipRespMAC: x.testSkipResponseMAC,
		skew:        dur(o.MaxClockSkew, 60*time.Second), nonceCache: pos(o.NonceCacheSize, 1<<18),
	}
	if neg {
		return exParams{}, fmt.Errorf("%w: negative Exchange field", zmux.ErrInvalidConfig)
	}
	// One coalescing time per end: the client's for UP pieces, the
	// server's for DOWN pieces.
	co := x.Coalesce
	if sv != nil {
		co = o.Coalesce
	}
	p.coalesce = max(co, 0)
	if co == 0 {
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

	exCIDLen      = 16
	exTimeLen     = 4
	exNonceLen    = 8
	exMACLen      = 16
	exFieldsLen   = exCIDLen + 2 + 8 + 8 + exTimeLen + exNonceLen + 4 // Carrier ID through Data Length
	exAuthLen     = exFieldsLen + exMACLen                            // the authentication header
	exReqOverhead = exAuthLen + exMACLen                              // the header and the Data MAC
	exRespHdr     = exMACLen + 1
	exReasonMax   = 256
)

var (
	exLabel = []byte("zmux-http-exchange/1")
	exQ     = []byte{'Q'}
	exD     = []byte{'D'}
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

// headerMAC is the Header MAC: it covers "Q" || fields, the request from
// its Carrier ID through its Data Length.
func (k *exKey) headerMAC(fields []byte) [exMACLen]byte { return k.mac(exQ, fields, nil) }

// dataMAC is the Data MAC: it covers "D" || Header MAC || Data.
func (k *exKey) dataMAC(hmac, data []byte) [exMACLen]byte { return k.mac(exD, hmac, data) }

// responseMAC covers "R" || Carrier ID || Kind || Sequence || Nonce ||
// Flags || Data.
func (k *exKey) responseMAC(cid []byte, kind byte, seq uint64, nonce []byte, flags byte, data []byte) [exMACLen]byte {
	var pre [1 + exCIDLen + 1 + 8 + exNonceLen + 1]byte
	b := append(pre[:0], 'R')
	b = append(b, cid...)
	b = append(b, kind)
	b = binary.BigEndian.AppendUint64(b, seq)
	b = append(b, nonce...)
	b = append(b, flags)
	return k.mac(b, data, nil)
}

// appendExAuth appends the authentication header of a request with n bytes
// of Data: its fields and their Header MAC; ts is the Time field.
func appendExAuth(b []byte, k *exKey, cid *[exCIDLen]byte, kind, flags byte, seq, ack uint64, ts uint32, nonce *[exNonceLen]byte, n int) []byte {
	start := len(b)
	b = append(b, cid[:]...)
	b = append(b, kind, flags)
	b = binary.BigEndian.AppendUint64(b, seq)
	b = binary.BigEndian.AppendUint64(b, ack)
	b = binary.BigEndian.AppendUint32(b, ts)
	b = append(b, nonce[:]...)
	b = binary.BigEndian.AppendUint32(b, uint32(n))
	mac := k.headerMAC(b[start:])
	return append(b, mac[:]...)
}

// appendExRequest appends a whole binary request body.
func appendExRequest(b []byte, k *exKey, cid *[exCIDLen]byte, kind, flags byte, seq, ack uint64, ts uint32, nonce *[exNonceLen]byte, data []byte) []byte {
	start := len(b)
	b = appendExAuth(b, k, cid, kind, flags, seq, ack, ts, nonce, len(data))
	b = append(b, data...)
	mac := k.dataMAC(b[start+exFieldsLen:start+exAuthLen], data)
	return append(b, mac[:]...)
}

// exReq is a parsed request. nonce and hmac alias the authentication
// header, data the body.
type exReq struct {
	cid   [exCIDLen]byte
	kind  byte
	flags byte
	ts    uint32
	seq   uint64
	ack   uint64
	dlen  uint32
	nonce []byte
	hmac  []byte
	data  []byte
}

// parseExAuth parses an authentication header.
func parseExAuth(b *[exAuthLen]byte) (q exReq) {
	copy(q.cid[:], b[:])
	q.kind, q.flags = b[exCIDLen], b[exCIDLen+1]
	off := exCIDLen + 2
	q.seq = binary.BigEndian.Uint64(b[off:])
	q.ack = binary.BigEndian.Uint64(b[off+8:])
	off += 16
	q.ts = binary.BigEndian.Uint32(b[off:])
	off += exTimeLen
	q.nonce = b[off : off+exNonceLen]
	off += exNonceLen
	q.dlen = binary.BigEndian.Uint32(b[off:])
	q.hmac = b[exFieldsLen:exAuthLen]
	return q
}

// valid reports whether q's Kind, Flags and Data are well formed.
func (q *exReq) valid() bool {
	switch q.kind {
	case exKindUp:
		return q.flags&^exFlagFin == 0
	case exKindDown:
		return q.flags == 0 && len(q.data) == 0
	case exKindClose:
		return q.flags == 0 && q.seq == 0 && len(q.data) <= exReasonMax
	}
	return false
}

// exDataLimit is the most Data a request of kind may declare: up for UP,
// the reason bound for CLOSE, none for DOWN or an unknown kind.
func exDataLimit(kind byte, up int) int {
	switch kind {
	case exKindUp:
		return up
	case exKindClose:
		return exReasonMax
	}
	return 0
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

// readExBody reads a body of exactly cl bytes (a complete HTTP body) and
// decodes it when b64; the result is the binary body. The buffer grows
// with the bytes that arrive, never from cl alone, up to cl.
func readExBody(r io.Reader, cl int, b64 bool) (*exBuf, []byte, error) {
	raw, n, err := readExGrow(r, cl)
	if err != nil {
		return nil, nil, err
	}
	if !b64 {
		return raw, raw.b[:n], nil
	}
	dec := getExBuf(base64.StdEncoding.DecodedLen(n))
	m, err := base64.StdEncoding.Strict().Decode(dec.b, raw.b[:n])
	raw.unref()
	if err != nil {
		dec.unref()
		return nil, nil, err
	}
	return dec, dec.b[:m], nil
}

// readExGrow reads exactly n bytes from r into a pooled buffer that grows
// as they arrive; on an error nothing is returned.
func readExGrow(r io.Reader, n int) (*exBuf, int, error) {
	var b *exBuf
	got := 0
	for got < n {
		b = exGrow(b, got, min(got+exReadChunk, n), n)
		m, err := r.Read(b.b[got:min(len(b.b), n)])
		got += m
		if got == n {
			break
		}
		if err != nil {
			if b != nil {
				b.unref()
			}
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			return nil, 0, err
		}
	}
	if b == nil {
		b = getExBuf(0)
	}
	return b, n, nil
}

// exReadChunk is the least a body buffer grows by before more bytes arrive.
const exReadChunk = 4 << 10

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
