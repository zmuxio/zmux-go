package zmux

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"hash"
	"io"
	"math/bits"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zmuxio/zmux-go/internal/wire"
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
// Trust: the exchange MACs bind every response to its request and, with a
// PSK, authenticate both ends, but they do not encrypt. When TLS ends at a
// proxy or CDN, that intermediary can read the carrier.

// HTTPExchangeConfig configures both ends of an HTTP exchange carrier. The
// zero value of every field selects its default. PSK and Base64 must be the
// same on both ends; the other fields are local to one end. The server's
// HoldTime must stay below the response timeout of every intermediary on
// the path, and the client's RequestTimeout above the HoldTime.
//
// The MACs authenticate (with a PSK) and bind responses to requests, but
// nothing is encrypted: when TLS is terminated by a proxy or CDN, that
// intermediary can read the carrier.
type HTTPExchangeConfig struct {
	// PSK is the pre-shared key. With a PSK the MACs authenticate both
	// ends; without one they only bind responses to requests, and the
	// carrier ID inside every body is a bearer secret.
	PSK []byte
	// Base64 sends every body base64-encoded (standard alphabet, padded),
	// for paths that do not pass binary bodies unchanged.
	Base64 bool

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
	// NoCacheBust leaves a URL without {nonce} unchanged (client).
	NoCacheBust bool
	// DownRequests is the number of DOWN requests the client keeps
	// outstanding (2); it is lowered while the server answers BUSY.
	DownRequests int
	// UpRequests bounds the UP requests outstanding at once (2).
	UpRequests int
	// UpPieceSize is the largest UP piece (1 MiB): the client's piece size
	// and the largest piece the server accepts; a larger one is answered
	// with GONE, which fails the carrier.
	UpPieceSize int
	// RequestTimeout bounds one attempt of one request (25 s); it must
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
	// byte while a request can take it (2 ms; negative: none). The client
	// coalesces UP pieces the same way.
	Coalesce time.Duration
	// DownPieceSize is the largest DOWN piece (1 MiB): the server's piece
	// size and the largest piece the client accepts; a larger one fails the
	// carrier.
	DownPieceSize int
	// DownWindow is the number of downlink pieces the server assigns ahead
	// of the client's acknowledgement (32); requests beyond it get BUSY.
	DownWindow int
	// UpStoreLimit bounds the uplink bytes the server holds per carrier
	// (4 MiB; at least twice UpPieceSize).
	UpStoreLimit int
	// IdleTimeout fails a server carrier without requests (60 s).
	IdleTimeout time.Duration
	// GoneMemory is how long the server remembers an ended carrier to
	// answer its late requests with GONE (60 s).
	GoneMemory time.Duration

	// testSkipResponseMAC disables the client's response MAC check (tests
	// only: shows that the check is what rejects replayed responses).
	testSkipResponseMAC bool
}

// exParams is a resolved HTTPExchangeConfig.
type exParams struct {
	psk                    []byte
	b64                    bool
	header                 http.Header
	instance               string
	downURL                string
	nonceParam             string
	cacheBust              bool
	downs, ups             int
	upPiece, downPiece     int
	reqTimeout             time.Duration
	backoffMin, backoffMax time.Duration
	deadline               time.Duration
	hold, coalesce         time.Duration
	window                 int
	upStore                int
	idle, goneFor          time.Duration
	skipRespMAC            bool
}

func (c *HTTPExchangeConfig) resolve() exParams {
	var x HTTPExchangeConfig
	if c != nil {
		x = *c
	}
	pos := func(v, d int) int {
		if v > 0 {
			return v
		}
		return d
	}
	dur := func(v, d time.Duration) time.Duration {
		if v > 0 {
			return v
		}
		return d
	}
	p := exParams{
		psk: x.PSK, b64: x.Base64, header: x.Header, instance: x.Instance,
		nonceParam: x.NonceParam, downURL: x.DownURL, cacheBust: !x.NoCacheBust,
		downs: pos(x.DownRequests, 2), ups: pos(x.UpRequests, 2),
		upPiece: pos(x.UpPieceSize, 1<<20), downPiece: pos(x.DownPieceSize, 1<<20),
		reqTimeout: dur(x.RequestTimeout, 25*time.Second),
		backoffMin: dur(x.BackoffMin, 20*time.Millisecond), backoffMax: dur(x.BackoffMax, 2*time.Second),
		deadline: dur(x.Deadline, 30*time.Second),
		hold:     dur(x.HoldTime, 20*time.Second), coalesce: dur(x.Coalesce, 2*time.Millisecond),
		window: pos(x.DownWindow, 32), upStore: pos(x.UpStoreLimit, 4<<20),
		idle: dur(x.IdleTimeout, 60*time.Second), goneFor: dur(x.GoneMemory, 60*time.Second),
		skipRespMAC: x.testSkipResponseMAC,
	}
	if x.Coalesce < 0 {
		p.coalesce = 0
	}
	if p.nonceParam == "" {
		p.nonceParam = "n"
	}
	p.backoffMax = max(p.backoffMax, p.backoffMin)
	p.upStore = max(p.upStore, 2*p.upPiece)
	return p
}

// ---- wire format (adapters/HTTP-EXCHANGE.md Sections 3-5) ----

const (
	exKindUp    = 0x01
	exKindDown  = 0x02
	exKindClose = 0x03

	exFlagFin  = 0x01 // request (UP) and response (DOWN)
	exFlagBusy = 0x02
	exFlagGone = 0x04

	exCIDLen    = 16
	exNonceLen  = 8
	exMACLen    = 16
	exReqHdrMax = exCIDLen + 2 + 8 + 8 + exNonceLen + exMACLen // with the longest varints
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

// appendExReqHdr appends a request from its Carrier ID through its Nonce.
func appendExReqHdr(b []byte, cid *[exCIDLen]byte, kind, flags byte, seq, ack uint64, nonce *[exNonceLen]byte) []byte {
	b = append(b, cid[:]...)
	b = append(b, kind, flags)
	b = wire.AppendVarint(b, seq)
	b = wire.AppendVarint(b, ack)
	return append(b, nonce[:]...)
}

// exReq is a parsed request; its slices alias the body.
type exReq struct {
	cid    [exCIDLen]byte
	kind   byte
	flags  byte
	seq    uint64
	ack    uint64
	seqRaw []byte
	nonce  []byte
	hdr    []byte // Carrier ID through Nonce
	mac    []byte
	data   []byte
	valid  bool // Kind, Flags, Data and reason follow Section 3
}

// parseExReq parses a binary request body; ok reports a complete header,
// q.valid whether its fields follow Section 3.
func parseExReq(b []byte) (q exReq, ok bool) {
	if len(b) < exCIDLen+2 {
		return q, false
	}
	copy(q.cid[:], b)
	q.kind, q.flags = b[exCIDLen], b[exCIDLen+1]
	off := exCIDLen + 2
	var n int
	if q.seq, n = wire.ReadVarint(b[off:]); n == 0 {
		return q, false
	}
	q.seqRaw = b[off : off+n]
	off += n
	if q.ack, n = wire.ReadVarint(b[off:]); n == 0 {
		return q, false
	}
	off += n
	if len(b) < off+exNonceLen+exMACLen {
		return q, false
	}
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
// a request of kind (Section 4); a malformed response fails the carrier.
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
