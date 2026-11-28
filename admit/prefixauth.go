package admit

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"hash"
	"io"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/internal/carrierio"
	"github.com/zmuxio/zmux-go/internal/httpx"
)

const (
	// PrefixTokenLen is the length of a binary ticket presentation.
	PrefixTokenLen = 64
	// PrefixAuthExporterLabel is the exporter label for a bound presentation.
	PrefixAuthExporterLabel = "EXPORTER-zmux-prefix-auth"
	// HeaderPrefixAuth is the default request field for a head presentation.
	HeaderPrefixAuth = httpx.FieldPrefixAuth

	prefixHeaderLen = 86
)

// PrefixCarrierOptions selects the form agreed for one entry.
type PrefixCarrierOptions struct {
	// HeaderField replaces HeaderPrefixAuth when both peers agree on a different field.
	HeaderField string
	// BindTLS requires a completed TLS 1.3 or QUIC handshake on the actual connection.
	BindTLS bool
	// AllowCleartext permits an unbound binary presentation where a third party
	// presenting the captured token first can cause only denial of service.
	AllowCleartext bool
}

func (o PrefixCarrierOptions) headerName() (string, error) {
	f := o.HeaderField
	if f == "" || strings.EqualFold(f, HeaderPrefixAuth) {
		return HeaderPrefixAuth, nil
	}
	if httpx.CheckField(f) != "" || strings.EqualFold(f, httpx.FieldPair) {
		return "", prefixConfigError("reserved or invalid prefix-authentication header")
	}
	return http.CanonicalHeaderKey(f), nil
}

type prefixExporter func(label string, context []byte, length int) ([]byte, error)

func (o PrefixCarrierOptions) binding(info *zmux.CarrierInfo, head bool, export prefixExporter) ([]byte, error) {
	if info == nil || info.EarlyData || zmux.EarlyRequest(info.Request) || info.TLS != nil && !info.TLS.HandshakeComplete {
		return nil, prefixConfigError("prefix authentication needs a 1-RTT carrier")
	}
	if o.BindTLS {
		if info.TLS == nil || info.TLS.Version != tls.VersionTLS13 {
			return nil, prefixConfigError("bound prefix authentication needs TLS 1.3 or QUIC")
		}
		if export == nil {
			export = info.TLS.ExportKeyingMaterial
		}
		b, err := export(PrefixAuthExporterLabel, []byte{}, 32)
		if err != nil || len(b) != 32 {
			return nil, prefixConfigError("prefix-authentication exporter unavailable")
		}
		return b, nil
	}
	if !info.Encrypted && info.TLS == nil && (head || !o.AllowCleartext) {
		return nil, prefixConfigError("prefix authentication on this entry needs an encrypted carrier")
	}
	return nil, nil
}

func prefixConfigError(message string) error {
	return fmt.Errorf("%w: %s", zmux.ErrInvalidConfig, message)
}

// PrefixCredential is issued by the ticket authority. Generate ID and Key
// independently with cryptographic randomness and keep Key confidential.
type PrefixCredential struct {
	ID  [16]byte
	Key [32]byte
}

// PrefixAuthOptions configures a presenter.
type PrefixAuthOptions struct {
	PrefixCarrierOptions
	// Slot is the agreed slot length, a positive whole number of seconds.
	Slot time.Duration
	// Clock defaults to time.Now.
	Clock func() time.Time
	// Random defaults to crypto/rand.Reader. A supplied reader must provide
	// cryptographic randomness and support concurrent use.
	Random io.Reader
}

// PrefixAuth presents a fresh token on each carrier. It implements
// zmux.CarrierCredential and is safe for concurrent use.
type PrefixAuth struct {
	opts        PrefixAuthOptions
	ticket      PrefixCredential
	generations []PrefixGeneration
	header      string
}

// NewPrefixAuth builds a presenter from its ticket and the provisioned outer
// key schedule. Each presentation uses the generation covering its issue slot.
func NewPrefixAuth(o PrefixAuthOptions, ticket PrefixCredential, generations ...PrefixGeneration) (*PrefixAuth, error) {
	header, err := o.headerName()
	if err != nil {
		return nil, err
	}
	if !validPrefixSlot(o.Slot) || len(generations) == 0 {
		return nil, prefixConfigError("prefix authentication needs a whole-second slot and an outer key schedule")
	}
	for _, g := range generations {
		if ticket.Key == g.Key {
			return nil, prefixConfigError("prefix authentication needs independent keys")
		}
	}
	generations, err = prefixSchedule(generations)
	if err != nil {
		return nil, err
	}
	if o.Clock == nil {
		o.Clock = time.Now
	}
	if o.Random == nil {
		o.Random = rand.Reader
	}
	return &PrefixAuth{opts: o, ticket: ticket, generations: generations, header: header}, nil
}

// Prefix returns the presentation for a byte stream or one binary message.
func (a *PrefixAuth) Prefix(info *zmux.CarrierInfo) ([]byte, error) {
	return a.present(info, false, nil)
}

// Header returns the presentation for an encrypted request head.
func (a *PrefixAuth) Header(info *zmux.CarrierInfo) (name, value string, err error) {
	b, err := a.present(info, true, nil)
	if err != nil {
		return "", "", err
	}
	return a.header, base64.RawURLEncoding.EncodeToString(b), nil
}

func (a *PrefixAuth) present(info *zmux.CarrierInfo, head bool, export prefixExporter) ([]byte, error) {
	if a == nil || a.opts.Clock == nil {
		return nil, prefixConfigError("missing prefix presenter")
	}
	binding, err := a.opts.binding(info, head, export)
	if err != nil {
		return nil, err
	}
	slot, ok := prefixSlot(a.opts.Clock(), a.opts.Slot)
	if !ok {
		return nil, prefixConfigError("prefix-authentication clock precedes the Unix epoch")
	}
	for _, g := range a.generations {
		if g.contains(slot) {
			return a.makeToken(slot, binding, g.Key)
		}
	}
	return nil, prefixConfigError("no outer key for the current issue slot")
}

func (a *PrefixAuth) makeToken(slot uint64, binding []byte, key [32]byte) ([]byte, error) {
	b := make([]byte, PrefixTokenLen)
	if _, err := io.ReadFull(a.opts.Random, b[:16]); err != nil {
		return nil, err
	}
	outer := hmac.New(sha256.New, key[:])
	mask := prefixMAC(outer, 0, b[:16])
	subtle.XORBytes(b[16:32], a.ticket.ID[:], mask[:])
	tag := prefixMAC(outer, 1, b[:32])
	copy(b[32:48], tag[:])
	tag = prefixTicketTag(hmac.New(sha256.New, a.ticket.Key[:]), a.ticket.ID, b[:48], slot, binding)
	copy(b[48:], tag[:])
	return b, nil
}

func validPrefixSlot(slot time.Duration) bool { return slot > 0 && slot%time.Second == 0 }

func prefixSlot(now time.Time, length time.Duration) (uint64, bool) {
	seconds := now.Unix()
	if seconds < 0 {
		return 0, false
	}
	return uint64(seconds) / uint64(length/time.Second), true
}

func prefixMAC(mac hash.Hash, domain byte, body []byte) [sha256.Size]byte {
	mac.Reset()
	mac.Write([]byte{domain})
	mac.Write(body)
	return prefixSum(mac)
}

func prefixTicketTag(mac hash.Hash, id [16]byte, envelope []byte, slot uint64, binding []byte) [sha256.Size]byte {
	mac.Reset()
	mac.Write([]byte{2})
	mac.Write(id[:])
	mac.Write(envelope)
	var end [9]byte
	binary.BigEndian.PutUint64(end[:8], slot)
	end[8] = byte(len(binding))
	mac.Write(end[:])
	mac.Write(binding)
	return prefixSum(mac)
}

func prefixSum(mac hash.Hash) (sum [sha256.Size]byte) {
	mac.Sum(sum[:0])
	return sum
}

// PrefixAuthVerifierOptions configures a verifier on one admission entry.
type PrefixAuthVerifierOptions struct {
	PrefixCarrierOptions
	// Store is shared by every entrance of the domain.
	Store PrefixStore
	// Clock defaults to time.Now.
	Clock func() time.Time
	// MaxSkew is the local ceiling on the store's Skew. Zero uses two slots.
	MaxSkew int
	// Reject selects the verdict on refusal. Zero selects zmux.VerdictClose.
	// zmux.VerdictFallback and zmux.VerdictStall are also supported.
	Reject zmux.AdmissionVerdict
}

// PrefixAuthVerifier verifies presentations against a shared ticket authority.
type PrefixAuthVerifier struct {
	opts   PrefixAuthVerifierOptions
	window PrefixWindow
	header string
}

// NewPrefixAuthVerifier configures a verifier for the store's domain.
func NewPrefixAuthVerifier(o PrefixAuthVerifierOptions) (*PrefixAuthVerifier, error) {
	header, err := o.headerName()
	if err != nil {
		return nil, err
	}
	if o.Store == nil {
		return nil, prefixConfigError("prefix verifier needs a ticket authority")
	}
	w := o.Store.Window()
	if o.MaxSkew == 0 {
		o.MaxSkew = 2
	}
	if !w.valid() || w.Skew > o.MaxSkew {
		return nil, prefixConfigError("invalid prefix slot length or skew")
	}
	switch o.Reject {
	case 0:
		o.Reject = zmux.VerdictClose
	case zmux.VerdictClose, zmux.VerdictFallback, zmux.VerdictStall:
	default:
		return nil, prefixConfigError("invalid prefix rejection verdict")
	}
	if o.Clock == nil {
		o.Clock = time.Now
	}
	return &PrefixAuthVerifier{opts: o, window: w, header: header}, nil
}

// Admission configures the listener's single admission decision and read
// boundary. Set the desired timeout and stall settings before attaching it to
// the listener.
func (v *PrefixAuthVerifier) Admission() *zmux.Admission {
	if v == nil || v.opts.Store == nil {
		return &zmux.Admission{} // NewListener reports the missing verifier.
	}
	return &zmux.Admission{Peek: PrefixTokenLen, MaxMessage: PrefixTokenLen, Decide: v.Decide, Refuse: v.opts.Reject}
}

// Decide accepts a stream prefix, a whole binary message or a request head at
// the configured entry. A stream may have bytes after the token; they remain
// available to the inner protocol. Configure listeners with Admission.
func (v *PrefixAuthVerifier) Decide(ctx context.Context, info zmux.CarrierInfo, prefix []byte) zmux.Decision {
	return v.decide(ctx, info, prefix, nil)
}

func (v *PrefixAuthVerifier) decide(ctx context.Context, info zmux.CarrierInfo, prefix []byte, export prefixExporter) zmux.Decision {
	if v == nil || v.opts.Store == nil {
		return zmux.Decision{Verdict: zmux.VerdictClose}
	}
	no := zmux.Decision{Verdict: v.opts.Reject}
	head := info.Stage == zmux.StageHead
	if head && info.HasAuthenticatedEnvelope() && !info.EarlyData && !zmux.EarlyRequest(info.Request) && ctx.Err() == nil {
		return zmux.Decision{Verdict: zmux.VerdictAccept}
	}
	token, skip := v.decode(&info, prefix, head)
	if len(token) != PrefixTokenLen {
		return no
	}
	binding, err := v.opts.binding(&info, head, export)
	if err != nil || ctx.Err() != nil {
		return no
	}
	id, ok := v.redeem(ctx, token, binding)
	if !ok {
		return no
	}
	d := zmux.Decision{Verdict: zmux.VerdictAccept, Skip: skip}
	if v.opts.BindTLS {
		d.Value = carrierio.BoundPrefixPrincipal(id)
	}
	return d
}

func (v *PrefixAuthVerifier) decode(info *zmux.CarrierInfo, prefix []byte, head bool) ([]byte, int) {
	if !head {
		if len(prefix) < PrefixTokenLen || info.Framing == zmux.FramingRecords && len(prefix) != PrefixTokenLen {
			return nil, 0
		}
		return prefix[:PrefixTokenLen], PrefixTokenLen
	}
	if info.Request == nil {
		return nil, 0
	}
	var field string
	for name, values := range info.Request.Header {
		if strings.EqualFold(name, v.header) {
			if field != "" || len(values) != 1 || len(values[0]) != prefixHeaderLen {
				return nil, 0
			}
			field = values[0]
		}
	}
	var token [PrefixTokenLen]byte
	if n, err := base64.RawURLEncoding.Strict().Decode(token[:], []byte(field)); err != nil || n != PrefixTokenLen {
		return nil, 0
	}
	return token[:], 0
}

// BoundPrincipal returns the ticket ID proven by a bound presentation on the
// session's accepted bootstrap carrier. Both halves of a split bootstrap
// must prove the same bound ticket ID to supply a principal.
// Call it on a session delivered by Listener.Accept. An unbound presentation
// supplies no principal; use the identity from an independently authenticated
// inner credential when applicable.
func BoundPrincipal(c *zmux.Conn) (id [16]byte, ok bool) {
	if c == nil {
		return id, false
	}
	return zmux.BootstrapAdmissionAs[carrierio.BoundPrefixPrincipal](c)
}

func (v *PrefixAuthVerifier) redeem(ctx context.Context, token, binding []byte) (id [16]byte, ok bool) {
	store := v.opts.Store
	local, ok := prefixSlot(v.opts.Clock(), v.window.Slot)
	if !ok {
		return id, false
	}
	schedule, hint := store.LocalSchedule()
	if schedule == nil || ctx.Err() != nil {
		return id, false
	}
	precheck := max(local, hint)
	skew := uint64(v.window.Skew)
	generation, ok := prefixOuterGeneration(schedule.generations, token, precheck, skew)
	defer clear(generation.Key[:])
	if !ok {
		return id, false
	}
	floor, err := store.Floor(ctx)
	if err != nil || ctx.Err() != nil {
		return id, false
	}
	current := max(local, floor)
	if !schedule.sameEligibleGenerations(precheck, current, skew) {
		clear(generation.Key[:])
		generation, ok = prefixOuterGeneration(schedule.generations, token, current, skew)
		if !ok {
			return id, false
		}
	}
	mask := prefixMAC(hmac.New(sha256.New, generation.Key[:]), 0, token[:16])
	subtle.XORBytes(id[:], token[16:32], mask[:])
	ticket, live, err := store.Lookup(ctx, id)
	if err != nil {
		return id, false
	}
	defer clear(ticket.Key[:])
	if !live || ticket.ID != id || !ticket.liveAt(current) {
		return id, false
	}
	slot, ok := matchPrefixSlot(hmac.New(sha256.New, ticket.Key[:]), id, token, binding, &generation, current, skew)
	if !ok || ctx.Err() != nil {
		return id, false
	}
	local, ok = prefixSlot(v.opts.Clock(), v.window.Slot)
	if !ok {
		return id, false
	}
	claim := PrefixClaim{ID: id, Nonce: [16]byte(token[:16]), CommitID: randomPrefixCommitID(),
		Version: ticket.Version, Schedule: schedule.version, Generation: generation.FirstSlot, IssueSlot: slot, CheckedSlot: current, LocalSlot: local}
	accepted, err := store.Claim(ctx, claim)
	if err != nil {
		resolve, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
		defer cancel()
		accepted, err = store.ResolveClaim(resolve, claim)
	}
	return id, accepted && err == nil && ctx.Err() == nil
}

func randomPrefixCommitID() (id [16]byte) {
	// crypto/rand.Read always fills the buffer.
	_, _ = rand.Read(id[:])
	return id
}

func prefixOuterGeneration(generations []PrefixGeneration, token []byte, current, skew uint64) (selected PrefixGeneration, unique bool) {
	var matches int
	for i := range generations {
		g := &generations[i]
		if !g.intersects(current, skew) {
			continue
		}
		tag := prefixMAC(hmac.New(sha256.New, g.Key[:]), 1, token[:32])
		if hmac.Equal(tag[:16], token[32:48]) {
			selected = *g
			matches++
		}
	}
	return selected, matches == 1
}

func matchPrefixSlot(mac hash.Hash, id [16]byte, token, binding []byte, g *PrefixGeneration, current, skew uint64) (slot uint64, matched bool) {
	for trial := uint64(0); trial <= 2*skew; trial++ {
		candidate, valid := prefixCandidate(current, skew, trial)
		tag := prefixTicketTag(mac, id, token[:48], candidate, binding)
		if hmac.Equal(tag[:16], token[48:]) && valid && g.contains(candidate) {
			slot, matched = candidate, true
		}
	}
	return slot, matched
}

func prefixCandidate(current, skew, trial uint64) (uint64, bool) {
	if trial < skew {
		d := skew - trial
		if d <= current {
			return current - d, true
		}
	} else {
		d := trial - skew
		if d <= math.MaxUint64-current {
			return current + d, true
		}
	}
	return 0, false
}

func prefixLastSlot(issue, skew uint64) uint64 { return issue + min(skew, math.MaxUint64-issue) }
