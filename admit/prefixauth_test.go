package admit

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash"
	"math"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/internal/firstmsg"
)

type vectorHex []byte

func (b *vectorHex) UnmarshalJSON(raw []byte) error {
	var h string
	if err := json.Unmarshal(raw, &h); err != nil {
		return err
	}
	v, err := hex.DecodeString(h)
	*b = v
	return err
}

func readPrefixFixture[T any](t *testing.T, name string) T {
	t.Helper()
	raw, err := firstmsg.Fixtures.ReadFile("testdata/" + name + ".json")
	if err != nil {
		t.Fatal(err)
	}
	var v T
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatal(err)
	}
	return v
}

type prefixVector struct {
	Name                             string
	Header                           string
	SlotSeconds                      int64     `json:"slot_seconds"`
	UnixSeconds                      int64     `json:"unix_seconds"`
	Slot                             uint64    `json:"slot"`
	TicketKey                        vectorHex `json:"ticket_key"`
	Mask                             vectorHex `json:"id_mask"`
	MaskedID                         vectorHex `json:"masked_id"`
	OuterTag                         vectorHex `json:"outer_tag"`
	TicketTag                        vectorHex `json:"ticket_tag"`
	Outer, ID, Nonce, Binding, Token vectorHex
}

func (v prefixVector) credential() PrefixCredential {
	return PrefixCredential{ID: [16]byte(v.ID), Key: [32]byte(v.TicketKey)}
}

func vectorExporter(t *testing.T, binding []byte) prefixExporter {
	t.Helper()
	return func(label string, context []byte, length int) ([]byte, error) {
		if label != PrefixAuthExporterLabel || len(context) != 0 || length != 32 {
			t.Errorf("exporter args: %q %x %d", label, context, length)
		}
		return binding, nil
	}
}

func prefixVectors(t *testing.T) []prefixVector {
	t.Helper()
	return readPrefixFixture[struct{ Prefix []prefixVector }](t, "first_message").Prefix
}

func TestFirstMessageBytes(t *testing.T) {
	for _, v := range prefixVectors(t) {
		t.Run(v.Name, func(t *testing.T) {
			carrier := PrefixCarrierOptions{BindTLS: len(v.Binding) > 0}
			opts := PrefixAuthOptions{Slot: time.Duration(v.SlotSeconds) * time.Second,
				PrefixCarrierOptions: carrier,
				Clock:                func() time.Time { return time.Unix(v.UnixSeconds, 0) },
				Random:               bytes.NewReader(v.Nonce)}
			a, err := NewPrefixAuth(opts, v.credential(), PrefixGeneration{Key: [32]byte(v.Outer), LastSlot: math.MaxUint64})
			if err != nil {
				t.Fatal(err)
			}
			info := &zmux.CarrierInfo{TLS: &tls.ConnectionState{HandshakeComplete: true, Version: tls.VersionTLS13}}
			token, err := a.present(info, false, vectorExporter(t, v.Binding))
			if err != nil {
				t.Fatal(err)
			}
			mask := prefixMAC(hmac.New(sha256.New, v.Outer), 0, v.Nonce)
			slot, ok := prefixSlot(opts.Clock(), opts.Slot)
			if !ok || slot != v.Slot || !bytes.Equal(mask[:16], v.Mask) ||
				!bytes.Equal(token, v.Token) || !bytes.Equal(token[16:32], v.MaskedID) ||
				!bytes.Equal(token[32:48], v.OuterTag) || !bytes.Equal(token[48:], v.TicketTag) ||
				base64.RawURLEncoding.EncodeToString(token) != v.Header {
				t.Fatalf("slot %d mask %x token %x", slot, mask[:16], token)
			}
		})
	}
}

type prefixScenarioAttempt struct {
	Input                  string
	OuterTransport         string     `json:"outer_transport"`
	AcceptFirstUseDoS      *bool      `json:"accept_first_use_dos"`
	Binding                *vectorHex `json:"binding_hex"`
	Offset                 int64      `json:"offset_seconds"`
	ExpireAt               *int64     `json:"expire_at_offset_seconds"`
	TakeBytes              *int       `json:"take_bytes"`
	XORByte                *int       `json:"xor_byte"`
	Append                 vectorHex  `json:"append_hex"`
	Count                  *int
	AppendText             string `json:"append_text"`
	Entrance, Expect, Then string
	Remaining              vectorHex `json:"expect_remaining_hex"`
	Concurrent             []string  `json:"concurrent_inputs"`
	Multiset               []string  `json:"expect_multiset"`
	EarlyData              bool      `json:"early_data"`
	XORUnusedBit           bool      `json:"xor_unused_bit"`
}

type prefixScenario struct {
	Name, Form     string
	AllowCleartext bool   `json:"accept_first_use_dos"`
	SingleUse      bool   `json:"single_use"`
	Capacity       int    `json:"claim_capacity"`
	Cutover        uint64 `json:"cutover_slot"`
	IncomingOuter  string `json:"incoming_outer_from"`
	Attempts       []prefixScenarioAttempt
}

type prefixScenarios struct {
	Base         int64  `json:"base_unix_seconds"`
	Slot         int64  `json:"slot_seconds"`
	Skew         int    `json:"skew_slots"`
	DefaultInput string `json:"default_input"`
	Cases        []prefixScenario
}

func TestFirstMessagePrefixScenarios(t *testing.T) {
	vectors := make(map[string]prefixVector)
	for _, v := range prefixVectors(t) {
		vectors[v.Name] = v
	}
	scenarios := readPrefixFixture[struct{ Prefix prefixScenarios }](t, "first_message_cases").Prefix
	for _, c := range scenarios.Cases {
		t.Run(c.Name, func(t *testing.T) {
			base := vectors[scenarios.DefaultInput]
			generations := []PrefixGeneration{{Key: [32]byte(base.Outer), LastSlot: math.MaxUint64}}
			if c.Cutover != 0 {
				incoming := c.IncomingOuter
				if incoming == "" {
					incoming = "rotated"
				}
				generations[0].LastSlot = c.Cutover - 1
				generations = append(generations, PrefixGeneration{
					Key: [32]byte(vectors[incoming].Outer), FirstSlot: c.Cutover, LastSlot: math.MaxUint64})
			}
			store, err := NewMemoryPrefixStore(PrefixWindow{Slot: time.Duration(scenarios.Slot) * time.Second, Skew: scenarios.Skew}, generations, c.Capacity)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.AddTicket(PrefixTicket{PrefixCredential: base.credential(), LastSlot: math.MaxUint64, SingleUse: c.SingleUse}); err != nil {
				t.Fatal(err)
			}
			for _, attempt := range c.Attempts {
				if attempt.ExpireAt != nil {
					store.Expire(time.Unix(scenarios.Base+*attempt.ExpireAt, 0))
					continue
				}
				if len(attempt.Concurrent) != 0 {
					verifyConcurrent(t, attempt, func(a prefixScenarioAttempt) string {
						return runPrefixScenario(t, scenarios.Base, c, a, vectors[a.Input], store)
					})
					continue
				}
				name := attempt.Input
				if name == "" {
					name = scenarios.DefaultInput
				}
				if got := runPrefixScenario(t, scenarios.Base, c, attempt, vectors[name], store); got != attempt.Expect {
					t.Fatalf("%s at offset %d (%s): %s, want %s", name, attempt.Offset, attempt.Entrance, got, attempt.Expect)
				}
				switch attempt.Then {
				case "revoke_ticket":
					store.RevokeTicket(base.credential().ID)
				case "", "end_carrier":
					// Each attempt uses a new carrier. Ending it retains the claim.
				default:
					t.Fatalf("unknown follow-up %q", attempt.Then)
				}
			}
		})
	}
}

func verifyConcurrent(t *testing.T, a prefixScenarioAttempt, verify func(prefixScenarioAttempt) string) {
	t.Helper()
	start := make(chan struct{})
	results := make([]string, len(a.Concurrent))
	var wg sync.WaitGroup
	for i, in := range a.Concurrent {
		wg.Go(func() {
			attempt := a
			attempt.Input = in
			<-start
			results[i] = verify(attempt)
		})
	}
	close(start)
	wg.Wait()
	want := slices.Clone(a.Multiset)
	slices.Sort(results)
	slices.Sort(want)
	if !slices.Equal(results, want) {
		t.Fatalf("concurrent attempts %v, want %v", results, want)
	}
}

func runPrefixScenario(t *testing.T, base int64, c prefixScenario, a prefixScenarioAttempt, vector prefixVector, store PrefixStore) string {
	t.Helper()
	if vector.Name == "" {
		t.Error("unknown prefix input")
		return "unknown input"
	}
	opts := PrefixAuthVerifierOptions{Store: store, Clock: func() time.Time { return time.Unix(base+a.Offset, 0) }}
	opts.BindTLS, opts.AllowCleartext = len(vector.Binding) != 0, c.AllowCleartext
	if a.AcceptFirstUseDoS != nil {
		opts.AllowCleartext = *a.AcceptFirstUseDoS
	}
	v, err := NewPrefixAuthVerifier(opts)
	if err != nil {
		t.Error(err)
		return "config error"
	}
	info := zmux.CarrierInfo{EarlyData: a.EarlyData}
	switch a.OuterTransport {
	case "", "tls13", "quic":
		info.TLS = &tls.ConnectionState{HandshakeComplete: true, Version: tls.VersionTLS13}
	case "tls12":
		info.TLS = &tls.ConnectionState{HandshakeComplete: true, Version: tls.VersionTLS12}
	case "cleartext":
	default:
		t.Errorf("unknown transport %q", a.OuterTransport)
	}
	prefix := slices.Concat(vector.Token, a.Append)
	if a.XORByte != nil {
		prefix[*a.XORByte] ^= 1
	}
	if a.TakeBytes != nil {
		prefix = prefix[:*a.TakeBytes]
	}
	switch c.Form {
	case "header":
		info.Stage = zmux.StageHead
		info.Request = &http.Request{Header: http.Header{}}
		field := vector.Header + a.AppendText
		if a.XORUnusedBit {
			const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
			last := strings.IndexByte(alphabet, field[len(field)-1]) ^ 1
			field = field[:len(field)-1] + alphabet[last:last+1]
		}
		count := 1
		if a.Count != nil {
			count = *a.Count
		}
		for range count {
			info.Request.Header.Add(HeaderPrefixAuth, field)
		}
		prefix = nil
	case "message":
		info.Framing = zmux.FramingRecords
	case "", "stream":
	default:
		t.Errorf("unknown form %q", c.Form)
	}
	binding := vector.Binding
	if a.Binding != nil {
		binding = *a.Binding
	}
	d := v.decide(t.Context(), info, prefix, vectorExporter(t, binding))
	if d.Verdict != zmux.VerdictAccept {
		if d.Verdict != zmux.VerdictClose || d.Value != nil || d.Skip != 0 {
			t.Errorf("refusal: %+v", d)
		}
		return "refused"
	}
	if d.Skip > len(prefix) || !bytes.Equal(prefix[d.Skip:], a.Remaining) || (d.Value != nil) != opts.BindTLS {
		t.Errorf("acceptance: %+v for %x", d, prefix)
	}
	return "accepted"
}

type countedMAC struct {
	hash.Hash
	trials int
}

func (m *countedMAC) Sum(b []byte) []byte {
	m.trials++
	return m.Hash.Sum(b)
}

func TestPrefixFixedSlotWork(t *testing.T) {
	var id [16]byte
	token := make([]byte, PrefixTokenLen)
	g := PrefixGeneration{LastSlot: math.MaxUint64}
	for _, tc := range []struct {
		current, skew, issue uint64
		accept               bool
	}{
		{50, 1, 49, true}, {50, 1, 51, true}, {50, 1, 52, false}, {50, 0, 50, true},
		{0, 1, 0, true}, {math.MaxUint64, 1, 0, false},
		{math.MaxUint64, 1, math.MaxUint64, true},
	} {
		mac := hmac.New(sha256.New, make([]byte, 32))
		tag := prefixTicketTag(mac, id, token[:48], tc.issue, nil)
		copy(token[48:], tag[:16])
		counted := &countedMAC{Hash: mac}
		slot, ok := matchPrefixSlot(counted, id, token, nil, &g, tc.current, tc.skew)
		if counted.trials != int(2*tc.skew+1) || ok != tc.accept || ok && slot != tc.issue {
			t.Fatalf("%+v: slot %d matched %v with %d trials", tc, slot, ok, counted.trials)
		}
	}
}

type prefixFaultStore struct {
	*MemoryPrefixStore
	beforeClaim, afterClaim              func()
	authoritativeFloor                   uint64
	lostReply                            bool
	resolved                             bool
	floors, lookups, claims, resolutions int
}

func (s *prefixFaultStore) Floor(ctx context.Context) (uint64, error) {
	s.floors++
	floor, err := s.MemoryPrefixStore.Floor(ctx)
	return max(floor, s.authoritativeFloor), err
}

func (s *prefixFaultStore) Lookup(ctx context.Context, id [16]byte) (PrefixTicket, bool, error) {
	s.lookups++
	return s.MemoryPrefixStore.Lookup(ctx, id)
}

func (s *prefixFaultStore) Claim(ctx context.Context, claim PrefixClaim) (bool, error) {
	s.claims++
	if s.beforeClaim != nil {
		s.beforeClaim()
	}
	ok, err := s.MemoryPrefixStore.Claim(ctx, claim)
	if ok && s.afterClaim != nil {
		s.afterClaim()
	}
	if ok && s.lostReply {
		return false, errors.New("receipt unavailable")
	}
	return ok, err
}

func (s *prefixFaultStore) ResolveClaim(ctx context.Context, claim PrefixClaim) (bool, error) {
	s.resolutions++
	if _, ok := ctx.Deadline(); !ok {
		return false, errors.New("receipt lookup has no deadline")
	}
	accepted, err := s.MemoryPrefixStore.ResolveClaim(ctx, claim)
	s.resolved = accepted
	return accepted, err
}

func TestPrefixConditionalClaim(t *testing.T) {
	v := prefixVectors(t)[0]
	for _, test := range []string{"bad A", "ambiguous A", "floor makes A ambiguous", "bad T", "revoked ticket", "revoked generation", "lost receipt", "cancelled before commit", "cancelled after commit"} {
		t.Run(test, func(t *testing.T) {
			g := []PrefixGeneration{{Key: [32]byte(v.Outer), LastSlot: math.MaxUint64}}
			switch test {
			case "ambiguous A":
				g[0].LastSlot = v.Slot
				g = append(g, PrefixGeneration{Key: g[0].Key, FirstSlot: v.Slot + 1, LastSlot: math.MaxUint64})
			case "floor makes A ambiguous":
				g[0].LastSlot = v.Slot + 1
				g = append(g, PrefixGeneration{Key: g[0].Key, FirstSlot: v.Slot + 2, LastSlot: math.MaxUint64})
			}
			m, err := NewMemoryPrefixStore(PrefixWindow{Slot: 30 * time.Second, Skew: 1}, g, 0)
			if err != nil {
				t.Fatal(err)
			}
			ticket := PrefixTicket{PrefixCredential: v.credential(), LastSlot: math.MaxUint64, SingleUse: true}
			if err := m.AddTicket(ticket); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			s := &prefixFaultStore{MemoryPrefixStore: m}
			token := slices.Clone(v.Token)
			wantFloor, wantLookup, wantClaim, wantResolve := 1, 1, 1, 0
			wantCommitted := false
			want := zmux.VerdictClose
			switch test {
			case "bad A":
				token[32] ^= 1
				wantFloor, wantLookup, wantClaim = 0, 0, 0
			case "ambiguous A":
				wantFloor, wantLookup, wantClaim = 0, 0, 0
			case "floor makes A ambiguous":
				s.authoritativeFloor = v.Slot + 1
				wantLookup, wantClaim = 0, 0
			case "bad T":
				token[48] ^= 1
				wantClaim = 0
			case "revoked ticket":
				s.beforeClaim = func() { m.RevokeTicket(ticket.ID) }
			case "revoked generation":
				s.beforeClaim = func() {
					if err := m.SetGenerations(nil); err != nil {
						t.Fatal(err)
					}
				}
			case "lost receipt":
				s.lostReply, wantResolve, want, wantCommitted = true, 1, zmux.VerdictAccept, true
			case "cancelled before commit":
				s.beforeClaim, wantResolve = cancel, 1
			case "cancelled after commit":
				s.afterClaim, s.lostReply, wantResolve, wantCommitted = cancel, true, 1, true
			}
			verifier, err := NewPrefixAuthVerifier(PrefixAuthVerifierOptions{Store: s, Clock: func() time.Time { return time.Unix(v.UnixSeconds, 0) }})
			if err != nil {
				t.Fatal(err)
			}
			d := verifier.Decide(ctx, zmux.CarrierInfo{Encrypted: true}, token)
			if d.Verdict != want || s.floors != wantFloor || s.lookups != wantLookup || s.claims != wantClaim ||
				s.resolutions != wantResolve || s.resolved != wantCommitted {
				t.Fatalf("verdict %v, floor reads %d, lookups %d, claims %d, resolutions %d, resolved %t",
					d.Verdict, s.floors, s.lookups, s.claims, s.resolutions, s.resolved)
			}
		})
	}
}

func TestPrefixPresenterCutover(t *testing.T) {
	v := prefixVectors(t)
	old, fresh, rotated := v[0], v[2], v[4]
	now := time.Unix(old.UnixSeconds, 0)
	a, err := NewPrefixAuth(PrefixAuthOptions{
		Slot: 30 * time.Second, Clock: func() time.Time { return now },
		Random: bytes.NewReader(slices.Concat(old.Nonce, fresh.Nonce, rotated.Nonce)),
	}, old.credential(),
		PrefixGeneration{Key: [32]byte(old.Outer), LastSlot: rotated.Slot - 1},
		PrefixGeneration{Key: [32]byte(rotated.Outer), FirstSlot: rotated.Slot, LastSlot: math.MaxUint64})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []prefixVector{old, fresh, rotated} {
		now = time.Unix(want.UnixSeconds, 0)
		token, err := a.Prefix(&zmux.CarrierInfo{Encrypted: true})
		if err != nil || !bytes.Equal(token, want.Token) {
			t.Fatalf("%s: %x, %v", want.Name, token, err)
		}
	}
}
