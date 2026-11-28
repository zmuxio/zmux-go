// Package prefixtest provides independent ticket domains for adapter tests.
package prefixtest

import (
	"crypto/rand"
	"math"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/admit"
)

// New issues a reusable test ticket and returns its presenter and verifier.
func New(t testing.TB, carrier admit.PrefixCarrierOptions, reject zmux.AdmissionVerdict) (*admit.PrefixAuth, *admit.PrefixAuthVerifier, [16]byte) {
	t.Helper()
	var ticket admit.PrefixCredential
	var outer [32]byte
	_, _ = rand.Read(ticket.ID[:])
	_, _ = rand.Read(ticket.Key[:])
	_, _ = rand.Read(outer[:])
	window := admit.PrefixWindow{Slot: time.Hour, Skew: 1}
	g := admit.PrefixGeneration{Key: outer, LastSlot: math.MaxUint64}
	store, err := admit.NewMemoryPrefixStore(window, []admit.PrefixGeneration{g}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AddTicket(admit.PrefixTicket{PrefixCredential: ticket, LastSlot: math.MaxUint64}); err != nil {
		t.Fatal(err)
	}
	auth, err := admit.NewPrefixAuth(admit.PrefixAuthOptions{PrefixCarrierOptions: carrier, Slot: window.Slot}, ticket, g)
	if err != nil {
		t.Fatal(err)
	}
	v, err := admit.NewPrefixAuthVerifier(admit.PrefixAuthVerifierOptions{PrefixCarrierOptions: carrier, Store: store, Reject: reject})
	if err != nil {
		t.Fatal(err)
	}
	return auth, v, ticket.ID
}
