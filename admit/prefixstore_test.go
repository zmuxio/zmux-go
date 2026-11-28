package admit

import (
	"errors"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go"
)

func TestMemoryPrefixStoreRetiredIDs(t *testing.T) {
	s, err := NewMemoryPrefixStore(PrefixWindow{Slot: time.Second, Skew: 1}, nil, 3)
	if err != nil {
		t.Fatal(err)
	}
	first := PrefixCredential{ID: [16]byte{1}, Key: [32]byte{7}}
	expiring := PrefixTicket{PrefixCredential: first, LastSlot: 10}
	if err := s.AddTicket(expiring); err != nil {
		t.Fatal(err)
	}
	second := PrefixCredential{ID: [16]byte{2}}
	revoked := PrefixTicket{PrefixCredential: second, LastSlot: 100}
	s.RevokeTicket(revoked.ID)
	s.Expire(time.Unix(12, 0))
	s.Expire(time.Unix(20, 0))
	got, live, err := s.Lookup(t.Context(), expiring.ID)
	if err != nil || live || got.Key != [32]byte{} {
		t.Fatalf("expired ticket retained a key or stayed live: %v, %v", live, err)
	}
	expiring.LastSlot = revoked.LastSlot
	for _, ticket := range []PrefixTicket{expiring, revoked} {
		if err := s.AddTicket(ticket); !errors.Is(err, zmux.ErrInvalidConfig) {
			t.Fatalf("installed barred ID %x: %v", ticket.ID, err)
		}
	}
	third := PrefixCredential{ID: [16]byte{3}}
	if err := s.AddTicket(PrefixTicket{PrefixCredential: third, LastSlot: 100}); err != nil {
		t.Fatal(err)
	}
	fourth := PrefixCredential{ID: [16]byte{4}}
	if err := s.AddTicket(PrefixTicket{PrefixCredential: fourth, LastSlot: 100}); !errors.Is(err, zmux.ErrInvalidConfig) {
		t.Fatalf("installed a new ID after reaching capacity: %v", err)
	}
}
