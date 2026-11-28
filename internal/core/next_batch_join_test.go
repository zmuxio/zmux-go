package core

import (
	"bytes"
	"testing"

	"github.com/zmuxio/zmux-go/internal/join"
)

func TestNextBatchHJoinEpochAuthorization(t *testing.T) {
	for _, scenario := range []string{"denied", "expired", "before_claim", "accepted", "after_reply"} {
		t.Run(scenario, func(t *testing.T) {
			s, _ := probeSession(DefaultConfig())
			s.role = RoleResponder
			s.mp.cap = 10
			suite, _ := join.LookupSuite(join.SuiteAES)
			group, _ := join.LookupGroup(join.GroupX25519)
			s.cfg.Local.SessionCrypto.Suites = []uint64{suite.ID}
			s.peer.SessionCrypto.Suites = []uint64{suite.ID}
			s.mp.usableGroups = []uint64{group.ID}
			old := &epoch{secret: bytes.Repeat([]byte{1}, 32), local: []byte("old-phase")}
			ep := &s.mp.ep
			*ep = epochs{list: []*epoch{old}, join: old, crypto: []byte("crypto"), st: epAnswered}
			if err := s.commit(nil, nil, nil, []byte("next-phase"), nil); err != nil {
				t.Fatal(err)
			}
			incoming := ep.list[1]
			if scenario == "expired" {
				s.useNext()
				incoming = old
			}
			key, err := join.PSK(suite, incoming.secret, join.RoleInitiator, ep.crypto)
			if err != nil {
				t.Fatal(err)
			}
			j, err := join.NewJoiner(join.JoinerConfig{
				Suite: suite, Group: group, PSK: key, Locator: incoming.local, Phase: uint8(incoming.n), Mode: join.Sealed,
			})
			if err != nil {
				t.Fatal(err)
			}
			msg, err := j.WriteHello(nil, join.HelloPayload{Pipe: join.Pipe{ID: 2, Direction: 2}})
			if err != nil {
				t.Fatal(err)
			}
			h, _, err := join.ParseHelloExact(msg, join.Stream)
			if err != nil {
				t.Fatal(err)
			}
			start, dropAt := ep.st, ep.dropAt
			pending, result := s.OpenHello(h, FramingBytes, nil)
			if result != JoinAccepted {
				t.Fatalf("OpenHello = %v", result)
			}
			if s.mp.additionalPipes != nil || ep.st != start || ep.dropAt != dropAt {
				t.Fatal("pending authorization mutated attachment or epoch state")
			}
			changeAuthorization := func() {
				policy := s.SessionPolicy()
				policy.Revision++
				policy.AuthGeneration++
				if err := s.SetSessionPolicy(policy); err != nil {
					t.Fatal(err)
				}
			}
			switch scenario {
			case "before_claim":
				changeAuthorization()
				if _, result, _ := s.ClaimHello(pending, Carrier{HasIn: true, HasOut: true}, 0, len(msg), nil); result != JoinAuthorizationChanged {
					t.Fatalf("claim under old generation = %v", result)
				}
				if s.mp.idRetired(pending.Pipe.ID) || ep.st != start || ep.dropAt != dropAt || s.PendingPeerJoins() != 0 {
					t.Fatal("stale authorization changed replay, epoch or pending state")
				}
			case "denied":
				// The driver discards the authenticated Hello at this point.
			case "expired":
				s.SetNow(ep.dropAt)
				if _, result, _ := s.ClaimHello(pending, Carrier{HasIn: true, HasOut: true}, 0, len(msg), nil); result != JoinClose {
					t.Fatalf("claim of expired epoch = %v", result)
				}
				if s.mp.additionalPipes != nil || s.mp.idRetired(pending.Pipe.ID) || ep.st != start {
					t.Fatal("claim of expired epoch changed the session")
				}
			case "accepted", "after_reply":
				if _, result, _ := s.ClaimHello(pending, Carrier{HasIn: true, HasOut: true}, 0, len(msg), nil); result != JoinAccepted {
					t.Fatalf("claim of valid epoch = %v", result)
				}
				if ep.st != epRetaining || ep.join != incoming || !s.mp.idRetired(pending.Pipe.ID) {
					t.Fatal("successful claim did not switch epoch and reserve pipe ID")
				}
				if scenario == "after_reply" {
					changeAuthorization()
					if result := s.ConfirmJoin(pending, Carrier{HasIn: true, HasOut: true}); result != JoinAuthorizationChanged {
						t.Fatalf("confirmation under old generation = %v", result)
					}
					if ep.st != epRetaining || ep.join != incoming || !s.mp.idRetired(pending.Pipe.ID) ||
						s.PendingPeerJoins() != 0 || s.pipe(pending.Pipe.ID) != nil {
						t.Fatal("stale confirmation undid the claim or installed a pipe")
					}
				}
			}
		})
	}
}
