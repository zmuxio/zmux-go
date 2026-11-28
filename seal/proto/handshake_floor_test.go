package proto

import (
	"errors"
	"slices"
	"sort"
	"testing"
)

func TestNextBatchFReplyAtHandshakeFloor(t *testing.T) {
	k := newVKey(t, "floor")
	for _, p := range allProfiles {
		if !profiles[p].hasPSK() {
			continue
		}
		reply := func(d int, signed bool) (n int, called bool, err error) {
			v := defaults(p, Sealed)
			i, r := v.side(t, true, nil, nil), v.side(t, false, nil, nil)
			m1, err := i.WriteMessage(nil, Payload{})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := r.ReadMessage(m1); err != nil {
				t.Fatal(err)
			}
			r.SetHead(FrameHeaderLen(Stream))
			pl := Payload{Credential: &Credential{Type: 1, Data: make([]byte, d)}, Proof: &Proof{Alg: k.Alg(), Pub: k.Public(), Sig: make([]byte, k.Alg().SigLen())}}
			if !signed {
				return 0, false, r.preparePayload(&pl, true)
			}
			msg, err := r.WriteMessageProof(nil, pl, func(m []byte) ([]byte, error) {
				called = true
				return k.Sign(m)
			})
			if err == nil {
				if _, err := i.ReadMessage(msg); err != nil {
					t.Fatal(err)
				}
			}
			return FrameHeaderLen(Stream) + len(msg), called, err
		}
		d := sort.Search(HandshakeFloor+1, func(n int) bool {
			_, _, err := reply(n, false)
			if err == nil {
				return false
			}
			if !errors.Is(err, ErrTooLarge) {
				t.Fatalf("profile %d: %v", p, err)
			}
			return true
		})
		if d <= 3000 || d > HandshakeFloor {
			t.Fatalf("profile %d: largest fitting credential %d", p, d-1)
		}
		if n, called, err := reply(d-1, true); err != nil || !called || n < HandshakeFloor-3 || n > HandshakeFloor {
			t.Fatalf("profile %d: fitting credential gives %d bytes, signed %v: %v", p, n, called, err)
		}
		if _, called, err := reply(d, true); !errors.Is(err, ErrTooLarge) || called {
			t.Fatalf("profile %d: oversized credential signed %v: %v", p, called, err)
		}
	}
}

func TestNextBatchFOutboundCredentialValidation(t *testing.T) {
	profile := allProfiles[slices.IndexFunc(allProfiles, func(p Profile) bool { return profiles[p].hasPSK() })]
	key := newVKey(t, "invalid")
	for _, signed := range []bool{false, true} {
		v := defaults(profile, Sealed)
		i, r := v.side(t, true, nil, nil), v.side(t, false, nil, nil)
		m, err := i.WriteMessage(nil, Payload{})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := r.ReadMessage(m); err != nil {
			t.Fatal(err)
		}
		pl := Payload{Credential: &Credential{Type: 0}, Proof: &Proof{Alg: key.Alg(), Pub: key.Public(), Sig: make([]byte, key.Alg().SigLen())}}
		if signed {
			_, err = r.WriteMessageProof(nil, pl, func([]byte) ([]byte, error) {
				t.Fatal("invalid credential reached signer")
				return nil, nil
			})
		} else {
			_, err = r.WriteMessage(nil, pl)
		}
		if !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("signed %v: %v", signed, err)
		}
	}
}
