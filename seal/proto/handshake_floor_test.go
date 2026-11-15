package proto

import (
	"errors"
	"testing"
)

// The largest Reply, with a proof of the largest KeyAlg, takes a credential
// up to the handshake message floor; one byte more is too_large, before
// anything is written.
func TestReplyAtHandshakeFloor(t *testing.T) {
	k := newVKey(t, "floor")
	for _, p := range allProfiles {
		if !profiles[p].hasPSK() {
			continue
		}
		reply := func(d int) (int, error) {
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
			msg, err := r.WriteMessageProof(nil, pl, k.Sign)
			return FrameHeaderLen(Stream) + len(msg), err
		}
		// The largest credential that fits: the message is exactly the floor.
		d := 0
		for {
			n, err := reply(d + 1)
			if err != nil {
				if !errors.Is(err, ErrTooLarge) {
					t.Fatalf("profile %d: %v", p, err)
				}
				break
			}
			if n > HandshakeFloor {
				t.Fatalf("profile %d: a Reply of %d bytes was written", p, n)
			}
			d++
		}
		if n, err := reply(d); err != nil || n < HandshakeFloor-3 {
			t.Fatalf("profile %d: the largest credential (%d bytes) gives a Reply of %d bytes, %v", p, d, n, err)
		}
		if d < 3000 {
			t.Fatalf("profile %d: a credential of only %d bytes fits", p, d)
		}
	}
}
