package join

import (
	"bytes"
	"errors"
	"testing"
)

// A Hello of Group 2 with the largest Locator takes a first batch only up to
// the handshake message floor; one byte more is too_large, and the Reply has
// the same bound.
func TestHandshakeMessageFloor(t *testing.T) {
	su, _ := LookupSuite(SuiteAES)
	psk, _ := PSK(su, bytes.Repeat([]byte{1}, 32), RoleInitiator, testSuites)
	g, _ := LookupGroup(GroupX25519MLKEM768)
	hello := func(batch int) ([]byte, int, error) {
		j, err := NewJoiner(JoinerConfig{Suite: su, Group: g, Mode: Trusted, Framing: Stream, Locator: bytes.Repeat([]byte{7}, 255), PSK: psk})
		if err != nil {
			t.Fatal(err)
		}
		pl := HelloPayload{Pipe: Pipe{ID: 2}, KeyShare: j.KeyShare()}
		plain, err := HelloPlainLen(pl)
		if err != nil {
			t.Fatal(err)
		}
		room := j.HelloRoom(plain)
		pl.FirstBatch = make([]byte, batch)
		b, err := j.WriteHello(nil, pl)
		return b, room, err
	}
	_, room, _ := hello(0)
	v, ok := FitValue(room)
	if !ok || v < 4096 || v >= 8192 {
		t.Fatalf("the room of a Hello is %d (item %d, %v)", room, v, ok)
	}
	b, _, err := hello(v)
	if err != nil || len(b) > HandshakeFloor {
		t.Fatalf("a Hello of %d bytes: %v", len(b), err)
	}
	if _, _, err = hello(v + 8); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("a Hello over the floor: %v", err)
	}
	// The Reply sizes its padding to the room under the floor.
	j, err := NewJoiner(JoinerConfig{Suite: su, Group: g, Mode: Trusted, Framing: Stream, Locator: []byte("L"), PSK: psk})
	if err != nil {
		t.Fatal(err)
	}
	a, _, err := Accept(mustHello(t, j), Stream, psk, nil, anyGroup)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Exchange(nil); err != nil {
		t.Fatal(err)
	}
	rp := ReplyPayload{KeyShare: a.KeyShare()}
	plain, _ := ReplyPlainLen(rp)
	room = a.ReplyRoom(plain)
	if v, ok = FitValue(room); !ok || v < 4096 {
		t.Fatalf("the room of a Reply is %d", room)
	}
	rp.Padding = v
	if r, err := a.WriteReply(nil, rp); err != nil || len(r) > HandshakeFloor {
		t.Fatalf("a Reply of %d bytes: %v", len(r), err)
	}
}

func mustHello(t *testing.T, j *Joiner) Hello {
	t.Helper()
	b, err := j.WriteHello(nil, HelloPayload{Pipe: Pipe{ID: 2}})
	if err != nil {
		t.Fatal(err)
	}
	h, _, err := ParseHelloExact(b, Stream)
	if err != nil {
		t.Fatal(err)
	}
	return h
}
