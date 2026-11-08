package seal_test

import (
	"context"
	"crypto/ecdh"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/seal"
	"github.com/zmuxio/zmux-go/seal/proto"
)

// helloVector is the responder's static key and the Hello of a negative
// secure-carrier vector.
func helloVector(t *testing.T, name string) (*ecdh.PrivateKey, []byte) {
	t.Helper()
	b, err := os.ReadFile("proto/testdata/vectors/negative.json")
	if err != nil {
		t.Fatal(err)
	}
	var vs []struct {
		Name            string `json:"name"`
		ResponderStatic string `json:"responder_static"`
		Steps           []struct {
			Op   string `json:"op"`
			Wire string `json:"wire"`
		} `json:"steps"`
	}
	if err := json.Unmarshal(b, &vs); err != nil {
		t.Fatal(err)
	}
	for _, v := range vs {
		if v.Name != name {
			continue
		}
		sk, _ := hex.DecodeString(v.ResponderStatic)
		key, err := ecdh.X25519().NewPrivateKey(sk)
		if err != nil {
			t.Fatal(err)
		}
		hello, _ := hex.DecodeString(v.Steps[0].Wire)
		return key, hello
	}
	t.Fatalf("no vector %s", name)
	return nil, nil
}

// A key exchange that fails while the server writes its Reply (a low-order
// ephemeral, an encapsulation key with a coefficient of q or more) is a
// rejection: it reaches OnReject as kex, and the default stalls the carrier.
func TestReplyKexRejected(t *testing.T) {
	for _, name := range []string{"low-order-initiator-ephemeral-no-psk", "encapsulation-key-coefficient-first-3329-no-psk"} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			key, hello := helloVector(t, name)
			got := make(chan zmux.RejectInfo, 1)
			_, addr := listen(t, ctx, &zmux.ListenerConfig{
				Config:   &zmux.Config{HandshakeTimeout: 400 * time.Millisecond},
				WrapPipe: &seal.ServerConfig{Key: key, Gate: &gate{}, AcceptedProfiles: []proto.Profile{4}},
				OnReject: func(i zmux.RejectInfo) zmux.RejectAction { got <- i; return zmux.RejectStall },
			})
			nc := dial(t, addr)
			defer func() { _ = nc.Close() }()
			if _, err := nc.Write(hello); err != nil {
				t.Fatal(err)
			}
			start := time.Now()
			_ = nc.SetReadDeadline(time.Now().Add(5 * time.Second))
			n, err := io.Copy(io.Discard, nc)
			if err != nil || n != 0 {
				t.Fatalf("carrier ended with %d bytes, %v", n, err)
			}
			if d := time.Since(start); d < 300*time.Millisecond {
				t.Fatalf("closed after %v, not stalled", d)
			}
			select {
			case i := <-got:
				if i.Reason != "kex" || i.Profile != 4 {
					t.Fatalf("%+v", i)
				}
			case <-ctx.Done():
				t.Fatal("no OnReject")
			}
		})
	}
}

// msgEnd is one end of an in-memory message carrier.
type msgEnd struct {
	in, out chan []byte
	done    chan struct{}
	once    *sync.Once
	finish  []byte // replaces the second message written, if set
	end     bool   // closes the carrier in place of the second message
	n       int
}

func msgPair() (a, b *msgEnd) {
	x, y, done, once := make(chan []byte, 4), make(chan []byte, 4), make(chan struct{}), new(sync.Once)
	return &msgEnd{in: x, out: y, done: done, once: once}, &msgEnd{in: y, out: x, done: done, once: once}
}

func (m *msgEnd) ReadMessage() ([]byte, error) {
	select {
	case b := <-m.in:
		return b, nil
	case <-m.done:
		return nil, io.EOF
	}
}

func (m *msgEnd) WriteMessage(b []byte) error {
	m.n++
	if m.n == 2 && m.end {
		_ = m.Close()
		return net.ErrClosed
	}
	if m.n == 2 && m.finish != nil {
		b = m.finish
	}
	select {
	case m.out <- append([]byte(nil), b...):
		return nil
	case <-m.done:
		return net.ErrClosed
	}
}

func (m *msgEnd) Read([]byte) (int, error)  { return 0, errors.New("message carrier") }
func (m *msgEnd) Write([]byte) (int, error) { return 0, errors.New("message carrier") }
func (m *msgEnd) Close() error              { m.once.Do(func() { close(m.done) }); return nil }
func (m *msgEnd) pipe() zmux.PipeIO         { return zmux.PipeIO{R: m, W: m, Framing: zmux.FramingRecords} }

// A Finish that breaks the framing (a message carrier's unit beyond 65535
// bytes) is a rejection, not a failure of the carrier.
func TestFinishFramingRejected(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	f := newFixture(t)
	a, b := msgPair()
	a.finish = make([]byte, 65536)
	_, _, cerr, serr := establishPair(t, ctx, &f.client, &f.server, a.pipe(), b.pipe())
	var rej *zmux.RejectError
	if !errors.As(serr, &rej) || rej.Reason != "too_large" {
		t.Fatalf("client %v server %v", cerr, serr)
	}
	// The end of the carrier before the Finish is no rejection.
	a, b = msgPair()
	a.end = true
	_, _, _, serr = establishPair(t, ctx, &f.client, &f.server, a.pipe(), b.pipe())
	if serr == nil || errors.As(serr, &rej) {
		t.Fatalf("carrier end: server %v", serr)
	}
}
