package seal_test

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/seal"
	"github.com/zmuxio/zmux-go/seal/proto"
)

// ErrCredentialsFailed is raised only on the side that judged the peer: a
// server that refuses a client's credentials (the verifier's rejection or
// panic, a proof that fails, a post-quantum authentication it requires) hands
// the carrier to OnReject, counts it and returns nothing of that class to its
// application; a verifier that is unavailable closes the carrier without a
// refusal. The client whose handshake the server refuses before its reply
// fails with ErrNoReply.
func TestServerCredentialDecisionsStayLocal(t *testing.T) {
	missing := proto.KeyID{1}
	revoked := errors.Join(errors.New("revoked"), seal.ErrAuth)
	for _, c := range []struct {
		anchor    seal.Anchor
		detail    error
		name      string
		requirePQ bool
		reject    bool // reaches OnReject and the statistics
	}{
		{name: "verifier rejection", anchor: credAnchor{err: revoked}, detail: revoked, requirePQ: false, reject: true},
		{name: "verifier panic", anchor: panicAnchor{}, detail: seal.ErrVerifyRejected, requirePQ: false, reject: true},
		{name: "proof missing", anchor: credAnchor{proof: &missing}, detail: seal.ErrProof, requirePQ: false, reject: true},
		{name: "post-quantum authentication required", anchor: credAnchor{}, detail: seal.ErrPQAuthRequired, requirePQ: true, reject: true},
		{name: "verifier unavailable", anchor: credAnchor{err: seal.ErrVerifyUnavailable}, requirePQ: false, reject: false},
	} {
		t.Run(c.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			f := newFixture(t)
			f.server.ClientAnchor, f.server.RequirePQAuth = c.anchor, c.requirePQ
			f.client.Credential = &proto.Credential{Type: 7, Data: []byte("ok")}
			got := make(chan zmux.RejectInfo, 1)
			l, err := zmux.NewListener(&zmux.ListenerConfig{
				WrapPipe: &f.server,
				OnReject: func(i zmux.RejectInfo) zmux.RejectAction { got <- i; return zmux.RejectClose },
			})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = l.Close() }()
			a, b := net.Pipe()
			herr := make(chan error, 1)
			go func() { herr <- l.HandlePipe(ctx, zmux.NetPipe(b), zmux.PipeHint{}) }()
			// The server judges after the client's last message: the client's
			// handshake ends, and the server's carrier close follows.
			if q, err := seal.Establish(ctx, zmux.NetPipe(a), &f.client); err == nil {
				_ = q.W.Close()
			}
			err = <-herr
			if err == nil || errors.Is(err, seal.ErrCredentialsFailed) || errors.Is(err, seal.ErrVerifyRejected) {
				t.Fatalf("HandlePipe returned %v to the application", err)
			}
			st := l.Stats()
			if !c.reject {
				if !errors.Is(err, seal.ErrVerifyUnavailable) || st.Rejected != 0 {
					t.Fatalf("unavailable verifier: %v, %+v", err, st)
				}
				select {
				case i := <-got:
					t.Fatalf("an unavailable verifier reached OnReject: %+v", i)
				default:
				}
				return
			}
			select {
			case i := <-got:
				if i.Reason != "credentials_failed" || !errors.Is(i.Err, seal.ErrCredentialsFailed) || !errors.Is(i.Err, c.detail) {
					t.Fatalf("OnReject info %+v", i)
				}
			default:
				t.Fatal("no OnReject")
			}
			if st.Rejected != 1 {
				t.Fatalf("stats %+v", st)
			}
		})
	}
}

// A client whose handshake the server refuses before its reply (an unknown
// Handle, a wrong pre-shared key) gets nothing from the wire and fails with
// ErrNoReply, not with a credentials failure.
func TestRefusedClientGetsNoReply(t *testing.T) {
	for _, c := range []struct {
		corrupt func(*seal.ClientConfig)
		name    string
	}{
		{name: "unknown handle", corrupt: func(c *seal.ClientConfig) { c.Handle = []byte("nobody-knows-me!") }},
		{name: "wrong pre-shared key", corrupt: func(c *seal.ClientConfig) { c.PSK = make([]byte, 32) }},
	} {
		t.Run(c.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			f := newFixture(t)
			c.corrupt(&f.client)
			a, b := net.Pipe()
			cerr, serr := establishErrs(t, ctx, &f.client, &f.server, zmux.NetPipe(a), zmux.NetPipe(b))
			if !errors.Is(cerr, seal.ErrNoReply) || errors.Is(cerr, seal.ErrCredentialsFailed) {
				t.Fatalf("client %v", cerr)
			}
			if _, ok := errors.AsType[*zmux.RejectError](serr); !ok {
				t.Fatalf("server %v", serr)
			}
		})
	}
}
