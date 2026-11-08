package seal_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/seal"
	"github.com/zmuxio/zmux-go/seal/proto"
)

// credAnchor passes a credential of type 7 holding "ok", and asks for a
// transcript signature of proof when set; it records its input.
type credAnchor struct {
	proof *proto.KeyID
	got   *seal.VerifyInput
	err   error
}

func (a credAnchor) Verify(_ context.Context, in seal.VerifyInput) (seal.Verdict, error) {
	if a.got != nil {
		*a.got = in
	}
	if a.err != nil {
		return seal.Verdict{}, a.err
	}
	if in.Credential == nil || in.Credential.Type != 7 || string(in.Credential.Data) != "ok" {
		return seal.Verdict{}, errors.New("no credential of type 7")
	}
	return seal.Verdict{Principal: seal.Principal{Type: 9, Bytes: []byte("node")}, ProofKey: a.proof}, nil
}

// The Anchor gets the credential, the PSK identity and the local context; the
// verdict's proof key makes the transcript signature required, and a verified
// ML-DSA-44 signature makes the peer PQ-authenticated, on both ends.
func TestVerifyAndTranscriptSignature(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	f := newFixture(t)
	sk, _ := seal.GenerateSigningKey(proto.KeyAlgMLDSA44)
	ck, _ := seal.GenerateSigningKey(proto.KeyAlgMLDSA44)
	sid, cid := sk.ID(), ck.ID()
	var gotC, gotS seal.VerifyInput
	g := &gate{handle: f.handle, psk: f.psk, cred: &proto.Credential{Type: 7, Data: []byte("ok")}}
	sc := f.server
	sc.Gate, sc.Signer, sc.LocalContext = g, seal.KeySigner(sk), "listener A"
	sc.ClientAnchor, sc.RequirePQAuth = credAnchor{proof: &cid, got: &gotS}, true
	cc := f.client
	cc.Anchor, cc.RequirePQAuth, cc.LocalContext = credAnchor{proof: &sid, got: &gotC}, true, "dialer B"
	cc.Credential, cc.Signer = &proto.Credential{Type: 7, Data: []byte("ok")}, seal.KeySigner(ck)
	x, y := net.Pipe()
	cp, sp, cerr, serr := establishPair(t, ctx, &cc, &sc, zmux.NetPipe(x), zmux.NetPipe(y))
	if cerr != nil || serr != nil {
		t.Fatal(cerr, serr)
	}
	_ = cp.W.Close()
	_ = sp.W.Close()
	cpeer, _ := seal.PeerOf(cp)
	speer, _ := seal.PeerOf(sp)
	if !cpeer.PQAuthenticated || !speer.PQAuthenticated || !cpeer.PQConfidential || string(cpeer.Verdict.Principal.Bytes) != "node" {
		t.Fatalf("client %+v server %+v", cpeer, speer)
	}
	if !bytes.Equal(gotC.PSKIdentity, f.handle) || gotC.LocalContext != "dialer B" || gotC.Role != seal.RoleServer ||
		gotS.LocalContext != "listener A" || gotS.Role != seal.RoleClient || len(g.asked) != 1 || g.asked[0].LocalContext != "listener A" {
		t.Fatalf("inputs %+v %+v %+v", gotC, gotS, g.asked)
	}
	if len(g.commits) != 1 || g.commits[0].Peer != speer {
		t.Fatalf("commits %+v", g.commits)
	}

	// Without the server's signature the client rejects it.
	sc.Signer = nil
	x, y = net.Pipe()
	if _, _, cerr, _ := establishPair(t, ctx, &cc, &sc, zmux.NetPipe(x), zmux.NetPipe(y)); !errors.Is(cerr, seal.ErrProof) {
		t.Fatalf("no signature: %v", cerr)
	}
	// A verdict without a proof key is not PQ-authenticated.
	cc.Anchor = credAnchor{}
	x, y = net.Pipe()
	if _, _, cerr, _ := establishPair(t, ctx, &cc, &sc, zmux.NetPipe(x), zmux.NetPipe(y)); !errors.Is(cerr, seal.ErrPQAuthRequired) {
		t.Fatalf("no PQ authentication: %v", cerr)
	}
}

// A rejection goes to the reject policy; a verifier that is unavailable closes
// the carrier without a refusal; a panic is a rejection.
func TestVerifyErrorKinds(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for _, c := range []struct {
		name   string
		anchor seal.Anchor
		reject bool
	}{
		{"rejection", credAnchor{err: errors.New("revoked")}, true},
		{"panic", panicAnchor{}, true},
		{"unavailable", credAnchor{err: fmt.Errorf("%w: down", seal.ErrVerifyUnavailable)}, false},
	} {
		f := newFixture(t)
		sc := f.server
		sc.ClientAnchor = c.anchor
		x, y := net.Pipe()
		_, _, _, serr := establishPair(t, ctx, &f.client, &sc, zmux.NetPipe(x), zmux.NetPipe(y))
		var re *zmux.RejectError
		if errors.As(serr, &re) != c.reject {
			t.Errorf("%s: %v", c.name, serr)
		}
		if !c.reject && !errors.Is(serr, seal.ErrVerifyUnavailable) {
			t.Errorf("%s: %v", c.name, serr)
		}
	}
	// On the client every failure fails Establish.
	f := newFixture(t)
	cc := f.client
	cc.Anchor = panicAnchor{}
	x, y := net.Pipe()
	if _, _, cerr, _ := establishPair(t, ctx, &cc, &f.server, zmux.NetPipe(x), zmux.NetPipe(y)); !errors.Is(cerr, seal.ErrVerifyRejected) || !errors.Is(cerr, seal.ErrCredentialsFailed) {
		t.Fatal(cerr)
	}
}

// A pin with a proof key is PQ-authenticated; RequirePQAuth with a pin without
// one, or without an Anchor, is the configuration error.
func TestPinPQAuth(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	f := newFixture(t)
	pk, _ := seal.GenerateSigningKey(proto.KeyAlgMLDSA44)
	id := seal.NodeKeyID(f.node.PublicKey().Bytes())
	cc := f.client
	cc.RequirePQAuth = true
	x, _ := net.Pipe()
	if _, err := seal.Establish(ctx, zmux.NetPipe(x), &cc); !errors.Is(err, proto.ErrInvalidConfig) {
		t.Fatal(err)
	}
	sc := f.server
	sc.RequirePQAuth = true
	if err := sc.Validate(); !errors.Is(err, proto.ErrInvalidConfig) {
		t.Fatal(err)
	}
	sc.ClientAnchor = seal.PinAnchor(id)
	if err := sc.Validate(); !errors.Is(err, proto.ErrInvalidConfig) {
		t.Fatal(err)
	}
	sc = f.server
	sc.Signer = seal.KeySigner(pk)
	cc.Anchor = seal.PinAnchor(id, pk.ID())
	x, y := net.Pipe()
	cp, sp, cerr, serr := establishPair(t, ctx, &cc, &sc, zmux.NetPipe(x), zmux.NetPipe(y))
	if cerr != nil || serr != nil {
		t.Fatal(cerr, serr)
	}
	_ = cp.W.Close()
	_ = sp.W.Close()
	if peer, _ := seal.PeerOf(cp); !peer.PQAuthenticated {
		t.Fatal("not PQ-authenticated")
	}
}

// cancelAnchor accepts the credential and cancels the server's context
// before it returns.
type cancelAnchor struct{ cancel context.CancelFunc }

func (a cancelAnchor) Verify(context.Context, seal.VerifyInput) (seal.Verdict, error) {
	a.cancel()
	return seal.Verdict{Principal: seal.Principal{Type: 9, Bytes: []byte("node")}}, nil
}

// A context cancelled once Verify passed fails WrapPipe with its error and
// closes the carrier, rather than returning nothing.
func TestVerifyThenCancel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	sctx, scancel := context.WithCancel(ctx)
	defer scancel()
	f := newFixture(t)
	sc := f.server
	sc.ClientAnchor = cancelAnchor{scancel}
	x, y := net.Pipe()
	type res struct {
		p   zmux.PipeIO
		err error
	}
	ch := make(chan res, 1)
	go func() {
		p, err := sc.WrapPipe(sctx, zmux.NetPipe(y), zmux.PipeHint{})
		ch <- res{p, err}
	}()
	go func() { _, _ = seal.Establish(ctx, zmux.NetPipe(x), &f.client) }()
	r := <-ch
	if !errors.Is(r.err, context.Canceled) || r.p.W != nil || r.p.R != nil {
		t.Fatalf("WrapPipe returned %+v, %v; want the context's error", r.p, r.err)
	}
	// The client end sees the server's end closed.
	_ = x.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 64)
	for {
		if _, err := x.Read(buf); err != nil {
			if errors.Is(err, os.ErrDeadlineExceeded) {
				t.Fatal("the server's carrier was left open")
			}
			break
		}
	}
	_ = x.Close()
}

// secretPanic panics with a value that must not reach the error text.
type secretPanic struct{}

func (secretPanic) Verify(context.Context, seal.VerifyInput) (seal.Verdict, error) {
	panic("token=hunter2")
}

func TestVerifierPanicHidesValue(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	f := newFixture(t)
	sc := f.server
	sc.ClientAnchor = secretPanic{}
	x, y := net.Pipe()
	_, _, _, serr := establishPair(t, ctx, &f.client, &sc, zmux.NetPipe(x), zmux.NetPipe(y))
	if serr == nil || strings.Contains(serr.Error(), "hunter2") || !strings.Contains(serr.Error(), "verifier panicked (string)") {
		t.Fatalf("error %v", serr)
	}
	var re *zmux.RejectError
	if errors.As(serr, &re) && re.Err != nil && strings.Contains(re.Err.Error(), "hunter2") {
		t.Fatalf("rejection %v", re.Err)
	}
}

// KeyID names a public key alone as SigningKey.ID and NodeKeyID name it, and
// refuses a key of the wrong length.
func TestKeyID(t *testing.T) {
	sk, err := seal.GenerateSigningKey(proto.KeyAlgMLDSA44)
	if err != nil {
		t.Fatal(err)
	}
	if id, err := seal.KeyID(sk.Alg(), sk.Public()); err != nil || id != sk.ID() {
		t.Fatalf("ML-DSA-44: %x, %v; want %x", id, err, sk.ID())
	}
	node := bytes.Repeat([]byte{9}, 32)
	if id, err := seal.KeyID(proto.KeyAlgX25519, node); err != nil || id != seal.NodeKeyID(node) {
		t.Fatalf("X25519: %x, %v", id, err)
	}
	if _, err := seal.KeyID(proto.KeyAlgX25519, node[:31]); err == nil {
		t.Fatal("a short key was named")
	}
}
