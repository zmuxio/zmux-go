package seal_test

import (
	"bytes"
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/seal"
	"github.com/zmuxio/zmux-go/seal/proto"
)

// Application data of messages 1, 2 and 3 reaches the peer with its message and
// protection; each source sees what is known of the peer then.
func TestAppData(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	f := newFixture(t)
	var asked []seal.AppDataRequest
	var gotS seal.VerifyInput
	cc, sc := f.client, f.server
	cc.AppData = func(_ context.Context, r seal.AppDataRequest) ([]byte, error) {
		asked = append(asked, r)
		return []byte{byte(r.Message)}, nil
	}
	var srvReq seal.AppDataRequest
	sc.AppData = func(_ context.Context, r seal.AppDataRequest) ([]byte, error) {
		srvReq = r
		return []byte("two"), nil
	}
	sc.ClientAnchor = recordAnchor{got: &gotS}
	x, y := net.Pipe()
	cp, sp, cerr, serr := establishPair(t, ctx, &cc, &sc, zmux.NetPipe(x), zmux.NetPipe(y))
	if cerr != nil || serr != nil {
		t.Fatal(cerr, serr)
	}
	_ = cp.W.Close()
	_ = sp.W.Close()
	cpeer, _ := seal.PeerOf(cp)
	speer, _ := seal.PeerOf(sp)
	// The Anchor sees nothing authenticated; the Peer, after it passed, the
	// data of message 3 as the client's.
	judged := []seal.AppData{{Message: 1, Data: []byte{1}}, {Message: 3, Data: []byte{3}, ForwardSecret: true}}
	want := []seal.AppData{{Message: 1, Data: []byte{1}}, {Message: 3, Data: []byte{3}, ForwardSecret: true, Authenticated: true}}
	if !appDataEqual(speer.AppData, want) || !appDataEqual(gotS.PeerAppData, judged) {
		t.Fatalf("server got %+v, anchor %+v", speer.AppData, gotS.PeerAppData)
	}
	if !appDataEqual(cpeer.AppData, []seal.AppData{{Message: 2, Data: []byte("two"), ForwardSecret: true, Authenticated: true}}) {
		t.Fatalf("client got %+v", cpeer.AppData)
	}
	if !appDataEqual(asked[1].Peer.PeerAppData, []seal.AppData{{Message: 2, Data: []byte("two"), ForwardSecret: true}}) {
		t.Fatalf("client's judged input %+v", asked[1].Peer.PeerAppData)
	}
	if len(asked) != 2 || asked[0].Peer != nil || asked[0].Verdict != nil || asked[1].Peer == nil || !bytes.Equal(asked[1].Peer.PeerStatic, f.node.PublicKey().Bytes()) {
		t.Fatalf("client requests %+v", asked)
	}
	if srvReq.Verdict != nil {
		t.Fatalf("server request with a verdict %+v", srvReq.Verdict)
	}
	if srvReq.Message != 2 || srvReq.Peer == nil || !bytes.Equal(srvReq.Peer.PSKIdentity, f.handle) || !appDataEqual(srvReq.Peer.PeerAppData, judged[:1]) {
		t.Fatalf("server request %+v", srvReq)
	}
}

// The message 3 source sees the Anchor's verdict of the server: its
// principal, proof key and value; without an Anchor, none.
func TestAppDataVerdict(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	f := newFixture(t)
	pk, _ := seal.GenerateSigningKey(proto.KeyAlgMLDSA44)
	id := pk.ID()
	cc, sc := f.client, f.server
	sc.Signer = seal.KeySigner(pk)
	cc.Anchor = verdictAnchor{seal.Verdict{Principal: seal.Principal{Type: 9, Bytes: []byte("node 7")}, ProofKey: &id, Value: "tenant"}}
	var got []seal.AppDataRequest
	cc.AppData = func(_ context.Context, r seal.AppDataRequest) ([]byte, error) { got = append(got, r); return nil, nil }
	x, y := net.Pipe()
	cp, sp, cerr, serr := establishPair(t, ctx, &cc, &sc, zmux.NetPipe(x), zmux.NetPipe(y))
	if cerr != nil || serr != nil {
		t.Fatal(cerr, serr)
	}
	_ = cp.W.Close()
	_ = sp.W.Close()
	v := got[1].Verdict
	if len(got) != 2 || got[0].Verdict != nil || v == nil || string(v.Principal.Bytes) != "node 7" || v.ProofKey == nil || *v.ProofKey != id || v.Value != "tenant" {
		t.Fatalf("requests %+v", got)
	}
	cc.Anchor, cc.AllowPSKOnly, got = nil, true, nil
	x, y = net.Pipe()
	cp, sp, cerr, serr = establishPair(t, ctx, &cc, &sc, zmux.NetPipe(x), zmux.NetPipe(y))
	if cerr != nil || serr != nil {
		t.Fatal(cerr, serr)
	}
	_ = cp.W.Close()
	_ = sp.W.Close()
	if len(got) != 2 || got[1].Verdict != nil {
		t.Fatalf("AllowPSKOnly requests %+v", got)
	}
}

type verdictAnchor struct{ v seal.Verdict }

func (a verdictAnchor) Verify(context.Context, seal.VerifyInput) (seal.Verdict, error) {
	return a.v, nil
}

// Message 1 of Profiles 4 and 5 carries no data; a source's error ends the
// handshake; data above the receiver's bound, or more than the message can
// hold, is too large.
func TestAppDataLimits(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	f := newFixture(t)
	cc, sc := f.client, f.server
	cc.Handle, cc.PSK, cc.AllowedProfiles = nil, nil, []proto.Profile{proto.ProfileEstablish}
	sc.AcceptedProfiles = []proto.Profile{proto.ProfileEstablish}
	var msgs []int
	cc.AppData = func(_ context.Context, r seal.AppDataRequest) ([]byte, error) {
		msgs = append(msgs, r.Message)
		return nil, nil
	}
	x, y := net.Pipe()
	cp, sp, cerr, serr := establishPair(t, ctx, &cc, &sc, zmux.NetPipe(x), zmux.NetPipe(y))
	if cerr != nil || serr != nil {
		t.Fatal(cerr, serr)
	}
	_ = cp.W.Close()
	_ = sp.W.Close()
	if len(msgs) != 1 || msgs[0] != 3 {
		t.Fatalf("asked for %v", msgs)
	}

	cc = f.client
	cc.AppData = func(context.Context, seal.AppDataRequest) ([]byte, error) { return nil, errors.New("no data") }
	x, _ = net.Pipe()
	if _, err := seal.Establish(ctx, zmux.NetPipe(x), &cc); err == nil {
		t.Fatal("source error ignored")
	}

	cc.AppData = func(context.Context, seal.AppDataRequest) ([]byte, error) { return make([]byte, 300), nil }
	x, y = net.Pipe()
	_, _, _, serr = establishPair(t, ctx, &cc, &f.server, zmux.NetPipe(x), zmux.NetPipe(y))
	if !errors.Is(serr, proto.ErrTooLarge) {
		t.Fatalf("over the bound: %v", serr)
	}

	// A negative bound leaves only the message's.
	sc = f.server
	sc.MaxAppData = -1
	x, y = net.Pipe()
	cp, sp, cerr, serr = establishPair(t, ctx, &cc, &sc, zmux.NetPipe(x), zmux.NetPipe(y))
	if cerr != nil || serr != nil {
		t.Fatalf("no bound: %v %v", cerr, serr)
	}
	_ = cp.W.Close()
	_ = sp.W.Close()

	cc.AppData = func(context.Context, seal.AppDataRequest) ([]byte, error) { return make([]byte, 70000), nil }
	x, _ = net.Pipe()
	if _, err := seal.Establish(ctx, zmux.NetPipe(x), &cc); !errors.Is(err, proto.ErrTooLarge) {
		t.Fatalf("too large to send: %v", err)
	}
}

type recordAnchor struct{ got *seal.VerifyInput }

func (a recordAnchor) Verify(_ context.Context, in seal.VerifyInput) (seal.Verdict, error) {
	*a.got = in
	return seal.Verdict{}, nil
}

func appDataEqual(a, b []seal.AppData) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Message != b[i].Message || !bytes.Equal(a[i].Data, b[i].Data) || a[i].ForwardSecret != b[i].ForwardSecret || a[i].Authenticated != b[i].Authenticated {
			return false
		}
	}
	return true
}
