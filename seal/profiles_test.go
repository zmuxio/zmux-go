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

func establishProfile(t *testing.T, ctx context.Context, cc *seal.ClientConfig, sc *seal.ServerConfig) (proto.Profile, error, error) {
	t.Helper()
	a, b := net.Pipe()
	cp, sp, cerr, serr := establishPair(t, ctx, cc, sc, zmux.NetPipe(a), zmux.NetPipe(b))
	if cerr != nil || serr != nil {
		_ = a.Close()
		_ = b.Close()
		return 0, cerr, serr
	}
	defer func() { _ = cp.W.Close(); _ = sp.W.Close() }()
	echo1(t, cp, sp)
	peer, ok := seal.PeerOf(cp)
	if !ok {
		t.Fatal("no peer")
	}
	return peer.Profile, nil, nil
}

// echo1 moves one record each way over an established pair.
func echo1(t *testing.T, cp, sp zmux.PipeIO) {
	t.Helper()
	msg := []byte("record over the carrier")
	for _, d := range [][2]zmux.PipeIO{{cp, sp}, {sp, cp}} {
		w, ok1 := d[0].W.(zmux.MessageWriter)
		r, ok2 := d[1].R.(zmux.MessageReader)
		if !ok1 || !ok2 {
			t.Fatal("not a message carrier")
		}
		go func() { _ = w.WriteMessage(msg) }()
		got, err := r.ReadMessage()
		if err != nil || string(got) != string(msg) {
			t.Fatalf("read %q: %v", got, err)
		}
	}
}

func rejectReason(err error) string {
	var rej *zmux.RejectError
	if errors.As(err, &rej) {
		return rej.Reason
	}
	return ""
}

// A client chooses a Profile with a pre-shared key by default; every Profile
// works when both ends list it.
func TestProfileChoice(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	f := newFixture(t)
	got, cerr, serr := establishProfile(t, ctx, &f.client, &f.server)
	if cerr != nil || serr != nil {
		t.Fatal(cerr, serr)
	}
	if got != proto.ProfileEstablishPSK && got != proto.ProfileEstablishPSKAES {
		t.Fatalf("default profile %v", got)
	}
	for _, p := range []proto.Profile{proto.ProfileEstablishPSK, proto.ProfileEstablishPSKAES, proto.ProfileEstablish, proto.ProfileEstablishAES} {
		f := newFixture(t)
		f.server.AcceptedProfiles = []proto.Profile{p}
		f.client.AllowedProfiles = []proto.Profile{p}
		if !p.HasPSK() {
			f.client.Handle, f.client.PSK = nil, nil
		}
		got, cerr, serr := establishProfile(t, ctx, &f.client, &f.server)
		if cerr != nil || serr != nil || got != p {
			t.Fatalf("%v: got %v, %v %v", p, got, cerr, serr)
		}
	}
}

func TestServerProfilePolicy(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for _, tc := range []struct {
		name    string
		client  []proto.Profile
		noPSK   bool
		refused bool
		server  func(*seal.ServerConfig)
	}{
		{"no-psk-off-by-default", []proto.Profile{proto.ProfileEstablish}, true, true, func(*seal.ServerConfig) {}},
		{"no-psk-listed", []proto.Profile{proto.ProfileEstablishAES}, true, false, func(c *seal.ServerConfig) { c.AcceptedProfiles = []proto.Profile{proto.ProfileEstablishAES} }},
		{"accepted-list", []proto.Profile{proto.ProfileEstablishPSKAES}, false, true, func(c *seal.ServerConfig) {
			c.AcceptedProfiles = []proto.Profile{proto.ProfileEstablishPSK}
		}},
	} {
		f := newFixture(t)
		f.client.AllowedProfiles = tc.client
		if tc.noPSK {
			f.client.Handle, f.client.PSK = nil, nil
		}
		tc.server(&f.server)
		_, cerr, serr := establishProfile(t, ctx, &f.client, &f.server)
		if tc.refused {
			if cerr == nil || rejectReason(serr) != "profile_disabled" {
				t.Fatalf("%s: client %v, server %v", tc.name, cerr, serr)
			}
		} else if cerr != nil || serr != nil {
			t.Fatalf("%s: client %v, server %v", tc.name, cerr, serr)
		}
	}
}
