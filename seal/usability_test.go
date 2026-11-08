package seal_test

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/seal"
	"github.com/zmuxio/zmux-go/seal/proto"
)

// A ServerConfig is validated when the listener is made.
func TestServerConfigValidate(t *testing.T) {
	f := newFixture(t)
	for _, c := range []struct {
		name string
		mod  func(*seal.ServerConfig)
		ok   bool
	}{
		{"default", func(*seal.ServerConfig) {}, true},
		{"no Key", func(s *seal.ServerConfig) { s.Key = nil }, false},
		{"no Gate", func(s *seal.ServerConfig) { s.Gate = nil }, false},
		{"Join among the Profiles", func(s *seal.ServerConfig) { s.AcceptedProfiles = []proto.Profile{1, 2} }, false},
		{"unknown Profile, not first", func(s *seal.ServerConfig) { s.AcceptedProfiles = []proto.Profile{1, 3, 77} }, false},
		{"unassigned Profile", func(s *seal.ServerConfig) { s.AcceptedProfiles = []proto.Profile{6} }, false},
		{"explicit Profiles", func(s *seal.ServerConfig) { s.AcceptedProfiles = []proto.Profile{4} }, true},
	} {
		sc := f.server
		c.mod(&sc)
		if err := sc.Validate(); c.ok != (err == nil) || err != nil && !errors.Is(err, proto.ErrInvalidConfig) {
			t.Errorf("%s: %v", c.name, err)
		}
		l, err := zmux.NewListener(&zmux.ListenerConfig{WrapPipe: &sc})
		if c.ok != (err == nil) {
			t.Errorf("%s: NewListener: %v", c.name, err)
		}
		if l != nil {
			_ = l.Close()
		}
	}
}

// profileGate records the Profile Credential is asked for.
type profileGate struct {
	gate
	got proto.Profile
}

func (g *profileGate) Credential(_ context.Context, r seal.CredentialRequest) (*proto.Credential, error) {
	g.got = r.Profile
	return g.cred, nil
}

// A refusal carries its local reason and error; the client, which gets no
// message 2, fails with NoReply; the gate learns the Profile.
func TestRejectReasonsAndNoReply(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	f := newFixture(t)
	for _, c := range []struct {
		name   string
		client func(*seal.ClientConfig)
		server func(*seal.ServerConfig)
		reason string
	}{
		{"unknown handle", func(c *seal.ClientConfig) { c.Handle = []byte("nobody-knows-me!") }, nil, "unknown_handle"},
		{"no PSK by default", func(c *seal.ClientConfig) { c.Handle, c.PSK = nil, nil }, nil, "profile_disabled"},
		{"Profile not listed", nil, func(s *seal.ServerConfig) { s.AcceptedProfiles = []proto.Profile{4, 5} }, "profile_disabled"},
		{"record mode", func(c *seal.ClientConfig) { c.Mode = zmux.Trusted }, nil, "record_mode"},
	} {
		cc, sc := f.client, f.server
		if c.client != nil {
			c.client(&cc)
		}
		if c.server != nil {
			c.server(&sc)
		}
		a, b := net.Pipe()
		_, _, cerr, serr := establishPair(t, ctx, &cc, &sc, zmux.NetPipe(a), zmux.NetPipe(b))
		var rej *zmux.RejectError
		if !errors.As(serr, &rej) || rej.Reason != c.reason {
			t.Errorf("%s: server %v", c.name, serr)
		}
		if c.reason == "unknown_handle" && (rej == nil || rej.Err == nil || errors.Unwrap(rej) == nil) {
			t.Errorf("%s: no underlying error", c.name)
		}
		if !errors.Is(cerr, seal.ErrNoReply) || !strings.Contains(cerr.Error(), "Handle is unknown") {
			t.Errorf("%s: client %v", c.name, cerr)
		}
	}
	g := &profileGate{gate: gate{handle: f.handle, psk: f.psk}}
	sc := f.server
	sc.Gate = g
	a, b := net.Pipe()
	cp, sp, cerr, serr := establishPair(t, ctx, &f.client, &sc, zmux.NetPipe(a), zmux.NetPipe(b))
	if cerr != nil || serr != nil {
		t.Fatal(cerr, serr)
	}
	_ = cp.W.Close()
	_ = sp.W.Close()
	if g.got != proto.ProfileEstablishPSK && g.got != proto.ProfileEstablishPSKAES {
		t.Errorf("Credential asked for Profile %d", g.got)
	}
}

// A Handle without a pre-shared key is named as such.
func TestHandleWithoutPSK(t *testing.T) {
	f := newFixture(t)
	cc := f.client
	cc.PSK = nil
	a, b := net.Pipe()
	defer func() { _ = b.Close() }()
	_, err := seal.Establish(context.Background(), zmux.NetPipe(a), &cc)
	if !errors.Is(err, proto.ErrInvalidConfig) || !strings.Contains(err.Error(), "Handle set without a pre-shared key") {
		t.Fatal(err)
	}
}

type panicAnchor struct{}

func (panicAnchor) Verify(context.Context, seal.VerifyInput) (seal.Verdict, error) {
	panic("anchor")
}

// A Profile without a pre-shared key is accepted only when listed.
func TestNoPSKProfileListed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	f := newFixture(t)
	cc, sc := f.client, f.server
	cc.Handle, cc.PSK = nil, nil
	sc.AcceptedProfiles = []proto.Profile{proto.ProfileEstablish, proto.ProfileEstablishAES}
	a, b := net.Pipe()
	cp, sp, cerr, serr := establishPair(t, ctx, &cc, &sc, zmux.NetPipe(a), zmux.NetPipe(b))
	if cerr != nil || serr != nil {
		t.Fatal(cerr, serr)
	}
	_ = cp.W.Close()
	_ = sp.W.Close()
}
