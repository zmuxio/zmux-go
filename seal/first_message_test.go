package seal_test

import (
	"bytes"
	"context"
	"crypto/ecdh"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/admit"
	"github.com/zmuxio/zmux-go/internal/firstmsg"
	"github.com/zmuxio/zmux-go/internal/prefixtest"
	"github.com/zmuxio/zmux-go/seal"
	"github.com/zmuxio/zmux-go/seal/proto"
)

type anonymousVector struct {
	Name, Framing     string
	Handshake         string
	AcceptProfile     *bool    `json:"accept_profile"`
	CapacityAvailable *bool    `json:"capacity_available"`
	AppendToHello     string   `json:"append_to_hello_hex"`
	TruncateNoise     int      `json:"truncate_noise_msg1_to"`
	AppendToNoise     string   `json:"append_to_noise_msg1_hex"`
	KEMCoefficient    uint16   `json:"replace_first_kem_coefficient"`
	Flights           []string `json:"expect_flights"`
}

type outputVector struct {
	Name, Handshake, Negative, Prefix string
	ServerFlights                     []string `json:"expect_server_flights"`
	OuterUpgrade                      bool     `json:"outer_http_upgrade"`
	Status                            int      `json:"host_status"`
	WantStatus                        int      `json:"expect_host_status"`
	WantInner                         string   `json:"expect_inner_output_hex"`
	Then                              string
}

type establishVector struct {
	Name, Handle, PSK, Binding string
	ResponderStatic            string `json:"responder_static"`
	Profile                    proto.Profile
	Mode                       proto.Mode
	Messages                   []struct {
		Stream  string `json:"wire_stream"`
		Message string `json:"wire_message"`
	}
}

func readFirstMessageJSON(t *testing.T, raw []byte, err error, out any) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, out); err != nil {
		t.Fatal(err)
	}
}

func firstMessageCases(t *testing.T) (anonymous []anonymousVector, responses []outputVector) {
	t.Helper()
	var cases struct {
		Anonymous struct{ Cases []anonymousVector }
		Responses []outputVector
	}
	raw, err := firstmsg.Fixtures.ReadFile("testdata/first_message_cases.json")
	readFirstMessageJSON(t, raw, err, &cases)
	return cases.Anonymous.Cases, cases.Responses
}

func establishVectors(t *testing.T) map[string]establishVector {
	t.Helper()
	var entries []establishVector
	raw, err := os.ReadFile("proto/testdata/vectors/handshake.json")
	readFirstMessageJSON(t, raw, err, &entries)
	out := make(map[string]establishVector, len(entries))
	for _, v := range entries {
		out[v.Name] = v
	}
	return out
}

func firstMessageHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func (v establishVector) server(t *testing.T) seal.ServerConfig {
	t.Helper()
	key, err := ecdh.X25519().NewPrivateKey(firstMessageHex(t, v.ResponderStatic))
	if err != nil {
		t.Fatal(err)
	}
	return seal.ServerConfig{StaticKey: key, Gate: &gate{handle: firstMessageHex(t, v.Handle), psk: firstMessageHex(t, v.PSK)},
		AcceptedProfiles: []proto.Profile{v.Profile}, AllowRecordMode: anyMode}
}

func (v establishVector) hello(t *testing.T, f proto.Framing) []byte {
	t.Helper()
	switch f {
	case proto.Stream:
		return firstMessageHex(t, v.Messages[0].Stream)
	default:
		return firstMessageHex(t, v.Messages[0].Message)
	}
}

type recordEstablish struct {
	*bytes.Reader
	stream io.Reader
	out    [][]byte
	tls    tls.ConnectionState
	endpoints
}

type endpoints struct{ exporter []byte }

func (e endpoints) CarrierSecure() bool { return len(e.exporter) != 0 }
func (e endpoints) ExportKeyingMaterial(string, []byte, int) ([]byte, error) {
	return e.exporter, nil
}

func (c *recordEstablish) Read(p []byte) (int, error) {
	if c.stream != nil {
		return c.stream.Read(p)
	}
	return c.Reader.Read(p)
}

func (c *recordEstablish) Close() error { return nil }

func (c *recordEstablish) ConnectionState() tls.ConnectionState { return c.tls }

func (c *recordEstablish) Write(p []byte) (int, error) {
	c.out = append(c.out, slices.Clone(p))
	return len(p), nil
}

func (c *recordEstablish) ReadMessage(limit int) (zmux.ReceivedMessage, error) {
	if c.Len() == 0 {
		return zmux.ReceivedMessage{}, io.EOF
	}
	if c.Len() > limit {
		return zmux.ReceivedMessage{}, zmux.ErrMessageTooLarge
	}
	msg, err := io.ReadAll(c.Reader)
	return zmux.OwnMessage(msg), err
}

func (c *recordEstablish) WriteMessage(msg []byte) error {
	_, err := c.Write(msg)
	return err
}

func (c *recordEstablish) Pipe(f proto.Framing) zmux.PipeIO {
	framing := zmux.FramingByteStream
	if f == proto.Message {
		framing = zmux.FramingRecords
	}
	return zmux.PipeIO{R: c, W: c, Framing: framing}
}

type secureMessage struct {
	*msgEnd
	endpoints
}

func TestFirstMessageAnonymousScenarios(t *testing.T) {
	cases, _ := firstMessageCases(t)
	vectors := establishVectors(t)
	for _, c := range cases {
		for _, f := range []proto.Framing{proto.Stream, proto.Message} {
			if c.Framing == "stream" && f != proto.Stream {
				continue
			}
			t.Run(c.Name+"/"+firstMessageFraming(f), func(t *testing.T) {
				name := c.Handshake
				if name == "" {
					name = "profile4-sealed-empty"
				}
				v := vectors[name]
				if len(c.Flights) == 3 {
					runAnonymousExchange(t, v, f)
					return
				}
				wire := editAnonymousHello(t, v.hello(t, f), c, f)
				cfg := v.server(t)
				if c.AcceptProfile != nil && !*c.AcceptProfile {
					cfg.AcceptedProfiles = nil
				}
				carrier := &recordEstablish{Reader: bytes.NewReader(wire)}
				if c.CapacityAvailable != nil && !*c.CapacityAvailable {
					holdAnonymousReply(t, &cfg, v.hello(t, proto.Stream))
				}
				_, err := cfg.WrapPipe(t.Context(), carrier.Pipe(f), zmux.PipeHint{})
				if err == nil || len(carrier.out) != 0 {
					t.Fatalf("rejection: %v, emitted %d flights", err, len(carrier.out))
				}
			})
		}
	}
}

func firstMessageFraming(f proto.Framing) string {
	if f == proto.Stream {
		return "stream"
	}
	return "message"
}

func editAnonymousHello(t *testing.T, wire []byte, v anonymousVector, f proto.Framing) []byte {
	t.Helper()
	h, _, err := proto.ParseHelloExact(wire, f)
	if err != nil {
		t.Fatal(err)
	}
	h.Msg1 = slices.Clone(h.Msg1)
	if v.TruncateNoise != 0 {
		h.Msg1 = h.Msg1[:v.TruncateNoise]
	}
	h.Msg1 = append(h.Msg1, firstMessageHex(t, v.AppendToNoise)...)
	if v.KEMCoefficient != 0 {
		h.Msg1[32] = byte(v.KEMCoefficient)
		h.Msg1[33] = h.Msg1[33]&0xf0 | byte(v.KEMCoefficient>>8)
	}
	wire, err = proto.AppendHello(nil, f, h)
	if err != nil {
		t.Fatal(err)
	}
	return append(wire, firstMessageHex(t, v.AppendToHello)...)
}

func runAnonymousExchange(t *testing.T, v establishVector, f proto.Framing) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	sc := v.server(t)
	cc := seal.ClientConfig{StaticKey: newKey(t), AllowedProfiles: []proto.Profile{v.Profile}, RecordMode: zmux.RecordMode(v.Mode)}
	id, err := proto.KeyIDOf(proto.KeyAlgX25519, sc.StaticKey.PublicKey().Bytes())
	if err != nil {
		t.Fatal(err)
	}
	cc.Anchor = seal.PinAnchor(id)
	binding := endpoints{exporter: firstMessageHex(t, v.Binding)}
	var client, server zmux.PipeIO
	if f == proto.Message {
		a, b := msgPair()
		aSecure := &secureMessage{msgEnd: a, endpoints: binding}
		bSecure := &secureMessage{msgEnd: b, endpoints: binding}
		client = zmux.PipeIO{R: aSecure, W: aSecure, Framing: zmux.FramingRecords}
		server = zmux.PipeIO{R: bSecure, W: bSecure, Framing: zmux.FramingRecords}
	} else {
		a, b := net.Pipe()
		client = zmux.NetPipe(&secureConn{Conn: a, exp: binding.exporter})
		server = zmux.NetPipe(&secureConn{Conn: b, exp: binding.exporter})
	}
	defer zmux.ClosePipeIO(client)
	defer zmux.ClosePipeIO(server)
	cp, sp, cerr, serr := establishPair(t, ctx, &cc, &sc, client, server)
	if cerr != nil || serr != nil {
		t.Fatalf("Establish: %v / %v", cerr, serr)
	}
	defer zmux.ClosePipeIO(cp)
	defer zmux.ClosePipeIO(sp)
	for _, p := range []zmux.PipeIO{cp, sp} {
		peer, ok := seal.PeerOf(p)
		if !ok || peer.Profile != v.Profile || peer.RecordMode != zmux.RecordMode(v.Mode) {
			t.Fatalf("peer %+v, %v", peer, ok)
		}
	}
}

type blockedSigner struct {
	seal.Signer
	started chan struct{}
	release chan struct{}
}

func (s *blockedSigner) SignProof(ctx context.Context, m []byte) ([]byte, error) {
	close(s.started)
	<-s.release
	return s.Signer.SignProof(ctx, m)
}

func holdAnonymousReply(t *testing.T, cfg *seal.ServerConfig, wire []byte) {
	t.Helper()
	// Use the fixed stream Hello to occupy one permit while the second
	// admission is attempted on the test's framing.
	limits, err := seal.NewHandshakeLimits(2, 1)
	if err != nil {
		t.Fatal(err)
	}
	cfg.HandshakeLimits = limits
	key, err := seal.GenerateSigningKey(proto.KeyAlgMLDSA44)
	if err != nil {
		t.Fatal(err)
	}
	s := &blockedSigner{Signer: seal.KeySigner(key), started: make(chan struct{}), release: make(chan struct{})}
	cfg.Signer = s
	carrier := &recordEstablish{Reader: bytes.NewReader(wire)}
	done := make(chan struct{})
	t.Cleanup(func() {
		close(s.release)
		<-done
	})
	go func() {
		defer close(done)
		_, _ = cfg.WrapPipe(t.Context(), carrier.Pipe(proto.Stream), zmux.PipeHint{})
	}()
	select {
	case <-s.started:
	case <-done:
		t.Fatal("first anonymous Hello did not reach the signer")
	case <-time.After(5 * time.Second):
		t.Fatal("first anonymous Hello did not reach the signer")
	}
}

func TestFirstMessageEstablishResponses(t *testing.T) {
	_, cases := firstMessageCases(t)
	vectors := establishVectors(t)
	for _, c := range cases {
		if c.Handshake == "" && c.Negative == "" {
			continue
		}
		t.Run(c.Name, func(t *testing.T) {
			name := c.Handshake
			if name == "" {
				name = "profile1-sealed-empty"
			}
			v := vectors[name]
			parts := []io.Reader{bytes.NewReader(v.hello(t, proto.Stream))}
			if c.Negative != "" {
				parts = establishNegativeInput(t, c.Negative)
			}
			sc := v.server(t)
			carrier := &recordEstablish{}
			config := &zmux.ListenerConfig{WrapPipe: &sc, OnReject: func(zmux.RejectInfo) zmux.RejectAction { return zmux.RejectClose }}
			if c.Prefix != "" {
				parts = append([]io.Reader{bytes.NewReader(admitTestCarrier(t, config, carrier, c.Prefix == "bound"))}, parts...)
			}
			l, err := zmux.NewListener(config)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = l.Close() }()
			carrier.stream = io.MultiReader(parts...)
			err = l.HandlePipe(t.Context(), carrier.Pipe(proto.Stream), zmux.PipeHint{})
			if err == nil || len(carrier.out) != len(c.ServerFlights) || c.Prefix != "" && l.Stats().Admission.Accepted != 1 {
				t.Fatalf("output flights %d, err %v, want %v", len(carrier.out), err, c.ServerFlights)
			}
			for i, flight := range c.ServerFlights {
				if flight != "reply" {
					t.Fatalf("unhandled flight %q", flight)
				}
				if _, _, err := proto.CutFrame(carrier.out[i], proto.Stream); err != nil {
					t.Fatalf("reply framing: %v", err)
				}
			}
		})
	}
}

func establishNegativeInput(t *testing.T, name string) []io.Reader {
	t.Helper()
	var entries []struct {
		Name  string
		Steps []struct{ Op, Wire string }
	}
	raw, err := os.ReadFile("proto/testdata/vectors/negative.json")
	readFirstMessageJSON(t, raw, err, &entries)
	for _, v := range entries {
		if v.Name != name {
			continue
		}
		var input []io.Reader
		for _, st := range v.Steps {
			if st.Op == "hello" || st.Op == "read" {
				input = append(input, bytes.NewReader(firstMessageHex(t, st.Wire)))
			}
		}
		return input
	}
	t.Fatalf("no vector %s", name)
	return nil
}

func admitTestCarrier(t *testing.T, config *zmux.ListenerConfig, carrier *recordEstablish, bound bool) []byte {
	t.Helper()
	auth, verifier, _ := prefixtest.New(t, admit.PrefixCarrierOptions{BindTLS: bound, AllowCleartext: !bound}, zmux.VerdictClose)
	config.Admission = verifier.Admission()
	info := &zmux.CarrierInfo{}
	if bound {
		x, y := net.Pipe()
		defer func() { _ = x.Close(); _ = y.Close() }()
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		client := tls.Client(x, &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS13})
		server := tls.Server(y, &tls.Config{Certificates: []tls.Certificate{testCert(t)}, MinVersion: tls.VersionTLS13})
		result := make(chan error, 1)
		go func() { result <- server.HandshakeContext(ctx) }()
		clientErr, serverErr := client.HandshakeContext(ctx), <-result
		if clientErr != nil || serverErr != nil {
			t.Fatalf("TLS: %v / %v", clientErr, serverErr)
		}
		state := client.ConnectionState()
		info.TLS, carrier.tls = &state, server.ConnectionState()
	}
	token, err := auth.Prefix(info)
	if err != nil {
		t.Fatal(err)
	}
	return token
}

// The host may commit an upgrade before running the deferred decision and
// inner handshake. An inner refusal then ends the upgraded carrier silently.
func TestFirstMessageCommittedUpgrade(t *testing.T) {
	anonymous, responses := firstMessageCases(t)
	v := establishVectors(t)["profile4-sealed-empty"]
	for _, c := range responses {
		if !c.OuterUpgrade {
			continue
		}
		t.Run(c.Name, func(t *testing.T) {
			sc := v.server(t)
			a := &zmux.Admission{Peek: 1, Decide: func(_ context.Context, info zmux.CarrierInfo, _ []byte) zmux.Decision {
				if info.Stage == zmux.StageHead {
					return zmux.Decision{Verdict: zmux.VerdictDefer}
				}
				return zmux.Decision{Verdict: zmux.VerdictAccept}
			}}
			l, err := zmux.NewListener(&zmux.ListenerConfig{Admission: a, WrapPipe: &sc,
				OnReject: func(zmux.RejectInfo) zmux.RejectAction { return zmux.RejectClose }})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = l.Close() }()
			w := httptest.NewRecorder()
			arrival, ok := l.Adapter().AdmitUpgrade(w, httptest.NewRequest(http.MethodGet, "/", http.NoBody), nil, nil)
			if !ok {
				t.Fatal("upgrade was not admitted")
			}
			defer arrival.Release()
			w.WriteHeader(c.Status)
			i := slices.IndexFunc(anonymous, func(x anonymousVector) bool { return x.Name == c.Then })
			if i == -1 {
				t.Fatalf("no modified Hello %q", c.Then)
			}
			wire := editAnonymousHello(t, v.hello(t, proto.Message), anonymous[i], proto.Message)
			carrier := &recordEstablish{Reader: bytes.NewReader(wire)}
			err = arrival.HandlePipe(t.Context(), carrier.Pipe(proto.Message), zmux.PipeHint{})
			if err == nil || w.Code != c.WantStatus || !bytes.Equal(bytes.Join(carrier.out, nil), firstMessageHex(t, c.WantInner)) {
				t.Fatalf("upgrade result: %d, %x, %v", w.Code, carrier.out, err)
			}
		})
	}
}
