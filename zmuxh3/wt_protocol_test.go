package zmuxh3

import (
	"bytes"
	"errors"
	"io"
	"maps"
	"math"
	"strings"
	"testing"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/quic-go/quicvarint"
)

func TestDraft16Settings(t *testing.T) {
	got := wtAdvertisedSettings(map[uint64]uint64{wtLegacyMaxSessions: 9, wtInitialMaxData: 1024, 0x40: 9})
	want := map[uint64]uint64{0x2c7cf000: 1, 0x2b603742: 1, 0x2b603743: 1, 0x14e9cd29: 1, 0xc671706a: 1, 0x40: 9}
	if !maps.Equal(got, want) {
		t.Fatalf("advertised SETTINGS %v, want %v", got, want)
	}
	var state quic.ConnectionState
	state.SupportsDatagrams.Local, state.SupportsDatagrams.Remote = true, true
	for _, tt := range []struct {
		name                                   string
		other                                  map[uint64]uint64
		want                                   error
		client, legacy, h3Datagrams, h3Connect bool
	}{
		{name: "draft-16", other: map[uint64]uint64{0x2c7cf000: 1, 0x40: 9}, client: true, h3Datagrams: true, h3Connect: true},
		{name: "legacy enable", other: map[uint64]uint64{0x2b603742: 1}, client: true, h3Datagrams: true, h3Connect: true},
		{name: "legacy Chromium max", other: map[uint64]uint64{0x14e9cd29: 3}, client: true, h3Datagrams: true, h3Connect: true},
		{name: "legacy Quinn max", other: map[uint64]uint64{0xc671706a: 3}, client: true, h3Datagrams: true, h3Connect: true},
		{name: "legacy draft-02 max", other: map[uint64]uint64{0x2b603743: 3}, client: true, h3Datagrams: true, h3Connect: true},
		{name: "legacy client token", legacy: true, h3Datagrams: true},
		{name: "new client missing enable", h3Datagrams: true, want: errWTSettings},
		{name: "invalid modern value", other: map[uint64]uint64{0x2c7cf000: 2, 0x2b603742: 1}, h3Datagrams: true, want: errWTSettingsValue},
		{name: "no extended connect", other: map[uint64]uint64{0x2c7cf000: 1}, client: true, h3Datagrams: true, want: errWTSettings},
		{name: "no HTTP datagrams", other: map[uint64]uint64{0x2c7cf000: 1}, h3Connect: true, want: errWTSettings},
	} {
		t.Run(tt.name, func(t *testing.T) {
			settings := &http3.Settings{EnableDatagrams: tt.h3Datagrams, EnableExtendedConnect: tt.h3Connect, Other: tt.other}
			if err := checkWTSettings(settings, state, tt.client, tt.legacy); !errors.Is(err, tt.want) {
				t.Fatalf("SETTINGS error %v, want %v", err, tt.want)
			}
		})
	}
	settings := &http3.Settings{EnableDatagrams: true, EnableExtendedConnect: true, Other: map[uint64]uint64{wtEnabled: 1}}
	state.SupportsDatagrams.Remote = false
	if got := checkWTSettings(settings, state, true, false); !errors.Is(got, errWTSettings) {
		t.Fatalf("without QUIC DATAGRAM support: %v", got)
	}
}

func TestDraft16StreamErrors(t *testing.T) {
	for _, tt := range []struct {
		code StreamErrorCode
		wire quic.StreamErrorCode
	}{
		{0, 0x52e4a40fa8db}, {29, 0x52e4a40fa8f8}, {30, 0x52e4a40fa8fa}, {math.MaxUint32, 0x52e5ac983162},
	} {
		if got := wtWireError(tt.code); got != tt.wire {
			t.Errorf("stream code %d encoded as %#x, want %#x", tt.code, got, tt.wire)
		}
		if got, ok := errors.AsType[*StreamError](wtStreamError(&quic.StreamError{ErrorCode: tt.wire, Remote: true})); !ok || got.ErrorCode != tt.code || !got.Remote {
			t.Errorf("wire code %#x decoded as %v", tt.wire, got)
		}
	}
	for _, code := range []quic.StreamErrorCode{wtFirstAppError - 1, 0x52e4a40fa8f9, wtLastAppError + 1, wtSessionGone, wtBufferedStreamRejected} {
		original := &quic.StreamError{ErrorCode: code, Remote: true}
		if got := wtStreamError(original); !errors.Is(got, original) {
			t.Errorf("transport or reserved code %#x mapped to %v", code, got)
		}
	}
}

func TestDraft16Capsules(t *testing.T) {
	reason := wtReason("x" + strings.Repeat("\u20ac", 1000))
	if len(reason) != 1024 || !strings.HasSuffix(reason, "\u20ac") {
		t.Fatalf("truncated to %d bytes", len(reason))
	}
	want := []byte{0x68, 0x43, 0x07, 0, 0, 0, 42, 'b', 'y', 'e'}
	if got := wtAppendClose(nil, 42, "bye"); !bytes.Equal(got, want) {
		t.Fatalf("close capsule %x, want %x", got, want)
	}
	for _, tt := range []struct {
		name string
		wire []byte
		want error
	}{
		{"FIN at boundary", nil, io.EOF},
		{"truncated type", []byte{0x68}, errWTCapsule},
		{"truncated length", want[:2], errWTCapsule},
		{"truncated payload", want[:len(want)-1], errWTCapsule},
		{"short close", []byte{0x68, 0x43, 3, 0, 0, 0}, errWTCapsule},
		{"invalid UTF-8", []byte{0x68, 0x43, 5, 0, 0, 0, 42, 0xff}, errWTCapsule},
		{"oversize close", wtAppendClose(nil, 0, strings.Repeat("x", 1025)), errWTCapsule},
		{"drain with payload", []byte{0x80, 0, 0x78, 0xae, 1, 0}, errWTCapsule},
		{"unknown truncated", []byte{4, 3, 0}, errWTCapsule},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := readWTCapsule(bytes.NewReader(tt.wire))
			if !errors.Is(err, tt.want) {
				t.Fatalf("capsule error %v, want %v", err, tt.want)
			}
		})
	}
	// Ignore an unused, nonempty flow control capsule before a valid close.
	wire := quicvarint.Append(nil, 0x190b4d3f)
	wire = append(wire, 1, 0xff)
	r := bytes.NewReader(append(wire, want...))
	if kind, closed, err := readWTCapsule(r); kind != 0x190b4d3f || closed != nil || err != nil {
		t.Fatalf("flow capsule: %x, %v, %v", kind, closed, err)
	}
	if kind, closed, err := readWTCapsule(r); kind != wtClose || err != nil ||
		*closed != (SessionError{ErrorCode: 42, Message: "bye", Remote: true}) {
		t.Fatalf("close capsule: %x, %v, %v", kind, closed, err)
	}
}
