package zmux

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/state"
	"github.com/zmuxio/zmux-go/internal/testutil"
)

// The wire fixture types list every field the vendored bundle uses, and
// TestWireFixtureFieldsAreAllAsserted rejects any other key, so a new upstream
// expectation fails loudly until the harness asserts it.
type wireFixture struct {
	ID             string             `json:"id"`
	Source         string             `json:"source"`
	Category       string             `json:"category"`
	Hex            string             `json:"hex"`
	Expect         wireFixtureExpect  `json:"expect"`
	ExpectError    string             `json:"expect_error"`
	ReceiverLimits *wireFixtureLimits `json:"receiver_limits"`
	Notes          string             `json:"notes"`
}

type wireFixtureLimits struct {
	MaxFramePayload          uint64 `json:"max_frame_payload"`
	MaxControlPayloadBytes   uint64 `json:"max_control_payload_bytes"`
	MaxExtensionPayloadBytes uint64 `json:"max_extension_payload_bytes"`
}

type wireFixtureExpect struct {
	PrefaceVersion  *uint64                  `json:"preface_ver"`
	Role            string                   `json:"role"`
	TieBreakerNonce *uint64                  `json:"tie_breaker_nonce"`
	MinProto        *uint64                  `json:"min_proto"`
	MaxProto        *uint64                  `json:"max_proto"`
	Capabilities    *uint64                  `json:"capabilities"`
	SettingsLen     *uint64                  `json:"settings_len"`
	Settings        map[string]uint64        `json:"settings"`
	FrameLength     *uint64                  `json:"frame_length"`
	FrameType       string                   `json:"frame_type"`
	Flags           []string                 `json:"flags"`
	StreamID        *uint64                  `json:"stream_id"`
	PayloadHex      *string                  `json:"payload_hex"`
	Decoded         wireFixtureDecodedExpect `json:"decoded"`
}

type wireFixtureDecodedExpect struct {
	MaxOffset                *uint64                   `json:"max_offset"`
	BlockedAt                *uint64                   `json:"blocked_at"`
	ErrorCode                *uint64                   `json:"error_code"`
	DebugText                *string                   `json:"debug_text"`
	DiagBlockDropped         *bool                     `json:"diag_block_dropped"`
	GoAway                   *wireGoAwayExpect         `json:"goaway"`
	ExtType                  string                    `json:"ext_type"`
	ExtTypeValue             *uint64                   `json:"ext_type_value"`
	ApplicationPayloadHex    *string                   `json:"application_payload_hex"`
	StreamMetadataTLVs       []wireMetadataTLVExpect   `json:"stream_metadata_tlvs"`
	OpenMetadataBlockDropped *bool                     `json:"open_metadata_block_dropped"`
	PingPaddingTag           *wirePingPaddingTagExpect `json:"ping_padding_tag"`
}

type wireGoAwayExpect struct {
	LastAcceptedBidiStreamID *uint64 `json:"last_accepted_bidi_stream_id"`
	LastAcceptedUniStreamID  *uint64 `json:"last_accepted_uni_stream_id"`
	ErrorCode                *uint64 `json:"error_code"`
}

type wirePingPaddingTagExpect struct {
	PingPaddingKey uint64 `json:"ping_padding_key"`
	TokenHex       string `json:"token_hex"`
	TagHex         string `json:"tag_hex"`
}

type wireMetadataTLVExpect struct {
	Type     string  `json:"type"`
	Value    *uint64 `json:"value"`
	ValueHex string  `json:"value_hex"`
}

func TestWireValidFixtures(t *testing.T) {
	t.Parallel()
	fixtures := loadWireFixtures(t, "wire_valid.ndjson")
	for _, fixture := range fixtures {
		fixture := fixture
		t.Run(fixture.ID, func(t *testing.T) {
			t.Parallel()
			raw := mustHex(t, fixture.Hex)
			switch fixture.Category {
			case "preface_valid":
				assertPrefaceFixture(t, raw, fixture.Expect)
			case "frame_valid":
				assertFrameFixture(t, raw, fixture.Expect)
			default:
				t.Fatalf("unsupported wire fixture category %q", fixture.Category)
			}
		})
	}
}

func TestWireInvalidFixtures(t *testing.T) {
	t.Parallel()
	fixtures := loadWireFixtures(t, "wire_invalid.ndjson")
	for _, fixture := range fixtures {
		fixture := fixture
		t.Run(fixture.ID, func(t *testing.T) {
			t.Parallel()
			raw := mustHex(t, fixture.Hex)
			_, _, err := ParseFrame(raw, fixtureLimits(fixture.ReceiverLimits))
			if err == nil {
				t.Fatal("expected fixture to fail")
			}
			code, ok := ErrorCodeOf(err)
			if !ok {
				t.Fatalf("expected WireError, got %v", err)
			}
			if got := code.String(); got != fixture.ExpectError {
				t.Fatalf("error code = %s, want %s", got, fixture.ExpectError)
			}
			if fixture.Category != "frame_invalid" {
				return
			}
			// The session reads frames from a stream, not a buffer; both
			// readers must classify a complete invalid frame the same way.
			for name, read := range map[string]func() error{
				"ReadFrame": func() error {
					_, err := ReadFrame(bytes.NewReader(raw), fixtureLimits(fixture.ReceiverLimits))
					return err
				},
				"session reader": func() error {
					_, _, _, err := readSessionFrameBuffered(bytes.NewReader(raw), fixtureLimits(fixture.ReceiverLimits), nil)
					return err
				},
			} {
				err := read()
				code, ok := ErrorCodeOf(err)
				if !ok || code.String() != fixture.ExpectError {
					t.Fatalf("%s err = %v, want %s", name, err, fixture.ExpectError)
				}
			}
		})
	}
}

func TestWireFixtureFieldsAreAllAsserted(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"wire_valid.ndjson", "wire_invalid.ndjson"} {
		file, err := os.Open(filepath.Join(testutil.LocateFixtureDir(t), name))
		if err != nil {
			t.Fatalf("open %s: %v", name, err)
		}
		decoder := json.NewDecoder(file)
		decoder.DisallowUnknownFields()
		for {
			var fixture wireFixture
			err := decoder.Decode(&fixture)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				_ = file.Close()
				t.Fatalf("%s: fixture %q has a field the harness does not assert: %v", name, fixture.ID, err)
			}
		}
		_ = file.Close()
	}
}

func loadWireFixtures(t *testing.T, name string) []wireFixture {
	t.Helper()
	return testutil.LoadFixtureNDJSON[wireFixture](t, name)
}

func fixtureLimits(in *wireFixtureLimits) Limits {
	limits := DefaultSettings().Limits()
	if in == nil {
		return limits
	}
	if in.MaxFramePayload != 0 {
		limits.MaxFramePayload = in.MaxFramePayload
	}
	if in.MaxControlPayloadBytes != 0 {
		limits.MaxControlPayloadBytes = in.MaxControlPayloadBytes
	}
	if in.MaxExtensionPayloadBytes != 0 {
		limits.MaxExtensionPayloadBytes = in.MaxExtensionPayloadBytes
	}
	return limits
}

func assertPrefaceFixture(t *testing.T, raw []byte, expect wireFixtureExpect) {
	t.Helper()

	preface, err := ParsePreface(raw)
	if err != nil {
		t.Fatalf("parse preface: %v", err)
	}
	if expect.PrefaceVersion != nil && uint64(preface.PrefaceVersion) != *expect.PrefaceVersion {
		t.Fatalf("preface_version = %d, want %d", preface.PrefaceVersion, *expect.PrefaceVersion)
	}
	if expect.Role != "" && preface.Role.String() != expect.Role {
		t.Fatalf("role = %q, want %q", preface.Role.String(), expect.Role)
	}
	if expect.TieBreakerNonce != nil && preface.TieBreakerNonce != *expect.TieBreakerNonce {
		t.Fatalf("tie_breaker_nonce = %d, want %d", preface.TieBreakerNonce, *expect.TieBreakerNonce)
	}
	if expect.MinProto != nil && preface.MinProto != *expect.MinProto {
		t.Fatalf("min_proto = %d, want %d", preface.MinProto, *expect.MinProto)
	}
	if expect.MaxProto != nil && preface.MaxProto != *expect.MaxProto {
		t.Fatalf("max_proto = %d, want %d", preface.MaxProto, *expect.MaxProto)
	}
	if expect.Capabilities != nil && uint64(preface.Capabilities) != *expect.Capabilities {
		t.Fatalf("capabilities = %d, want %d", preface.Capabilities, *expect.Capabilities)
	}
	if expect.SettingsLen != nil {
		// settings_len is the raw field: it counts preface_padding, which the
		// parsed preface drops, so re-encoded settings can be shorter.
		if got := rawPrefaceSettingsLen(t, raw); got != *expect.SettingsLen {
			t.Fatalf("settings_len = %d, want %d", got, *expect.SettingsLen)
		}
	}
	if expect.Settings != nil {
		assertPrefaceFixtureSettings(t, preface.Settings, expect.Settings)
	}

	read, err := ReadPreface(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("read preface: %v", err)
	}
	if read != preface {
		t.Fatalf("ReadPreface = %+v, ParsePreface = %+v", read, preface)
	}
	// Padding does not survive decode and re-encode, so round-trip the
	// decoded preface rather than the bytes.
	encoded, err := preface.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal parsed preface: %v", err)
	}
	reparsed, err := ParsePreface(encoded)
	if err != nil {
		t.Fatalf("parse re-encoded preface: %v", err)
	}
	if reparsed != preface {
		t.Fatalf("re-encoded preface = %+v, want %+v", reparsed, preface)
	}
}

// rawPrefaceSettingsLen reads settings_len from the preface bytes:
// magic, preface_ver and role, then the tie_breaker_nonce, min_proto,
// max_proto, capabilities and settings_len varints.
func rawPrefaceSettingsLen(t *testing.T, raw []byte) uint64 {
	t.Helper()

	offset := len(Magic) + 2
	var value uint64
	for i := 0; i < 5; i++ {
		if offset > len(raw) {
			t.Fatalf("preface % x ends before settings_len", raw)
		}
		v, n, err := ParseVarint(raw[offset:])
		if err != nil {
			t.Fatalf("parse preface header varint %d: %v", i, err)
		}
		offset += n
		value = v
	}
	return value
}

var fixtureSettingNames = []string{
	"initial_max_stream_data_bidi_locally_opened",
	"initial_max_stream_data_bidi_peer_opened",
	"initial_max_stream_data_uni",
	"initial_max_data",
	"max_incoming_streams_bidi",
	"max_incoming_streams_uni",
	"max_frame_payload",
	"max_control_payload_bytes",
	"max_extension_payload_bytes",
	"scheduler_hints",
	"ping_padding_key",
}

func fixtureSettingValue(settings Settings, name string) (uint64, bool) {
	switch name {
	case "initial_max_stream_data_bidi_locally_opened":
		return settings.InitialMaxStreamDataBidiLocallyOpened, true
	case "initial_max_stream_data_bidi_peer_opened":
		return settings.InitialMaxStreamDataBidiPeerOpened, true
	case "initial_max_stream_data_uni":
		return settings.InitialMaxStreamDataUni, true
	case "initial_max_data":
		return settings.InitialMaxData, true
	case "max_incoming_streams_bidi":
		return settings.MaxIncomingStreamsBidi, true
	case "max_incoming_streams_uni":
		return settings.MaxIncomingStreamsUni, true
	case "max_frame_payload":
		return settings.MaxFramePayload, true
	case "max_control_payload_bytes":
		return settings.MaxControlPayloadBytes, true
	case "max_extension_payload_bytes":
		return settings.MaxExtensionPayloadBytes, true
	case "scheduler_hints":
		return uint64(settings.SchedulerHints), true
	case "ping_padding_key":
		return settings.PingPaddingKey, true
	default:
		return 0, false
	}
}

// assertPrefaceFixtureSettings checks the settings a fixture lists and that
// every other setting keeps its default.
func assertPrefaceFixtureSettings(t *testing.T, got Settings, expect map[string]uint64) {
	t.Helper()

	for name := range expect {
		if _, ok := fixtureSettingValue(got, name); !ok {
			t.Fatalf("fixture expects unknown setting %q", name)
		}
	}
	defaults := DefaultSettings()
	for _, name := range fixtureSettingNames {
		want, listed := expect[name]
		if !listed {
			want, _ = fixtureSettingValue(defaults, name)
		}
		if value, _ := fixtureSettingValue(got, name); value != want {
			t.Fatalf("setting %s = %d, want %d", name, value, want)
		}
	}
}

func assertFrameFixture(t *testing.T, raw []byte, expect wireFixtureExpect) {
	t.Helper()

	frame, n, err := ParseFrame(raw, DefaultSettings().Limits())
	if err != nil {
		t.Fatalf("parse frame: %v", err)
	}
	if n != len(raw) {
		t.Fatalf("consumed %d bytes, want %d", n, len(raw))
	}

	length, _, err := ParseVarint(raw)
	if err != nil {
		t.Fatalf("parse frame_length: %v", err)
	}
	if expect.FrameLength != nil && length != *expect.FrameLength {
		t.Fatalf("frame_length = %d, want %d", length, *expect.FrameLength)
	}
	if expect.FrameType != "" && frame.Type.String() != expect.FrameType {
		t.Fatalf("frame_type = %q, want %q", frame.Type.String(), expect.FrameType)
	}
	if expect.StreamID != nil && frame.StreamID != *expect.StreamID {
		t.Fatalf("stream_id = %d, want %d", frame.StreamID, *expect.StreamID)
	}
	if expect.Flags != nil {
		gotFlags := frameFlagNames(frame.Flags)
		if !slices.Equal(gotFlags, expect.Flags) {
			t.Fatalf("flags = %v, want %v", gotFlags, expect.Flags)
		}
	}
	if expect.PayloadHex != nil {
		if got := fmt.Sprintf("%x", frame.Payload); got != strings.ToLower(*expect.PayloadHex) {
			t.Fatalf("payload_hex = %s, want %s", got, strings.ToLower(*expect.PayloadHex))
		}
	}

	read, err := ReadFrame(bytes.NewReader(raw), DefaultSettings().Limits())
	if err != nil {
		t.Fatalf("read frame: %v", err)
	}
	if read.Type != frame.Type || read.Flags != frame.Flags || read.StreamID != frame.StreamID || !bytes.Equal(read.Payload, frame.Payload) {
		t.Fatalf("ReadFrame = %+v, ParseFrame = %+v", read, frame)
	}

	assertDecodedFrameFixture(t, frame, expect.Decoded)
}

func assertDecodedFrameFixture(t *testing.T, frame Frame, expect wireFixtureDecodedExpect) {
	t.Helper()

	if expect.MaxOffset != nil || expect.BlockedAt != nil {
		value, _, err := ParseVarint(frame.Payload)
		if err != nil {
			t.Fatalf("parse varint payload: %v", err)
		}
		if expect.MaxOffset != nil && value != *expect.MaxOffset {
			t.Fatalf("max_offset = %d, want %d", value, *expect.MaxOffset)
		}
		if expect.BlockedAt != nil && value != *expect.BlockedAt {
			t.Fatalf("blocked_at = %d, want %d", value, *expect.BlockedAt)
		}
	}
	if expect.ErrorCode != nil || expect.DebugText != nil || expect.DiagBlockDropped != nil {
		assertErrorPayloadFixture(t, frame, expect)
	}
	if expect.GoAway != nil {
		goAway, err := parseGOAWAYPayload(frame.Payload)
		if err != nil {
			t.Fatalf("parse GOAWAY payload: %v", err)
		}
		for _, field := range []struct {
			name string
			got  uint64
			want *uint64
		}{
			{"last_accepted_bidi_stream_id", goAway.LastAcceptedBidi, expect.GoAway.LastAcceptedBidiStreamID},
			{"last_accepted_uni_stream_id", goAway.LastAcceptedUni, expect.GoAway.LastAcceptedUniStreamID},
			{"error_code", goAway.Code, expect.GoAway.ErrorCode},
		} {
			if field.want == nil {
				t.Fatalf("goaway expectation is missing %s", field.name)
			}
			if field.got != *field.want {
				t.Fatalf("goaway %s = %d, want %d", field.name, field.got, *field.want)
			}
		}
	}
	if expect.PingPaddingTag != nil {
		assertPingPaddingTagFixture(t, frame, *expect.PingPaddingTag)
	}

	switch frame.Type {
	case FrameTypeDATA:
		assertDataPayloadFixture(t, frame, expect)
	case FrameTypeEXT:
		extType, n, err := ParseVarint(frame.Payload)
		if err != nil {
			t.Fatalf("parse EXT subtype: %v", err)
		}
		if expect.ExtType != "" && extSubtypeName(extType) != expect.ExtType {
			t.Fatalf("ext_type = %q, want %q", extSubtypeName(extType), expect.ExtType)
		}
		if expect.ExtTypeValue != nil && extType != *expect.ExtTypeValue {
			t.Fatalf("ext_type value = %d, want %d", extType, *expect.ExtTypeValue)
		}
		// Only PRIORITY_UPDATE has a TLV body; other subtypes are opaque.
		if EXTSubtype(extType) == EXTPriorityUpdate {
			tlvs, err := ParseTLVs(frame.Payload[n:])
			if err != nil {
				t.Fatalf("parse EXT TLVs: %v", err)
			}
			assertMetadataTLVs(t, tlvs, expect.StreamMetadataTLVs)
		} else if expect.StreamMetadataTLVs != nil {
			t.Fatalf("stream_metadata_tlvs expected on EXT subtype %d", extType)
		}
	default:
		if expect.ExtType != "" || expect.ExtTypeValue != nil || expect.StreamMetadataTLVs != nil ||
			expect.ApplicationPayloadHex != nil || expect.OpenMetadataBlockDropped != nil {
			t.Fatalf("decoded DATA/EXT expectation unsupported for frame type %s", frame.Type)
		}
	}
}

// assertErrorPayloadFixture checks error_code, debug_text and
// diag_block_dropped of a frame whose payload ends in a DIAG block: the
// error-carrying frames, or GOAWAY after its watermarks.
func assertErrorPayloadFixture(t *testing.T, frame Frame, expect wireFixtureDecodedExpect) {
	t.Helper()

	var (
		code      uint64
		reason    string
		mandatory int
	)
	if frame.Type == FrameTypeGOAWAY {
		goAway, err := parseGOAWAYPayload(frame.Payload)
		if err != nil {
			t.Fatalf("parse %s payload: %v", frame.Type, err)
		}
		code, reason, mandatory = goAway.Code, goAway.Reason, 3
	} else {
		var err error
		code, reason, err = parseErrorPayload(frame.Payload)
		if err != nil {
			t.Fatalf("parse %s payload: %v", frame.Type, err)
		}
		mandatory = 1
	}
	if expect.ErrorCode != nil && code != *expect.ErrorCode {
		t.Fatalf("error_code = %d, want %d", code, *expect.ErrorCode)
	}
	if expect.DebugText != nil && reason != *expect.DebugText {
		t.Fatalf("debug_text = %q, want %q", reason, *expect.DebugText)
	}
	if expect.DiagBlockDropped != nil && *expect.DiagBlockDropped {
		diag := frame.Payload
		for i := 0; i < mandatory; i++ {
			_, n, err := ParseVarint(diag)
			if err != nil {
				t.Fatalf("parse %s mandatory field %d: %v", frame.Type, i, err)
			}
			diag = diag[n:]
		}
		if len(diag) == 0 {
			t.Fatal("diag_block_dropped expected, but the payload carries no DIAG block")
		}
		if reason != "" {
			t.Fatalf("debug_text = %q from a dropped DIAG block, want none", reason)
		}
	}
}

func assertDataPayloadFixture(t *testing.T, frame Frame, expect wireFixtureDecodedExpect) {
	t.Helper()

	payload, err := parseDataPayload(frame.Payload, frame.Flags)
	if err != nil {
		t.Fatalf("parse DATA payload: %v", err)
	}
	assertMetadataTLVs(t, payload.MetadataTLVs, expect.StreamMetadataTLVs)
	if expect.ApplicationPayloadHex != nil {
		if got := fmt.Sprintf("%x", payload.AppData); got != strings.ToLower(*expect.ApplicationPayloadHex) {
			t.Fatalf("application_payload_hex = %s, want %s", got, strings.ToLower(*expect.ApplicationPayloadHex))
		}
	}
	if expect.OpenMetadataBlockDropped == nil {
		return
	}
	metadataLen, _, err := ParseVarint(frame.Payload)
	if err != nil || frame.Flags&FrameFlagOpenMetadata == 0 {
		t.Fatalf("open_metadata_block_dropped expected on DATA without an OPEN_METADATA block (err %v)", err)
	}
	dropped := len(payload.MetadataTLVs) == 0 && !payload.Metadata.HasPriority && !payload.Metadata.HasGroup &&
		payload.Metadata.OpenInfo == nil && payload.OpenInfo == nil
	if metadataLen == 0 {
		t.Fatal("open_metadata_block_dropped expected on an empty metadata block")
	}
	if dropped != *expect.OpenMetadataBlockDropped {
		t.Fatalf("open metadata block dropped = %v, want %v (parsed %+v)", dropped, *expect.OpenMetadataBlockDropped, payload)
	}
}

// assertPingPaddingTagFixture checks the ping_padding_tag known-answer vector
// (SPEC 6.4) against the tag the session uses to recognize padded PINGs.
func assertPingPaddingTagFixture(t *testing.T, frame Frame, expect wirePingPaddingTagExpect) {
	t.Helper()

	token := mustHex(t, expect.TokenHex)
	tag := mustHex(t, expect.TagHex)
	if len(token) != pingNonceBytes || len(tag) != pingPaddingTagBytes {
		t.Fatalf("ping_padding_tag token/tag lengths = %d/%d, want %d/%d", len(token), len(tag), pingNonceBytes, pingPaddingTagBytes)
	}
	if !bytes.HasPrefix(frame.Payload, append(append([]byte(nil), token...), tag...)) {
		t.Fatalf("PING payload %x does not start with token %x and tag %x", frame.Payload, token, tag)
	}
	if got := pingPaddingTag(expect.PingPaddingKey, binary.BigEndian.Uint64(token)); got != binary.BigEndian.Uint64(tag) {
		t.Fatalf("ping_padding_tag(%#x, %x) = %016x, want %x", expect.PingPaddingKey, token, got, tag)
	}
	if !hasPingPaddingTag(frame.Payload, expect.PingPaddingKey) {
		t.Fatal("padded PING is not recognized under its ping_padding_key")
	}
	if hasPingPaddingTag(frame.Payload, expect.PingPaddingKey+1) {
		t.Fatal("padded PING is recognized under a different ping_padding_key")
	}
}

func assertMetadataTLVs(t *testing.T, actual []TLV, expect []wireMetadataTLVExpect) {
	t.Helper()

	if expect == nil {
		return
	}
	if len(actual) != len(expect) {
		t.Fatalf("metadata TLV count = %d, want %d", len(actual), len(expect))
	}
	for i := range expect {
		want := expect[i]
		got := actual[i]
		if gotName := streamMetadataTypeName(got.Type); gotName != want.Type {
			t.Fatalf("metadata TLV %d type = %q, want %q", i, gotName, want.Type)
		}
		if want.Value != nil {
			value, err := parseMetadataVarint(got.Value)
			if err != nil {
				t.Fatalf("parse metadata TLV %d varint: %v", i, err)
			}
			if value != *want.Value {
				t.Fatalf("metadata TLV %d value = %d, want %d", i, value, *want.Value)
			}
		}
		if want.ValueHex != "" {
			if valueHex := fmt.Sprintf("%x", got.Value); valueHex != strings.ToLower(want.ValueHex) {
				t.Fatalf("metadata TLV %d value_hex = %s, want %s", i, valueHex, strings.ToLower(want.ValueHex))
			}
		}
	}
}

func frameFlagNames(flags byte) []string {
	var out []string
	if flags&FrameFlagOpenMetadata != 0 {
		out = append(out, "OPEN_METADATA")
	}
	if flags&FrameFlagFIN != 0 {
		out = append(out, "FIN")
	}
	return out
}

func extSubtypeName(v uint64) string {
	switch EXTSubtype(v) {
	case EXTPriorityUpdate:
		return "PRIORITY_UPDATE"
	default:
		return fmt.Sprintf("ext_subtype(%d)", v)
	}
}

func streamMetadataTypeName(v uint64) string {
	switch StreamMetadataType(v) {
	case MetadataStreamPriority:
		return "stream_priority"
	case MetadataStreamGroup:
		return "stream_group"
	case MetadataOpenInfo:
		return "open_info"
	default:
		return fmt.Sprintf("stream_metadata_type(%d)", v)
	}
}

type caseSetFile struct {
	Sets map[string][]string `json:"sets"`
}

type fixtureIndexFile struct {
	Files []fixtureIndexEntry `json:"files"`
}

type fixtureIndexEntry struct {
	Path  string `json:"path"`
	Kind  string `json:"kind"`
	Count int    `json:"count"`
}

// Keep fixture metadata checks serial.
func TestCaseSetCodecIDsMatchWireFixtures(t *testing.T) {
	sets := loadCaseSets(t)

	validFixtures := loadWireFixtures(t, "wire_valid.ndjson")
	invalidFixtures := loadWireFixtures(t, "wire_invalid.ndjson")

	validIDs := fixtureIDsFromWire(validFixtures)
	invalidIDs := fixtureIDsFromWire(invalidFixtures)

	assertSameIDs(t, "codec_valid", validIDs, sets["codec_valid"])
	assertSameIDs(t, "codec_invalid", invalidIDs, sets["codec_invalid"])
}

func TestStateFixtureSupportMapMatchesVendoredBundle(t *testing.T) {
	assertSupportedFixtureMapMatchesLoadedIDs(
		t,
		"state fixtures",
		fixtureIDsFromState(loadStateFixtures(t)),
		supportedStateFixtureIDs,
	)
}

func TestInvalidFixtureSupportMapMatchesVendoredBundle(t *testing.T) {
	assertSupportedFixtureMapMatchesLoadedIDs(
		t,
		"invalid fixtures",
		fixtureIDsFromInvalid(loadInvalidFixtures(t)),
		supportedInvalidFixtureIDs,
	)
}

func TestInvalidFrameFixtureSupportMapMatchesVendoredBundle(t *testing.T) {
	ids := make([]string, 0, len(loadFrameInvalidFixtures(t)))
	for _, fixture := range loadFrameInvalidFixtures(t) {
		ids = append(ids, fixture.ID)
	}
	assertSupportedFixtureMapMatchesLoadedIDs(
		t,
		"invalid frame fixtures",
		ids,
		supportedFrameInvalidFixtureIDs,
	)
}

func TestCaseSetStreamLifecycleContainsSupportedStateFixtures(t *testing.T) {
	sets := loadCaseSets(t)
	streamLifecycle := make(map[string]struct{}, len(sets["stream_lifecycle"]))
	for _, id := range sets["stream_lifecycle"] {
		streamLifecycle[id] = struct{}{}
	}

	fixtures := loadStateFixtures(t)
	byID := make(map[string]stateFixture, len(fixtures))
	for _, fixture := range fixtures {
		byID[fixture.ID] = fixture
	}
	for id, supported := range supportedStateFixtureIDs {
		if !supported {
			continue
		}
		if fixture, ok := byID[id]; ok && fixture.Scope == "session" {
			continue
		}
		if _, ok := streamLifecycle[id]; !ok {
			t.Fatalf("supported non-session state fixture %q is missing from case_sets.stream_lifecycle", id)
		}
	}
}

func TestCaseSetSessionLifecycleContainsSupportedSessionStateFixtures(t *testing.T) {
	sets := loadCaseSets(t)
	sessionLifecycle := make(map[string]struct{}, len(sets["session_lifecycle"]))
	for _, id := range sets["session_lifecycle"] {
		sessionLifecycle[id] = struct{}{}
	}

	for _, fixture := range loadStateFixtures(t) {
		if !supportedStateFixtureIDs[fixture.ID] || fixture.Scope != "session" {
			continue
		}
		if _, ok := sessionLifecycle[fixture.ID]; !ok {
			t.Fatalf("supported session state fixture %q is missing from case_sets.session_lifecycle", fixture.ID)
		}
	}
}

func TestCaseSetPrefaceContainsSupportedInvalidEstablishmentFixtures(t *testing.T) {
	sets := loadCaseSets(t)
	assertSetContainsSupportedIDs(
		t,
		"preface",
		sets["preface"],
		supportedInvalidFixtureIDs,
		func(id string) bool { return id == "preface_duplicate_setting_id" || strings.HasPrefix(id, "preface_") },
	)
}

func TestCaseSetOpenMetadataContainsSupportedInvalidFrameFixtures(t *testing.T) {
	sets := loadCaseSets(t)
	assertSetContainsSupportedIDs(
		t,
		"open_metadata",
		sets["open_metadata"],
		supportedFrameInvalidFixtureIDs,
		func(id string) bool { return strings.HasPrefix(id, "frame_data_open_metadata_") },
	)
	openMetadata := make(map[string]struct{}, len(sets["open_metadata"]))
	for _, id := range sets["open_metadata"] {
		openMetadata[id] = struct{}{}
	}
	for id, supported := range supportedStateFixtureIDs {
		if !supported || !strings.Contains(id, "open_metadata") {
			continue
		}
		if _, ok := openMetadata[id]; !ok {
			t.Fatalf("supported open_metadata state fixture %q is missing from case_sets.open_metadata", id)
		}
	}
}

func TestCaseSetPriorityUpdateContainsSupportedInvalidFrameFixtures(t *testing.T) {
	sets := loadCaseSets(t)
	assertSetContainsSupportedIDs(
		t,
		"priority_update",
		sets["priority_update"],
		supportedFrameInvalidFixtureIDs,
		func(id string) bool { return strings.HasPrefix(id, "frame_priority_update_") },
	)
}

func TestCaseSetUnidirectionalContainsSupportedInvalidFixtures(t *testing.T) {
	sets := loadCaseSets(t)
	assertSetContainsSupportedIDs(
		t,
		"unidirectional",
		sets["unidirectional"],
		supportedFrameInvalidFixtureIDs,
		func(id string) bool { return strings.HasSuffix(id, "_wrong_side_uni") },
	)
}

func TestCaseSetFlowControlContainsSupportedFixtures(t *testing.T) {
	sets := loadCaseSets(t)
	assertSetContainsSupportedIDs(
		t,
		"flow_control",
		sets["flow_control"],
		supportedFrameInvalidFixtureIDs,
		func(id string) bool {
			return id == "frame_data_exceeds_stream_max_data" || id == "frame_data_exceeds_session_max_data"
		},
	)
	assertSetContainsSupportedIDs(
		t,
		"flow_control",
		sets["flow_control"],
		supportedInvalidFixtureIDs,
		func(id string) bool { return id == "late_data_after_close_read_exceeds_session_aggregate_cap" },
	)
	flowControl := make(map[string]struct{}, len(sets["flow_control"]))
	for _, id := range sets["flow_control"] {
		flowControl[id] = struct{}{}
	}
	for _, fixture := range loadStateFixtures(t) {
		if !supportedStateFixtureIDs[fixture.ID] || fixture.Scope != "flow_control" {
			continue
		}
		if _, ok := flowControl[fixture.ID]; !ok {
			t.Fatalf("supported flow-control state fixture %q is missing from case_sets.flow_control", fixture.ID)
		}
	}
}

func TestFixtureIndexCountsMatchLoadedFixtures(t *testing.T) {
	index := loadFixtureIndex(t)
	gotCounts := map[string]int{
		"wire_valid":    len(loadWireFixtures(t, "wire_valid.ndjson")),
		"wire_invalid":  len(loadWireFixtures(t, "wire_invalid.ndjson")),
		"state_cases":   len(loadStateFixtures(t)),
		"invalid_cases": len(loadInvalidFixtures(t)),
	}

	for kind, want := range gotCounts {
		entry, ok := index[kind]
		if !ok {
			t.Fatalf("fixture index missing kind %q", kind)
		}
		if entry.Count != want {
			t.Fatalf("fixture index count for %s = %d, want %d", kind, entry.Count, want)
		}
	}
}

func TestCaseSetIDsResolveToKnownFixtures(t *testing.T) {
	sets := loadCaseSets(t)
	catalog := loadFixtureCatalog(t)

	for setName, ids := range sets {
		seen := make(map[string]struct{}, len(ids))
		for _, id := range ids {
			if _, ok := catalog[id]; !ok {
				t.Fatalf("case_sets.%s references unknown fixture id %q", setName, id)
			}
			if _, dup := seen[id]; dup {
				t.Fatalf("case_sets.%s repeats fixture id %q", setName, id)
			}
			seen[id] = struct{}{}
		}
	}
}

func TestFixtureIDsAreGloballyUniqueAcrossBundles(t *testing.T) {
	catalog := loadFixtureCatalog(t)

	want := len(loadWireFixtures(t, "wire_valid.ndjson")) +
		len(loadWireFixtures(t, "wire_invalid.ndjson")) +
		len(loadStateFixtures(t)) +
		len(loadInvalidFixtures(t))
	if len(catalog) != want {
		t.Fatalf("fixture catalog size = %d, want %d unique ids", len(catalog), want)
	}
}

func TestFixtureIndexPathsMatchKnownBundleFiles(t *testing.T) {
	index := loadFixtureIndex(t)
	wantPaths := map[string]string{
		"wire_valid":    filepath.Join("fixtures", "wire_valid.ndjson"),
		"wire_invalid":  filepath.Join("fixtures", "wire_invalid.ndjson"),
		"state_cases":   filepath.Join("fixtures", "state_cases.ndjson"),
		"invalid_cases": filepath.Join("fixtures", "invalid_cases.ndjson"),
	}

	if len(index) != len(wantPaths) {
		t.Fatalf("fixture index kinds = %d, want %d", len(index), len(wantPaths))
	}

	for kind, want := range wantPaths {
		entry, ok := index[kind]
		if !ok {
			t.Fatalf("fixture index missing kind %q", kind)
		}
		if got := filepath.ToSlash(entry.Path); got != filepath.ToSlash(want) {
			t.Fatalf("fixture index path for %s = %q, want %q", kind, got, filepath.ToSlash(want))
		}
	}
}

func loadCaseSets(t *testing.T) map[string][]string {
	t.Helper()
	file := testutil.ReadFixtureJSON[caseSetFile](t, "case_sets.json")
	if len(file.Sets) == 0 {
		t.Fatal("no case sets found in case_sets.json")
	}
	return file.Sets
}

func loadFixtureIndex(t *testing.T) map[string]fixtureIndexEntry {
	t.Helper()
	file := testutil.ReadFixtureJSON[fixtureIndexFile](t, "index.json")
	if len(file.Files) == 0 {
		t.Fatal("no fixture index entries found in index.json")
	}

	out := make(map[string]fixtureIndexEntry, len(file.Files))
	for _, entry := range file.Files {
		out[entry.Kind] = entry
	}
	return out
}

func fixtureIDsFromWire(fixtures []wireFixture) []string {
	ids := make([]string, 0, len(fixtures))
	for _, fixture := range fixtures {
		ids = append(ids, fixture.ID)
	}
	return ids
}

func fixtureIDsFromState(fixtures []stateFixture) []string {
	ids := make([]string, 0, len(fixtures))
	for _, fixture := range fixtures {
		ids = append(ids, fixture.ID)
	}
	return ids
}

func fixtureIDsFromInvalid(fixtures []invalidFixture) []string {
	ids := make([]string, 0, len(fixtures))
	for _, fixture := range fixtures {
		ids = append(ids, fixture.ID)
	}
	return ids
}

func loadFixtureCatalog(t *testing.T) map[string]string {
	t.Helper()

	catalog := make(map[string]string)
	addIDs := func(source string, ids []string) {
		for _, id := range ids {
			if prev, ok := catalog[id]; ok {
				t.Fatalf("fixture id %q appears in both %s and %s", id, prev, source)
			}
			catalog[id] = source
		}
	}

	addIDs("wire_valid.ndjson", fixtureIDsFromWire(loadWireFixtures(t, "wire_valid.ndjson")))
	addIDs("wire_invalid.ndjson", fixtureIDsFromWire(loadWireFixtures(t, "wire_invalid.ndjson")))
	addIDs("state_cases.ndjson", fixtureIDsFromState(loadStateFixtures(t)))
	addIDs("invalid_cases.ndjson", fixtureIDsFromInvalid(loadInvalidFixtures(t)))
	return catalog
}

func assertSameIDs(t *testing.T, name string, got, want []string) {
	t.Helper()

	got = append([]string(nil), got...)
	want = append([]string(nil), want...)
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("%s ids = %v, want %v", name, got, want)
	}
}

func assertSetContainsSupportedIDs(t *testing.T, setName string, setIDs []string, supported map[string]bool, pick func(string) bool) {
	t.Helper()

	lookup := make(map[string]struct{}, len(setIDs))
	for _, id := range setIDs {
		lookup[id] = struct{}{}
	}

	for id, ok := range supported {
		if !ok || !pick(id) {
			continue
		}
		if _, present := lookup[id]; !present {
			t.Fatalf("supported fixture %q is missing from case_sets.%s", id, setName)
		}
	}
}

func assertSupportedFixtureMapMatchesLoadedIDs(t *testing.T, name string, loadedIDs []string, supported map[string]bool) {
	t.Helper()

	loaded := make(map[string]struct{}, len(loadedIDs))
	for _, id := range loadedIDs {
		loaded[id] = struct{}{}
	}

	for _, id := range loadedIDs {
		if !supported[id] {
			t.Fatalf("%s support map is missing vendored fixture id %q", name, id)
		}
	}

	for id, ok := range supported {
		if !ok {
			continue
		}
		if _, exists := loaded[id]; !exists {
			t.Fatalf("%s support map contains unknown or no-longer-vendored fixture id %q", name, id)
		}
	}
}

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	s = strings.ReplaceAll(s, " ", "")
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatalf("decode hex %q: %v", s, err)
	}
	return b
}

func mustGoAwayPayload(t *testing.T, bidi, uni, code uint64, reason string) []byte {
	t.Helper()

	payload, err := buildGoAwayPayload(bidi, uni, code, reason)
	if err != nil {
		t.Fatalf("build goaway payload: %v", err)
	}
	return payload
}

func mustClosePayload(t *testing.T, code uint64, reason string) []byte {
	t.Helper()

	payload := mustEncodeVarint(code)
	payload = appendDebugTextTLVCapped(payload, reason, DefaultSettings().MaxControlPayloadBytes)
	return payload
}

type invalidFixture struct {
	ID             string                 `json:"id"`
	Category       string                 `json:"category"`
	Description    string                 `json:"description"`
	ExpectedResult invalidExpectedFixture `json:"expected_result"`
	InputShape     json.RawMessage        `json:"input_shape"`
	Hex            string                 `json:"hex"`
}

type invalidExpectedFixture struct {
	Scope  string `json:"scope"`
	Error  string `json:"error"`
	Action string `json:"action"`
}

func TestInvalidFixturesSupportedScenarios(t *testing.T) {
	t.Parallel()
	for _, fixture := range loadInvalidFixtures(t) {
		fixture := fixture
		t.Run(fixture.ID, func(t *testing.T) {
			t.Parallel()
			if !supportedInvalidFixtureIDs[fixture.ID] {
				t.Fatalf("invalid fixture %q missing from support map", fixture.ID)
			}
			runInvalidFixture(t, fixture)
		})
	}
}

var supportedInvalidFixtureIDs = map[string]bool{
	"preface_duplicate_setting_id":                                   true,
	"preface_invalid_role_value":                                     true,
	"preface_auto_equal_nonce_conflict":                              true,
	"preface_auto_zero_nonce":                                        true,
	"preface_frame_payload_limit_too_small":                          true,
	"preface_control_payload_limit_too_small":                        true,
	"preface_extension_payload_limit_too_small":                      true,
	"preface_invalid_magic":                                          true,
	"preface_unsupported_preface_ver":                                true,
	"preface_no_protocol_version_overlap":                            true,
	"preface_explicit_same_role_conflict":                            true,
	"preface_settings_len_exceeds_limit":                             true,
	"preface_duplicate_padding":                                      true,
	"preface_duplicate_unknown_setting_id":                           true,
	"preface_setting_empty_value":                                    true,
	"preface_setting_truncated_value":                                true,
	"preface_setting_noncanonical_value":                             true,
	"preface_setting_trailing_byte":                                  true,
	"preface_settings_tlv_overrun":                                   true,
	"frame_length_too_small":                                         true,
	"frame_ext_payload_underflow":                                    true,
	"frame_length_smaller_than_stream_id_prefix":                     true,
	"frame_ping_with_forbidden_fin_flag":                             true,
	"frame_pong_too_short":                                           true,
	"frame_ping_payload_exceeds_local_echoable_limit":                true,
	"frame_abort_on_stream_zero":                                     true,
	"frame_max_data_trailing_garbage":                                true,
	"frame_blocked_trailing_garbage":                                 true,
	"frame_unknown_core_type":                                        true,
	"frame_priority_update_duplicate_singleton":                      true,
	"frame_priority_update_truncated_tlv_header":                     true,
	"frame_priority_update_tlv_value_overrun":                        true,
	"frame_priority_update_noncanonical_value":                       true,
	"frame_priority_update_without_capability":                       true,
	"frame_priority_update_on_unused_stream":                         true,
	"frame_priority_update_on_terminal_stream":                       true,
	"frame_unknown_ext_subtype":                                      true,
	"frame_data_open_metadata_without_capability":                    true,
	"frame_data_open_metadata_on_open_stream":                        true,
	"frame_data_open_metadata_duplicate_singleton":                   true,
	"frame_first_max_data_on_unused_stream":                          true,
	"frame_first_blocked_on_unused_stream":                           true,
	"frame_first_stop_sending_on_unused_stream":                      true,
	"frame_first_reset_on_unused_stream":                             true,
	"frame_data_exceeds_stream_max_data":                             true,
	"frame_data_exceeds_session_max_data":                            true,
	"frame_peer_stream_id_gap":                                       true,
	"frame_blocked_wrong_side_uni":                                   true,
	"frame_data_wrong_side_uni":                                      true,
	"frame_max_data_wrong_side_uni":                                  true,
	"frame_stop_sending_wrong_side_uni":                              true,
	"frame_reset_wrong_side_uni":                                     true,
	"hidden_control_opened_stream_exceeds_hard_cap_without_shedding": true,
	"local_provisional_open_cancel_must_not_burn_stream_id":          true,
	"late_data_after_close_read_exceeds_session_aggregate_cap":       true,
	"rapid_open_abort_churn_without_local_limit":                     true,
	"session_goaway_last_accepted_increase":                          true,
}

var supportedFrameInvalidFixtureIDs = map[string]bool{
	"frame_length_too_small":                          true,
	"frame_ext_payload_underflow":                     true,
	"frame_length_smaller_than_stream_id_prefix":      true,
	"frame_ping_with_forbidden_fin_flag":              true,
	"frame_pong_too_short":                            true,
	"frame_ping_payload_exceeds_local_echoable_limit": true,
	"frame_abort_on_stream_zero":                      true,
	"frame_max_data_trailing_garbage":                 true,
	"frame_blocked_trailing_garbage":                  true,
	"frame_unknown_core_type":                         true,
	"frame_priority_update_duplicate_singleton":       true,
	"frame_priority_update_truncated_tlv_header":      true,
	"frame_priority_update_tlv_value_overrun":         true,
	"frame_priority_update_noncanonical_value":        true,
	"frame_priority_update_without_capability":        true,
	"frame_priority_update_on_unused_stream":          true,
	"frame_priority_update_on_terminal_stream":        true,
	"frame_unknown_ext_subtype":                       true,
	"frame_data_open_metadata_without_capability":     true,
	"frame_data_open_metadata_on_open_stream":         true,
	"frame_data_open_metadata_duplicate_singleton":    true,
	"frame_data_exceeds_stream_max_data":              true,
	"frame_first_max_data_on_unused_stream":           true,
	"frame_first_blocked_on_unused_stream":            true,
	"frame_first_stop_sending_on_unused_stream":       true,
	"frame_first_reset_on_unused_stream":              true,
	"frame_data_exceeds_session_max_data":             true,
	"frame_peer_stream_id_gap":                        true,
	"frame_blocked_wrong_side_uni":                    true,
	"frame_data_wrong_side_uni":                       true,
	"frame_max_data_wrong_side_uni":                   true,
	"frame_stop_sending_wrong_side_uni":               true,
	"frame_reset_wrong_side_uni":                      true,
	"session_goaway_last_accepted_increase":           true,
}

func loadInvalidFixtures(t *testing.T) []invalidFixture {
	t.Helper()
	return testutil.LoadFixtureNDJSON[invalidFixture](t, "invalid_cases.ndjson")
}

func loadFrameInvalidFixtures(t *testing.T) []invalidFixture {
	t.Helper()
	fixtures := loadInvalidFixtures(t)
	filtered := make([]invalidFixture, 0, len(fixtures))
	for _, fixture := range fixtures {
		if isFrameInvalidFixture(fixture.ID) {
			filtered = append(filtered, fixture)
		}
	}
	return filtered
}

func isFrameInvalidFixture(id string) bool {
	return strings.HasPrefix(id, "frame_") || id == "session_goaway_last_accepted_increase"
}

func runInvalidFixture(t *testing.T, fixture invalidFixture) {
	t.Helper()

	var err error
	if raw, ok := invalidFixtureFrameBytes(t, fixture); ok {
		err = runInvalidFrameBytes(raw)
	} else if fixture.Category == "preface" && fixture.Hex != "" {
		err = runInvalidPrefaceHex(t, fixture)
	} else {
		switch fixture.ID {
		case "preface_duplicate_setting_id":
			err = runInvalidPrefaceDuplicateSettingID(t, fixture)
		case "preface_auto_equal_nonce_conflict",
			"preface_explicit_same_role_conflict",
			"preface_no_protocol_version_overlap":
			err = runInvalidPrefaceNegotiation(t, fixture)
		case "preface_frame_payload_limit_too_small",
			"preface_control_payload_limit_too_small",
			"preface_extension_payload_limit_too_small":
			err = runInvalidPrefaceMinLimit(t, fixture)
		case "frame_ping_payload_exceeds_local_echoable_limit":
			err = runInvalidFramePingPayloadExceedsLocalEchoableLimit(t)
		case "frame_priority_update_duplicate_singleton":
			err = runInvalidPriorityUpdateDuplicateSingleton(t)
		case "frame_priority_update_without_capability":
			err = runInvalidPriorityUpdateWithoutCapability(t)
		case "frame_priority_update_on_unused_stream":
			err = runInvalidPriorityUpdateOnUnusedStream(t)
		case "frame_priority_update_on_terminal_stream":
			err = runInvalidPriorityUpdateOnTerminalStream(t)
		case "frame_unknown_ext_subtype":
			err = runInvalidUnknownExtSubtype(t)
		case "frame_data_open_metadata_without_capability":
			err = runInvalidDataOpenMetadataWithoutCapability(t)
		case "frame_data_open_metadata_on_open_stream":
			err = runInvalidDataOpenMetadataOnOpenStream(t)
		case "frame_data_open_metadata_duplicate_singleton":
			err = runInvalidDataOpenMetadataDuplicateSingleton(t)
		case "frame_first_max_data_on_unused_stream",
			"frame_first_blocked_on_unused_stream",
			"frame_first_stop_sending_on_unused_stream",
			"frame_first_reset_on_unused_stream":
			err = runInvalidFirstFrameOnUnusedStream(t, fixture)
		case "frame_data_exceeds_stream_max_data":
			err = runInvalidDataExceedsStreamMaxData(t)
		case "frame_data_exceeds_session_max_data":
			err = runInvalidDataExceedsSessionMaxData(t)
		case "frame_peer_stream_id_gap":
			err = runInvalidPeerStreamIDGap(t)
		case "frame_blocked_wrong_side_uni":
			err = runInvalidBlockedWrongSideUni(t)
		case "frame_data_wrong_side_uni":
			err = runInvalidDataWrongSideUni(t)
		case "frame_max_data_wrong_side_uni":
			err = runInvalidMaxDataWrongSideUni(t)
		case "frame_stop_sending_wrong_side_uni":
			err = runInvalidStopSendingWrongSideUni(t)
		case "frame_reset_wrong_side_uni":
			err = runInvalidResetWrongSideUni(t)
		case "hidden_control_opened_stream_exceeds_hard_cap_without_shedding":
			err = runInvalidHiddenControlOpenedHardCap(t)
		case "local_provisional_open_cancel_must_not_burn_stream_id":
			err = runInvalidLocalProvisionalOpenCancelMustNotBurnID(t)
		case "late_data_after_close_read_exceeds_session_aggregate_cap":
			err = runInvalidLateDataAfterCloseReadAggregateCap(t)
		case "rapid_open_abort_churn_without_local_limit":
			err = runInvalidRapidOpenAbortChurn(t)
		case "session_goaway_last_accepted_increase":
			err = runInvalidSessionGoAwayIncrease(t)
		default:
			t.Fatalf("unsupported invalid fixture %q", fixture.ID)
		}
	}

	if fixture.ExpectedResult.Action != "" {
		if err != nil {
			t.Fatalf("fixture %q err = %v, want nil action success", fixture.ID, err)
		}
		return
	}

	want := fixtureErrorCode(t, fixture.ExpectedResult.Error)
	if !IsErrorCode(err, want) {
		t.Fatalf("fixture %q err = %v, want %s", fixture.ID, err, want)
	}
	assertInvalidFixtureScope(t, fixture, err)
}

// assertInvalidFixtureScope checks that the runner's error has the fixture's
// scope. Stream-scoped runners return an ApplicationError only after they saw
// the stream-local ABORT; session and establishment failures are wire errors
// that end the session.
func assertInvalidFixtureScope(t *testing.T, fixture invalidFixture, err error) {
	t.Helper()

	var appErr *ApplicationError
	streamLocal := errors.As(err, &appErr)
	switch fixture.ExpectedResult.Scope {
	case "stream":
		if !streamLocal {
			t.Fatalf("fixture %q err = %v, want a stream-local %s", fixture.ID, err, fixture.ExpectedResult.Error)
		}
	case "session", "session_establishment":
		var wireErr *WireError
		if streamLocal || !errors.As(err, &wireErr) {
			t.Fatalf("fixture %q err = %v, want a %s %s error", fixture.ID, err, fixture.ExpectedResult.Scope, fixture.ExpectedResult.Error)
		}
	default:
		t.Fatalf("fixture %q has unsupported error scope %q", fixture.ID, fixture.ExpectedResult.Scope)
	}
}

// invalidFixtureFrameBytes returns the complete raw frame of an invalid
// fixture that a receiver rejects from the frame bytes alone: the fixture's
// hex when it has one, else the frame its input_shape describes.
func invalidFixtureFrameBytes(t *testing.T, fixture invalidFixture) ([]byte, bool) {
	t.Helper()

	if fixture.Category == "frame" && fixture.Hex != "" {
		return mustHex(t, fixture.Hex), true
	}
	switch fixture.ID {
	case "frame_length_too_small":
		// frame_length = 1; the receiver rejects it from the length alone.
		return []byte{0x01}, true
	case "frame_ext_payload_underflow":
		return rawInvalidFrameBytes(t, byte(FrameTypeEXT), 0, nil), true
	case "frame_length_smaller_than_stream_id_prefix":
		// frame_length = 2 while stream_id 16384 needs a 4-byte encoding.
		return append([]byte{0x02, byte(FrameTypeDATA)}, mustEncodeVarint(16384)...), true
	case "frame_pong_too_short":
		return rawInvalidFrameBytes(t, byte(FrameTypePONG), 0, []byte{1, 2, 3, 4, 5, 6, 7}), true
	case "frame_abort_on_stream_zero":
		return rawInvalidFrameBytes(t, byte(FrameTypeABORT), 0, mustEncodeVarint(uint64(CodeCancelled))), true
	case "frame_max_data_trailing_garbage":
		return rawInvalidFrameBytes(t, byte(FrameTypeMAXDATA), 0, append(mustEncodeVarint(1024), 0x01)), true
	case "frame_blocked_trailing_garbage":
		return rawInvalidFrameBytes(t, byte(FrameTypeBLOCKED), 0, append(mustEncodeVarint(1024), 0x01)), true
	case "frame_priority_update_truncated_tlv_header":
		payload := mustAppendInvalidVarint(t, nil, uint64(EXTPriorityUpdate))
		payload = mustAppendInvalidVarint(t, payload, uint64(MetadataStreamPriority))
		return rawInvalidFrameBytes(t, byte(FrameTypeEXT), 4, payload), true
	case "frame_priority_update_tlv_value_overrun":
		payload := mustAppendInvalidVarint(t, nil, uint64(EXTPriorityUpdate))
		payload = mustAppendInvalidVarint(t, payload, uint64(MetadataStreamPriority))
		payload = mustAppendInvalidVarint(t, payload, 2)
		payload = append(payload, 0x01)
		return rawInvalidFrameBytes(t, byte(FrameTypeEXT), 4, payload), true
	default:
		return nil, false
	}
}

// runInvalidFrameBytes rejects raw through both frame readers, which must
// report the same error.
func runInvalidFrameBytes(raw []byte) error {
	limits := DefaultSettings().Limits()
	_, _, err := ParseFrame(raw, limits)
	_, readErr := ReadFrame(bytes.NewReader(raw), limits)
	parseCode, parseOK := ErrorCodeOf(err)
	readCode, readOK := ErrorCodeOf(readErr)
	if !parseOK || !readOK || parseCode != readCode {
		return fmt.Errorf("ParseFrame err = %v, ReadFrame err = %v, want the same wire error", err, readErr)
	}
	return err
}

// runInvalidPrefaceHex parses a fixture's raw peer preface with both preface
// readers, which must agree, and negotiates a preface they accept against a
// compatible local preface.
func runInvalidPrefaceHex(t *testing.T, fixture invalidFixture) error {
	t.Helper()

	raw := mustHex(t, fixture.Hex)
	peer, err := ParsePreface(raw)
	read, readErr := ReadPreface(bytes.NewReader(raw))
	parseCode, parseOK := ErrorCodeOf(err)
	readCode, readOK := ErrorCodeOf(readErr)
	if (err == nil) != (readErr == nil) || parseOK != readOK || parseCode != readCode {
		return fmt.Errorf("ParsePreface err = %v, ReadPreface err = %v, want the same result", err, readErr)
	}
	if err != nil {
		return err
	}
	if read != peer {
		return fmt.Errorf("ReadPreface = %+v, ParsePreface = %+v", read, peer)
	}
	localRole := RoleResponder
	if peer.Role == RoleResponder {
		localRole = RoleInitiator
	}
	_, err = NegotiatePrefaces(fixturePreface(localRole, 0), peer)
	return err
}

type invalidPrefaceShape struct {
	SettingIDs           []uint64 `json:"setting_ids"`
	LocalRole            string   `json:"local_role"`
	PeerRole             string   `json:"peer_role"`
	LocalTieBreakerNonce uint64   `json:"local_tie_breaker_nonce"`
	PeerTieBreakerNonce  uint64   `json:"peer_tie_breaker_nonce"`
	LocalMinProto        *uint64  `json:"local_min_proto"`
	LocalMaxProto        *uint64  `json:"local_max_proto"`
	PeerMinProto         *uint64  `json:"peer_min_proto"`
	PeerMaxProto         *uint64  `json:"peer_max_proto"`
	Setting              string   `json:"setting"`
	Value                *uint64  `json:"value"`
}

func decodeInvalidPrefaceShape(t *testing.T, fixture invalidFixture) invalidPrefaceShape {
	t.Helper()

	var shape invalidPrefaceShape
	if err := json.Unmarshal(fixture.InputShape, &shape); err != nil {
		t.Fatalf("decode input_shape for %s: %v", fixture.ID, err)
	}
	return shape
}

func fixtureRole(t *testing.T, name string) Role {
	t.Helper()

	for _, role := range []Role{RoleInitiator, RoleResponder, RoleAuto} {
		if role.String() == name {
			return role
		}
	}
	t.Fatalf("unsupported fixture role %q", name)
	return 0
}

func runInvalidPrefaceDuplicateSettingID(t *testing.T, fixture invalidFixture) error {
	t.Helper()

	shape := decodeInvalidPrefaceShape(t, fixture)
	if len(shape.SettingIDs) < 2 {
		t.Fatalf("fixture %s lists %d setting ids, want a duplicate", fixture.ID, len(shape.SettingIDs))
	}
	var settings []byte
	for i, id := range shape.SettingIDs {
		settings = mustAppendInvalidTLV(t, settings, id, mustEncodeVarint(uint64(100+i)))
	}
	_, err := ParsePreface(buildFixturePrefaceBytes(byte(RoleInitiator), 0, settings))
	return err
}

func runInvalidPrefaceNegotiation(t *testing.T, fixture invalidFixture) error {
	t.Helper()

	shape := decodeInvalidPrefaceShape(t, fixture)
	local := fixturePreface(RoleInitiator, 0)
	peer := fixturePreface(RoleResponder, 0)
	if shape.LocalRole != "" {
		local = fixturePreface(fixtureRole(t, shape.LocalRole), shape.LocalTieBreakerNonce)
	}
	if shape.PeerRole != "" {
		peer = fixturePreface(fixtureRole(t, shape.PeerRole), shape.PeerTieBreakerNonce)
	}
	for _, field := range []struct {
		dst *uint64
		src *uint64
	}{
		{&local.MinProto, shape.LocalMinProto},
		{&local.MaxProto, shape.LocalMaxProto},
		{&peer.MinProto, shape.PeerMinProto},
		{&peer.MaxProto, shape.PeerMaxProto},
	} {
		if field.src != nil {
			*field.dst = *field.src
		}
	}
	_, err := NegotiatePrefaces(local, peer)
	return err
}

func runInvalidPrefaceMinLimit(t *testing.T, fixture invalidFixture) error {
	t.Helper()

	shape := decodeInvalidPrefaceShape(t, fixture)
	if shape.Value == nil {
		t.Fatalf("fixture %s has no setting value", fixture.ID)
	}
	peer := fixturePreface(RoleResponder, 0)
	switch shape.Setting {
	case "max_frame_payload":
		peer.Settings.MaxFramePayload = *shape.Value
	case "max_control_payload_bytes":
		peer.Settings.MaxControlPayloadBytes = *shape.Value
	case "max_extension_payload_bytes":
		peer.Settings.MaxExtensionPayloadBytes = *shape.Value
	default:
		t.Fatalf("fixture %s sets unsupported setting %q", fixture.ID, shape.Setting)
	}
	_, err := NegotiatePrefaces(fixturePreface(RoleInitiator, 0), peer)
	return err
}

func runInvalidFramePingPayloadExceedsLocalEchoableLimit(t *testing.T) error {
	t.Helper()
	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()
	c.config.local.Settings.MaxControlPayloadBytes = 4096
	c.config.peer.Settings.MaxControlPayloadBytes = 16384
	err := testQueueFrame(c, Frame{
		Type:     FrameTypePING,
		StreamID: 0,
		Payload:  make([]byte, 8000),
	})
	if !IsErrorCode(err, CodeFrameSize) {
		return fmt.Errorf("ping payload send err = %v, want %s", err, CodeFrameSize)
	}
	return nil
}

func runInvalidPriorityUpdateDuplicateSingleton(t *testing.T) error {
	t.Helper()
	c, _, stop := newInvalidFrameConn(t, CapabilityPriorityUpdate|CapabilityPriorityHints)
	defer stop()
	stream := seedStateFixtureStream(t, c, state.FirstLocalStreamID(c.config.negotiated.LocalRole, true), "bidi", "local_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})
	testMarkLocalOpenCommitted(stream)
	var payload []byte
	payload = mustAppendInvalidVarint(t, payload, uint64(EXTPriorityUpdate))
	payload = mustAppendInvalidTLV(t, payload, uint64(MetadataStreamPriority), mustEncodeVarint(2))
	payload = mustAppendInvalidTLV(t, payload, uint64(MetadataStreamPriority), mustEncodeVarint(3))
	if err := c.handleExtFrame(Frame{Type: FrameTypeEXT, StreamID: stream.id, Payload: payload}); err != nil {
		return err
	}
	meta := stream.Metadata()
	if meta.Priority != 0 || meta.Group != nil {
		return fmt.Errorf("metadata after duplicate PRIORITY_UPDATE = %+v, want unchanged", meta)
	}
	return nil
}

func runInvalidPriorityUpdateWithoutCapability(t *testing.T) error {
	t.Helper()
	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()
	stream := seedStateFixtureStream(t, c, state.FirstLocalStreamID(c.config.negotiated.LocalRole, true), "bidi", "local_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})
	testMarkLocalOpenCommitted(stream)
	payload, err := buildPriorityUpdatePayload(CapabilityPriorityUpdate|CapabilityPriorityHints, MetadataUpdate{Priority: invalidUint64Ptr(2)}, c.config.peer.Settings.MaxExtensionPayloadBytes)
	if err != nil {
		return err
	}
	if err := c.handleExtFrame(Frame{Type: FrameTypeEXT, StreamID: stream.id, Payload: payload}); err != nil {
		return err
	}
	meta := stream.Metadata()
	if meta.Priority != 0 || meta.Group != nil {
		return fmt.Errorf("metadata after unnegotiated PRIORITY_UPDATE = %+v, want unchanged", meta)
	}
	return nil
}

func runInvalidPriorityUpdateOnUnusedStream(t *testing.T) error {
	t.Helper()
	c, _, stop := newInvalidFrameConn(t, CapabilityPriorityUpdate|CapabilityPriorityHints)
	defer stop()
	payload, err := buildPriorityUpdatePayload(CapabilityPriorityUpdate|CapabilityPriorityHints, MetadataUpdate{Priority: invalidUint64Ptr(2)}, c.config.peer.Settings.MaxExtensionPayloadBytes)
	if err != nil {
		return err
	}
	return c.handleExtFrame(Frame{Type: FrameTypeEXT, StreamID: state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), Payload: payload})
}

func runInvalidPriorityUpdateOnTerminalStream(t *testing.T) error {
	t.Helper()
	c, _, stop := newInvalidFrameConn(t, CapabilityPriorityUpdate|CapabilityPriorityHints)
	defer stop()
	stream := seedStateFixtureStream(t, c, state.FirstLocalStreamID(c.config.negotiated.LocalRole, true), "bidi", "local_owned", stateHalfExpect{
		SendHalf: "send_aborted",
		RecvHalf: "recv_aborted",
	})
	testMarkLocalOpenCommitted(stream)
	payload, err := buildPriorityUpdatePayload(CapabilityPriorityUpdate|CapabilityPriorityHints, MetadataUpdate{Priority: invalidUint64Ptr(2)}, c.config.peer.Settings.MaxExtensionPayloadBytes)
	if err != nil {
		return err
	}
	if err := c.handleExtFrame(Frame{Type: FrameTypeEXT, StreamID: stream.id, Payload: payload}); err != nil {
		return err
	}
	meta := stream.Metadata()
	if meta.Priority != 0 || meta.Group != nil {
		return fmt.Errorf("metadata after terminal PRIORITY_UPDATE = %+v, want unchanged", meta)
	}
	return nil
}

func runInvalidUnknownExtSubtype(t *testing.T) error {
	t.Helper()
	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()
	payload := mustAppendInvalidVarint(t, nil, 99)
	return c.handleExtFrame(Frame{Type: FrameTypeEXT, StreamID: 0, Payload: payload})
}

func runInvalidDataOpenMetadataWithoutCapability(t *testing.T) error {
	t.Helper()
	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()
	return c.handleDataFrame(Frame{
		Type:     FrameTypeDATA,
		Flags:    FrameFlagOpenMetadata,
		StreamID: state.FirstPeerStreamID(c.config.negotiated.LocalRole, true),
		Payload:  invalidOpenMetadataPayload(t, []TLV{{Type: uint64(MetadataOpenInfo), Value: []byte("a")}}, []byte("hi")),
	})
}

func runInvalidDataOpenMetadataOnOpenStream(t *testing.T) error {
	t.Helper()
	c, _, stop := newInvalidFrameConn(t, CapabilityOpenMetadata)
	defer stop()
	stream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})
	return c.handleDataFrame(Frame{
		Type:     FrameTypeDATA,
		Flags:    FrameFlagOpenMetadata,
		StreamID: stream.id,
		Payload:  invalidOpenMetadataPayload(t, []TLV{{Type: uint64(MetadataOpenInfo), Value: []byte("a")}}, []byte("hi")),
	})
}

func runInvalidDataOpenMetadataDuplicateSingleton(t *testing.T) error {
	t.Helper()
	c, _, stop := newInvalidFrameConn(t, CapabilityOpenMetadata)
	defer stop()
	streamID := state.FirstPeerStreamID(c.config.negotiated.LocalRole, true)
	if err := c.handleDataFrame(Frame{
		Type:     FrameTypeDATA,
		Flags:    FrameFlagOpenMetadata,
		StreamID: streamID,
		Payload: invalidOpenMetadataPayload(t, []TLV{
			{Type: uint64(MetadataOpenInfo), Value: []byte("a")},
			{Type: uint64(MetadataOpenInfo), Value: []byte("b")},
		}, []byte("hi")),
	}); err != nil {
		return err
	}
	stream := c.registry.streams[streamID]
	if stream == nil {
		return fmt.Errorf("expected stream to open")
	}
	if got := string(stream.OpenInfo()); got != "" {
		return fmt.Errorf("open_info = %q, want empty", got)
	}
	if got := string(stream.readBuf); got != "hi" {
		return fmt.Errorf("readBuf = %q, want %q", got, "hi")
	}
	return nil
}

func runInvalidDataExceedsStreamMaxData(t *testing.T) error {
	t.Helper()

	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()

	stream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})
	stream.recvReceived = 1000
	stream.recvAdvertised = 1004
	if err := c.handleDataFrame(Frame{
		Type:     FrameTypeDATA,
		StreamID: stream.id,
		Payload:  []byte("12345678"),
	}); err != nil {
		return err
	}

	frame, err := awaitInvalidFixtureFrame(frames)
	if err != nil {
		return err
	}
	if frame.Type != FrameTypeABORT || frame.StreamID != stream.id {
		return fmt.Errorf("queued frame = %+v, want ABORT on stream %d", frame, stream.id)
	}
	want := mustEncodeVarint(uint64(CodeFlowControl))
	if string(frame.Payload) != string(want) {
		return fmt.Errorf("ABORT payload = %x, want %x", frame.Payload, want)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.registry.streams[stream.id]; ok {
		return fmt.Errorf("stream %d remained live after local FLOW_CONTROL abort", stream.id)
	}
	if !c.hasTerminalMarkerLocked(stream.id) {
		return fmt.Errorf("stream %d missing terminal marker after local FLOW_CONTROL abort", stream.id)
	}
	return &ApplicationError{Code: uint64(CodeFlowControl)}
}

type invalidStreamShape struct {
	StreamID           uint64 `json:"stream_id"`
	IncomingFrame      string `json:"incoming_frame"`
	InitialStreamState string `json:"initial_stream_state"`
}

// invalidFirstFrame builds the peer's first non-opening frame on an unused
// stream from an input_shape's incoming_frame.
func invalidFirstFrame(t *testing.T, incoming string, streamID uint64) Frame {
	t.Helper()

	switch incoming {
	case "MAX_DATA":
		return Frame{Type: FrameTypeMAXDATA, StreamID: streamID, Payload: mustEncodeVarint(64)}
	case "BLOCKED":
		return Frame{Type: FrameTypeBLOCKED, StreamID: streamID, Payload: mustEncodeVarint(64)}
	case "STOP_SENDING":
		return Frame{Type: FrameTypeStopSending, StreamID: streamID, Payload: mustEncodeVarint(uint64(CodeCancelled))}
	case "RESET":
		return Frame{Type: FrameTypeRESET, StreamID: streamID, Payload: mustEncodeVarint(uint64(CodeCancelled))}
	default:
		t.Fatalf("unsupported first frame %q", incoming)
		return Frame{}
	}
}

// runInvalidFirstFrameOnUnusedStream sends MAX_DATA, BLOCKED, STOP_SENDING or
// RESET as the first frame on the peer's next unused stream ID. The result
// must be a session PROTOCOL error that leaves no stream state behind: the
// local side must not answer the unseen ID with a first-frame ABORT (SPEC 9.1).
func runInvalidFirstFrameOnUnusedStream(t *testing.T, fixture invalidFixture) error {
	t.Helper()

	var shape invalidStreamShape
	if err := json.Unmarshal(fixture.InputShape, &shape); err != nil {
		t.Fatalf("decode input_shape for %s: %v", fixture.ID, err)
	}
	if shape.InitialStreamState != "idle" {
		t.Fatalf("fixture %s initial_stream_state = %q, want idle", fixture.ID, shape.InitialStreamState)
	}
	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()
	streamID := state.FirstPeerStreamID(c.config.negotiated.LocalRole, true)
	if streamID != shape.StreamID {
		t.Fatalf("fixture %s stream_id = %d, harness peer's next bidi stream = %d", fixture.ID, shape.StreamID, streamID)
	}

	err := c.handleFrame(invalidFirstFrame(t, shape.IncomingFrame, streamID))

	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.registry.streams[streamID]; ok {
		return fmt.Errorf("first %s on unused stream %d created a live stream (err %v)", shape.IncomingFrame, streamID, err)
	}
	if c.hasTerminalMarkerLocked(streamID) {
		return fmt.Errorf("first %s on unused stream %d left a terminal marker (err %v)", shape.IncomingFrame, streamID, err)
	}
	return err
}

func runInvalidDataExceedsSessionMaxData(t *testing.T) error {
	t.Helper()

	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	stream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, true), "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})
	stream.recvAdvertised = 2000
	c.flow.recvSessionReceived = 1000
	c.flow.recvSessionAdvertised = 1004
	return c.handleDataFrame(Frame{
		Type:     FrameTypeDATA,
		StreamID: stream.id,
		Payload:  []byte("12345678"),
	})
}

func runInvalidBlockedWrongSideUni(t *testing.T) error {
	t.Helper()

	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()
	stream := seedStateFixtureStream(t, c, state.FirstLocalStreamID(c.config.negotiated.LocalRole, false), "uni_local_send_only", "local_owned", stateHalfExpect{SendHalf: "send_open"})
	streamID := stream.id
	if err := c.handleBlockedFrame(Frame{
		Type:     FrameTypeBLOCKED,
		StreamID: streamID,
		Payload:  mustEncodeVarint(64),
	}); err != nil {
		return err
	}
	return expectInvalidStreamStateAbort(c, frames, streamID)
}

func runInvalidDataWrongSideUni(t *testing.T) error {
	t.Helper()

	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()
	stream := seedStateFixtureStream(t, c, state.FirstLocalStreamID(c.config.negotiated.LocalRole, false), "uni_local_send_only", "local_owned", stateHalfExpect{SendHalf: "send_open"})
	streamID := stream.id
	if err := c.handleDataFrame(Frame{
		Type:     FrameTypeDATA,
		StreamID: streamID,
		Payload:  []byte("x"),
	}); err != nil {
		return err
	}
	return expectInvalidStreamStateAbort(c, frames, streamID)
}

func runInvalidMaxDataWrongSideUni(t *testing.T) error {
	t.Helper()

	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()
	stream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, false), "uni_local_send_only", "peer_owned", stateHalfExpect{RecvHalf: "recv_open"})
	streamID := stream.id
	if err := c.handleMaxDataFrame(Frame{
		Type:     FrameTypeMAXDATA,
		StreamID: streamID,
		Payload:  mustEncodeVarint(64),
	}); err != nil {
		return err
	}
	return expectInvalidStreamStateAbort(c, frames, streamID)
}

func runInvalidStopSendingWrongSideUni(t *testing.T) error {
	t.Helper()

	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()
	stream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, false), "uni_local_send_only", "peer_owned", stateHalfExpect{RecvHalf: "recv_open"})
	streamID := stream.id
	if err := c.handleStopSendingFrame(Frame{
		Type:     FrameTypeStopSending,
		StreamID: streamID,
		Payload:  mustEncodeVarint(uint64(CodeCancelled)),
	}); err != nil {
		return err
	}
	return expectInvalidStreamStateAbort(c, frames, streamID)
}

func runInvalidResetWrongSideUni(t *testing.T) error {
	t.Helper()

	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()
	stream := seedStateFixtureStream(t, c, state.FirstLocalStreamID(c.config.negotiated.LocalRole, false), "uni_local_send_only", "local_owned", stateHalfExpect{SendHalf: "send_open"})
	streamID := stream.id
	if err := c.handleResetFrame(Frame{
		Type:     FrameTypeRESET,
		StreamID: streamID,
		Payload:  mustEncodeVarint(uint64(CodeCancelled)),
	}); err != nil {
		return err
	}
	return expectInvalidStreamStateAbort(c, frames, streamID)
}

func runInvalidPeerStreamIDGap(t *testing.T) error {
	t.Helper()

	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	return c.handleDataFrame(Frame{
		Type:     FrameTypeDATA,
		StreamID: state.FirstPeerStreamID(c.config.negotiated.LocalRole, true) + 4,
		Payload:  []byte("x"),
	})
}

func runInvalidSessionGoAwayIncrease(t *testing.T) error {
	t.Helper()

	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()
	c.sessionControl.peerGoAwayBidi = state.FirstLocalStreamID(c.config.negotiated.LocalRole, true) + 80
	c.sessionControl.peerGoAwayUni = state.FirstLocalStreamID(c.config.negotiated.LocalRole, false) + 96
	return c.handleGoAwayFrame(Frame{
		Type:    FrameTypeGOAWAY,
		Payload: mustGoAwayPayload(t, c.sessionControl.peerGoAwayBidi, c.sessionControl.peerGoAwayUni+4, uint64(CodeNoError), ""),
	})
}

func awaitInvalidFixtureFrame(frames <-chan Frame) (Frame, error) {
	select {
	case frame := <-frames:
		return frame, nil
	case <-time.After(testSignalTimeout):
		return Frame{}, fmt.Errorf("timed out waiting for queued frame")
	}
}

func expectInvalidQueuedCloseWithError(frames <-chan Frame, streamID uint64, code ErrorCode) error {
	frame, err := awaitInvalidFixtureFrame(frames)
	if err != nil {
		return err
	}
	if frame.Type != FrameTypeABORT || frame.StreamID != streamID {
		return fmt.Errorf("queued frame = %+v, want ABORT on stream %d", frame, streamID)
	}
	want := mustEncodeVarint(uint64(code))
	if string(frame.Payload) != string(want) {
		return fmt.Errorf("ABORT payload = %x, want %x", frame.Payload, want)
	}
	return &ApplicationError{Code: uint64(code)}
}

func expectInvalidStreamStateAbort(c *Conn, frames <-chan Frame, streamID uint64) error {
	if err := expectInvalidQueuedCloseWithError(frames, streamID, CodeStreamState); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.registry.streams[streamID]; ok {
		return fmt.Errorf("stream %d remained live after local STREAM_STATE abort", streamID)
	}
	if !c.hasTerminalMarkerLocked(streamID) {
		return fmt.Errorf("stream %d missing terminal marker after local STREAM_STATE abort", streamID)
	}
	return &ApplicationError{Code: uint64(CodeStreamState)}
}

func runInvalidLocalProvisionalOpenCancelMustNotBurnID(t *testing.T) error {
	client, _, stop := newInvalidPolicyConn(t)
	defer stop()
	ctx, cancel := testContext(t)
	defer cancel()

	first, err := client.OpenStream(ctx)
	if err != nil {
		return err
	}
	if err := first.CloseWithError(uint64(CodeCancelled), ""); err != nil {
		return err
	}
	if got := first.StreamID(); got != 0 {
		return fmt.Errorf("provisional cancel exposed consumed id %d", got)
	}

	second, err := client.OpenStream(ctx)
	if err != nil {
		return err
	}
	if _, err := second.Write([]byte("x")); err != nil {
		return err
	}
	if got := second.StreamID(); got != state.FirstLocalStreamID(client.config.negotiated.LocalRole, true) {
		return fmt.Errorf("next committed stream id = %d, want %d", got, state.FirstLocalStreamID(client.config.negotiated.LocalRole, true))
	}
	return nil
}

func runInvalidHiddenControlOpenedHardCap(t *testing.T) error {
	c, _, stop := newInvalidPolicyConn(t)
	defer stop()

	start := state.FirstPeerStreamID(c.config.negotiated.LocalRole, true)
	for i := 0; i < 65; i++ {
		streamID := start + uint64(i*4)
		if err := c.handleAbortFrame(Frame{
			Type:     FrameTypeABORT,
			StreamID: streamID,
			Payload:  mustEncodeVarint(uint64(CodeCancelled)),
		}); err != nil {
			return err
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if got := len(c.registry.streams); got != 0 {
		return fmt.Errorf("hidden control-opened live streams = %d, want 0", got)
	}
	if got := len(c.queues.acceptBidi.items) + len(c.queues.acceptUni.items); got != 0 {
		return fmt.Errorf("hidden control-opened streams entered accept queues: %d", got)
	}
	if got := len(c.registry.tombstones); got != hiddenControlRetainedHardCap {
		return fmt.Errorf("hidden control-opened tombstones = %d, want %d", got, hiddenControlRetainedHardCap)
	}
	newestID := start + uint64(hiddenControlRetainedHardCap*4)
	if _, ok := c.registry.tombstones[newestID]; ok {
		return fmt.Errorf("newest hidden control-opened tombstone %d retained past hard cap", newestID)
	}
	if _, ok := c.usedStreamMarkerForLocked(newestID); !ok {
		return fmt.Errorf("newest hidden control-opened marker %d was not preserved after reap", newestID)
	}
	return nil
}

func runInvalidLateDataAfterCloseReadAggregateCap(t *testing.T) error {
	c, _, stop := newInvalidPolicyConn(t)
	defer stop()

	streams := buildLateTailFixtureStreams(t, c, c.config.negotiated.LocalRole)
	for i := 0; i < 8; i++ {
		for _, stream := range streams {
			if err := c.handleDataFrame(Frame{
				Type:     FrameTypeDATA,
				StreamID: stream.id,
				Payload:  []byte("overflow"),
			}); err != nil {
				return err
			}
			if len(stream.readBuf) != 0 || stream.recvBuffer != 0 {
				return errors.New("late tail buffered after CloseRead")
			}
		}
	}
	if c.flow.recvSessionUsed != 0 {
		return fmt.Errorf("recvSessionUsed = %d, want 0", c.flow.recvSessionUsed)
	}
	return nil
}

func runInvalidRapidOpenAbortChurn(t *testing.T) error {
	c, _, stop := newInvalidPolicyConn(t)
	defer stop()

	start := state.FirstPeerStreamID(c.config.negotiated.LocalRole, true)
	for i := 0; i < hiddenAbortChurnThreshold+1; i++ {
		streamID := start + uint64(i*4)
		err := c.handleAbortFrame(Frame{
			Type:     FrameTypeABORT,
			StreamID: streamID,
			Payload:  mustEncodeVarint(uint64(CodeCancelled)),
		})
		if i < hiddenAbortChurnThreshold {
			if err != nil {
				return fmt.Errorf("early churn guard trigger after %d events: %w", i+1, err)
			}
			continue
		}
		if !IsErrorCode(err, CodeProtocol) {
			return fmt.Errorf("final churn event err = %v, want %s", err, CodeProtocol)
		}
		return nil
	}
	return fmt.Errorf("hidden abort churn did not trigger local policy after %d events", hiddenAbortChurnThreshold+1)
}

func newInvalidPolicyConn(t *testing.T) (*Conn, <-chan Frame, func()) {
	t.Helper()

	c, frames, stop := newHandlerTestConn(t)
	c.mu.Lock()
	configureTestConnHandshakeLocked(c, RoleResponder, RoleInitiator, 0)
	c.signals.acceptCh = make(chan struct{}, 1)
	c.mu.Unlock()
	return c, frames, stop
}

func rawInvalidFrameBytes(t *testing.T, code byte, streamID uint64, payload []byte) []byte {
	t.Helper()

	streamBytes, err := EncodeVarint(streamID)
	if err != nil {
		t.Fatalf("encode stream id: %v", err)
	}
	var out []byte
	out = mustAppendInvalidVarint(t, out, uint64(1+len(streamBytes)+len(payload)))
	out = append(out, code)
	out = append(out, streamBytes...)
	out = append(out, payload...)
	return out
}

func mustAppendInvalidVarint(t *testing.T, dst []byte, v uint64) []byte {
	t.Helper()

	out, err := AppendVarint(dst, v)
	if err != nil {
		t.Fatalf("append varint %d: %v", v, err)
	}
	return out
}

func mustAppendInvalidTLV(t *testing.T, dst []byte, typ uint64, value []byte) []byte {
	t.Helper()

	out, err := AppendTLV(dst, typ, value)
	if err != nil {
		t.Fatalf("append tlv type=%d: %v", typ, err)
	}
	return out
}

func invalidOpenMetadataPayload(t *testing.T, tlvs []TLV, appData []byte) []byte {
	t.Helper()

	var metadata []byte
	for _, tlv := range tlvs {
		metadata = mustAppendInvalidTLV(t, metadata, tlv.Type, tlv.Value)
	}
	var payload []byte
	payload = mustAppendInvalidVarint(t, payload, uint64(len(metadata)))
	payload = append(payload, metadata...)
	payload = append(payload, appData...)
	return payload
}

func fixturePreface(role Role, nonce uint64) Preface {
	return Preface{
		PrefaceVersion:  PrefaceVersion,
		Role:            role,
		TieBreakerNonce: nonce,
		MinProto:        ProtoVersion,
		MaxProto:        ProtoVersion,
		Settings:        DefaultSettings(),
	}
}

func buildFixturePrefaceBytes(role byte, nonce uint64, settingsTLV []byte) []byte {
	out := make([]byte, 0, 16+len(settingsTLV))
	out = append(out, []byte(Magic)...)
	out = append(out, PrefaceVersion, role)
	out = mustAppendFixtureVarint(out, nonce)
	out = mustAppendFixtureVarint(out, ProtoVersion)
	out = mustAppendFixtureVarint(out, ProtoVersion)
	out = mustAppendFixtureVarint(out, 0)
	out = mustAppendFixtureVarint(out, uint64(len(settingsTLV)))
	out = append(out, settingsTLV...)
	return out
}

func mustAppendFixtureVarint(dst []byte, value uint64) []byte {
	out, err := AppendVarint(dst, value)
	if err != nil {
		panic(err)
	}
	return out
}

func fixtureErrorCode(t *testing.T, name string) ErrorCode {
	t.Helper()

	switch name {
	case "NO_ERROR":
		return CodeNoError
	case "PROTOCOL":
		return CodeProtocol
	case "UNSUPPORTED_VERSION":
		return CodeUnsupportedVersion
	case "ROLE_CONFLICT":
		return CodeRoleConflict
	case "FLOW_CONTROL":
		return CodeFlowControl
	case "STREAM_STATE":
		return CodeStreamState
	case "FRAME_SIZE":
		return CodeFrameSize
	default:
		t.Fatalf("unsupported fixture error code %q", name)
		return 0
	}
}

func expectQueuedStreamStateAbort(t *testing.T, c *Conn, frames <-chan Frame, streamID uint64) {
	t.Helper()

	frame := awaitQueuedFrame(t, frames)
	if frame.Type != FrameTypeABORT || frame.StreamID != streamID {
		t.Fatalf("queued frame = %+v, want ABORT on stream %d", frame, streamID)
	}
	if string(frame.Payload) != string(mustEncodeVarint(uint64(CodeStreamState))) {
		t.Fatalf("ABORT payload = %x, want %x", frame.Payload, mustEncodeVarint(uint64(CodeStreamState)))
	}
	assertLocallyAbortedStream(t, c, streamID, "STREAM_STATE")
}

func TestPeerMaxDataOnLocalRecvOnlyUniAbortsStreamState(t *testing.T) {
	t.Parallel()
	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	stream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, false), "uni_local_send_only", "peer_owned", stateHalfExpect{
		RecvHalf: "recv_open",
	})

	if err := c.handleMaxDataFrame(Frame{
		Type:     FrameTypeMAXDATA,
		StreamID: stream.id,
		Payload:  mustEncodeVarint(64),
	}); err != nil {
		t.Fatalf("handle MAX_DATA: %v", err)
	}
	expectQueuedStreamStateAbort(t, c, frames, stream.id)
}

func TestPeerBlockedOnLocalSendOnlyUniAbortsStreamState(t *testing.T) {
	t.Parallel()
	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	stream := seedStateFixtureStream(t, c, state.FirstLocalStreamID(c.config.negotiated.LocalRole, false), "uni_local_send_only", "local_owned", stateHalfExpect{
		SendHalf: "send_open",
	})

	if err := c.handleBlockedFrame(Frame{
		Type:     FrameTypeBLOCKED,
		StreamID: stream.id,
		Payload:  mustEncodeVarint(64),
	}); err != nil {
		t.Fatalf("handle BLOCKED: %v", err)
	}
	expectQueuedStreamStateAbort(t, c, frames, stream.id)
}

func TestPeerDataOnLocalSendOnlyUniAbortsStreamState(t *testing.T) {
	t.Parallel()
	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	stream := seedStateFixtureStream(t, c, state.FirstLocalStreamID(c.config.negotiated.LocalRole, false), "uni_local_send_only", "local_owned", stateHalfExpect{
		SendHalf: "send_open",
	})

	if err := c.handleDataFrame(Frame{
		Type:     FrameTypeDATA,
		StreamID: stream.id,
		Payload:  []byte("x"),
	}); err != nil {
		t.Fatalf("handle DATA: %v", err)
	}
	expectQueuedStreamStateAbort(t, c, frames, stream.id)
}

func TestUnseenStreamMaxDataIsProtocolError(t *testing.T) {
	t.Parallel()
	c, _, stop := newHandlerTestConn(t)
	defer stop()

	err := c.handleMaxDataFrame(Frame{
		Type:     FrameTypeMAXDATA,
		StreamID: 4,
		Payload:  mustEncodeVarint(64),
	})
	if !IsErrorCode(err, CodeProtocol) {
		t.Fatalf("handle MAX_DATA err = %v, want %s", err, CodeProtocol)
	}
}

func TestUnseenStreamBlockedIsProtocolError(t *testing.T) {
	t.Parallel()
	c, _, stop := newHandlerTestConn(t)
	defer stop()

	err := c.handleBlockedFrame(Frame{
		Type:     FrameTypeBLOCKED,
		StreamID: 4,
		Payload:  mustEncodeVarint(64),
	})
	if !IsErrorCode(err, CodeProtocol) {
		t.Fatalf("handle BLOCKED err = %v, want %s", err, CodeProtocol)
	}
}

func TestUnseenStreamStopSendingIsProtocolError(t *testing.T) {
	t.Parallel()
	c, _, stop := newHandlerTestConn(t)
	defer stop()

	err := c.handleStopSendingFrame(Frame{
		Type:     FrameTypeStopSending,
		StreamID: 4,
		Payload:  mustEncodeVarint(uint64(CodeCancelled)),
	})
	if !IsErrorCode(err, CodeProtocol) {
		t.Fatalf("handle STOP_SENDING err = %v, want %s", err, CodeProtocol)
	}
}

func TestUnseenStreamResetIsProtocolError(t *testing.T) {
	t.Parallel()
	c, _, stop := newHandlerTestConn(t)
	defer stop()

	err := c.handleResetFrame(Frame{
		Type:     FrameTypeRESET,
		StreamID: 4,
		Payload:  mustEncodeVarint(uint64(CodeCancelled)),
	})
	if !IsErrorCode(err, CodeProtocol) {
		t.Fatalf("handle RESET err = %v, want %s", err, CodeProtocol)
	}
}

func mustParseWireExampleFrame(t *testing.T, hex string) Frame {
	t.Helper()

	raw := mustHex(t, hex)
	frame, n, err := ParseFrame(raw, DefaultSettings().Limits())
	if err != nil {
		t.Fatalf("parse wire example frame %q: %v", hex, err)
	}
	if n != len(raw) {
		t.Fatalf("parse wire example frame %q consumed %d bytes, want %d", hex, n, len(raw))
	}
	return frame
}

func assertWireExampleFrame(t *testing.T, got, want Frame) {
	t.Helper()

	if got.Type != want.Type || got.Flags != want.Flags || got.StreamID != want.StreamID || !bytes.Equal(got.Payload, want.Payload) {
		t.Fatalf("frame = %+v payload=%x, want %+v payload=%x", got, got.Payload, want, want.Payload)
	}
}

func TestWireExamplePairwiseStopSendingGracefulConclusion(t *testing.T) {
	t.Parallel()

	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()

	stopFrame := mustParseWireExampleFrame(t, "03 03 04 08")
	wantFin := mustParseWireExampleFrame(t, "02 41 04")

	stream := seedStateFixtureStream(t, c, stopFrame.StreamID, "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})
	testMarkLocalOpenCommitted(stream)
	stream.sendSent = 64
	stream.queuedDataBytes = 64
	stream.inflightQueued = 64
	c.flow.sendSessionUsed = 64

	if err := c.handleFrame(stopFrame); err != nil {
		t.Fatalf("handle STOP_SENDING: %v", err)
	}

	assertWireExampleFrame(t, awaitQueuedFrame(t, frames), wantFin)
	if !stream.sendFinReached() {
		t.Fatal("sendFinReached() = false, want graceful sender conclusion")
	}
	if stream.sendReset != nil {
		t.Fatalf("sendReset = %v, want nil", stream.sendReset)
	}
}

func TestWireExamplePairwiseStopSendingAbortiveConclusion(t *testing.T) {
	t.Parallel()

	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()

	stopFrame := mustParseWireExampleFrame(t, "03 03 04 08")
	wantReset := mustParseWireExampleFrame(t, "03 07 04 08")

	stream := seedStateFixtureStream(t, c, stopFrame.StreamID, "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})
	testMarkLocalOpenCommitted(stream)
	stream.sendSent = 3
	c.flow.sendSessionUsed = 3

	if err := c.handleFrame(stopFrame); err != nil {
		t.Fatalf("handle STOP_SENDING: %v", err)
	}

	assertWireExampleFrame(t, awaitQueuedFrame(t, frames), wantReset)
	if stream.sendReset == nil || stream.sendReset.Code != uint64(CodeCancelled) {
		t.Fatalf("sendReset = %v, want code %d", stream.sendReset, uint64(CodeCancelled))
	}
	if stream.sendFinReached() {
		t.Fatal("sendFinReached() = true, want abortive sender conclusion")
	}
}

func TestWireExamplePairwisePeerFinThenLateData(t *testing.T) {
	t.Parallel()

	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()

	finFrame := mustParseWireExampleFrame(t, "02 41 04")
	lateData := mustParseWireExampleFrame(t, "04 01 04 68 69")

	if err := c.handleFrame(finFrame); err != nil {
		t.Fatalf("handle DATA|FIN: %v", err)
	}
	if err := c.handleFrame(lateData); err != nil {
		t.Fatalf("handle late DATA: %v", err)
	}

	assertInvalidQueuedAbortCode(t, frames, finFrame.StreamID, CodeStreamClosed)

	c.mu.Lock()
	if stream, ok := c.registry.streams[finFrame.StreamID]; ok {
		c.mu.Unlock()
		t.Fatalf("stream %d remained live after late DATA: %+v", finFrame.StreamID, stream)
	}
	if !c.hasTerminalMarkerLocked(finFrame.StreamID) {
		c.mu.Unlock()
		t.Fatalf("stream %d missing terminal marker after late DATA", finFrame.StreamID)
	}
	c.mu.Unlock()
}

func TestWireExamplePairwisePeerResetThenLateDataIgnored(t *testing.T) {
	t.Parallel()

	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()

	resetFrame := mustParseWireExampleFrame(t, "03 07 04 08")
	lateData := mustParseWireExampleFrame(t, "04 01 04 68 69")

	stream := seedStateFixtureStream(t, c, resetFrame.StreamID, "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})

	if err := c.handleFrame(resetFrame); err != nil {
		t.Fatalf("handle RESET: %v", err)
	}
	if err := c.handleFrame(lateData); err != nil {
		t.Fatalf("handle late DATA after RESET: %v", err)
	}

	assertNoQueuedFrame(t, frames)

	c.mu.Lock()
	if stream.recvReset == nil || stream.recvReset.Code != uint64(CodeCancelled) {
		c.mu.Unlock()
		t.Fatalf("recvReset = %v, want code %d", stream.recvReset, uint64(CodeCancelled))
	}
	if stream.lateDataReceived != 2 {
		c.mu.Unlock()
		t.Fatalf("lateDataReceived = %d, want 2", stream.lateDataReceived)
	}
	if c.ingress.aggregateLateData != 2 {
		c.mu.Unlock()
		t.Fatalf("aggregateLateData = %d, want 2", c.ingress.aggregateLateData)
	}
	c.mu.Unlock()
}

func TestWireExamplePairwisePeerAbortThenLateDataIgnored(t *testing.T) {
	t.Parallel()

	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()

	abortFrame := mustParseWireExampleFrame(t, "03 08 04 08")
	lateData := mustParseWireExampleFrame(t, "04 01 04 68 69")

	stream := seedStateFixtureStream(t, c, abortFrame.StreamID, "bidi", "peer_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})

	if err := c.handleFrame(abortFrame); err != nil {
		t.Fatalf("handle ABORT: %v", err)
	}
	if err := c.handleFrame(lateData); err != nil {
		t.Fatalf("handle late DATA after ABORT: %v", err)
	}

	assertNoQueuedFrame(t, frames)

	c.mu.Lock()
	if stream.recvAbort == nil || stream.recvAbort.Code != uint64(CodeCancelled) {
		c.mu.Unlock()
		t.Fatalf("recvAbort = %v, want code %d", stream.recvAbort, uint64(CodeCancelled))
	}
	if stream.lateDataReceived != 0 {
		c.mu.Unlock()
		t.Fatalf("lateDataReceived = %d, want 0 for fully terminal live stream", stream.lateDataReceived)
	}
	if c.ingress.aggregateLateData != 2 {
		c.mu.Unlock()
		t.Fatalf("aggregateLateData = %d, want 2", c.ingress.aggregateLateData)
	}
	c.mu.Unlock()
}

func TestWireExamplePairwisePermissiveGoAwayThenRejectTooNewPeerStream(t *testing.T) {
	t.Parallel()

	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()

	goAwayFrame := mustParseWireExampleFrame(t, "05 09 00 04 00 00")
	tooNewOpener := mustParseWireExampleFrame(t, "04 01 08 68 69")
	wantAbort := Frame{
		Type:     FrameTypeABORT,
		StreamID: tooNewOpener.StreamID,
		Payload:  mustEncodeVarint(uint64(CodeRefusedStream)),
	}

	parsed, err := parseGOAWAYPayload(goAwayFrame.Payload)
	if err != nil {
		t.Fatalf("parse GOAWAY payload: %v", err)
	}

	c.mu.Lock()
	c.sessionControl.localGoAwayBidi = parsed.LastAcceptedBidi
	c.sessionControl.localGoAwayUni = parsed.LastAcceptedUni
	c.mu.Unlock()

	if err := c.handleFrame(tooNewOpener); err != nil {
		t.Fatalf("handle too-new peer opener: %v", err)
	}

	assertWireExampleFrame(t, awaitQueuedFrame(t, frames), wantAbort)
}

func TestWireExamplePairwiseZeroLengthOpenMetadataThenLaterPayload(t *testing.T) {
	t.Parallel()

	c, frames, stop := newInvalidFrameConn(t, CapabilityOpenMetadata)
	defer stop()

	openFrame := mustParseWireExampleFrame(t, "08 21 04 05 03 03 73 73 68")
	dataFrame := mustParseWireExampleFrame(t, "04 01 04 68 69")

	if err := c.handleFrame(openFrame); err != nil {
		t.Fatalf("handle zero-length DATA|OPEN_METADATA: %v", err)
	}
	assertNoQueuedFrame(t, frames)

	c.mu.Lock()
	stream := c.registry.streams[openFrame.StreamID]
	if stream == nil {
		c.mu.Unlock()
		t.Fatalf("stream %d missing after OPEN_METADATA opener", openFrame.StreamID)
	}
	if got := string(stream.openInfo); got != "ssh" {
		c.mu.Unlock()
		t.Fatalf("openInfo = %q, want %q", got, "ssh")
	}
	if stream.recvBuffer != 0 {
		c.mu.Unlock()
		t.Fatalf("recvBuffer = %d, want 0 after zero-length opener", stream.recvBuffer)
	}
	c.mu.Unlock()

	if err := c.handleFrame(dataFrame); err != nil {
		t.Fatalf("handle later DATA: %v", err)
	}
	assertNoQueuedFrame(t, frames)

	c.mu.Lock()
	if got := string(stream.readBuf); got != "hi" {
		c.mu.Unlock()
		t.Fatalf("readBuf = %q, want %q", got, "hi")
	}
	if stream.recvBuffer != 2 {
		c.mu.Unlock()
		t.Fatalf("recvBuffer = %d, want 2 after later payload", stream.recvBuffer)
	}
	c.mu.Unlock()
}

func TestWireExamplePairwiseSessionBlockedThenMaxDataAdvancement(t *testing.T) {
	t.Parallel()

	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	blockedFrame := mustParseWireExampleFrame(t, "04 06 00 44 00")
	wantMaxData := mustParseWireExampleFrame(t, "04 02 00 48 00")

	c.mu.Lock()
	c.flow.recvSessionAdvertised = 1024
	c.flow.recvSessionReceived = 1024
	c.flow.recvSessionPending = 1024
	c.flow.recvSessionUsed = c.sessionDataHWMLocked()
	c.mu.Unlock()

	if err := c.handleFrame(blockedFrame); err != nil {
		t.Fatalf("handle session BLOCKED: %v", err)
	}

	c.mu.Lock()
	urgent, advisory := testDrainPendingControlFrames(c)
	if len(advisory) != 0 {
		c.mu.Unlock()
		t.Fatalf("advisory frames = %d, want 0", len(advisory))
	}
	if len(urgent) != 1 {
		c.mu.Unlock()
		t.Fatalf("urgent frames = %d, want 1", len(urgent))
	}
	got := urgent[0]
	if c.flow.recvSessionAdvertised != 2048 {
		c.mu.Unlock()
		t.Fatalf("recvSessionAdvertised = %d, want 2048", c.flow.recvSessionAdvertised)
	}
	c.mu.Unlock()

	assertWireExampleFrame(t, got, wantMaxData)
}

func TestWireExamplePairwiseOpenMetadataPriorityAndGroup(t *testing.T) {
	t.Parallel()

	c, frames, stop := newInvalidFrameConn(t, CapabilityOpenMetadata|CapabilityPriorityHints|CapabilityStreamGroups)
	defer stop()

	frame := mustParseWireExampleFrame(t, "0b 21 04 06 01 01 02 02 01 05 68 69")

	if err := c.handleFrame(frame); err != nil {
		t.Fatalf("handle DATA|OPEN_METADATA with priority/group: %v", err)
	}
	assertNoQueuedFrame(t, frames)

	c.mu.Lock()
	stream := c.registry.streams[frame.StreamID]
	if stream == nil {
		c.mu.Unlock()
		t.Fatalf("stream %d missing after opener", frame.StreamID)
	}
	if got := string(stream.readBuf); got != "hi" {
		c.mu.Unlock()
		t.Fatalf("readBuf = %q, want %q", got, "hi")
	}
	c.mu.Unlock()

	meta := stream.Metadata()
	if meta.Priority != 2 {
		t.Fatalf("priority = %d, want 2", meta.Priority)
	}
	if meta.Group == nil || *meta.Group != 5 {
		t.Fatalf("group = %v, want 5", meta.Group)
	}
}
