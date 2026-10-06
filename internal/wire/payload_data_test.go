package wire

import (
	"bytes"
	"errors"
	"testing"
)

func TestParseStreamMetadataTLVsRoundTrip(t *testing.T) {
	t.Parallel()

	tlvs := []TLV{
		{Type: uint64(MetadataStreamPriority), Value: mustEncodeVarintForPayloadDataTest(t, 7)},
		{Type: uint64(MetadataStreamGroup), Value: mustEncodeVarintForPayloadDataTest(t, 11)},
		{Type: uint64(MetadataOpenInfo), Value: []byte("ssh")},
	}

	parsed, ok, err := ParseStreamMetadataTLVs(tlvs)
	if err != nil {
		t.Fatalf("ParseStreamMetadataTLVs err = %v", err)
	}
	if !ok {
		t.Fatal("ParseStreamMetadataTLVs ok = false, want true")
	}
	if !parsed.HasPriority || parsed.Priority != 7 {
		t.Fatalf("priority = (%v, %d), want (true, 7)", parsed.HasPriority, parsed.Priority)
	}
	if !parsed.HasGroup || parsed.Group != 11 {
		t.Fatalf("group = (%v, %d), want (true, 11)", parsed.HasGroup, parsed.Group)
	}
	if !bytes.Equal(parsed.OpenInfo, []byte("ssh")) {
		t.Fatalf("OpenInfo = %q, want %q", parsed.OpenInfo, "ssh")
	}
}

func TestParseStreamMetadataTLVsRejectsDuplicateSingleton(t *testing.T) {
	t.Parallel()

	parsed, ok, err := ParseStreamMetadataTLVs([]TLV{
		{Type: uint64(MetadataOpenInfo), Value: []byte("a")},
		{Type: uint64(MetadataOpenInfo), Value: []byte("b")},
	})
	if err != nil {
		t.Fatalf("ParseStreamMetadataTLVs err = %v, want nil", err)
	}
	if ok {
		t.Fatalf("ParseStreamMetadataTLVs ok = true, want false; parsed=%+v", parsed)
	}
}

func TestParseStreamMetadataTLVsViewAliasesOpenInfo(t *testing.T) {
	t.Parallel()

	tlvs := []TLV{
		{Type: uint64(MetadataOpenInfo), Value: []byte("ssh")},
	}

	parsed, ok, err := ParseStreamMetadataTLVsView(tlvs)
	if err != nil {
		t.Fatalf("ParseStreamMetadataTLVsView err = %v", err)
	}
	if !ok {
		t.Fatal("ParseStreamMetadataTLVsView ok = false, want true")
	}

	tlvs[0].Value[2] = 'x'
	if !bytes.Equal(parsed.OpenInfo, []byte("ssx")) {
		t.Fatalf("OpenInfo after source mutation = %q, want %q", parsed.OpenInfo, "ssx")
	}
}

func TestParseStreamMetadataBytesViewAliasesOpenInfoSource(t *testing.T) {
	t.Parallel()

	raw, err := BuildOpenMetadataPrefix(CapabilityOpenMetadata|CapabilityPriorityHints|CapabilityStreamGroups, nil, nil, []byte("ssh"), 1024)
	if err != nil {
		t.Fatalf("BuildOpenMetadataPrefix err = %v", err)
	}
	metadataLen, n, err := ParseVarint(raw)
	if err != nil {
		t.Fatalf("ParseVarint err = %v", err)
	}
	parsed, ok, err := ParseStreamMetadataBytesView(raw[n : n+int(metadataLen)])
	if err != nil {
		t.Fatalf("ParseStreamMetadataBytesView err = %v", err)
	}
	if !ok {
		t.Fatal("ParseStreamMetadataBytesView ok = false, want true")
	}

	raw[len(raw)-1] = 'x'
	if !bytes.Equal(parsed.OpenInfo, []byte("ssx")) {
		t.Fatalf("OpenInfo after source mutation = %q, want %q", parsed.OpenInfo, "ssx")
	}
}

func TestParseDataPayloadRetainsOpenInfoAfterSourceMutation(t *testing.T) {
	t.Parallel()

	raw, err := BuildOpenMetadataPrefix(CapabilityOpenMetadata, nil, nil, []byte("ssh"), 1024)
	if err != nil {
		t.Fatalf("BuildOpenMetadataPrefix err = %v", err)
	}
	payload, err := ParseDataPayload(raw, FrameFlagOpenMetadata)
	if err != nil {
		t.Fatalf("ParseDataPayload err = %v", err)
	}

	for i := range raw {
		raw[i] ^= 0xff
	}

	if !bytes.Equal(payload.OpenInfo, []byte("ssh")) {
		t.Fatalf("OpenInfo after source mutation = %q, want %q", payload.OpenInfo, "ssh")
	}
}

func TestParseDataPayloadRetainsMetadataTLVsAfterSourceMutation(t *testing.T) {
	t.Parallel()

	priority := uint64(7)
	raw, err := BuildOpenMetadataPrefix(CapabilityOpenMetadata|CapabilityPriorityHints, &priority, nil, []byte("ssh"), 1024)
	if err != nil {
		t.Fatalf("BuildOpenMetadataPrefix err = %v", err)
	}
	payload, err := ParseDataPayload(raw, FrameFlagOpenMetadata)
	if err != nil {
		t.Fatalf("ParseDataPayload err = %v", err)
	}
	if len(payload.MetadataTLVs) == 0 {
		t.Fatal("MetadataTLVs empty, want parsed metadata")
	}
	values := make([][]byte, len(payload.MetadataTLVs))
	for i := range payload.MetadataTLVs {
		values[i] = append([]byte(nil), payload.MetadataTLVs[i].Value...)
	}

	for i := range raw {
		raw[i] ^= 0xff
	}

	for i := range payload.MetadataTLVs {
		if !bytes.Equal(payload.MetadataTLVs[i].Value, values[i]) {
			t.Fatalf("MetadataTLVs[%d].Value after source mutation = %x, want %x", i, payload.MetadataTLVs[i].Value, values[i])
		}
	}
}

func TestParseDataPayloadOpenInfoDoesNotAliasMetadataTLVs(t *testing.T) {
	t.Parallel()

	raw, err := BuildOpenMetadataPrefix(CapabilityOpenMetadata, nil, nil, []byte("ssh"), 1024)
	if err != nil {
		t.Fatalf("BuildOpenMetadataPrefix err = %v", err)
	}
	payload, err := ParseDataPayload(raw, FrameFlagOpenMetadata)
	if err != nil {
		t.Fatalf("ParseDataPayload err = %v", err)
	}

	for i := range payload.MetadataTLVs {
		if StreamMetadataType(payload.MetadataTLVs[i].Type) == MetadataOpenInfo {
			payload.MetadataTLVs[i].Value[0] = 'x'
		}
	}

	if !bytes.Equal(payload.OpenInfo, []byte("ssh")) {
		t.Fatalf("OpenInfo after MetadataTLVs mutation = %q, want %q", payload.OpenInfo, "ssh")
	}
}

func TestParseDataPayloadViewAliasesOpenInfoSource(t *testing.T) {
	t.Parallel()

	raw, err := BuildOpenMetadataPrefix(CapabilityOpenMetadata, nil, nil, []byte("ssh"), 1024)
	if err != nil {
		t.Fatalf("BuildOpenMetadataPrefix err = %v", err)
	}
	payload, err := ParseDataPayloadView(raw, FrameFlagOpenMetadata)
	if err != nil {
		t.Fatalf("ParseDataPayloadView err = %v", err)
	}

	raw[len(raw)-1] = 'x'

	if !bytes.Equal(payload.OpenInfo, []byte("ssx")) {
		t.Fatalf("OpenInfo after source mutation = %q, want %q", payload.OpenInfo, "ssx")
	}
}

func mustEncodeVarintForPayloadDataTest(t *testing.T, v uint64) []byte {
	t.Helper()

	out, err := EncodeVarint(v)
	if err != nil {
		t.Fatalf("EncodeVarint(%d): %v", v, err)
	}
	return out
}

func TestParseDataPayloadAppDataDecodesOnlyMetadataLength(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		payload []byte
		flags   byte
		want    []byte
		wantErr error
	}{
		{name: "no_open_metadata", payload: []byte("app"), want: []byte("app")},
		// A truncated TLV inside the metadata block is skipped uninterpreted.
		{name: "malformed_tlvs_skipped", payload: []byte{0x02, 0x01, 0x05, 'a', 'p', 'p'}, flags: FrameFlagOpenMetadata, want: []byte("app")},
		{name: "empty_metadata", payload: []byte{0x00, 'x'}, flags: FrameFlagOpenMetadata, want: []byte("x")},
		{name: "missing_metadata_len", payload: nil, flags: FrameFlagOpenMetadata, wantErr: ErrTruncatedVarint},
		{name: "truncated_metadata_len", payload: []byte{0x40}, flags: FrameFlagOpenMetadata, wantErr: ErrTruncatedVarint},
		{name: "non_canonical_metadata_len", payload: []byte{0x40, 0x01, 0x00}, flags: FrameFlagOpenMetadata, wantErr: ErrNonCanonicalVarint},
		{name: "metadata_len_overrun", payload: []byte{0x05, 0x00}, flags: FrameFlagOpenMetadata, wantErr: ErrTLVValueOverrun},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := ParseDataPayloadAppData(tc.payload, tc.flags)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("ParseDataPayloadAppData err = %v, want %v", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseDataPayloadAppData err = %v", err)
			}
			if !bytes.Equal(got, tc.want) {
				t.Fatalf("app data = %q, want %q", got, tc.want)
			}
		})
	}
}
