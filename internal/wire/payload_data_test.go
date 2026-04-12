package wire

import (
	"bytes"
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
