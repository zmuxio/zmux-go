package wire

import (
	"bytes"
	"errors"
	"testing"
)

func TestBuildOpenMetadataPrefixCopiesOpenInfoIntoEncodedPrefix(t *testing.T) {
	t.Parallel()

	openInfo := []byte("ssh")
	prefix, err := BuildOpenMetadataPrefix(CapabilityOpenMetadata, nil, nil, openInfo, 1024)
	if err != nil {
		t.Fatalf("BuildOpenMetadataPrefix err = %v", err)
	}

	openInfo[0] = 'x'

	payload, err := ParseDataPayload(prefix, FrameFlagOpenMetadata)
	if err != nil {
		t.Fatalf("ParseDataPayload err = %v", err)
	}
	if !bytes.Equal(payload.OpenInfo, []byte("ssh")) {
		t.Fatalf("OpenInfo = %q, want %q", payload.OpenInfo, "ssh")
	}
}

func TestBuildOpenMetadataPrefixValidation(t *testing.T) {
	t.Parallel()

	if _, err := BuildOpenMetadataPrefix(0, nil, nil, []byte("ssh"), 1024); !errors.Is(err, ErrOpenInfoUnavailable) {
		t.Fatalf("missing capability err = %v, want %v", err, ErrOpenInfoUnavailable)
	}

	if _, err := BuildOpenMetadataPrefix(CapabilityOpenMetadata, nil, nil, []byte("ssh"), 1); !errors.Is(err, ErrOpenMetadataTooLarge) {
		t.Fatalf("oversize err = %v, want %v", err, ErrOpenMetadataTooLarge)
	}
}

func TestAppendOpenMetadataPrefixTightensOversizedReuseBuffer(t *testing.T) {
	t.Parallel()

	oversized := make([]byte, 0, 64)
	prefix, err := AppendOpenMetadataPrefix(oversized, CapabilityOpenMetadata, nil, nil, []byte("ssh"), 1024)
	if err != nil {
		t.Fatalf("AppendOpenMetadataPrefix err = %v", err)
	}

	if got := cap(prefix); got != len(prefix) {
		t.Fatalf("cap(prefix) = %d, want tight cap %d", got, len(prefix))
	}
}

func TestAppendOpenMetadataPrefixReusesExactBuffer(t *testing.T) {
	t.Parallel()

	first, err := BuildOpenMetadataPrefix(CapabilityOpenMetadata, nil, nil, []byte("ssh"), 1024)
	if err != nil {
		t.Fatalf("BuildOpenMetadataPrefix err = %v", err)
	}
	second, err := AppendOpenMetadataPrefix(first[:0], CapabilityOpenMetadata, nil, nil, []byte("ssh"), 1024)
	if err != nil {
		t.Fatalf("AppendOpenMetadataPrefix err = %v", err)
	}

	if len(first) == 0 || len(second) == 0 {
		t.Fatal("unexpected empty prefix")
	}
	if &second[0] != &first[0] {
		t.Fatal("AppendOpenMetadataPrefix did not reuse exact-size buffer")
	}
}
