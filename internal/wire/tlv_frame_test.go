package wire

import (
	"errors"
	"testing"
)

func TestParseTLVsViewAliasesValueBytes(t *testing.T) {
	t.Parallel()

	raw, err := AppendTLV(nil, 1, []byte("ssh"))
	if err != nil {
		t.Fatalf("AppendTLV err = %v", err)
	}
	tlvs, err := ParseTLVsView(raw)
	if err != nil {
		t.Fatalf("ParseTLVsView err = %v", err)
	}
	if len(tlvs) != 1 {
		t.Fatalf("len(ParseTLVsView) = %d, want 1", len(tlvs))
	}

	raw[len(raw)-1] = 'x'
	if string(tlvs[0].Value) != "ssx" {
		t.Fatalf("TLV value after source mutation = %q, want %q", tlvs[0].Value, "ssx")
	}
}

func TestParseTLVsPreservesNonCanonicalVarintError(t *testing.T) {
	t.Parallel()

	_, err := ParseTLVs([]byte{0x40, 0x01, 0x00})
	if !errors.Is(err, ErrNonCanonicalVarint) {
		t.Fatalf("ParseTLVs err = %v, want %v", err, ErrNonCanonicalVarint)
	}
}

func TestParseDIAGReasonPreservesNonCanonicalVarintError(t *testing.T) {
	t.Parallel()

	_, err := ParseDIAGReason([]byte{0x40, 0x01, 0x00})
	if !errors.Is(err, ErrNonCanonicalVarint) {
		t.Fatalf("ParseDIAGReason err = %v, want %v", err, ErrNonCanonicalVarint)
	}
}

func TestParseDIAGReasonTruncatedVarintReturnsErrTruncatedTLV(t *testing.T) {
	t.Parallel()

	_, err := ParseDIAGReason([]byte{0x40})
	if !errors.Is(err, ErrTruncatedTLV) {
		t.Fatalf("ParseDIAGReason err = %v, want %v", err, ErrTruncatedTLV)
	}
}

func TestFrameTotalLenRejectsIntOverflow(t *testing.T) {
	t.Parallel()

	maxInt := int(^uint(0) >> 1)
	if uint64(maxInt) >= MaxVarint62 {
		t.Skip("frame length overflow is unreachable on this architecture")
	}
	if _, err := frameTotalLen(uint64(maxInt), 1); !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("frameTotalLen err = %v, want %v", err, ErrPayloadTooLarge)
	}
}
