package wire

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

func TestReadFrameNonCanonicalFrameLengthIsProtocol(t *testing.T) {
	t.Parallel()

	// frame_length=2 in two bytes, then DATA on stream 4.
	raw := []byte{0x40, 0x02, 0x01, 0x04}
	for _, read := range []struct {
		name string
		fn   func([]byte) error
	}{
		{"ReadFrame", func(raw []byte) error {
			_, err := ReadFrame(bytes.NewReader(raw), Limits{})
			return err
		}},
		{"ReadFrame_unbuffered", func(raw []byte) error {
			_, err := ReadFrame(&greedyReader{data: raw}, Limits{})
			return err
		}},
		{"ReadSessionFrameBuffered", func(raw []byte) error {
			_, _, _, err := ReadSessionFrameBuffered(bytes.NewReader(raw), Limits{}, nil)
			return err
		}},
		{"ParseFrame", func(raw []byte) error {
			_, _, err := ParseFrame(raw, Limits{})
			return err
		}},
	} {
		err := read.fn(raw)
		if !IsErrorCode(err, CodeProtocol) {
			t.Fatalf("%s(% x) err = %v, want %s", read.name, raw, err, CodeProtocol)
		}
		if !errors.Is(err, ErrNonCanonicalVarint) {
			t.Fatalf("%s(% x) err = %v, want it to wrap %v", read.name, raw, err, ErrNonCanonicalVarint)
		}
	}
}

func TestReadFrameFrameLengthTransportEndsKeepTransportErrors(t *testing.T) {
	t.Parallel()

	if _, err := ReadFrame(bytes.NewReader(nil), Limits{}); err != io.EOF {
		t.Fatalf("ReadFrame(empty) err = %v, want exactly io.EOF at a frame boundary", err)
	}
	// The transport ends inside a two-byte frame_length, as it would inside
	// stream_id: an unexpected EOF, not a peer protocol error.
	_, err := ReadFrame(bytes.NewReader([]byte{0x40}), Limits{})
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("ReadFrame(truncated frame_length) err = %v, want %v", err, io.ErrUnexpectedEOF)
	}
	if _, ok := ErrorCodeOf(err); ok {
		t.Fatalf("ReadFrame(truncated frame_length) err = %v, want no wire error code", err)
	}
	transportErr := errors.New("transport broke")
	_, err = ReadFrame(io.MultiReader(bytes.NewReader([]byte{0x40}), &failingReader{err: transportErr}), Limits{})
	if !errors.Is(err, transportErr) {
		t.Fatalf("ReadFrame(transport error in frame_length) err = %v, want %v", err, transportErr)
	}
}

type failingReader struct {
	err error
}

func (r *failingReader) Read([]byte) (int, error) {
	return 0, r.err
}

func TestEXTPayloadShorterThanExtTypeIsProtocol(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		raw  []byte
	}{
		{"empty_payload", []byte{0x02, 0x0b, 0x00}},
		{"truncated_ext_type", []byte{0x03, 0x0b, 0x00, 0x40}},
		{"non_canonical_ext_type", []byte{0x04, 0x0b, 0x04, 0x40, 0x01}},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if _, _, err := ParseFrame(tc.raw, Limits{}); !IsErrorCode(err, CodeProtocol) {
				t.Fatalf("ParseFrame(% x) err = %v, want %s (SPEC §6.11)", tc.raw, err, CodeProtocol)
			}
			if _, err := ReadFrame(bytes.NewReader(tc.raw), Limits{}); !IsErrorCode(err, CodeProtocol) {
				t.Fatalf("ReadFrame(% x) err = %v, want %s", tc.raw, err, CodeProtocol)
			}
			if _, _, _, err := ReadSessionFrameBuffered(bytes.NewReader(tc.raw), Limits{}, nil); !IsErrorCode(err, CodeProtocol) {
				t.Fatalf("ReadSessionFrameBuffered(% x) err = %v, want %s", tc.raw, err, CodeProtocol)
			}
		})
	}
}

func TestSessionFrameReaderDefersPriorityUpdateSubtypeRules(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name       string
		raw        []byte
		strictCode ErrorCode
	}{
		// PRIORITY_UPDATE on stream 4 whose TLV header is truncated.
		{"truncated_tlv", []byte{0x04, 0x0b, 0x04, 0x01, 0x01}, CodeFrameSize},
		// PRIORITY_UPDATE on stream 4 whose TLV value overruns the payload.
		{"tlv_value_overrun", []byte{0x06, 0x0b, 0x04, 0x01, 0x01, 0x02, 0x01}, CodeFrameSize},
		// Well-formed PRIORITY_UPDATE on stream 0.
		{"stream_zero", []byte{0x06, 0x0b, 0x00, 0x01, 0x01, 0x01, 0x02}, CodeProtocol},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if _, _, err := ParseFrame(tc.raw, Limits{}); !IsErrorCode(err, tc.strictCode) {
				t.Fatalf("ParseFrame(% x) err = %v, want %s", tc.raw, err, tc.strictCode)
			}
			if _, err := ReadFrame(bytes.NewReader(tc.raw), Limits{}); !IsErrorCode(err, tc.strictCode) {
				t.Fatalf("ReadFrame(% x) err = %v, want %s", tc.raw, err, tc.strictCode)
			}
			// The session decides only after its capability check (SPEC §7.6).
			frame, _, _, err := ReadSessionFrameBuffered(bytes.NewReader(tc.raw), Limits{}, nil)
			if err != nil {
				t.Fatalf("ReadSessionFrameBuffered(% x) err = %v, want the frame handed to the session", tc.raw, err)
			}
			if frame.Type != FrameTypeEXT || !bytes.Equal(frame.Payload, tc.raw[3:]) {
				t.Fatalf("ReadSessionFrameBuffered(% x) = %+v, want the EXT frame", tc.raw, frame)
			}
		})
	}
}

func TestPriorityUpdateDuplicateSingletonDoesNotSkipLaterTLVStructure(t *testing.T) {
	t.Parallel()

	// priority=2, duplicate priority=3, then a truncated TLV header.
	dupThenTruncated := []byte{0x01, 0x01, 0x02, 0x01, 0x01, 0x03, 0x01}
	if _, _, err := ParsePriorityUpdatePayload(dupThenTruncated); !errors.Is(err, ErrTruncatedTLV) {
		t.Fatalf("ParsePriorityUpdatePayload(dup then truncated) err = %v, want %v", err, ErrTruncatedTLV)
	}
	ext := append([]byte{byte(EXTPriorityUpdate)}, dupThenTruncated...)
	if err := ValidateEXTPayload(4, ext); !IsErrorCode(err, CodeFrameSize) {
		t.Fatalf("ValidateEXTPayload(dup then truncated) err = %v, want %s", err, CodeFrameSize)
	}
	raw := append([]byte{0x0a, 0x0b, 0x04}, ext...)
	if _, _, err := ParseFrame(raw, Limits{}); !IsErrorCode(err, CodeFrameSize) {
		t.Fatalf("ParseFrame(% x) err = %v, want %s", raw, err, CodeFrameSize)
	}

	// Group duplicate followed by a TLV whose value overruns the payload.
	dupThenOverrun := []byte{0x02, 0x01, 0x05, 0x02, 0x01, 0x06, 0x01, 0x04, 0x00}
	if _, _, err := ParsePriorityUpdatePayload(dupThenOverrun); !errors.Is(err, ErrTLVValueOverrun) {
		t.Fatalf("ParsePriorityUpdatePayload(dup then overrun) err = %v, want %v", err, ErrTLVValueOverrun)
	}

	// A duplicate with a well-formed tail still only drops the update.
	dupOnly := []byte{0x01, 0x01, 0x02, 0x01, 0x01, 0x03, 0x3f, 0x00}
	meta, ok, err := ParsePriorityUpdatePayload(dupOnly)
	if err != nil || ok || meta.HasPriority || meta.HasGroup {
		t.Fatalf("ParsePriorityUpdatePayload(dup only) = (%+v, %v, %v), want dropped update without error", meta, ok, err)
	}
}

func TestStreamMetadataDuplicateSingletonDoesNotSkipLaterTLVStructure(t *testing.T) {
	t.Parallel()

	dupThenTruncated := []byte{0x01, 0x01, 0x02, 0x01, 0x01, 0x03, 0x01}
	if _, _, err := ParseStreamMetadataBytesView(dupThenTruncated); !errors.Is(err, ErrTruncatedTLV) {
		t.Fatalf("ParseStreamMetadataBytesView(dup then truncated) err = %v, want %v", err, ErrTruncatedTLV)
	}
	payload := append([]byte{byte(len(dupThenTruncated))}, dupThenTruncated...)
	payload = append(payload, 'h', 'i')
	if _, err := ParseDataPayloadView(payload, FrameFlagOpenMetadata); !errors.Is(err, ErrTruncatedTLV) {
		t.Fatalf("ParseDataPayloadView(dup then truncated) err = %v, want %v", err, ErrTruncatedTLV)
	}

	dupOnly := []byte{0x03, 0x01, 'a', 0x03, 0x01, 'b'}
	meta, ok, err := ParseStreamMetadataBytesView(dupOnly)
	if err != nil || ok || meta.OpenInfo != nil {
		t.Fatalf("ParseStreamMetadataBytesView(dup only) = (%+v, %v, %v), want dropped block without error", meta, ok, err)
	}
}

func TestDIAGDuplicateDoesNotSkipLaterTLVStructure(t *testing.T) {
	t.Parallel()

	// debug_text "a", duplicate debug_text "b", then a truncated TLV header.
	dupThenTruncated := []byte{0x01, 0x01, 'a', 0x01, 0x01, 'b', 0x01}
	if _, err := ParseDIAGReason(dupThenTruncated); !errors.Is(err, ErrTruncatedTLV) {
		t.Fatalf("ParseDIAGReason(dup then truncated) err = %v, want %v", err, ErrTruncatedTLV)
	}
	dupThenOverrun := []byte{0x02, 0x01, 0x05, 0x02, 0x01, 0x06, 0x01, 0x04, 'x'}
	if _, err := ParseDIAGReason(dupThenOverrun); !errors.Is(err, ErrTLVValueOverrun) {
		t.Fatalf("ParseDIAGReason(dup then overrun) err = %v, want %v", err, ErrTLVValueOverrun)
	}

	reason, err := ParseDIAGReason([]byte{0x01, 0x01, 'a', 0x01, 0x01, 'b', 0x3f, 0x00})
	if err != nil || reason != "" {
		t.Fatalf("ParseDIAGReason(dup only) = (%q, %v), want dropped reason without error", reason, err)
	}
	reason, err = ParseDIAGReason([]byte{0x01, 0x02, 'o', 'k', 0x3f, 0x00})
	if err != nil || reason != "ok" {
		t.Fatalf("ParseDIAGReason(single) = (%q, %v), want %q", reason, err, "ok")
	}
}
