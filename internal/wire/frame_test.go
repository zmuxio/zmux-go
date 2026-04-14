package wire

import (
	"bytes"
	"errors"
	"testing"
)

func TestReadFrameBufferedRejectsOversizedPayloadBeforeReadingBody(t *testing.T) {
	t.Parallel()

	limits := Limits{
		MaxFramePayload:          1 << 20,
		MaxControlPayloadBytes:   16,
		MaxExtensionPayloadBytes: 16,
	}
	frameLen := limits.MaxControlPayloadBytes + 3
	raw, err := AppendVarint(nil, frameLen)
	if err != nil {
		t.Fatalf("AppendVarint err = %v", err)
	}
	raw = append(raw, byte(FrameTypePING))
	raw, err = AppendVarint(raw, 0)
	if err != nil {
		t.Fatalf("AppendVarint stream id err = %v", err)
	}

	_, _, handle, err := ReadFrameBuffered(bytes.NewReader(raw), limits, nil)
	if !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("ReadFrameBuffered err = %v, want %v", err, ErrPayloadTooLarge)
	}
	if handle != nil {
		t.Fatalf("ReadFrameBuffered handle = %v, want nil", handle)
	}
}
