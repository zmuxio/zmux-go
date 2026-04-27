package wire

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

type greedyReader struct {
	data []byte
}

func (r *greedyReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	n := copy(p, r.data)
	r.data = r.data[n:]
	return n, nil
}

type countingByteReader struct {
	data  []byte
	reads int
}

func (r *countingByteReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	n := copy(p, r.data)
	r.reads += n
	r.data = r.data[n:]
	return n, nil
}

func (r *countingByteReader) ReadByte() (byte, error) {
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	r.reads++
	b := r.data[0]
	r.data = r.data[1:]
	return b, nil
}

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

func TestReadFrameBufferedRejectsShortStreamIDBeforeReadingPastFrame(t *testing.T) {
	t.Parallel()

	raw := []byte{2, byte(FrameTypeDATA), 0xc0}
	reader := &countingByteReader{data: raw}

	_, _, handle, err := ReadFrameBuffered(reader, Limits{}, nil)
	if !errors.Is(err, ErrShortFrame) {
		t.Fatalf("ReadFrameBuffered err = %v, want %v", err, ErrShortFrame)
	}
	if handle != nil {
		t.Fatalf("ReadFrameBuffered handle = %v, want nil", handle)
	}
	if reader.reads != len(raw) {
		t.Fatalf("reader reads = %d, want %d without reading past declared frame", reader.reads, len(raw))
	}
}

func TestReadFrameBufferedNonByteReaderDoesNotOverreadNextFrame(t *testing.T) {
	t.Parallel()

	first, err := (Frame{Type: FrameTypeDATA, StreamID: 4, Payload: []byte("one")}).AppendBinary(nil)
	if err != nil {
		t.Fatalf("AppendBinary(first) err = %v", err)
	}
	second, err := (Frame{Type: FrameTypeDATA, StreamID: 8, Payload: []byte("two")}).AppendBinary(nil)
	if err != nil {
		t.Fatalf("AppendBinary(second) err = %v", err)
	}
	reader := &greedyReader{data: append(append([]byte(nil), first...), second...)}

	frame, buf, handle, err := ReadFrameBuffered(reader, Limits{}, nil)
	if err != nil {
		t.Fatalf("ReadFrameBuffered(first) err = %v", err)
	}
	if frame.Type != FrameTypeDATA || frame.StreamID != 4 || !bytes.Equal(frame.Payload, []byte("one")) {
		t.Fatalf("first frame = (%s, %d, %q), want (DATA, 4, %q)", frame.Type, frame.StreamID, frame.Payload, "one")
	}
	ReleaseReadFrameBuffer(buf, handle)

	frame, buf, handle, err = ReadFrameBuffered(reader, Limits{}, nil)
	if err != nil {
		t.Fatalf("ReadFrameBuffered(second) err = %v", err)
	}
	defer ReleaseReadFrameBuffer(buf, handle)
	if frame.Type != FrameTypeDATA || frame.StreamID != 8 || !bytes.Equal(frame.Payload, []byte("two")) {
		t.Fatalf("second frame = (%s, %d, %q), want (DATA, 8, %q)", frame.Type, frame.StreamID, frame.Payload, "two")
	}
}

func TestRetainedFrameReadBufferRejectsForeignSameCapBuffer(t *testing.T) {
	t.Parallel()

	owned := make([]byte, 0, minFrameReadBufferBucketSize)
	foreign := make([]byte, 0, minFrameReadBufferBucketSize)
	handle := &FrameReadBufferHandle{buf: owned}

	got := retainedFrameReadBuffer(foreign, handle, minFrameReadBufferBucketSize)
	if !sameBufferBackingStart(got, owned) {
		t.Fatal("retainedFrameReadBuffer replaced handle backing with a foreign buffer")
	}
	if sameBufferBackingStart(got, foreign) {
		t.Fatal("retainedFrameReadBuffer retained foreign backing")
	}
}

func TestRetainedFrameReadBufferAcceptsOwnedSameCapBuffer(t *testing.T) {
	t.Parallel()

	owned := make([]byte, 0, minFrameReadBufferBucketSize)
	handle := &FrameReadBufferHandle{buf: owned}
	ownedView := owned[:minFrameReadBufferBucketSize/2]

	got := retainedFrameReadBuffer(ownedView, handle, minFrameReadBufferBucketSize)
	if !sameBufferBackingStart(got, owned) {
		t.Fatal("retainedFrameReadBuffer rejected the owned backing")
	}
}

func TestReadPrefaceNonByteReaderDoesNotOverreadFollowingFrame(t *testing.T) {
	t.Parallel()

	want := Preface{
		PrefaceVersion:  PrefaceVersion,
		Role:            RoleInitiator,
		TieBreakerNonce: 1,
		MinProto:        ProtoVersion,
		MaxProto:        ProtoVersion,
		Settings:        DefaultSettings(),
	}
	prefaceBytes, err := want.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary err = %v", err)
	}
	frameBytes, err := (Frame{Type: FrameTypeDATA, StreamID: 4, Payload: []byte("after-preface")}).AppendBinary(nil)
	if err != nil {
		t.Fatalf("AppendBinary(frame) err = %v", err)
	}
	reader := &greedyReader{data: append(append([]byte(nil), prefaceBytes...), frameBytes...)}

	got, err := ReadPreface(reader)
	if err != nil {
		t.Fatalf("ReadPreface err = %v", err)
	}
	if got.Role != want.Role || got.MinProto != want.MinProto || got.MaxProto != want.MaxProto {
		t.Fatalf("ReadPreface = %#v, want role/proto from %#v", got, want)
	}

	frame, buf, handle, err := ReadFrameBuffered(reader, Limits{}, nil)
	if err != nil {
		t.Fatalf("ReadFrameBuffered(after preface) err = %v", err)
	}
	defer ReleaseReadFrameBuffer(buf, handle)
	if frame.Type != FrameTypeDATA || frame.StreamID != 4 || !bytes.Equal(frame.Payload, []byte("after-preface")) {
		t.Fatalf("frame after preface = (%s, %d, %q), want (DATA, 4, %q)", frame.Type, frame.StreamID, frame.Payload, "after-preface")
	}
}
