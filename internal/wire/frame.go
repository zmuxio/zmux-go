package wire

import (
	"errors"
	"fmt"
	"io"
)

const maxInboundFrameHeaderOverhead = 9

func (f Frame) MarshalBinary() ([]byte, error) {
	return f.AppendBinary(nil)
}

func AppendFrameHeaderTrusted(dst []byte, code byte, streamID uint64, payloadLen uint64) ([]byte, error) {
	streamLen, err := VarintLen(streamID)
	if err != nil {
		return nil, WrapError(CodeProtocol, "marshal frame", err)
	}
	length := uint64(1+streamLen) + payloadLen
	dst, err = AppendVarint(dst, length)
	if err != nil {
		return nil, WrapError(CodeProtocol, "marshal frame", err)
	}
	dst = append(dst, code)
	dst, err = AppendVarint(dst, streamID)
	if err != nil {
		return nil, WrapError(CodeProtocol, "marshal frame", err)
	}
	return dst, nil
}

func AppendFrameHeaderTrustedCachedStreamID(dst []byte, code byte, streamID uint64, packedStreamID uint64, streamIDLen uint8, payloadLen uint64) ([]byte, error) {
	if streamIDLen == 0 {
		return AppendFrameHeaderTrusted(dst, code, streamID, payloadLen)
	}
	length := uint64(1+streamIDLen) + payloadLen
	var err error
	dst, err = AppendVarint(dst, length)
	if err != nil {
		return nil, WrapError(CodeProtocol, "marshal frame", err)
	}
	dst = append(dst, code)
	dst, err = AppendPackedVarint(dst, packedStreamID, streamIDLen)
	if err != nil {
		return nil, WrapError(CodeProtocol, "marshal frame", err)
	}
	return dst, nil
}

func (f Frame) AppendBinary(dst []byte) ([]byte, error) {
	if err := ValidateFrame(f, NormalizeLimits(Limits{}), false); err != nil {
		return nil, err
	}
	dst, err := AppendFrameHeaderTrusted(dst, f.Code(), f.StreamID, uint64(len(f.Payload)))
	if err != nil {
		return nil, err
	}
	dst = append(dst, f.Payload...)
	return dst, nil
}

func ParseFrame(src []byte, limits Limits) (Frame, int, error) {
	limits = NormalizeLimits(limits)

	frameLen, n, err := ParseVarint(src)
	if err != nil {
		return Frame{}, 0, WrapError(CodeProtocol, "parse frame_length", err)
	}
	if frameLen < 2 {
		return Frame{}, 0, FrameSizeError("parse frame_length", ErrShortFrame)
	}
	if len(src) < n+1 {
		return Frame{}, 0, io.ErrUnexpectedEOF
	}
	code := src[n]
	frameType := FrameType(code & 0x1f)
	flags := code & 0xe0
	if !frameType.Valid() {
		return Frame{}, 0, WrapError(CodeProtocol, "parse frame code", ErrInvalidFrameType)
	}

	total, err := frameTotalLen(frameLen, n)
	if err != nil {
		return Frame{}, 0, err
	}
	frame, err := parseBufferedFrame(src, frameLen, n, total, code, frameType, flags, limits)
	if err != nil {
		return Frame{}, 0, err
	}
	return frame, total, nil
}

func parseBufferedFrame(src []byte, frameLen uint64, n int, total int, code byte, frameType FrameType, flags byte, limits Limits) (Frame, error) {
	streamID, streamLen, err := ParseVarint(src[n+1:])
	if err != nil {
		if errors.Is(err, ErrTruncatedVarint) {
			return Frame{}, io.ErrUnexpectedEOF
		}
		return Frame{}, WrapError(CodeProtocol, "parse stream_id", err)
	}
	if frameLen < uint64(1+streamLen) {
		return Frame{}, FrameSizeError("parse frame", ErrShortFrame)
	}

	payloadLen := frameLen - uint64(1+streamLen)
	if payloadLen > InboundPayloadLimit(frameType, limits) {
		return Frame{}, FrameSizeError("parse frame", ErrPayloadTooLarge)
	}

	if len(src) < total {
		return Frame{}, io.ErrUnexpectedEOF
	}

	body := src[n:total]
	payload := body[1+streamLen:]
	frame := Frame{
		Length:   frameLen,
		Type:     FrameType(code & 0x1f),
		Flags:    flags,
		StreamID: streamID,
		Payload:  payload,
	}
	if err := ValidateFrame(frame, limits, true); err != nil {
		return Frame{}, err
	}
	return frame, nil
}

func ReadFrameBuffered(r io.Reader, limits Limits, dst []byte) (Frame, []byte, *FrameReadBufferHandle, error) {
	limits = NormalizeLimits(limits)

	br, ok := r.(io.ByteReader)
	if !ok {
		br = &exactByteReader{reader: r}
	}

	frameLen, n, err := ReadVarint(br)
	if err != nil {
		return Frame{}, dst, nil, err
	}
	if frameLen < 2 {
		return Frame{}, dst, nil, FrameSizeError("read frame_length", ErrShortFrame)
	}
	if frameLen > maxInboundFrameLen(limits) {
		return Frame{}, dst, nil, FrameSizeError("read frame_length", ErrPayloadTooLarge)
	}

	var prefix [maxInboundFrameHeaderOverhead]byte
	code, err := br.ReadByte()
	if err != nil {
		if err == io.EOF {
			return Frame{}, dst, nil, io.ErrUnexpectedEOF
		}
		return Frame{}, dst, nil, err
	}
	prefix[0] = code
	frameType := FrameType(code & 0x1f)
	flags := code & 0xe0
	if !frameType.Valid() {
		return Frame{}, dst, nil, WrapError(CodeProtocol, "parse frame code", ErrInvalidFrameType)
	}

	firstStreamByte, err := br.ReadByte()
	if err != nil {
		if err == io.EOF {
			return Frame{}, dst, nil, io.ErrUnexpectedEOF
		}
		return Frame{}, dst, nil, err
	}
	prefix[1] = firstStreamByte
	streamLen := 1 << (firstStreamByte >> 6)
	for i := 1; i < streamLen; i++ {
		b, err := br.ReadByte()
		if err != nil {
			if err == io.EOF {
				return Frame{}, dst, nil, io.ErrUnexpectedEOF
			}
			return Frame{}, dst, nil, err
		}
		prefix[1+i] = b
	}
	_, parsedStreamLen, err := ParseVarint(prefix[1 : 1+streamLen])
	if err != nil {
		if errors.Is(err, ErrTruncatedVarint) {
			return Frame{}, dst, nil, io.ErrUnexpectedEOF
		}
		return Frame{}, dst, nil, WrapError(CodeProtocol, "parse stream_id", err)
	}
	if frameLen < uint64(1+parsedStreamLen) {
		return Frame{}, dst, nil, FrameSizeError("read frame", ErrShortFrame)
	}
	payloadLen := frameLen - uint64(1+parsedStreamLen)
	if payloadLen > InboundPayloadLimit(frameType, limits) {
		return Frame{}, dst, nil, FrameSizeError("read frame", ErrPayloadTooLarge)
	}

	total, err := frameTotalLen(frameLen, n)
	if err != nil {
		return Frame{}, dst, nil, err
	}
	var handle *FrameReadBufferHandle
	if cap(dst) < total {
		dst, handle = acquireFrameReadBuffer(total)
	} else {
		dst = dst[:total]
	}
	header, err := AppendVarint(dst[:0], frameLen)
	if err != nil {
		return Frame{}, dst, handle, WrapError(CodeProtocol, "read frame", err)
	}
	copy(dst[len(header):len(header)+1+parsedStreamLen], prefix[:1+parsedStreamLen])
	if _, err := io.ReadFull(r, dst[len(header)+1+parsedStreamLen:]); err != nil {
		return Frame{}, dst, handle, err
	}

	frame, err := parseBufferedFrame(dst, frameLen, n, total, code, frameType, flags, limits)
	if err != nil {
		return Frame{}, dst, handle, err
	}
	return frame, dst, handle, nil
}

type exactByteReader struct {
	reader io.Reader
	buf    [1]byte
}

func (r *exactByteReader) ReadByte() (byte, error) {
	if r == nil || r.reader == nil {
		return 0, io.ErrUnexpectedEOF
	}
	if _, err := io.ReadFull(r.reader, r.buf[:]); err != nil {
		return 0, err
	}
	return r.buf[0], nil
}

func maxInboundFrameLen(limits Limits) uint64 {
	maxPayload := limits.MaxFramePayload
	if limits.MaxControlPayloadBytes > maxPayload {
		maxPayload = limits.MaxControlPayloadBytes
	}
	if limits.MaxExtensionPayloadBytes > maxPayload {
		maxPayload = limits.MaxExtensionPayloadBytes
	}
	if maxPayload > MaxVarint62-maxInboundFrameHeaderOverhead {
		return MaxVarint62
	}
	return maxPayload + maxInboundFrameHeaderOverhead
}

func frameTotalLen(frameLen uint64, n int) (int, error) {
	maxInt := int(^uint(0) >> 1)
	if n < 0 || n > maxInt {
		return 0, FrameSizeError("parse frame_length", ErrPayloadTooLarge)
	}
	if frameLen > uint64(maxInt-n) {
		return 0, FrameSizeError("parse frame_length", ErrPayloadTooLarge)
	}
	return n + int(frameLen), nil
}

func ReadFrame(r io.Reader, limits Limits) (Frame, error) {
	frame, _, _, err := ReadFrameBuffered(r, limits, nil)
	return frame, err
}

func InboundPayloadLimit(frameType FrameType, limits Limits) uint64 {
	switch frameType {
	case FrameTypeDATA:
		return limits.MaxFramePayload
	case FrameTypeMAXDATA, FrameTypeBLOCKED, FrameTypePING, FrameTypePONG,
		FrameTypeStopSending, FrameTypeRESET, FrameTypeABORT, FrameTypeGOAWAY, FrameTypeCLOSE:
		return limits.MaxControlPayloadBytes
	case FrameTypeEXT:
		return limits.MaxExtensionPayloadBytes
	default:
		return 0
	}
}

func NormalizeLimits(limits Limits) Limits {
	def := DefaultSettings().Limits()
	if limits.MaxFramePayload == 0 {
		limits.MaxFramePayload = def.MaxFramePayload
	}
	if limits.MaxControlPayloadBytes == 0 {
		limits.MaxControlPayloadBytes = def.MaxControlPayloadBytes
	}
	if limits.MaxExtensionPayloadBytes == 0 {
		limits.MaxExtensionPayloadBytes = def.MaxExtensionPayloadBytes
	}
	return limits
}

func ValidateFrame(f Frame, limits Limits, inbound bool) error {
	if !f.Type.Valid() {
		return WrapError(CodeProtocol, "validate frame", ErrInvalidFrameType)
	}
	if err := validateFrameFlags(f.Type, f.Flags); err != nil {
		return err
	}
	if err := validateFrameScope(f); err != nil {
		return err
	}

	switch f.Type {
	case FrameTypeDATA:
		if err := validateInboundPayloadLimit(inbound, f.Payload, limits.MaxFramePayload, "validate DATA payload"); err != nil {
			return err
		}
		return validateDataPayload(f.Payload, f.Flags)
	case FrameTypeMAXDATA, FrameTypeBLOCKED:
		if err := validateInboundPayloadLimit(inbound, f.Payload, limits.MaxControlPayloadBytes, "validate control payload"); err != nil {
			return err
		}
		return validateExactOneVarintPayload(f.Type, f.Payload)
	case FrameTypePING, FrameTypePONG:
		if err := validateInboundPayloadLimit(inbound, f.Payload, limits.MaxControlPayloadBytes, "validate control payload"); err != nil {
			return err
		}
		if len(f.Payload) < 8 {
			return FrameSizeError("validate ping/pong payload", ErrShortFrame)
		}
		return nil
	case FrameTypeStopSending, FrameTypeRESET, FrameTypeABORT, FrameTypeCLOSE:
		if err := validateInboundPayloadLimit(inbound, f.Payload, limits.MaxControlPayloadBytes, "validate control payload"); err != nil {
			return err
		}
		return validateErrorAndDIAGPayload(f.Type, f.Payload)
	case FrameTypeGOAWAY:
		if err := validateInboundPayloadLimit(inbound, f.Payload, limits.MaxControlPayloadBytes, "validate control payload"); err != nil {
			return err
		}
		return ValidateGOAWAYPayload(f.Payload)
	case FrameTypeEXT:
		if err := validateInboundPayloadLimit(inbound, f.Payload, limits.MaxExtensionPayloadBytes, "validate EXT payload"); err != nil {
			return err
		}
		return ValidateEXTPayload(f.StreamID, f.Payload)
	default:
		return WrapError(CodeProtocol, "validate frame", ErrInvalidFrameType)
	}
}

func validateInboundPayloadLimit(inbound bool, payload []byte, limit uint64, op string) error {
	if inbound && uint64(len(payload)) > limit {
		return FrameSizeError(op, ErrPayloadTooLarge)
	}
	return nil
}

func FrameSizeError(op string, err error) error {
	return WrapError(CodeFrameSize, op, err)
}

func validateDataPayload(payload []byte, flags byte) error {
	if flags&FrameFlagOpenMetadata == 0 {
		return nil
	}
	metadataLen, n, err := ParseVarint(payload)
	if err != nil {
		return FrameSizeError("validate OPEN_METADATA length", err)
	}
	remaining := payload[n:]
	if uint64(len(remaining)) < metadataLen {
		return FrameSizeError("validate OPEN_METADATA payload", ErrTLVValueOverrun)
	}
	if err := validateTLVs(remaining[:metadataLen]); err != nil {
		return FrameSizeError("validate OPEN_METADATA payload", err)
	}
	return nil
}

func validateExactOneVarintPayload(frameType FrameType, payload []byte) error {
	value, n, err := ParseVarint(payload)
	if err != nil {
		return FrameSizeError(fmt.Sprintf("validate %s payload", frameType), err)
	}
	if value > MaxVarint62 {
		return WrapError(CodeProtocol, fmt.Sprintf("validate %s payload", frameType), ErrValueTooLarge)
	}
	if n != len(payload) {
		return WrapError(CodeProtocol, fmt.Sprintf("validate %s payload", frameType), fmt.Errorf("unexpected trailing bytes"))
	}
	return nil
}

func validateErrorAndDIAGPayload(frameType FrameType, payload []byte) error {
	_, n, err := ParseVarint(payload)
	if err != nil {
		return FrameSizeError(fmt.Sprintf("validate %s payload", frameType), err)
	}
	if err := validateTLVs(payload[n:]); err != nil {
		return FrameSizeError(fmt.Sprintf("validate %s payload", frameType), err)
	}
	return nil
}

func ValidateGOAWAYPayload(payload []byte) error {
	offset := 0
	for i := 0; i < 3; i++ {
		_, n, err := ParseVarint(payload[offset:])
		if err != nil {
			return FrameSizeError("validate GOAWAY payload", err)
		}
		offset += n
	}
	if err := validateTLVs(payload[offset:]); err != nil {
		return FrameSizeError("validate GOAWAY payload", err)
	}
	return nil
}

func ValidateEXTPayload(streamID uint64, payload []byte) error {
	extType, n, err := ParseVarint(payload)
	if err != nil {
		return FrameSizeError("validate EXT payload", err)
	}
	switch EXTSubtype(extType) {
	case EXTPriorityUpdate:
		if streamID == 0 {
			return WrapError(CodeProtocol, "validate EXT payload", fmt.Errorf("PRIORITY_UPDATE requires non-zero stream_id"))
		}
		if _, _, err := ParsePriorityUpdatePayload(payload[n:]); err != nil {
			return FrameSizeError("validate EXT payload", err)
		}
	}
	return nil
}

func validateFrameFlags(frameType FrameType, flags byte) error {
	if frameType != FrameTypeDATA {
		if flags != 0 {
			return WrapError(CodeProtocol, "validate frame flags", ErrInvalidFlags)
		}
		return nil
	}
	switch flags {
	case 0, FrameFlagOpenMetadata, FrameFlagFIN, FrameFlagOpenMetadata | FrameFlagFIN:
		return nil
	default:
		return WrapError(CodeProtocol, "validate frame flags", ErrInvalidFlags)
	}
}

func validateFrameScope(f Frame) error {
	switch f.Type {
	case FrameTypeDATA, FrameTypeStopSending, FrameTypeRESET, FrameTypeABORT:
		if f.StreamID == 0 {
			return WrapError(CodeProtocol, "validate frame scope", fmt.Errorf("%s requires non-zero stream_id", f.Type))
		}
	case FrameTypePING, FrameTypePONG, FrameTypeGOAWAY, FrameTypeCLOSE:
		if f.StreamID != 0 {
			return WrapError(CodeProtocol, "validate frame scope", fmt.Errorf("%s requires stream_id = 0", f.Type))
		}
	}
	return nil
}
