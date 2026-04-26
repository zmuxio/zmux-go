package wire

import (
	"errors"
	"unicode/utf8"
)

func BuildOpenMetadataPrefix(caps Capabilities, priority *uint64, group *uint64, openInfo []byte, maxFramePayload uint64) ([]byte, error) {
	return AppendOpenMetadataPrefix(nil, caps, priority, group, openInfo, maxFramePayload)
}

func AppendOpenMetadataPrefix(dst []byte, caps Capabilities, priority *uint64, group *uint64, openInfo []byte, maxFramePayload uint64) ([]byte, error) {
	if len(openInfo) > 0 && !caps.CanCarryOpenInfo() {
		return nil, ErrOpenInfoUnavailable
	}
	if !caps.SupportsOpenMetadataCarriage() {
		return nil, nil
	}

	metadataLen := 0
	if priority != nil && caps.CanCarryPriorityOnOpen() {
		n, err := streamMetadataVarintTLVLen(MetadataStreamPriority, *priority)
		if err != nil {
			return nil, err
		}
		metadataLen += n
	}
	if group != nil && caps.CanCarryGroupOnOpen() {
		n, err := streamMetadataVarintTLVLen(MetadataStreamGroup, *group)
		if err != nil {
			return nil, err
		}
		metadataLen += n
	}
	if len(openInfo) > 0 {
		n, err := streamMetadataBytesTLVLen(MetadataOpenInfo, len(openInfo))
		if err != nil {
			return nil, err
		}
		metadataLen += n
	}
	if metadataLen == 0 {
		return nil, nil
	}

	prefixLen, err := VarintLen(uint64(metadataLen))
	if err != nil {
		return nil, err
	}
	totalLen := prefixLen + metadataLen
	if uint64(totalLen) > maxFramePayload {
		return nil, ErrOpenMetadataTooLarge
	}

	dst = tightPayloadBuildBuffer(dst, totalLen)
	dst, err = AppendVarint(dst, uint64(metadataLen))
	if err != nil {
		return nil, err
	}
	if priority != nil && caps.CanCarryPriorityOnOpen() {
		dst, err = appendStreamMetadataVarintTLV(dst, MetadataStreamPriority, *priority)
		if err != nil {
			return nil, err
		}
	}
	if group != nil && caps.CanCarryGroupOnOpen() {
		dst, err = appendStreamMetadataVarintTLV(dst, MetadataStreamGroup, *group)
		if err != nil {
			return nil, err
		}
	}
	if len(openInfo) > 0 {
		dst, err = AppendTLV(dst, uint64(MetadataOpenInfo), openInfo)
		if err != nil {
			return nil, err
		}
	}
	return dst, nil
}

func BuildPriorityUpdatePayload(caps Capabilities, priority, group *uint64, maxPayload uint64) ([]byte, error) {
	return AppendPriorityUpdatePayload(nil, caps, priority, group, maxPayload)
}

func AppendPriorityUpdatePayload(dst []byte, caps Capabilities, priority, group *uint64, maxPayload uint64) ([]byte, error) {
	if priority == nil && group == nil {
		return nil, ErrEmptyMetadataUpdate
	}

	subtypeLen, err := VarintLen(uint64(EXTPriorityUpdate))
	if err != nil {
		return nil, err
	}
	totalLen := subtypeLen
	if priority != nil {
		if !caps.CanCarryPriorityInUpdate() {
			return nil, ErrPriorityUpdateUnavailable
		}
		n, err := streamMetadataVarintTLVLen(MetadataStreamPriority, *priority)
		if err != nil {
			return nil, err
		}
		totalLen += n
	}
	if group != nil {
		if !caps.CanCarryGroupInUpdate() {
			return nil, ErrPriorityUpdateUnavailable
		}
		n, err := streamMetadataVarintTLVLen(MetadataStreamGroup, *group)
		if err != nil {
			return nil, err
		}
		totalLen += n
	}
	if uint64(totalLen) > maxPayload {
		return nil, ErrPriorityUpdateTooLarge
	}

	dst = tightPayloadBuildBuffer(dst, totalLen)
	dst, err = AppendVarint(dst, uint64(EXTPriorityUpdate))
	if err != nil {
		return nil, err
	}
	if priority != nil {
		dst, err = appendStreamMetadataVarintTLV(dst, MetadataStreamPriority, *priority)
		if err != nil {
			return nil, err
		}
	}
	if group != nil {
		dst, err = appendStreamMetadataVarintTLV(dst, MetadataStreamGroup, *group)
		if err != nil {
			return nil, err
		}
	}
	return dst, nil
}

type GoAwayPayload struct {
	LastAcceptedBidi uint64
	LastAcceptedUni  uint64
	Code             uint64
	Reason           string
}

func ParseGOAWAYPayload(payload []byte) (GoAwayPayload, error) {
	offset := 0
	read := func() (uint64, error) {
		v, n, err := ParseVarint(payload[offset:])
		if err != nil {
			return 0, err
		}
		offset += n
		return v, nil
	}

	bidi, err := read()
	if err != nil {
		return GoAwayPayload{}, err
	}
	uni, err := read()
	if err != nil {
		return GoAwayPayload{}, err
	}
	code, err := read()
	if err != nil {
		return GoAwayPayload{}, err
	}
	reason, err := ParseDIAGReason(payload[offset:])
	if err != nil {
		return GoAwayPayload{}, err
	}
	return GoAwayPayload{
		LastAcceptedBidi: bidi,
		LastAcceptedUni:  uni,
		Code:             code,
		Reason:           reason,
	}, nil
}

func BuildGoAwayPayload(lastAcceptedBidi, lastAcceptedUni, code uint64, reason string) ([]byte, error) {
	var payload []byte
	var err error
	payload, err = AppendVarint(payload, lastAcceptedBidi)
	if err != nil {
		return nil, err
	}
	payload, err = AppendVarint(payload, lastAcceptedUni)
	if err != nil {
		return nil, err
	}
	payload, err = AppendVarint(payload, code)
	if err != nil {
		return nil, err
	}
	if reason != "" {
		payload = AppendDebugTextTLV(payload, reason)
	}
	return payload, nil
}

func ParseErrorPayload(payload []byte) (uint64, string, error) {
	code, n, err := ParseVarint(payload)
	if err != nil {
		return 0, "", err
	}
	reason, err := ParseDIAGReason(payload[n:])
	if err != nil {
		return 0, "", err
	}
	return code, reason, nil
}

func ParseDIAGReason(payload []byte) (string, error) {
	var (
		seenStandard uint8
		debugText    []byte
	)

	for len(payload) > 0 {
		typ, nType, err := ParseVarint(payload)
		if err != nil {
			if errors.Is(err, ErrTruncatedVarint) {
				return "", ErrTruncatedTLV
			}
			return "", err
		}
		payload = payload[nType:]

		length, nLen, err := ParseVarint(payload)
		if err != nil {
			if errors.Is(err, ErrTruncatedVarint) {
				return "", ErrTruncatedTLV
			}
			return "", err
		}
		payload = payload[nLen:]
		if uint64(len(payload)) < length {
			return "", ErrTLVValueOverrun
		}

		value := payload[:length]
		payload = payload[length:]

		switch DIAGType(typ) {
		case DIAGDebugText:
			if seenStandard&diagSeenDebugText != 0 {
				return "", nil
			}
			seenStandard |= diagSeenDebugText
			debugText = value
		case DIAGRetryAfterMillis:
			if seenStandard&diagSeenRetryAfterMillis != 0 {
				return "", nil
			}
			seenStandard |= diagSeenRetryAfterMillis
		case DIAGOffendingStreamID:
			if seenStandard&diagSeenOffendingStreamID != 0 {
				return "", nil
			}
			seenStandard |= diagSeenOffendingStreamID
		case DIAGOffendingFrameType:
			if seenStandard&diagSeenOffendingFrameType != 0 {
				return "", nil
			}
			seenStandard |= diagSeenOffendingFrameType
		}
	}

	if len(debugText) == 0 {
		return "", nil
	}
	if !utf8.Valid(debugText) {
		return "", nil
	}
	return string(debugText), nil
}

const (
	diagSeenDebugText uint8 = 1 << iota
	diagSeenRetryAfterMillis
	diagSeenOffendingStreamID
	diagSeenOffendingFrameType
)

func AppendDebugTextTLV(payload []byte, reason string) []byte {
	if reason == "" {
		return payload
	}
	out, err := appendStringTLV(payload, uint64(DIAGDebugText), reason)
	if err != nil {
		return payload
	}
	return out
}

func AppendDebugTextTLVCapped(payload []byte, reason string, maxPayload uint64) []byte {
	if reason == "" {
		return payload
	}
	if !utf8.ValidString(reason) {
		return payload
	}
	if uint64(len(payload)) >= maxPayload {
		return payload
	}

	n := cappedDebugTextValueLen(len(reason), maxPayload-uint64(len(payload)))
	n = utf8PrefixLen(reason, n)
	if n == 0 {
		return payload
	}
	out, err := appendStringTLV(payload, uint64(DIAGDebugText), reason[:n])
	if err != nil || uint64(len(out)) > maxPayload {
		return payload
	}
	return out
}

func appendStringTLV(dst []byte, typ uint64, value string) ([]byte, error) {
	var err error
	dst, err = AppendVarint(dst, typ)
	if err != nil {
		return nil, err
	}
	dst, err = AppendVarint(dst, uint64(len(value)))
	if err != nil {
		return nil, err
	}
	dst = append(dst, value...)
	return dst, nil
}

func utf8PrefixLen(s string, n int) int {
	if n >= len(s) {
		return len(s)
	}
	if n <= 0 {
		return 0
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return n
}

func cappedDebugTextValueLen(reasonLen int, remaining uint64) int {
	if reasonLen <= 0 {
		return 0
	}
	typeLen, err := VarintLen(uint64(DIAGDebugText))
	if err != nil {
		return 0
	}
	if remaining <= uint64(typeLen) {
		return 0
	}

	available := remaining - uint64(typeLen)
	valueLen := uint64(reasonLen)
	if valueLen > available {
		valueLen = available
	}
	if valueLen > MaxVarint62 {
		valueLen = MaxVarint62
	}

	for valueLen > 0 {
		lenLen, err := VarintLen(valueLen)
		if err != nil {
			return 0
		}
		if valueLen+uint64(lenLen) <= available {
			return int(valueLen)
		}
		if available <= uint64(lenLen) {
			return 0
		}
		valueLen = available - uint64(lenLen)
	}
	return 0
}

func tightPayloadBuildBuffer(dst []byte, totalLen int) []byte {
	if totalLen <= 0 {
		return nil
	}
	if cap(dst) == totalLen {
		return dst[:0]
	}
	return make([]byte, 0, totalLen)
}

func streamMetadataVarintTLVLen(typ StreamMetadataType, value uint64) (int, error) {
	typeLen, err := VarintLen(uint64(typ))
	if err != nil {
		return 0, err
	}
	valueLen, err := VarintLen(value)
	if err != nil {
		return 0, err
	}
	lenLen, err := VarintLen(uint64(valueLen))
	if err != nil {
		return 0, err
	}
	return typeLen + lenLen + valueLen, nil
}

func streamMetadataBytesTLVLen(typ StreamMetadataType, valueLen int) (int, error) {
	typeLen, err := VarintLen(uint64(typ))
	if err != nil {
		return 0, err
	}
	lengthLen, err := VarintLen(uint64(valueLen))
	if err != nil {
		return 0, err
	}
	return typeLen + lengthLen + valueLen, nil
}

func appendStreamMetadataVarintTLV(dst []byte, typ StreamMetadataType, value uint64) ([]byte, error) {
	valueLen, err := VarintLen(value)
	if err != nil {
		return nil, err
	}
	dst, err = AppendVarint(dst, uint64(typ))
	if err != nil {
		return nil, err
	}
	dst, err = AppendVarint(dst, uint64(valueLen))
	if err != nil {
		return nil, err
	}
	return AppendVarint(dst, value)
}
