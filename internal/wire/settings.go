package wire

import "fmt"

func MarshalSettingsTLV(s Settings) ([]byte, error) {
	defaults := DefaultSettings()
	entries := []struct {
		id    SettingID
		value uint64
		def   uint64
	}{
		{SettingInitialMaxStreamDataBidiLocallyOpened, s.InitialMaxStreamDataBidiLocallyOpened, defaults.InitialMaxStreamDataBidiLocallyOpened},
		{SettingInitialMaxStreamDataBidiPeerOpened, s.InitialMaxStreamDataBidiPeerOpened, defaults.InitialMaxStreamDataBidiPeerOpened},
		{SettingInitialMaxStreamDataUni, s.InitialMaxStreamDataUni, defaults.InitialMaxStreamDataUni},
		{SettingInitialMaxData, s.InitialMaxData, defaults.InitialMaxData},
		{SettingMaxIncomingStreamsBidi, s.MaxIncomingStreamsBidi, defaults.MaxIncomingStreamsBidi},
		{SettingMaxIncomingStreamsUni, s.MaxIncomingStreamsUni, defaults.MaxIncomingStreamsUni},
		{SettingMaxFramePayload, s.MaxFramePayload, defaults.MaxFramePayload},
		{SettingIdleTimeoutMillis, s.IdleTimeoutMillis, defaults.IdleTimeoutMillis},
		{SettingKeepaliveHintMillis, s.KeepaliveHintMillis, defaults.KeepaliveHintMillis},
		{SettingMaxControlPayloadBytes, s.MaxControlPayloadBytes, defaults.MaxControlPayloadBytes},
		{SettingMaxExtensionPayloadBytes, s.MaxExtensionPayloadBytes, defaults.MaxExtensionPayloadBytes},
		{SettingSchedulerHints, uint64(s.SchedulerHints), uint64(defaults.SchedulerHints)},
		{SettingPingPaddingKey, s.PingPaddingKey, defaults.PingPaddingKey},
	}
	var out []byte
	var err error
	for _, entry := range entries {
		if entry.value == entry.def {
			continue
		}
		out, err = appendSettingVarintTLV(out, entry.id, entry.value)
		if err != nil {
			return nil, WrapError(CodeProtocol, "marshal settings", err)
		}
	}
	return out, nil
}

func appendSettingVarintTLV(dst []byte, id SettingID, value uint64) ([]byte, error) {
	valueLen, err := VarintLen(value)
	if err != nil {
		return nil, err
	}
	dst, err = AppendVarint(dst, uint64(id))
	if err != nil {
		return nil, err
	}
	dst, err = AppendVarint(dst, uint64(valueLen))
	if err != nil {
		return nil, err
	}
	return AppendVarint(dst, value)
}

func ParseSettingsTLV(src []byte) (Settings, error) {
	settings := DefaultSettings()
	if len(src) == 0 {
		return settings, nil
	}
	seenKnown := uint16(0)
	var seenUnknown map[uint64]struct{}
	for len(src) > 0 {
		typ, nType, err := ParseVarint(src)
		if err != nil {
			return Settings{}, WrapError(CodeProtocol, "parse settings", err)
		}
		src = src[nType:]

		length, nLen, err := ParseVarint(src)
		if err != nil {
			return Settings{}, WrapError(CodeProtocol, "parse settings", err)
		}
		src = src[nLen:]
		if uint64(len(src)) < length {
			return Settings{}, WrapError(CodeProtocol, "parse settings", ErrTLVValueOverrun)
		}
		valueBytes := src[:length]
		src = src[length:]

		if bit, known := knownSettingSeenBit(typ); known {
			if seenKnown&bit != 0 {
				return Settings{}, WrapError(CodeProtocol, "parse settings", fmt.Errorf("duplicate setting id %d", typ))
			}
			seenKnown |= bit
			if SettingID(typ) == SettingPrefacePadding {
				// Padding carries arbitrary bytes and intentionally has no semantic value.
				continue
			}
		} else {
			if seenUnknown != nil {
				if _, dup := seenUnknown[typ]; dup {
					return Settings{}, WrapError(CodeProtocol, "parse settings", fmt.Errorf("duplicate setting id %d", typ))
				}
			} else {
				seenUnknown = make(map[uint64]struct{}, 1)
			}
			seenUnknown[typ] = struct{}{}
			continue
		}

		value, n, err := ParseVarint(valueBytes)
		if err != nil {
			return Settings{}, WrapError(CodeProtocol, "parse settings", err)
		}
		if n != len(valueBytes) {
			return Settings{}, WrapError(CodeProtocol, "parse settings", fmt.Errorf("setting %d has trailing bytes", typ))
		}

		switch SettingID(typ) {
		case SettingInitialMaxStreamDataBidiLocallyOpened:
			settings.InitialMaxStreamDataBidiLocallyOpened = value
		case SettingInitialMaxStreamDataBidiPeerOpened:
			settings.InitialMaxStreamDataBidiPeerOpened = value
		case SettingInitialMaxStreamDataUni:
			settings.InitialMaxStreamDataUni = value
		case SettingInitialMaxData:
			settings.InitialMaxData = value
		case SettingMaxIncomingStreamsBidi:
			settings.MaxIncomingStreamsBidi = value
		case SettingMaxIncomingStreamsUni:
			settings.MaxIncomingStreamsUni = value
		case SettingMaxFramePayload:
			settings.MaxFramePayload = value
		case SettingIdleTimeoutMillis:
			settings.IdleTimeoutMillis = value
		case SettingKeepaliveHintMillis:
			settings.KeepaliveHintMillis = value
		case SettingMaxControlPayloadBytes:
			settings.MaxControlPayloadBytes = value
		case SettingMaxExtensionPayloadBytes:
			settings.MaxExtensionPayloadBytes = value
		case SettingSchedulerHints:
			settings.SchedulerHints = SchedulerHintFromCode(value)
		case SettingPingPaddingKey:
			settings.PingPaddingKey = value
		default:
		}
	}
	return settings, nil
}

func knownSettingSeenBit(typ uint64) (uint16, bool) {
	switch SettingID(typ) {
	case SettingInitialMaxStreamDataBidiLocallyOpened:
		return 1 << 0, true
	case SettingInitialMaxStreamDataBidiPeerOpened:
		return 1 << 1, true
	case SettingInitialMaxStreamDataUni:
		return 1 << 2, true
	case SettingInitialMaxData:
		return 1 << 3, true
	case SettingMaxIncomingStreamsBidi:
		return 1 << 4, true
	case SettingMaxIncomingStreamsUni:
		return 1 << 5, true
	case SettingMaxFramePayload:
		return 1 << 6, true
	case SettingIdleTimeoutMillis:
		return 1 << 7, true
	case SettingKeepaliveHintMillis:
		return 1 << 8, true
	case SettingMaxControlPayloadBytes:
		return 1 << 9, true
	case SettingMaxExtensionPayloadBytes:
		return 1 << 10, true
	case SettingSchedulerHints:
		return 1 << 11, true
	case SettingPingPaddingKey:
		return 1 << 12, true
	case SettingPrefacePadding:
		return 1 << 13, true
	default:
		return 0, false
	}
}
