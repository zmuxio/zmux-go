package wire

type DataPayload struct {
	MetadataTLVs []TLV
	Metadata     ParsedStreamMetadata
	OpenInfo     []byte
	AppData      []byte
}

type ParsedStreamMetadata struct {
	HasPriority bool
	Priority    uint64
	HasGroup    bool
	Group       uint64
	OpenInfo    []byte
}

func ParseDataPayload(payload []byte, flags byte) (DataPayload, error) {
	return parseDataPayload(payload, flags, true)
}

func ParseDataPayloadView(payload []byte, flags byte) (DataPayload, error) {
	if flags&FrameFlagOpenMetadata == 0 {
		return DataPayload{AppData: payload}, nil
	}

	metadataLen, n, err := ParseVarint(payload)
	if err != nil {
		return DataPayload{}, err
	}
	remaining := payload[n:]
	if uint64(len(remaining)) < metadataLen {
		return DataPayload{}, ErrTLVValueOverrun
	}

	metadataRaw := remaining[:metadataLen]
	appData := remaining[metadataLen:]
	parsed, ok, err := parseStreamMetadataBytes(metadataRaw, false)
	if err != nil {
		return DataPayload{}, err
	}
	if !ok {
		return DataPayload{AppData: appData}, nil
	}
	return DataPayload{
		Metadata: parsed,
		OpenInfo: parsed.OpenInfo,
		AppData:  appData,
	}, nil
}

// ParseDataPayloadAppData returns the application data of a DATA payload,
// decoding only the OPEN_METADATA metadata_len prefix. The metadata TLVs are
// skipped uninterpreted, so a receiver that refuses an opening DATA can still
// account its application bytes without parsing the metadata.
func ParseDataPayloadAppData(payload []byte, flags byte) ([]byte, error) {
	if flags&FrameFlagOpenMetadata == 0 {
		return payload, nil
	}
	metadataLen, n, err := ParseVarint(payload)
	if err != nil {
		return nil, err
	}
	remaining := payload[n:]
	if uint64(len(remaining)) < metadataLen {
		return nil, ErrTLVValueOverrun
	}
	return remaining[metadataLen:], nil
}

func parseDataPayload(payload []byte, flags byte, cloneMetadata bool) (DataPayload, error) {
	if flags&FrameFlagOpenMetadata == 0 {
		return DataPayload{AppData: payload}, nil
	}

	metadataLen, n, err := ParseVarint(payload)
	if err != nil {
		return DataPayload{}, err
	}
	remaining := payload[n:]
	if uint64(len(remaining)) < metadataLen {
		return DataPayload{}, ErrTLVValueOverrun
	}

	metadataRaw := remaining[:metadataLen]
	appData := remaining[metadataLen:]
	tlvs, err := parseTLVs(metadataRaw, cloneMetadata)
	if err != nil {
		return DataPayload{}, err
	}
	parsed, ok, err := parseStreamMetadataTLVs(tlvs, cloneMetadata)
	if err != nil {
		return DataPayload{}, err
	}
	if !ok {
		return DataPayload{AppData: appData}, nil
	}
	return DataPayload{
		MetadataTLVs: tlvs,
		Metadata:     parsed,
		OpenInfo:     parsed.OpenInfo,
		AppData:      appData,
	}, nil
}

func ParseStreamMetadataTLVs(tlvs []TLV) (ParsedStreamMetadata, bool, error) {
	return parseStreamMetadataTLVs(tlvs, true)
}

func ParseStreamMetadataTLVsView(tlvs []TLV) (ParsedStreamMetadata, bool, error) {
	return parseStreamMetadataTLVs(tlvs, false)
}

func ParseStreamMetadataBytesView(src []byte) (ParsedStreamMetadata, bool, error) {
	return parseStreamMetadataBytes(src, false)
}

func parseStreamMetadataTLVs(tlvs []TLV, copyOpenInfo bool) (ParsedStreamMetadata, bool, error) {
	const (
		seenPriority = 1 << iota
		seenGroup
		seenOpenInfo
	)

	seenSingleton := uint8(0)
	out := ParsedStreamMetadata{}
	for _, tlv := range tlvs {
		var seenBit uint8
		switch StreamMetadataType(tlv.Type) {
		case MetadataStreamPriority:
			seenBit = seenPriority
		case MetadataStreamGroup:
			seenBit = seenGroup
		case MetadataOpenInfo:
			seenBit = seenOpenInfo
		}
		if seenBit != 0 {
			if seenSingleton&seenBit != 0 {
				return ParsedStreamMetadata{}, false, nil
			}
			seenSingleton |= seenBit
		}

		switch StreamMetadataType(tlv.Type) {
		case MetadataStreamPriority:
			value, err := ParseMetadataVarint(tlv.Value)
			if err != nil {
				return ParsedStreamMetadata{}, false, err
			}
			out.HasPriority = true
			out.Priority = value
		case MetadataStreamGroup:
			value, err := ParseMetadataVarint(tlv.Value)
			if err != nil {
				return ParsedStreamMetadata{}, false, err
			}
			out.HasGroup = true
			out.Group = value
		case MetadataOpenInfo:
			if copyOpenInfo {
				out.OpenInfo = append([]byte(nil), tlv.Value...)
			} else {
				out.OpenInfo = tlv.Value
			}
		}
	}
	return out, true, nil
}

func parseStreamMetadataBytes(src []byte, copyOpenInfo bool) (ParsedStreamMetadata, bool, error) {
	const (
		seenPriority = 1 << iota
		seenGroup
		seenOpenInfo
	)

	seenSingleton := uint8(0)
	dropped := false
	out := ParsedStreamMetadata{}
	err := walkTLVs(src, func(typ uint64, value []byte) error {
		// A duplicate singleton drops the block, but the walk goes on so a
		// later structural error still rejects it (SPEC §7.2).
		if dropped {
			return nil
		}
		var seenBit uint8
		switch StreamMetadataType(typ) {
		case MetadataStreamPriority:
			seenBit = seenPriority
		case MetadataStreamGroup:
			seenBit = seenGroup
		case MetadataOpenInfo:
			seenBit = seenOpenInfo
		}
		if seenBit != 0 {
			if seenSingleton&seenBit != 0 {
				dropped = true
				return nil
			}
			seenSingleton |= seenBit
		}

		switch StreamMetadataType(typ) {
		case MetadataStreamPriority:
			parsed, err := ParseMetadataVarint(value)
			if err != nil {
				return err
			}
			out.HasPriority = true
			out.Priority = parsed
		case MetadataStreamGroup:
			parsed, err := ParseMetadataVarint(value)
			if err != nil {
				return err
			}
			out.HasGroup = true
			out.Group = parsed
		case MetadataOpenInfo:
			if copyOpenInfo {
				out.OpenInfo = append([]byte(nil), value...)
			} else {
				out.OpenInfo = value
			}
		}
		return nil
	})
	if err != nil {
		return ParsedStreamMetadata{}, false, err
	}
	if dropped {
		return ParsedStreamMetadata{}, false, nil
	}
	return out, true, nil
}

func ParseMetadataVarint(value []byte) (uint64, error) {
	v, n, err := ParseVarint(value)
	if err != nil {
		return 0, err
	}
	if n != len(value) {
		return 0, ErrTLVValueOverrun
	}
	return v, nil
}

func ParsePriorityUpdatePayload(payload []byte) (ParsedStreamMetadata, bool, error) {
	out := ParsedStreamMetadata{}
	dropped := false
	err := walkTLVs(payload, func(typ uint64, value []byte) error {
		// A duplicate singleton drops the update, but the walk goes on so a
		// later structural error still rejects the frame (SPEC §7.2).
		if dropped {
			return nil
		}
		switch StreamMetadataType(typ) {
		case MetadataStreamPriority:
			if out.HasPriority {
				dropped = true
				return nil
			}
			parsed, err := ParseMetadataVarint(value)
			if err != nil {
				return err
			}
			out.HasPriority = true
			out.Priority = parsed
		case MetadataStreamGroup:
			if out.HasGroup {
				dropped = true
				return nil
			}
			parsed, err := ParseMetadataVarint(value)
			if err != nil {
				return err
			}
			out.HasGroup = true
			out.Group = parsed
		case MetadataOpenInfo:
		}
		return nil
	})
	if err != nil {
		return ParsedStreamMetadata{}, false, err
	}
	if dropped {
		return ParsedStreamMetadata{}, false, nil
	}
	return out, true, nil
}
