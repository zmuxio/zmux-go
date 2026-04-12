package wire

import (
	"bufio"
	"fmt"
	"io"
)

type Preface struct {
	PrefaceVersion  byte
	Role            Role
	TieBreakerNonce uint64
	MinProto        uint64
	MaxProto        uint64
	Capabilities    Capabilities
	Settings        Settings
}

func ReadPreface(r io.Reader) (Preface, error) {
	br, ok := r.(io.ByteReader)
	if !ok {
		buf := bufio.NewReader(r)
		br = buf
		r = buf
	}

	role, fixed, err := readPrefaceFixedHeader(r)
	if err != nil {
		return Preface{}, err
	}

	tie, _, err := ReadVarint(br)
	if err != nil {
		return Preface{}, WrapError(CodeProtocol, "parse preface", err)
	}
	minProto, _, err := ReadVarint(br)
	if err != nil {
		return Preface{}, WrapError(CodeProtocol, "parse preface", err)
	}
	maxProto, _, err := ReadVarint(br)
	if err != nil {
		return Preface{}, WrapError(CodeProtocol, "parse preface", err)
	}
	caps, _, err := ReadVarint(br)
	if err != nil {
		return Preface{}, WrapError(CodeProtocol, "parse preface", err)
	}
	settingsLen, _, err := ReadVarint(br)
	if err != nil {
		return Preface{}, WrapError(CodeProtocol, "parse preface", err)
	}
	if settingsLen > MaxPrefaceSettingsBytes {
		return Preface{}, WrapError(CodeFrameSize, "parse preface", fmt.Errorf("settings_tlv exceeds %d bytes", MaxPrefaceSettingsBytes))
	}

	settingsBytes := make([]byte, settingsLen)
	if _, err := io.ReadFull(r, settingsBytes); err != nil {
		return Preface{}, err
	}
	settings, err := ParseSettingsTLV(settingsBytes)
	if err != nil {
		return Preface{}, err
	}

	return buildPreface(role, fixed[4], tie, minProto, maxProto, caps, settings), nil
}

func ParsePreface(data []byte) (Preface, error) {
	p, n, err := ParsePrefacePrefix(data)
	if err != nil {
		return Preface{}, err
	}
	if n != len(data) {
		return Preface{}, WrapError(CodeProtocol, "parse preface", fmt.Errorf("unexpected trailing bytes after preface"))
	}
	return p, nil
}

func ParsePrefacePrefix(data []byte) (Preface, int, error) {
	role, version, offset, err := parsePrefacePrefixHeader(data)
	if err != nil {
		return Preface{}, 0, err
	}

	read := func() (uint64, error) {
		v, n, err := ParseVarint(data[offset:])
		if err != nil {
			return 0, err
		}
		offset += n
		return v, nil
	}

	tie, err := read()
	if err != nil {
		return Preface{}, 0, WrapError(CodeProtocol, "parse preface", err)
	}
	minProto, err := read()
	if err != nil {
		return Preface{}, 0, WrapError(CodeProtocol, "parse preface", err)
	}
	maxProto, err := read()
	if err != nil {
		return Preface{}, 0, WrapError(CodeProtocol, "parse preface", err)
	}
	caps, err := read()
	if err != nil {
		return Preface{}, 0, WrapError(CodeProtocol, "parse preface", err)
	}
	settingsLen, err := read()
	if err != nil {
		return Preface{}, 0, WrapError(CodeProtocol, "parse preface", err)
	}
	if settingsLen > MaxPrefaceSettingsBytes {
		return Preface{}, 0, WrapError(CodeFrameSize, "parse preface", fmt.Errorf("settings_tlv exceeds %d bytes", MaxPrefaceSettingsBytes))
	}
	if uint64(len(data[offset:])) < settingsLen {
		return Preface{}, 0, io.ErrUnexpectedEOF
	}

	settings, err := ParseSettingsTLV(data[offset : offset+int(settingsLen)])
	if err != nil {
		return Preface{}, 0, err
	}
	offset += int(settingsLen)

	return buildPreface(role, version, tie, minProto, maxProto, caps, settings), offset, nil
}

func (p Preface) MarshalBinary() ([]byte, error) {
	if err := validatePrefaceForMarshal(p); err != nil {
		return nil, err
	}

	settingsBuf, err := MarshalSettingsTLV(p.Settings)
	if err != nil {
		return nil, err
	}
	if len(settingsBuf) > MaxPrefaceSettingsBytes {
		return nil, WrapError(CodeFrameSize, "marshal preface", fmt.Errorf("settings_tlv exceeds %d bytes", MaxPrefaceSettingsBytes))
	}

	dst := make([]byte, 0, 16+len(settingsBuf))
	dst = append(dst, []byte(Magic)...)
	dst = append(dst, p.PrefaceVersion, byte(p.Role))
	dst, err = AppendVarint(dst, p.TieBreakerNonce)
	if err != nil {
		return nil, WrapError(CodeProtocol, "marshal preface", err)
	}
	dst, err = AppendVarint(dst, p.MinProto)
	if err != nil {
		return nil, WrapError(CodeProtocol, "marshal preface", err)
	}
	dst, err = AppendVarint(dst, p.MaxProto)
	if err != nil {
		return nil, WrapError(CodeProtocol, "marshal preface", err)
	}
	dst, err = AppendVarint(dst, uint64(p.Capabilities))
	if err != nil {
		return nil, WrapError(CodeProtocol, "marshal preface", err)
	}
	dst, err = AppendVarint(dst, uint64(len(settingsBuf)))
	if err != nil {
		return nil, WrapError(CodeProtocol, "marshal preface", err)
	}
	dst = append(dst, settingsBuf...)
	return dst, nil
}

func validatePrefaceForMarshal(p Preface) error {
	if p.PrefaceVersion != PrefaceVersion {
		return WrapError(CodeUnsupportedVersion, "marshal preface", ErrUnsupportedPrefaceVer)
	}
	if !p.Role.Valid() {
		return WrapError(CodeProtocol, "marshal preface", ErrInvalidRole)
	}
	if p.MinProto == 0 || p.MaxProto == 0 {
		return WrapError(CodeProtocol, "marshal preface", fmt.Errorf("protocol version bounds must be non-zero"))
	}
	if p.Role == RoleAuto && p.TieBreakerNonce == 0 {
		return WrapError(CodeProtocol, "marshal preface", fmt.Errorf("role=auto requires non-zero tie-breaker nonce"))
	}
	return nil
}

type Negotiated struct {
	Proto        uint64
	Capabilities Capabilities
	LocalRole    Role
	PeerRole     Role
	PeerSettings Settings
}

func NegotiatePrefaces(local, peer Preface) (Negotiated, error) {
	if !local.Role.Valid() || !peer.Role.Valid() {
		return Negotiated{}, WrapError(CodeProtocol, "negotiate prefaces", ErrInvalidRole)
	}
	if local.Role == RoleAuto && local.TieBreakerNonce == 0 {
		return Negotiated{}, WrapError(CodeProtocol, "negotiate prefaces", fmt.Errorf("local auto role requires non-zero nonce"))
	}
	if peer.Role == RoleAuto && peer.TieBreakerNonce == 0 {
		return Negotiated{}, WrapError(CodeProtocol, "negotiate prefaces", fmt.Errorf("peer auto role requires non-zero nonce"))
	}

	negotiatedProto := minUint64(local.MaxProto, peer.MaxProto)
	if negotiatedProto < maxUint64(local.MinProto, peer.MinProto) {
		return Negotiated{}, WrapError(CodeUnsupportedVersion, "negotiate prefaces", fmt.Errorf("no compatible protocol version"))
	}
	if local.Settings.MaxFramePayload < 16384 || peer.Settings.MaxFramePayload < 16384 ||
		local.Settings.MaxControlPayloadBytes < 4096 || peer.Settings.MaxControlPayloadBytes < 4096 ||
		local.Settings.MaxExtensionPayloadBytes < 4096 || peer.Settings.MaxExtensionPayloadBytes < 4096 {
		return Negotiated{}, WrapError(CodeProtocol, "negotiate prefaces", fmt.Errorf("receive limits below compatibility floor"))
	}

	localRole, peerRole, err := ResolveRoles(local.Role, local.TieBreakerNonce, peer.Role, peer.TieBreakerNonce)
	if err != nil {
		return Negotiated{}, err
	}
	return Negotiated{
		Proto:        negotiatedProto,
		Capabilities: local.Capabilities & peer.Capabilities,
		LocalRole:    localRole,
		PeerRole:     peerRole,
		PeerSettings: peer.Settings,
	}, nil
}

func ResolveRoles(localRole Role, localNonce uint64, peerRole Role, peerNonce uint64) (Role, Role, error) {
	switch {
	case localRole == RoleInitiator && peerRole == RoleResponder:
		return RoleInitiator, RoleResponder, nil
	case localRole == RoleResponder && peerRole == RoleInitiator:
		return RoleResponder, RoleInitiator, nil
	case localRole == RoleInitiator && peerRole == RoleAuto:
		return RoleInitiator, RoleResponder, nil
	case localRole == RoleResponder && peerRole == RoleAuto:
		return RoleResponder, RoleInitiator, nil
	case localRole == RoleAuto && peerRole == RoleInitiator:
		return RoleResponder, RoleInitiator, nil
	case localRole == RoleAuto && peerRole == RoleResponder:
		return RoleInitiator, RoleResponder, nil
	case localRole == RoleInitiator && peerRole == RoleInitiator:
		return 0, 0, WrapError(CodeRoleConflict, "resolve roles", fmt.Errorf("both peers explicitly requested initiator"))
	case localRole == RoleResponder && peerRole == RoleResponder:
		return 0, 0, WrapError(CodeRoleConflict, "resolve roles", fmt.Errorf("both peers explicitly requested responder"))
	case localRole == RoleAuto && peerRole == RoleAuto:
		if localNonce == peerNonce {
			return 0, 0, WrapError(CodeRoleConflict, "resolve roles", fmt.Errorf("equal auto-role nonces"))
		}
		if localNonce > peerNonce {
			return RoleInitiator, RoleResponder, nil
		}
		return RoleResponder, RoleInitiator, nil
	default:
		return 0, 0, WrapError(CodeProtocol, "resolve roles", ErrInvalidRole)
	}
}

func readPrefaceFixedHeader(r io.Reader) (Role, [6]byte, error) {
	var fixed [6]byte
	if _, err := io.ReadFull(r, fixed[:]); err != nil {
		return 0, fixed, err
	}
	if string(fixed[:4]) != Magic {
		return 0, fixed, WrapError(CodeProtocol, "parse preface", ErrInvalidMagic)
	}
	role, err := validateParsedPrefaceHeader(fixed[4], Role(fixed[5]))
	if err != nil {
		return 0, fixed, err
	}
	return role, fixed, nil
}

func parsePrefacePrefixHeader(data []byte) (Role, byte, int, error) {
	if len(data) < 6 {
		return 0, 0, 0, io.ErrUnexpectedEOF
	}
	if string(data[:4]) != Magic {
		return 0, 0, 0, WrapError(CodeProtocol, "parse preface", ErrInvalidMagic)
	}
	role, err := validateParsedPrefaceHeader(data[4], Role(data[5]))
	if err != nil {
		return 0, 0, 0, err
	}
	return role, data[4], 6, nil
}

func validateParsedPrefaceHeader(version byte, role Role) (Role, error) {
	if version != PrefaceVersion {
		return 0, WrapError(CodeUnsupportedVersion, "parse preface", ErrUnsupportedPrefaceVer)
	}
	if !role.Valid() {
		return 0, WrapError(CodeProtocol, "parse preface", ErrInvalidRole)
	}
	return role, nil
}

func buildPreface(role Role, version byte, tie, minProto, maxProto, caps uint64, settings Settings) Preface {
	return Preface{
		PrefaceVersion:  version,
		Role:            role,
		TieBreakerNonce: tie,
		MinProto:        minProto,
		MaxProto:        maxProto,
		Capabilities:    Capabilities(caps),
		Settings:        settings,
	}
}

func minUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func maxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
