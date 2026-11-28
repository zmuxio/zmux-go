package zmuxh3

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"maps"
	"strings"
	"unicode/utf8"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/quic-go/quicvarint"
)

const (
	wtProtocol               = "webtransport-h3"
	wtEnabled                = 0x2c7cf000
	wtLegacyEnabled          = 0x2b603742
	wtLegacyDraft02Sessions  = 0x2b603743
	wtLegacyChromiumSessions = 0x14e9cd29
	wtLegacyMaxSessions      = 0xc671706a
	wtInitialMaxUni          = 0x2b64
	wtInitialMaxBidi         = 0x2b65
	wtInitialMaxData         = 0x2b61

	wtBidi = 0x41
	wtUni  = 0x54

	wtClose = 0x2843
	wtDrain = 0x78ae

	wtBufferedStreamRejected quic.StreamErrorCode      = 0x3994bd84
	wtSessionGone            quic.StreamErrorCode      = 0x170d7b68
	wtRequirementsNotMet     quic.ApplicationErrorCode = 0x212c0d48

	wtFirstAppError = 0x52e4a40fa8db
	wtLastAppError  = 0x52e5ac983162
	wtMaxReason     = 1024
)

var (
	errWTSettings     = errors.New("zmuxh3: peer did not negotiate WebTransport draft-16 and HTTP Datagrams")
	errWTSessionLimit = errors.New("zmuxh3: the connection already carried a WebTransport session")
	errWTCapsule      = errors.New("zmuxh3: malformed WebTransport capsule")
)

func wtAdvertisedSettings(other map[uint64]uint64) map[uint64]uint64 {
	settings := maps.Clone(other)
	if settings == nil {
		settings = make(map[uint64]uint64, 5)
	}
	delete(settings, wtInitialMaxUni)
	delete(settings, wtInitialMaxBidi)
	delete(settings, wtInitialMaxData)
	settings[wtEnabled] = 1
	settings[wtLegacyEnabled] = 1
	settings[wtLegacyDraft02Sessions] = 1
	settings[wtLegacyChromiumSessions] = 1
	settings[wtLegacyMaxSessions] = 1
	return settings
}

func wtProtocolAccepted(protocol string) bool {
	return protocol == wtProtocol || protocol == "webtransport"
}

var errWTSettingsValue = errors.New("zmuxh3: invalid WebTransport SETTINGS value")

func checkWTSettings(settings *http3.Settings, state quic.ConnectionState, client, legacy bool) error {
	value := settings.Other[wtEnabled]
	if value > 1 || settings.Other[wtInitialMaxUni] > 1<<60 || settings.Other[wtInitialMaxBidi] > 1<<60 {
		return errWTSettingsValue
	}
	enabled := value == 1 || settings.Other[wtLegacyEnabled] > 0 ||
		settings.Other[wtLegacyDraft02Sessions] > 0 || settings.Other[wtLegacyChromiumSessions] > 0 ||
		settings.Other[wtLegacyMaxSessions] > 0
	if (!enabled && !legacy) || !settings.EnableDatagrams || client && !settings.EnableExtendedConnect ||
		!state.SupportsDatagrams.Local || !state.SupportsDatagrams.Remote {
		return errWTSettings
	}
	return nil
}

// SessionErrorCode is the 32-bit application code carried in a close capsule.
type SessionErrorCode uint32

// SessionError reports a WebTransport session close.
type SessionError struct {
	Message   string
	ErrorCode SessionErrorCode
	Remote    bool
}

func (e *SessionError) Error() string {
	return fmt.Sprintf("WebTransport session closed (%#x): %s", e.ErrorCode, e.Message)
}

// StreamErrorCode is the application code of a WebTransport data stream reset.
type StreamErrorCode uint32

// StreamError reports the application code of a data stream reset or stop.
type StreamError struct {
	ErrorCode StreamErrorCode
	Remote    bool
}

func (e *StreamError) Error() string {
	return fmt.Sprintf("WebTransport stream reset (%#x)", e.ErrorCode)
}

func wtWireError(code StreamErrorCode) quic.StreamErrorCode {
	return wtFirstAppError + quic.StreamErrorCode(uint64(code)+uint64(code)/30)
}

func wtStreamError(err error) error {
	e, ok := errors.AsType[*quic.StreamError](err)
	if !ok {
		return err
	}
	code := e.ErrorCode
	if code < wtFirstAppError || code > wtLastAppError || (code-0x21)%31 == 0 {
		return err
	}
	offset := code - wtFirstAppError
	return &StreamError{ErrorCode: StreamErrorCode(offset - offset/31), Remote: e.Remote}
}

func wtReason(reason string) string {
	reason = strings.ToValidUTF8(reason, "\uFFFD")
	if len(reason) <= wtMaxReason {
		return reason
	}
	end := wtMaxReason
	for !utf8.RuneStart(reason[end]) {
		end--
	}
	return reason[:end]
}

func wtAppendClose(b []byte, code SessionErrorCode, reason string) []byte {
	b = quicvarint.Append(b, wtClose)
	b = quicvarint.Append(b, uint64(4+len(reason)))
	b = binary.BigEndian.AppendUint32(b, uint32(code))
	return append(b, reason...)
}

// Only EOF at a capsule boundary is a clean FIN. Read the rest of a
// varint in one call and report a missing suffix as a framing error.
func wtReadVarint(r io.Reader) (uint64, error) {
	var b [8]byte
	if _, err := io.ReadFull(r, b[:1]); err != nil {
		return 0, err
	}
	n := 1 << (b[0] >> 6)
	if _, err := io.ReadFull(r, b[1:n]); err != nil {
		return 0, wtCapsuleError(err)
	}
	v, _, err := quicvarint.Parse(b[:n])
	return v, err
}

func wtCapsuleError(err error) error {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return errWTCapsule
	}
	return err
}

// readWTCapsule validates close and drain. Unknown capsules (including
// unnegotiated flow control) are discarded with bounded scratch space.
func readWTCapsule(r io.Reader) (uint64, *SessionError, error) {
	kind, err := wtReadVarint(r)
	if err != nil {
		return 0, nil, err
	}
	length, err := wtReadVarint(r)
	if err != nil {
		return 0, nil, wtCapsuleError(err)
	}
	switch kind {
	case wtClose:
		if length < 4 || length > 4+wtMaxReason {
			return 0, nil, errWTCapsule
		}
		var payload [4 + wtMaxReason]byte
		if _, err := io.ReadFull(r, payload[:length]); err != nil {
			return 0, nil, wtCapsuleError(err)
		}
		if !utf8.Valid(payload[4:length]) {
			return 0, nil, errWTCapsule
		}
		return kind, &SessionError{ErrorCode: SessionErrorCode(binary.BigEndian.Uint32(payload[:4])), Message: string(payload[4:length]), Remote: true}, nil
	case wtDrain:
		if length != 0 {
			return 0, nil, errWTCapsule
		}
	default:
		if _, err := io.CopyN(io.Discard, r, int64(length)); err != nil {
			return 0, nil, wtCapsuleError(err)
		}
	}
	return kind, nil, nil
}
