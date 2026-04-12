package wire

import (
	"errors"
	"testing"
)

func TestParseSettingsTLVRoundTrip(t *testing.T) {
	t.Parallel()

	settings := DefaultSettings()
	settings.InitialMaxData = 12345
	settings.MaxFramePayload = 8192
	settings.SchedulerHints = SchedulerGroupFair

	raw, err := MarshalSettingsTLV(settings)
	if err != nil {
		t.Fatalf("MarshalSettingsTLV err = %v", err)
	}

	parsed, err := ParseSettingsTLV(raw)
	if err != nil {
		t.Fatalf("ParseSettingsTLV err = %v", err)
	}
	if parsed.InitialMaxData != settings.InitialMaxData {
		t.Fatalf("InitialMaxData = %d, want %d", parsed.InitialMaxData, settings.InitialMaxData)
	}
	if parsed.MaxFramePayload != settings.MaxFramePayload {
		t.Fatalf("MaxFramePayload = %d, want %d", parsed.MaxFramePayload, settings.MaxFramePayload)
	}
	if parsed.SchedulerHints != settings.SchedulerHints {
		t.Fatalf("SchedulerHints = %d, want %d", parsed.SchedulerHints, settings.SchedulerHints)
	}
}

func TestParseSettingsTLVRejectsDuplicateKnownSetting(t *testing.T) {
	t.Parallel()

	var raw []byte
	value, err := EncodeVarint(10)
	if err != nil {
		t.Fatalf("EncodeVarint err = %v", err)
	}
	raw, err = AppendTLV(raw, uint64(SettingInitialMaxData), value)
	if err != nil {
		t.Fatalf("AppendTLV first err = %v", err)
	}
	raw, err = AppendTLV(raw, uint64(SettingInitialMaxData), value)
	if err != nil {
		t.Fatalf("AppendTLV second err = %v", err)
	}

	if _, err := ParseSettingsTLV(raw); err == nil {
		t.Fatal("ParseSettingsTLV err = nil, want duplicate-setting error")
	}
}

func TestParseSettingsTLVRejectsDuplicateUnknownSetting(t *testing.T) {
	t.Parallel()

	var raw []byte
	value, err := EncodeVarint(10)
	if err != nil {
		t.Fatalf("EncodeVarint err = %v", err)
	}
	raw, err = AppendTLV(raw, 99, value)
	if err != nil {
		t.Fatalf("AppendTLV first err = %v", err)
	}
	raw, err = AppendTLV(raw, 99, value)
	if err != nil {
		t.Fatalf("AppendTLV second err = %v", err)
	}

	if _, err := ParseSettingsTLV(raw); err == nil {
		t.Fatal("ParseSettingsTLV err = nil, want duplicate-setting error")
	}
}

func TestKnownSettingSeenBitRecognizesConfiguredRange(t *testing.T) {
	t.Parallel()

	for _, id := range []SettingID{
		SettingInitialMaxStreamDataBidiLocallyOpened,
		SettingInitialMaxStreamDataBidiPeerOpened,
		SettingInitialMaxStreamDataUni,
		SettingInitialMaxData,
		SettingMaxIncomingStreamsBidi,
		SettingMaxIncomingStreamsUni,
		SettingMaxFramePayload,
		SettingIdleTimeoutMillis,
		SettingKeepaliveHintMillis,
		SettingMaxControlPayloadBytes,
		SettingMaxExtensionPayloadBytes,
		SettingSchedulerHints,
	} {
		if bit, ok := knownSettingSeenBit(uint64(id)); !ok || bit == 0 {
			t.Fatalf("knownSettingSeenBit(%d) = (%d, %v), want non-zero true", id, bit, ok)
		}
	}
	if bit, ok := knownSettingSeenBit(99); ok || bit != 0 {
		t.Fatalf("knownSettingSeenBit(99) = (%d, %v), want (0, false)", bit, ok)
	}
}

func TestParseSettingsTLVRejectsTrailingBytesInValue(t *testing.T) {
	t.Parallel()

	raw, err := AppendTLV(nil, uint64(SettingInitialMaxData), []byte{0x01, 0x02})
	if err != nil {
		t.Fatalf("AppendTLV err = %v", err)
	}
	if _, err := ParseSettingsTLV(raw); err == nil {
		t.Fatal("ParseSettingsTLV err = nil, want trailing-bytes error")
	} else if !errors.Is(err, ErrTLVValueOverrun) && !IsErrorCode(err, CodeProtocol) {
		t.Fatalf("ParseSettingsTLV err = %v, want protocol-wrapped parse error", err)
	}
}
