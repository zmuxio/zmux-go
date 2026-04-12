package zmux

import (
	"testing"

	"github.com/zmuxio/zmux-go/internal/wire"
)

func TestPublicProtocolAliasesRemainPinned(t *testing.T) {
	t.Parallel()

	_ = New

	checks := []struct {
		name string
		got  func() uint64
		want func() uint64
	}{
		{
			name: "MaxPrefaceSettingsBytes",
			got:  func() uint64 { return MaxPrefaceSettingsBytes },
			want: func() uint64 { return wire.MaxPrefaceSettingsBytes },
		},
		{
			name: "SchedulerUnspecifiedOrBalanced",
			got:  func() uint64 { return uint64(SchedulerUnspecifiedOrBalanced) },
			want: func() uint64 { return uint64(wire.SchedulerUnspecifiedOrBalanced) },
		},
		{
			name: "SettingInitialMaxStreamDataBidiLocallyOpened",
			got:  func() uint64 { return uint64(SettingInitialMaxStreamDataBidiLocallyOpened) },
			want: func() uint64 { return uint64(wire.SettingInitialMaxStreamDataBidiLocallyOpened) },
		},
		{
			name: "SettingInitialMaxStreamDataBidiPeerOpened",
			got:  func() uint64 { return uint64(SettingInitialMaxStreamDataBidiPeerOpened) },
			want: func() uint64 { return uint64(wire.SettingInitialMaxStreamDataBidiPeerOpened) },
		},
		{
			name: "SettingInitialMaxStreamDataUni",
			got:  func() uint64 { return uint64(SettingInitialMaxStreamDataUni) },
			want: func() uint64 { return uint64(wire.SettingInitialMaxStreamDataUni) },
		},
		{
			name: "SettingInitialMaxData",
			got:  func() uint64 { return uint64(SettingInitialMaxData) },
			want: func() uint64 { return uint64(wire.SettingInitialMaxData) },
		},
		{
			name: "SettingMaxIncomingStreamsUni",
			got:  func() uint64 { return uint64(SettingMaxIncomingStreamsUni) },
			want: func() uint64 { return uint64(wire.SettingMaxIncomingStreamsUni) },
		},
		{
			name: "SettingIdleTimeoutMillis",
			got:  func() uint64 { return uint64(SettingIdleTimeoutMillis) },
			want: func() uint64 { return uint64(wire.SettingIdleTimeoutMillis) },
		},
		{
			name: "SettingKeepaliveHintMillis",
			got:  func() uint64 { return uint64(SettingKeepaliveHintMillis) },
			want: func() uint64 { return uint64(wire.SettingKeepaliveHintMillis) },
		},
		{
			name: "SettingSchedulerHints",
			got:  func() uint64 { return uint64(SettingSchedulerHints) },
			want: func() uint64 { return uint64(wire.SettingSchedulerHints) },
		},
		{
			name: "DIAGOffendingStreamID",
			got:  func() uint64 { return uint64(DIAGOffendingStreamID) },
			want: func() uint64 { return uint64(wire.DIAGOffendingStreamID) },
		},
		{
			name: "DIAGOffendingFrameType",
			got:  func() uint64 { return uint64(DIAGOffendingFrameType) },
			want: func() uint64 { return uint64(wire.DIAGOffendingFrameType) },
		},
		{
			name: "CodeStreamLimit",
			got:  func() uint64 { return uint64(CodeStreamLimit) },
			want: func() uint64 { return uint64(wire.CodeStreamLimit) },
		},
		{
			name: "CodeSessionClosing",
			got:  func() uint64 { return uint64(CodeSessionClosing) },
			want: func() uint64 { return uint64(wire.CodeSessionClosing) },
		},
	}

	for _, tc := range checks {
		if got, want := tc.got(), tc.want(); got != want {
			t.Fatalf("%s = %d, want %d", tc.name, got, want)
		}
	}
}
