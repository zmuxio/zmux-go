package runtime

import "testing"

func TestLowWatermark(t *testing.T) {
	if got := LowWatermark(9); got != 4 {
		t.Fatalf("LowWatermark(9) = %d, want 4", got)
	}
}

func TestQueueWouldBlock(t *testing.T) {
	tests := []struct {
		name       string
		blocked    bool
		session    uint64
		stream     uint64
		req        uint64
		sessionHWM uint64
		streamHWM  uint64
		want       bool
	}{
		{name: "zero_request", req: 0, sessionHWM: 8, streamHWM: 4, want: false},
		{name: "memory_blocked", blocked: true, req: 1, sessionHWM: 8, streamHWM: 4, want: true},
		{name: "session_crosses_hwm", session: 4, req: 5, sessionHWM: 8, streamHWM: 8, want: true},
		{name: "stream_crosses_hwm", stream: 2, req: 3, sessionHWM: 8, streamHWM: 4, want: true},
		{name: "session_at_zero_still_respects_limit", session: 0, req: 9, sessionHWM: 8, streamHWM: 8, want: true},
		{name: "below_limits", session: 3, stream: 1, req: 2, sessionHWM: 8, streamHWM: 4, want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := QueueWouldBlock(tc.blocked, tc.session, tc.stream, tc.req, tc.sessionHWM, tc.streamHWM); got != tc.want {
				t.Fatalf("QueueWouldBlock(...) = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestCrossedLowWatermark(t *testing.T) {
	if !CrossedLowWatermark(6, 4, 4) {
		t.Fatal("CrossedLowWatermark() = false, want true")
	}
	if CrossedLowWatermark(4, 3, 4) {
		t.Fatal("CrossedLowWatermark() = true, want false when previous is not above LWM")
	}
}

func TestGainedCredit(t *testing.T) {
	if !GainedCredit(0, 1) {
		t.Fatal("GainedCredit(0, 1) = false, want true")
	}
	if GainedCredit(1, 2) {
		t.Fatal("GainedCredit(1, 2) = true, want false")
	}
}

func TestMemoryWakeNeeded(t *testing.T) {
	if !MemoryWakeNeeded(8, 7, 8) {
		t.Fatal("MemoryWakeNeeded(8, 7, 8) = false, want true")
	}
	if !MemoryWakeNeeded(7, 6, 8) {
		t.Fatal("MemoryWakeNeeded(7, 6, 8) = false, want true")
	}
	if MemoryWakeNeeded(9, 8, 8) {
		t.Fatal("MemoryWakeNeeded(9, 8, 8) = true, want false")
	}
	if MemoryWakeNeeded(8, 8, 8) {
		t.Fatal("MemoryWakeNeeded(8, 8, 8) = true, want false")
	}
}

func TestProjectedExceedsThreshold(t *testing.T) {
	if !ProjectedExceedsThreshold(7, 2, 8) {
		t.Fatal("ProjectedExceedsThreshold(7, 2, 8) = false, want true")
	}
	if !ProjectedExceedsThreshold(0, 9, 8) {
		t.Fatal("ProjectedExceedsThreshold(0, 9, 8) = false, want true")
	}
}
