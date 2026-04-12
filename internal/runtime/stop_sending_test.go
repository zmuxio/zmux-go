package runtime

import (
	"testing"
	"time"
)

func TestEvaluateStopSendingGracefulRejectsAbortiveOrUnopenedPaths(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		input StopSendingGracefulInput
	}{
		{
			name: "recv abortive",
			input: StopSendingGracefulInput{
				RecvAbortive:  true,
				LocalOpened:   true,
				SendCommitted: true,
			},
		},
		{
			name: "needs local opener",
			input: StopSendingGracefulInput{
				NeedsLocalOpener: true,
				LocalOpened:      true,
				SendCommitted:    true,
			},
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := EvaluateStopSendingGraceful(tc.input); got.Attempt {
				t.Fatalf("EvaluateStopSendingGraceful(%+v).Attempt = true, want false", tc.input)
			}
		})
	}
}

func TestEvaluateStopSendingGracefulAllowsCommittedEmptyTail(t *testing.T) {
	t.Parallel()

	got := EvaluateStopSendingGraceful(StopSendingGracefulInput{
		LocalOpened:   true,
		SendCommitted: true,
	})
	if !got.Attempt {
		t.Fatalf("EvaluateStopSendingGraceful(...).Attempt = false, want true")
	}
	if got.CommittedTail != 0 {
		t.Fatalf("CommittedTail = %d, want 0", got.CommittedTail)
	}
}

func TestEvaluateStopSendingGracefulUsesInflightTailWithoutRateBudget(t *testing.T) {
	t.Parallel()

	got := EvaluateStopSendingGraceful(StopSendingGracefulInput{
		LocalOpened:      true,
		SendCommitted:    true,
		QueuedDataBytes:  1024,
		InflightQueued:   64,
		FragmentCap:      256,
		SendRateEstimate: 0,
	})
	if !got.Attempt {
		t.Fatalf("Attempt = false, want true when only the small in-flight tail is unavoidable")
	}
	if got.TailBudget != 64 {
		t.Fatalf("TailBudget = %d, want 64", got.TailBudget)
	}
}

func TestEvaluateStopSendingGracefulUsesCommittedTailWithRateBudget(t *testing.T) {
	t.Parallel()

	got := EvaluateStopSendingGraceful(StopSendingGracefulInput{
		LocalOpened:      true,
		SendCommitted:    true,
		QueuedDataBytes:  768,
		InflightQueued:   0,
		FragmentCap:      256,
		SendRateEstimate: 16 << 10,
		DrainWindow:      100 * time.Millisecond,
	})
	if !got.Attempt {
		t.Fatalf("Attempt = false, want true on fast-link queued-only tail budget")
	}
	if got.QueuedOnlyTail != 768 {
		t.Fatalf("QueuedOnlyTail = %d, want 768", got.QueuedOnlyTail)
	}
	if got.TailBudget < 768 {
		t.Fatalf("TailBudget = %d, want >= 768", got.TailBudget)
	}
}

func TestEvaluateStopSendingGracefulUsesInflightTailEvenWithRateBudget(t *testing.T) {
	t.Parallel()

	got := EvaluateStopSendingGraceful(StopSendingGracefulInput{
		LocalOpened:      true,
		SendCommitted:    true,
		QueuedDataBytes:  768,
		InflightQueued:   64,
		FragmentCap:      256,
		SendRateEstimate: 1024,
		DrainWindow:      100 * time.Millisecond,
	})
	if !got.Attempt {
		t.Fatalf("Attempt = false, want true when the small unavoidable in-flight tail fits within the rate budget")
	}
	if got.InflightTail != 64 {
		t.Fatalf("InflightTail = %d, want 64", got.InflightTail)
	}
	if got.QueuedOnlyTail <= got.TailBudget {
		t.Fatalf("QueuedOnlyTail = %d, TailBudget = %d, want queued-only tail to remain above budget in this case", got.QueuedOnlyTail, got.TailBudget)
	}
}

func TestEvaluateStopSendingGracefulExplicitTailCapWins(t *testing.T) {
	t.Parallel()

	got := EvaluateStopSendingGraceful(StopSendingGracefulInput{
		LocalOpened:      true,
		SendCommitted:    true,
		QueuedDataBytes:  384,
		InflightQueued:   0,
		FragmentCap:      256,
		SendRateEstimate: 16 << 10,
		ExplicitTailCap:  256,
		DrainWindow:      100 * time.Millisecond,
	})
	if got.Attempt {
		t.Fatalf("Attempt = true, want false when explicit tail cap is lower than committed tail")
	}
	if got.TailBudget != 256 {
		t.Fatalf("TailBudget = %d, want 256", got.TailBudget)
	}
}

func TestEvaluateStopSendingGracefulUsesInflightTailWithExplicitTailCap(t *testing.T) {
	t.Parallel()

	got := EvaluateStopSendingGraceful(StopSendingGracefulInput{
		LocalOpened:      true,
		SendCommitted:    true,
		QueuedDataBytes:  384,
		InflightQueued:   64,
		FragmentCap:      256,
		SendRateEstimate: 16 << 10,
		ExplicitTailCap:  256,
		DrainWindow:      100 * time.Millisecond,
	})
	if !got.Attempt {
		t.Fatalf("Attempt = false, want true when explicit tail cap still covers the unavoidable in-flight tail")
	}
	if got.InflightTail != 64 {
		t.Fatalf("InflightTail = %d, want 64", got.InflightTail)
	}
}

func TestStopSendingDrainWindowUsesOverrideOrDefault(t *testing.T) {
	t.Parallel()

	if got := StopSendingDrainWindow(0); got != RepoDefaultStopSendingDrainWindow {
		t.Fatalf("StopSendingDrainWindow(0) = %v, want %v", got, RepoDefaultStopSendingDrainWindow)
	}
	override := 250 * time.Millisecond
	if got := StopSendingDrainWindow(override); got != override {
		t.Fatalf("StopSendingDrainWindow(%v) = %v, want %v", override, got, override)
	}
}
