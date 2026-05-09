package runtime

import (
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/wire"
)

func TestCollectReadyBatchIntoStopsOnClosedLane(t *testing.T) {
	t.Parallel()

	lane := make(chan int)
	close(lane)

	got := CollectReadyBatchInto([]int{1}, lane, 4, nil)
	if len(got) != 1 || got[0] != 1 {
		t.Fatalf("CollectReadyBatchInto closed lane = %v, want [1]", got)
	}
}

func TestFrameAndFramesBufferedBytes(t *testing.T) {
	t.Parallel()

	frame := wire.Frame{Type: wire.FrameTypeDATA, Payload: []byte("abc")}
	if got := FrameBufferedBytes(frame); got != 4 {
		t.Fatalf("FrameBufferedBytes = %d, want 4", got)
	}
	if got := FramesBufferedBytes([]wire.Frame{frame, frame}); got != 8 {
		t.Fatalf("FramesBufferedBytes = %d, want 8", got)
	}
}

func TestEffectiveDeadlineChoosesEarlierNonZeroDeadline(t *testing.T) {
	t.Parallel()

	now := time.Now()
	earlier := now.Add(50 * time.Millisecond)
	later := now.Add(100 * time.Millisecond)

	if got := EffectiveDeadline(time.Time{}, later); !got.Equal(later) {
		t.Fatalf("EffectiveDeadline(zero, later) = %v, want %v", got, later)
	}
	if got := EffectiveDeadline(later, time.Time{}); !got.Equal(later) {
		t.Fatalf("EffectiveDeadline(later, zero) = %v, want %v", got, later)
	}
	if got := EffectiveDeadline(later, earlier); !got.Equal(earlier) {
		t.Fatalf("EffectiveDeadline(later, earlier) = %v, want %v", got, earlier)
	}
	if got := EffectiveDeadline(earlier, later); !got.Equal(earlier) {
		t.Fatalf("EffectiveDeadline(earlier, later) = %v, want %v", got, earlier)
	}
}
