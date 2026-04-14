package runtime

import (
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/wire"
)

func TestDequeuePreferUrgentAdvisoryOrder(t *testing.T) {
	t.Parallel()

	t.Run("advisory before normal", func(t *testing.T) {
		closed := make(chan struct{})
		urgent := make(chan int, 1)
		advisory := make(chan int, 1)
		normal := make(chan int, 1)

		advisory <- 2
		normal <- 3
		if got, lane, ok := DequeuePreferUrgentAdvisory(closed, urgent, advisory, normal); !ok || got != 2 || lane != advisory {
			t.Fatalf("advisory-first dequeue = (%d,%v,%v), want (2, advisory, true)", got, lane, ok)
		}
	})

	t.Run("urgent before advisory", func(t *testing.T) {
		closed := make(chan struct{})
		urgent := make(chan int, 1)
		advisory := make(chan int, 1)
		normal := make(chan int, 1)

		urgent <- 1
		advisory <- 2
		normal <- 3
		if got, lane, ok := DequeuePreferUrgentAdvisory(closed, urgent, advisory, normal); !ok || got != 1 || lane != urgent {
			t.Fatalf("urgent-first dequeue = (%d,%v,%v), want (1, urgent, true)", got, lane, ok)
		}
	})

	t.Run("closed advisory lane falls back to normal", func(t *testing.T) {
		closed := make(chan struct{})
		urgent := make(chan int)
		advisory := make(chan int)
		normal := make(chan int, 1)
		close(advisory)
		normal <- 7

		if got, lane, ok := DequeuePreferUrgentAdvisory(closed, urgent, advisory, normal); !ok || lane != normal || got != 7 {
			t.Fatalf("closed advisory dequeue = (%d,%v,%v), want (7, normal, true)", got, lane, ok)
		}
	})
}

func TestCollectReadyBatchIntoStopsOnClosedLane(t *testing.T) {
	t.Parallel()

	lane := make(chan int)
	close(lane)

	got := CollectReadyBatchInto([]int{1}, lane, 4, nil)
	if len(got) != 1 || got[0] != 1 {
		t.Fatalf("CollectReadyBatchInto closed lane = %v, want [1]", got)
	}
}

func TestCollectAlternatingReadyBatchIntoStopsOnClosedLane(t *testing.T) {
	t.Parallel()

	primary := make(chan int)
	secondary := make(chan int, 1)
	close(primary)
	secondary <- 2

	got := CollectAlternatingReadyBatchInto([]int{1}, primary, secondary, false, 4, nil)
	if len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("CollectAlternatingReadyBatchInto closed lane = %v, want [1 2]", got)
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
