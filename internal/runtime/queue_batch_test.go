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
