package runtime

import (
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/wire"
)

func TestFrameChunkSpans(t *testing.T) {
	frames := []wire.Frame{
		{Type: wire.FrameTypePING, Payload: make([]byte, 8)},
		{Type: wire.FrameTypePING, Payload: make([]byte, 8)},
		{Type: wire.FrameTypePING, Payload: make([]byte, 8)},
	}

	t.Run("frame cap", func(t *testing.T) {
		spans := FrameChunkSpans(frames, 2, 0)
		if len(spans) != 2 || spans[0] != (ChunkSpan{Start: 0, End: 2}) || spans[1] != (ChunkSpan{Start: 2, End: 3}) {
			t.Fatalf("FrameChunkSpans(frame cap) = %#v", spans)
		}
	})

	t.Run("byte cap", func(t *testing.T) {
		spans := FrameChunkSpans(frames, 16, 18)
		if len(spans) != 2 || spans[0] != (ChunkSpan{Start: 0, End: 2}) || spans[1] != (ChunkSpan{Start: 2, End: 3}) {
			t.Fatalf("FrameChunkSpans(byte cap) = %#v", spans)
		}
	})
}

func TestSendAndWaitByDeadline(t *testing.T) {
	t.Run("send succeeds", func(t *testing.T) {
		closed := make(chan struct{})
		lane := make(chan int, 1)
		if !SendByDeadline(time.Now().Add(100*time.Millisecond), closed, lane, 7) {
			t.Fatal("SendByDeadline() = false, want true")
		}
		if got := <-lane; got != 7 {
			t.Fatalf("lane value = %d, want 7", got)
		}
	})

	t.Run("send times out", func(t *testing.T) {
		closed := make(chan struct{})
		lane := make(chan int)
		if SendByDeadline(time.Now().Add(10*time.Millisecond), closed, lane, 7) {
			t.Fatal("SendByDeadline() = true, want false")
		}
	})

	t.Run("send to nil lane returns", func(t *testing.T) {
		if SendByDeadline(time.Time{}, nil, nil, 7) {
			t.Fatal("SendByDeadline() = true, want false")
		}
	})

	t.Run("wait returns on done", func(t *testing.T) {
		closed := make(chan struct{})
		done := make(chan error, 1)
		done <- nil
		WaitByDeadline(time.Now().Add(100*time.Millisecond), closed, done)
	})

	t.Run("wait with nil signals returns", func(t *testing.T) {
		returned := make(chan struct{})
		go func() {
			defer close(returned)
			WaitByDeadline[struct{}](time.Time{}, nil, nil)
		}()

		select {
		case <-returned:
		case <-time.After(100 * time.Millisecond):
			t.Fatal("WaitByDeadline() blocked with nil signals and no deadline")
		}
	})
}
