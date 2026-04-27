package runtime

import (
	"io"
	"time"

	"github.com/zmuxio/zmux-go/internal/wire"
)

type ChunkSpan struct {
	Start int
	End   int
}

func FrameBufferedBytes(frame wire.Frame) uint64 {
	return SaturatingAdd(1, uint64(len(frame.Payload)))
}

func FramesBufferedBytes(frames []wire.Frame) uint64 {
	total := uint64(0)
	for _, frame := range frames {
		total = SaturatingAdd(total, FrameBufferedBytes(frame))
	}
	return total
}

func FrameChunkSpans(frames []wire.Frame, maxFrames int, maxBytes uint64) []ChunkSpan {
	if len(frames) == 0 {
		return nil
	}
	if maxFrames <= 0 {
		maxFrames = len(frames)
	}

	spans := make([]ChunkSpan, 0, len(frames))
	start := 0
	chunkBytes := uint64(0)
	for i, frame := range frames {
		frameBytes := FrameBufferedBytes(frame)
		overFrameCap := i-start >= maxFrames
		overByteCap := maxBytes > 0 && i > start && SaturatingAdd(chunkBytes, frameBytes) > maxBytes
		if overFrameCap || overByteCap {
			spans = append(spans, ChunkSpan{Start: start, End: i})
			start = i
			chunkBytes = 0
		}
		chunkBytes = SaturatingAdd(chunkBytes, frameBytes)
	}
	spans = append(spans, ChunkSpan{Start: start, End: len(frames)})
	return spans
}

func SendByDeadline[T any](deadline time.Time, closed <-chan struct{}, lane chan<- T, value T) bool {
	if lane == nil {
		return false
	}
	if deadline.IsZero() {
		select {
		case <-closed:
			return false
		case lane <- value:
			return true
		}
	}
	delay := time.Until(deadline)
	if delay <= 0 {
		return false
	}
	timer := time.NewTimer(delay)
	defer stopTimer(timer)
	select {
	case <-closed:
		return false
	case lane <- value:
		return true
	case <-timer.C:
		return false
	}
}

func WaitByDeadline[T any](deadline time.Time, closed <-chan struct{}, done <-chan T) {
	if deadline.IsZero() && closed == nil && done == nil {
		return
	}
	if deadline.IsZero() {
		select {
		case <-closed:
		case <-done:
		}
		return
	}
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return
	}
	timer := time.NewTimer(remaining)
	defer stopTimer(timer)
	select {
	case <-closed:
	case <-done:
	case <-timer.C:
	}
}

func stopTimer(timer *time.Timer) {
	if timer == nil {
		return
	}
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}

func CollectReadyBatchInto[T any](batch []T, lane <-chan T, max int, order func([]T) []T) []T {
	for len(batch) < max {
		select {
		case req, ok := <-lane:
			if !ok {
				if order != nil {
					return order(batch)
				}
				return batch
			}
			batch = append(batch, req)
		default:
			if order != nil {
				return order(batch)
			}
			return batch
		}
	}
	if order != nil {
		return order(batch)
	}
	return batch
}

func CollectAlternatingReadyBatchInto[T any](batch []T, primary <-chan T, secondary <-chan T, preferSecondary bool, max int, order func([]T) []T) []T {
	for len(batch) < max {
		if preferSecondary {
			select {
			case req, ok := <-secondary:
				if !ok {
					secondary = nil
					break
				}
				batch = append(batch, req)
				preferSecondary = false
				continue
			default:
			}
			select {
			case req, ok := <-primary:
				if !ok {
					primary = nil
					break
				}
				batch = append(batch, req)
				preferSecondary = true
				continue
			default:
				if primary == nil && secondary == nil {
					if order != nil {
						return order(batch)
					}
					return batch
				}
				if order != nil {
					return order(batch)
				}
				return batch
			}
		}

		select {
		case req, ok := <-primary:
			if !ok {
				primary = nil
				break
			}
			batch = append(batch, req)
			preferSecondary = true
			continue
		default:
		}
		select {
		case req, ok := <-secondary:
			if !ok {
				secondary = nil
				break
			}
			batch = append(batch, req)
			preferSecondary = false
			continue
		default:
			if primary == nil && secondary == nil {
				if order != nil {
					return order(batch)
				}
				return batch
			}
			if order != nil {
				return order(batch)
			}
			return batch
		}
	}
	if order != nil {
		return order(batch)
	}
	return batch
}

func DequeuePreferUrgentAdvisory[T any](closed <-chan struct{}, urgent <-chan T, advisory <-chan T, normal <-chan T) (T, <-chan T, bool) {
	var zero T

	for {
		select {
		case <-closed:
			return zero, nil, false
		case req, ok := <-urgent:
			if !ok {
				urgent = nil
				break
			}
			return req, urgent, true
		default:
		}

		select {
		case <-closed:
			return zero, nil, false
		case req, ok := <-urgent:
			if !ok {
				urgent = nil
				break
			}
			return req, urgent, true
		case req, ok := <-advisory:
			if !ok {
				advisory = nil
				break
			}
			return req, advisory, true
		default:
		}

		if urgent == nil && advisory == nil && normal == nil {
			return zero, nil, false
		}

		select {
		case <-closed:
			return zero, nil, false
		case req, ok := <-urgent:
			if !ok {
				urgent = nil
				continue
			}
			return req, urgent, true
		case req, ok := <-advisory:
			if !ok {
				advisory = nil
				continue
			}
			return req, advisory, true
		case req, ok := <-normal:
			if !ok {
				normal = nil
				continue
			}
			return req, normal, true
		}
	}
}

func EffectiveDeadline(current, override time.Time) time.Time {
	switch {
	case current.IsZero():
		return override
	case override.IsZero():
		return current
	case override.Before(current):
		return override
	default:
		return current
	}
}

func WriteAll(w io.Writer, p []byte) error {
	for len(p) > 0 {
		n, err := w.Write(p)
		if n < 0 || n > len(p) {
			return io.ErrShortWrite
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrNoProgress
		}
		p = p[n:]
	}
	return nil
}
