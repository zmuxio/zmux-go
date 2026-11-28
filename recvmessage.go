package zmux

import (
	"errors"
	"io"

	"github.com/zmuxio/zmux-go/internal/core"
)

// ReceivedMessage owns one complete carrier message. Move it by value
// between holders. The current holder must release it or take its bytes.
type ReceivedMessage struct {
	end         error
	span        core.Span
	generation  uint64
	framedBytes uint32
}

// OwnMessage transfers an allocation and its valid prefix to zmux.
// data must start at the beginning of its backing, and the caller gives up
// every alias. A carrier that can lend bytes only briefly must copy them
// before transfer.
func OwnMessage(data []byte) ReceivedMessage {
	if cap(data) > core.RecvBlockSize && len(data) <= core.RecvBlockSize {
		return CopyMessage(data)
	}
	return ownMessageSpan(core.NewBuf(data).Span(0, len(data)))
}

// CopyMessage adapts short-lived carrier data to an owned message.
func CopyMessage(data []byte) ReceivedMessage {
	if len(data) == 0 || len(data) > core.RecvBlockSize {
		return ownMessageSpan(core.NewBuf(append([]byte(nil), data...)).Span(0, len(data)))
	}
	buf := core.GetRecvBuf(len(data))
	copy(buf.B, data)
	return ownMessageSpan(buf.Span(0, len(data)))
}

func ownMessageSpan(v core.Span) ReceivedMessage {
	return ReceivedMessage{span: v, generation: v.Buf.BeginMessage()}
}

// Bytes borrows the valid bytes until the message is released or moved.
func (m *ReceivedMessage) Bytes() []byte { return m.span.Bytes() }

// Len is the number of valid message bytes.
func (m *ReceivedMessage) Len() int { return m.span.Len() }

// Present reports whether m owns a message, including an empty one.
func (m *ReceivedMessage) Present() bool { return m.span.Buf != nil }

// Release returns the message's backing to its allocator.
func (m *ReceivedMessage) Release() {
	if v := m.take(); v.Buf != nil {
		v.Buf.Unref()
	}
}

func (m *ReceivedMessage) take() core.Span {
	v, generation := m.span, m.generation
	*m = ReceivedMessage{}
	if v.Buf == nil || !v.Buf.EndMessage(generation) {
		return core.Span{}
	}
	return v
}

// TakeBytes transfers the message's bytes to a caller using Go slice
// ownership. The detached allocation is no longer available to the pool.
func (m *ReceivedMessage) TakeBytes() []byte {
	v := m.take()
	if v.Buf == nil {
		return nil
	}
	return v.Detach()
}

// delivery reports the complete message and the terminal read together at
// a MessageReader boundary. Between boundaries, replay retains the end with
// the message until the protocol consumes it.
func (m *ReceivedMessage) delivery(err error) (ReceivedMessage, error) {
	if err == nil {
		err, m.end = m.end, nil
	}
	return *m, err
}

func readMessage(r io.Reader, limit int) (ReceivedMessage, error) {
	mr, ok := r.(MessageReader)
	if !ok {
		return ReceivedMessage{}, errors.New("zmux: Records framing needs a MessageReader")
	}
	m, err := mr.ReadMessage(limit)
	if m.Len() > limit {
		m.Release()
		return ReceivedMessage{}, ErrMessageTooLarge
	}
	if err != nil {
		if !m.Present() {
			return ReceivedMessage{}, err
		}
		m.end = err
	}
	return m, nil
}
