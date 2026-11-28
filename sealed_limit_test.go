package zmux

import (
	"bytes"
	"errors"
	"testing"
)

// tagOpener opens a message by dropping its sealTag trailing bytes.
type tagOpener struct{}

func (tagOpener) OpenInPlace([]byte) error { return nil }

type tagSealer struct{ tagOpener }

func (tagSealer) Seal(dst, pt []byte) ([]byte, error) {
	return append(append(dst, pt...), make([]byte, sealTag)...), nil
}
func (tagSealer) Overhead() int { return sealTag }

// A sealed reader asks the carrier below for no more than limit plus the
// seal's bytes, so a record over the limit is refused after reading at
// most limit+1 plaintext bytes, not join.MaxMessage.
func TestSealedReaderLimit(t *testing.T) {
	const limit = 100
	for _, c := range []struct {
		plain   int
		tooLong bool
	}{{limit - 1, false}, {limit, false}, {limit + 1, true}} {
		m := &limitMsgCarrier{firstMsgCarrier: newFirstMsgCarrier(make([]byte, c.plain+sealTag))}
		p := SealRecords(PipeIO{R: m, W: m, Framing: FramingRecords}, tagSealer{}, tagOpener{}, RecordMode(0), nil)
		msg, err := p.R.(MessageReader).ReadMessage(limit)
		msg.Release()
		if got := errors.Is(err, ErrMessageTooLarge); got != c.tooLong || (err != nil) != c.tooLong {
			t.Fatalf("%d bytes: err %v", c.plain, err)
		}
		if len(m.limits) != 1 || m.limits[0] != limit+sealTag {
			t.Fatalf("%d bytes: lower limits %v, want [%d]", c.plain, m.limits, limit+sealTag)
		}
	}
}

// On a byte stream the Size field is checked before the body is read.
func TestSealedReaderLimitStream(t *testing.T) {
	const limit = 100
	frame := func(n int) *bytes.Reader {
		return bytes.NewReader(append([]byte{byte(n >> 8), byte(n)}, make([]byte, n)...))
	}
	for _, c := range []struct {
		plain   int
		tooLong bool
	}{{limit, false}, {limit + 1, true}} {
		src := frame(c.plain + sealTag)
		p := SealRecords(PipeIO{R: readerOnly{src}, Framing: FramingByteStream}, tagSealer{}, tagOpener{}, RecordMode(0), nil)
		msg, err := p.R.(MessageReader).ReadMessage(limit)
		msg.Release()
		if errors.Is(err, ErrMessageTooLarge) != c.tooLong {
			t.Fatalf("%d bytes: err %v", c.plain, err)
		}
		if c.tooLong && src.Len() != c.plain+sealTag {
			t.Fatalf("the body was read: %d bytes left", src.Len())
		}
	}
}

type readerOnly struct{ *bytes.Reader }

func (readerOnly) Close() error { return nil }
