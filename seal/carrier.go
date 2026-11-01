package seal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"sync"
	"sync/atomic"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/varint"
)

var (
	errClosed      = errors.New("seal: carrier closed")
	errNotReady    = errors.New("seal: handshake not complete")
	errByteRead    = errors.New("seal: message carrier: use ReadMessage")
	errLowerFramed = errors.New("seal: message carrier without MessageReader or MessageWriter")
)

// lower is the carrier below the transform, in either framing.
type lower struct {
	p    zmux.PipeIO
	msgs bool
	br   *bufio.Reader
	mr   zmux.MessageReader
	mw   zmux.MessageWriter
	mbw  zmux.MessageBatchWriter

	closeOnce sync.Once
}

func newLower(p zmux.PipeIO) (*lower, error) {
	l := &lower{p: p, msgs: p.Framing == zmux.FramingRecords}
	if l.msgs {
		var ok1, ok2 bool
		l.mr, ok1 = p.R.(zmux.MessageReader)
		l.mw, ok2 = p.W.(zmux.MessageWriter)
		if !ok1 || !ok2 {
			return nil, errLowerFramed
		}
		l.mbw, _ = p.W.(zmux.MessageBatchWriter)
	} else {
		if p.R == nil || p.W == nil {
			return nil, errors.New("seal: carrier needs both halves")
		}
		l.br = bufio.NewReaderSize(p.R, 16<<10)
	}
	return l, nil
}

// overhead is what each frame adds below the transform.
func (l *lower) overhead() int {
	if l.msgs {
		return msgOverhead
	}
	return frameOverhead
}

func (l *lower) readFull(b []byte) error {
	_, err := io.ReadFull(l.br, b)
	return err
}

// write writes b as one write: one message, or one Write.
func (l *lower) write(b []byte) error {
	if l.msgs {
		return l.mw.WriteMessage(b)
	}
	_, err := l.p.W.Write(b)
	return err
}

// writeMsgs writes msgs in one call where the carrier takes batches.
func (l *lower) writeMsgs(msgs [][]byte) error {
	if l.mbw != nil {
		return l.mbw.WriteMessages(msgs)
	}
	for _, m := range msgs {
		if err := l.mw.WriteMessage(m); err != nil {
			return err
		}
	}
	return nil
}

func (l *lower) close() {
	l.closeOnce.Do(func() {
		if l.p.W != nil {
			_ = l.p.W.Close()
		}
		if l.p.R != nil && any(l.p.R) != any(l.p.W) {
			_ = l.p.R.Close()
		}
	})
}

// readHandshake reads one handshake message whose tokens take head bytes
// (Len included as its last two): one message, or head, Len and the
// encrypted payload of a byte stream.
func (l *lower) readHandshake(head int) ([]byte, error) {
	if l.msgs {
		return l.mr.ReadMessage()
	}
	b := make([]byte, head, head+maxPayload+tagLen)
	if err := l.readFull(b); err != nil {
		return nil, unexpected(err)
	}
	n := int(binary.BigEndian.Uint16(b[head-lenLen:]))
	b = b[:head+n+tagLen]
	if err := l.readFull(b[head:]); err != nil {
		return nil, unexpected(err)
	}
	return b, nil
}

func unexpected(err error) error {
	if errors.Is(err, io.EOF) {
		return io.ErrUnexpectedEOF
	}
	return err
}

// carrier is what both ends share once their keys exist: the transport
// cipher states, the exporter and the lower carrier.
type carrier struct {
	low *lower

	wmu  sync.Mutex
	send frameState
	wbuf []byte
	wmsg [][]byte

	rmu     sync.Mutex
	recv    frameState
	pending [][]byte // records of a handshake payload, delivered first

	exp   [32]byte
	expOK atomic.Bool

	closed    chan struct{}
	closeOnce sync.Once
	failErr   atomic.Pointer[error]
}

func (c *carrier) init(low *lower) {
	c.low = low
	c.closed = make(chan struct{})
}

// fail ends the carrier after err (an authentication failure, a malformed
// frame or a carrier error); it sends nothing.
func (c *carrier) fail(err error) error {
	c.failErr.CompareAndSwap(nil, &err)
	c.shut()
	return err
}

func (c *carrier) shut() {
	c.closeOnce.Do(func() { close(c.closed) })
	c.low.close()
}

func (c *carrier) err() error {
	if p := c.failErr.Load(); p != nil {
		return *p
	}
	return errClosed
}

// Close closes the carrier and the one below it.
func (c *carrier) Close() error {
	c.shut()
	return nil
}

// CloseWrite ends the sending side of the carrier below, where it offers
// CloseWrite; frames are written as they are taken, so nothing is held.
func (c *carrier) CloseWrite() error {
	if cw, ok := c.low.p.W.(zmux.CloseWriter); ok {
		return cw.CloseWrite()
	}
	return nil
}

// Abort aborts the carrier below (its Aborter, else Close).
func (c *carrier) Abort(cause error) {
	if a, ok := c.low.p.W.(zmux.Aborter); ok {
		a.Abort(cause)
	}
	c.shut()
}

// ExportKeyingMaterial exports keying material from the carrier's
// handshake (KeyingMaterialExporter): HKDF-Expand(HKDF-Extract(exporter
// secret, label), context, length); a nil context is an empty one. It
// fails before the handshake has completed.
func (c *carrier) ExportKeyingMaterial(label string, context []byte, length int) ([]byte, error) {
	if !c.expOK.Load() {
		return nil, errNotReady
	}
	if length == 0 {
		return []byte{}, nil
	}
	return export(&c.exp, label, context, length)
}

// Read is not used on a message carrier.
func (c *carrier) Read([]byte) (int, error) { return 0, errByteRead }

// writeFrames seals msgs and writes them as one write below. Called with
// wmu held, after the handshake.
func (c *carrier) writeFrames(msgs [][]byte) error {
	select {
	case <-c.closed:
		return c.err()
	default:
	}
	if len(msgs) == 0 {
		return nil
	}
	withLen := !c.low.msgs
	b := c.wbuf[:0]
	if !withLen {
		c.wmsg = c.wmsg[:0]
	}
	for _, m := range msgs {
		off := len(b)
		var err error
		if b, err = c.send.seal(b, m, withLen); err != nil {
			return c.fail(err)
		}
		if !withLen {
			c.wmsg = append(c.wmsg, b[off:len(b):len(b)])
		}
	}
	var err error
	if withLen {
		err = c.low.write(b)
	} else if len(c.wmsg) == 1 {
		err = c.low.write(c.wmsg[0])
	} else {
		err = c.low.writeMsgs(c.wmsg)
	}
	if cap(b) <= 1<<20 {
		c.wbuf = b[:0]
	} else {
		c.wbuf = nil
	}
	clear(c.wmsg)
	if err != nil {
		return c.fail(err)
	}
	return nil
}

// readFrame reads and opens the next frame. Called with rmu held.
func (c *carrier) readFrame() ([]byte, error) {
	if c.low.msgs {
		m, err := c.low.mr.ReadMessage()
		if err != nil {
			return nil, c.fail(err)
		}
		if len(m) <= tagLen {
			return nil, c.fail(errRecordLen)
		}
		pt, err := c.recv.open(m, len(m)-tagLen)
		if err != nil {
			return nil, c.fail(err)
		}
		return pt, nil
	}
	var h [lenLen]byte
	if _, err := io.ReadFull(c.low.br, h[:]); err != nil {
		return nil, c.fail(err) // io.EOF: the peer ended at a frame boundary
	}
	n := int(binary.BigEndian.Uint16(h[:]))
	if n == 0 {
		return nil, c.fail(errRecordLen)
	}
	b := make([]byte, n+tagLen)
	if err := c.low.readFull(b); err != nil {
		return nil, c.fail(unexpected(err))
	}
	pt, err := c.recv.open(b, n)
	if err != nil {
		return nil, c.fail(err)
	}
	return pt, nil
}

// nextPending pops a record of a handshake payload. Called with rmu held.
func (c *carrier) nextPending() ([]byte, bool) {
	if len(c.pending) == 0 {
		return nil, false
	}
	m := c.pending[0]
	c.pending[0] = nil
	c.pending = c.pending[1:]
	return m, true
}

// appendRecords appends recs to b in the byte-stream form of records
// (Size, then Frames) while the total stays within limit; it returns the
// number of records taken.
func appendRecords(b []byte, recs [][]byte, limit int) ([]byte, int) {
	n := 0
	for _, r := range recs {
		need := varint.SizeVarint(uint64(len(r))) + len(r)
		if len(b)+need > limit {
			break
		}
		b = varint.AppendVarint(b, uint64(len(r)))
		b = append(b, r...)
		n++
	}
	return b, n
}

// parseRecords splits the byte-stream records of a handshake payload.
// The records alias b.
func parseRecords(b []byte) ([][]byte, error) {
	var out [][]byte
	for len(b) > 0 {
		v, n := varint.ConsumeVarint(b)
		if n <= 0 || v == 0 || v > maxRecord || uint64(len(b)-n) < v {
			return nil, errMalformed
		}
		out = append(out, b[n:n+int(v):n+int(v)])
		b = b[n+int(v):]
	}
	return out, nil
}
