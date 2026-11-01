package zmux

import (
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"time"
)

// PipeConn returns p as a net.Conn, for a layer such as tls.Server or
// tls.Client to run over a carrier (ListenerConfig.WrapPipe, or a
// Candidate's Dial). On a byte-stream carrier Read and Write go to p's
// halves as they are; on a message carrier (FramingRecords) each Write is
// one message and Read returns the bytes of the messages in order. Every
// Write is exactly one write on p, never merged with another, and a layer
// over the conn must keep that: one write on p for each write it takes.
//
// Read on a carrier without a receiving half returns io.EOF, and Write
// without a sending half an error. Close closes both halves, once;
// CloseWrite ends the sending half where it offers CloseWrite. Addresses
// and deadlines are those of the halves that have them; a deadline with no
// half to take it is os.ErrNoDeadline. The conn exposes none of p's other
// capabilities.
func PipeConn(p PipeIO) net.Conn {
	c := &pipeConn{p: p}
	if p.Framing == FramingRecords {
		c.mr, _ = p.R.(MessageReader)
		c.mw, _ = p.W.(MessageWriter)
	}
	return c
}

var errNoSendingHalf = errors.New("zmux: pipe without a sending half")

type pipeConn struct {
	p    PipeIO
	mr   MessageReader
	mw   MessageWriter
	once sync.Once

	rmu sync.Mutex
	buf []byte // the rest of the current message
}

func (c *pipeConn) Read(b []byte) (int, error) {
	if c.p.R == nil {
		return 0, io.EOF
	}
	if c.p.Framing != FramingRecords {
		return c.p.R.Read(b)
	}
	if c.mr == nil {
		return 0, errors.New("zmux: message pipe without a MessageReader")
	}
	c.rmu.Lock()
	defer c.rmu.Unlock()
	for len(c.buf) == 0 {
		if len(b) == 0 {
			return 0, nil
		}
		m, err := c.mr.ReadMessage()
		if err != nil {
			return 0, err
		}
		c.buf = m
	}
	n := copy(b, c.buf)
	c.buf = c.buf[n:]
	return n, nil
}

func (c *pipeConn) Write(b []byte) (int, error) {
	if c.p.W == nil {
		return 0, errNoSendingHalf
	}
	if c.p.Framing != FramingRecords {
		return c.p.W.Write(b)
	}
	if c.mw == nil {
		return 0, errors.New("zmux: message pipe without a MessageWriter")
	}
	if err := c.mw.WriteMessage(b); err != nil {
		return 0, err
	}
	return len(b), nil
}

func (c *pipeConn) Close() error {
	c.once.Do(func() { closePipeIO(c.p) })
	return nil
}

// CloseWrite ends the sending half gracefully where it offers CloseWrite.
func (c *pipeConn) CloseWrite() error {
	if cw, ok := c.p.W.(closeWriter); ok {
		return cw.CloseWrite()
	}
	return errors.ErrUnsupported
}

type pipeAddr struct{}

func (pipeAddr) Network() string { return "zmux" }
func (pipeAddr) String() string  { return "pipe" }

func (c *pipeConn) addr(local bool) net.Addr {
	for _, x := range []any{c.p.R, c.p.W} {
		if local {
			if a, ok := x.(interface{ LocalAddr() net.Addr }); ok && a.LocalAddr() != nil {
				return a.LocalAddr()
			}
		} else if a, ok := x.(interface{ RemoteAddr() net.Addr }); ok && a.RemoteAddr() != nil {
			return a.RemoteAddr()
		}
	}
	return pipeAddr{}
}

func (c *pipeConn) LocalAddr() net.Addr  { return c.addr(true) }
func (c *pipeConn) RemoteAddr() net.Addr { return c.addr(false) }

func (c *pipeConn) SetDeadline(t time.Time) error {
	errR, errW := c.SetReadDeadline(t), c.SetWriteDeadline(t)
	if errR != nil {
		return errR
	}
	return errW
}

func (c *pipeConn) SetReadDeadline(t time.Time) error {
	if d, ok := c.p.R.(interface{ SetReadDeadline(time.Time) error }); ok {
		return d.SetReadDeadline(t)
	}
	if c.p.R == nil {
		return nil
	}
	return os.ErrNoDeadline
}

func (c *pipeConn) SetWriteDeadline(t time.Time) error {
	if d, ok := c.p.W.(interface{ SetWriteDeadline(time.Time) error }); ok {
		return d.SetWriteDeadline(t)
	}
	if c.p.W == nil {
		return nil
	}
	return os.ErrNoDeadline
}
