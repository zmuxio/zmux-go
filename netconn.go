package zmux

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"time"
)

// ReadHalf is the minimal directional read-side contract required by
// JoinedConn.
type ReadHalf interface {
	io.Reader
	CloseRead() error
	SetReadDeadline(time.Time) error
	LocalAddr() net.Addr
	RemoteAddr() net.Addr
}

// WriteHalf is the minimal directional write-side contract required by
// JoinedConn.
type WriteHalf interface {
	io.Writer
	CloseWrite() error
	SetWriteDeadline(time.Time) error
	LocalAddr() net.Addr
	RemoteAddr() net.Addr
}

// JoinedConn adapts an independent read half plus write half into a net.Conn.
//
// Either half may be nil. A nil read half makes Read return ErrStreamNotReadable.
// A nil write half makes Write return ErrStreamNotWritable.
//
// Directional pause operations hand ownership of the currently attached half to
// the caller. While paused, upper-layer operations in that direction block until
// the handle resumes or the joined-level deadline/close wakes them. Close only
// closes halves that are still attached at the time Close runs; detached halves
// remain caller-owned.
type JoinedConn struct {
	mu sync.Mutex

	readNotify  chan struct{}
	writeNotify chan struct{}
	closedCh    chan struct{}

	readHalf  ReadHalf
	writeHalf WriteHalf

	readPaused  bool
	writePaused bool

	activeReadOps          int
	activeWriteOps         int
	activeReadDeadlineOps  int
	activeWriteDeadlineOps int

	readDeadline     time.Time
	writeDeadline    time.Time
	readDeadlineGen  uint64
	writeDeadlineGen uint64

	closed bool
}

// PausedReadHalf owns a detached read half until Resume reattaches it.
type PausedReadHalf struct {
	conn    *JoinedConn
	current ReadHalf
	resumed bool
}

// PausedWriteHalf owns a detached write half until Resume reattaches it.
type PausedWriteHalf struct {
	conn    *JoinedConn
	current WriteHalf
	resumed bool
}

// JoinConn adapts one read half plus one write half into a net.Conn-compatible
// wrapper. Either half may be nil.
func JoinConn(read ReadHalf, write WriteHalf) *JoinedConn {
	return &JoinedConn{
		readNotify:  make(chan struct{}),
		writeNotify: make(chan struct{}),
		closedCh:    make(chan struct{}),
		readHalf:    read,
		writeHalf:   write,
	}
}

// ReadHalf returns the currently attached read half. It returns nil while the
// read side is paused, detached, or absent.
func (c *JoinedConn) ReadHalf() ReadHalf {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.readHalf
}

// WriteHalf returns the currently attached write half. It returns nil while the
// write side is paused, detached, or absent.
func (c *JoinedConn) WriteHalf() WriteHalf {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.writeHalf
}

func (c *JoinedConn) broadcastReadLocked() {
	close(c.readNotify)
	c.readNotify = make(chan struct{})
}

func (c *JoinedConn) broadcastWriteLocked() {
	close(c.writeNotify)
	c.writeNotify = make(chan struct{})
}

func (c *JoinedConn) initLocked() {
	if c.readNotify == nil {
		c.readNotify = make(chan struct{})
	}
	if c.writeNotify == nil {
		c.writeNotify = make(chan struct{})
	}
	if c.closedCh == nil {
		c.closedCh = make(chan struct{})
	}
}

func (c *JoinedConn) Read(p []byte) (int, error) {
	if c == nil {
		return 0, ErrSessionClosed
	}
	readHalf, err := c.enterRead()
	if err != nil {
		return 0, err
	}
	defer c.leaveRead()

	if readHalf == nil {
		return 0, ErrStreamNotReadable
	}
	return readHalf.Read(p)
}

func (c *JoinedConn) Write(p []byte) (int, error) {
	if c == nil {
		return 0, ErrSessionClosed
	}
	writeHalf, err := c.enterWrite()
	if err != nil {
		return 0, err
	}
	defer c.leaveWrite()

	if writeHalf == nil {
		return 0, ErrStreamNotWritable
	}
	return writeHalf.Write(p)
}

// CloseRead closes the currently attached read half. If no read half is
// attached, CloseRead is a no-op.
func (c *JoinedConn) CloseRead() error {
	if c == nil {
		return ErrSessionClosed
	}

	readHalf, err := c.enterRead()
	if err != nil {
		if errors.Is(err, ErrSessionClosed) {
			return nil
		}
		return err
	}
	defer c.leaveRead()

	if readHalf == nil {
		return nil
	}
	return readHalf.CloseRead()
}

// CloseWrite closes the currently attached write half. If no write half is
// attached, CloseWrite is a no-op.
func (c *JoinedConn) CloseWrite() error {
	if c == nil {
		return ErrSessionClosed
	}

	writeHalf, err := c.enterWrite()
	if err != nil {
		if errors.Is(err, ErrSessionClosed) {
			return nil
		}
		return err
	}
	defer c.leaveWrite()

	if writeHalf == nil {
		return nil
	}
	return writeHalf.CloseWrite()
}

// Close closes the currently attached halves and wakes all blocked operations.
// Halves that were already detached through PauseRead or PauseWrite remain
// caller-owned and are not closed by JoinedConn.
func (c *JoinedConn) Close() error {
	if c == nil {
		return ErrSessionClosed
	}

	c.mu.Lock()
	c.initLocked()
	if c.closed {
		c.mu.Unlock()
		return nil
	}

	c.closed = true
	close(c.closedCh)

	readHalf := c.readHalf
	writeHalf := c.writeHalf

	c.readHalf = nil
	c.writeHalf = nil
	c.readPaused = false
	c.writePaused = false
	c.broadcastReadLocked()
	c.broadcastWriteLocked()
	c.mu.Unlock()

	var readErr error
	if readHalf != nil {
		readErr = readHalf.CloseRead()
	}
	if writeHalf != nil {
		if err := writeHalf.CloseWrite(); err != nil && readErr == nil {
			return err
		}
	}
	return readErr
}

func (c *JoinedConn) SetDeadline(t time.Time) error {
	if c == nil {
		return ErrSessionClosed
	}
	if err := c.SetReadDeadline(t); err != nil {
		return err
	}
	return c.SetWriteDeadline(t)
}

func (c *JoinedConn) SetReadDeadline(t time.Time) error {
	if c == nil {
		return ErrSessionClosed
	}

	c.mu.Lock()
	c.initLocked()
	if c.closed {
		c.mu.Unlock()
		return ErrSessionClosed
	}
	c.readDeadline = t
	c.readDeadlineGen++
	readHalf := c.readHalf
	if readHalf != nil {
		c.activeReadDeadlineOps++
	}
	c.broadcastReadLocked()
	c.mu.Unlock()

	if readHalf == nil {
		return nil
	}
	err := readHalf.SetReadDeadline(t)
	c.mu.Lock()
	if c.activeReadDeadlineOps > 0 {
		c.activeReadDeadlineOps--
	}
	c.broadcastReadLocked()
	c.mu.Unlock()
	return err
}

func (c *JoinedConn) SetWriteDeadline(t time.Time) error {
	if c == nil {
		return ErrSessionClosed
	}

	c.mu.Lock()
	c.initLocked()
	if c.closed {
		c.mu.Unlock()
		return ErrSessionClosed
	}
	c.writeDeadline = t
	c.writeDeadlineGen++
	writeHalf := c.writeHalf
	if writeHalf != nil {
		c.activeWriteDeadlineOps++
	}
	c.broadcastWriteLocked()
	c.mu.Unlock()

	if writeHalf == nil {
		return nil
	}
	err := writeHalf.SetWriteDeadline(t)
	c.mu.Lock()
	if c.activeWriteDeadlineOps > 0 {
		c.activeWriteDeadlineOps--
	}
	c.broadcastWriteLocked()
	c.mu.Unlock()
	return err
}

func (c *JoinedConn) LocalAddr() net.Addr {
	if c == nil {
		return streamAddr{endpoint: "local"}
	}

	c.mu.Lock()
	readHalf, writeHalf := c.readHalf, c.writeHalf
	c.mu.Unlock()

	if readHalf != nil {
		if addr := readHalf.LocalAddr(); addr != nil {
			return addr
		}
	}
	if writeHalf != nil {
		if addr := writeHalf.LocalAddr(); addr != nil {
			return addr
		}
	}
	return streamAddr{endpoint: "local"}
}

func (c *JoinedConn) RemoteAddr() net.Addr {
	if c == nil {
		return streamAddr{endpoint: "remote"}
	}

	c.mu.Lock()
	readHalf, writeHalf := c.readHalf, c.writeHalf
	c.mu.Unlock()

	if readHalf != nil {
		if addr := readHalf.RemoteAddr(); addr != nil {
			return addr
		}
	}
	if writeHalf != nil {
		if addr := writeHalf.RemoteAddr(); addr != nil {
			return addr
		}
	}
	return streamAddr{endpoint: "remote"}
}

// PauseRead waits for the read side to become quiescent, detaches the current
// read half, and returns a handle that owns it. The returned handle may stage a
// replacement half before Resume reattaches it.
func (c *JoinedConn) PauseRead(ctx context.Context) (*PausedReadHalf, error) {
	if c == nil {
		return nil, ErrSessionClosed
	}

	current, err := c.pauseReadHalf(ctx)
	if err != nil {
		return nil, err
	}
	return &PausedReadHalf{conn: c, current: current}, nil
}

// PauseWrite waits for the write side to become quiescent, detaches the
// current write half, and returns a handle that owns it. The returned handle
// may stage a replacement half before Resume reattaches it.
func (c *JoinedConn) PauseWrite(ctx context.Context) (*PausedWriteHalf, error) {
	if c == nil {
		return nil, ErrSessionClosed
	}

	current, err := c.pauseWriteHalf(ctx)
	if err != nil {
		return nil, err
	}
	return &PausedWriteHalf{conn: c, current: current}, nil
}

// Current returns the read half currently owned by the pause handle.
func (p *PausedReadHalf) Current() ReadHalf {
	if p == nil {
		return nil
	}
	return p.current
}

// Set stages next as the read half to attach on Resume and returns the
// previously staged half. Passing nil detaches the read side.
func (p *PausedReadHalf) Set(next ReadHalf) ReadHalf {
	if p == nil {
		return nil
	}
	prev := p.current
	p.current = next
	return prev
}

// Resume reattaches the staged read half and re-enables upper-layer reads.
func (p *PausedReadHalf) Resume() error {
	if p == nil || p.conn == nil {
		return ErrSessionClosed
	}
	if p.resumed {
		return nil
	}

	current := p.current
	for {
		p.conn.mu.Lock()
		if p.conn.closed {
			p.conn.mu.Unlock()
			p.resumed = true
			return ErrSessionClosed
		}
		deadline := p.conn.readDeadline
		gen := p.conn.readDeadlineGen
		p.conn.mu.Unlock()
		if current != nil {
			if err := current.SetReadDeadline(deadline); err != nil {
				return err
			}
		}
		p.conn.mu.Lock()
		if p.conn.closed {
			p.conn.mu.Unlock()
			p.resumed = true
			return ErrSessionClosed
		}
		if current != nil && p.conn.readDeadlineGen != gen {
			p.conn.mu.Unlock()
			continue
		}
		p.conn.readHalf = current
		p.conn.readPaused = false
		p.conn.broadcastReadLocked()
		p.conn.mu.Unlock()
		p.resumed = true
		return nil
	}
}

// Current returns the write half currently owned by the pause handle.
func (p *PausedWriteHalf) Current() WriteHalf {
	if p == nil {
		return nil
	}
	return p.current
}

// Set stages next as the write half to attach on Resume and returns the
// previously staged half. Passing nil detaches the write side.
func (p *PausedWriteHalf) Set(next WriteHalf) WriteHalf {
	if p == nil {
		return nil
	}
	prev := p.current
	p.current = next
	return prev
}

// Resume reattaches the staged write half and re-enables upper-layer writes.
func (p *PausedWriteHalf) Resume() error {
	if p == nil || p.conn == nil {
		return ErrSessionClosed
	}
	if p.resumed {
		return nil
	}

	current := p.current
	for {
		p.conn.mu.Lock()
		if p.conn.closed {
			p.conn.mu.Unlock()
			p.resumed = true
			return ErrSessionClosed
		}
		deadline := p.conn.writeDeadline
		gen := p.conn.writeDeadlineGen
		p.conn.mu.Unlock()
		if current != nil {
			if err := current.SetWriteDeadline(deadline); err != nil {
				return err
			}
		}
		p.conn.mu.Lock()
		if p.conn.closed {
			p.conn.mu.Unlock()
			p.resumed = true
			return ErrSessionClosed
		}
		if current != nil && p.conn.writeDeadlineGen != gen {
			p.conn.mu.Unlock()
			continue
		}
		p.conn.writeHalf = current
		p.conn.writePaused = false
		p.conn.broadcastWriteLocked()
		p.conn.mu.Unlock()
		p.resumed = true
		return nil
	}
}

func (c *JoinedConn) enterRead() (ReadHalf, error) {
	for {
		c.mu.Lock()
		c.initLocked()
		switch {
		case c.closed:
			c.mu.Unlock()
			return nil, ErrSessionClosed
		case c.readPaused:
			notifyCh, closedCh, deadline := c.readNotify, c.closedCh, c.readDeadline
			c.mu.Unlock()
			if err := waitJoinedStateLocal(notifyCh, closedCh, deadline); err != nil {
				return nil, err
			}
		default:
			readHalf := c.readHalf
			c.activeReadOps++
			c.mu.Unlock()
			return readHalf, nil
		}
	}
}

func (c *JoinedConn) leaveRead() {
	c.mu.Lock()
	if c.activeReadOps > 0 {
		c.activeReadOps--
	}
	c.broadcastReadLocked()
	c.mu.Unlock()
}

func (c *JoinedConn) enterWrite() (WriteHalf, error) {
	for {
		c.mu.Lock()
		c.initLocked()
		switch {
		case c.closed:
			c.mu.Unlock()
			return nil, ErrSessionClosed
		case c.writePaused:
			notifyCh, closedCh, deadline := c.writeNotify, c.closedCh, c.writeDeadline
			c.mu.Unlock()
			if err := waitJoinedStateLocal(notifyCh, closedCh, deadline); err != nil {
				return nil, err
			}
		default:
			writeHalf := c.writeHalf
			c.activeWriteOps++
			c.mu.Unlock()
			return writeHalf, nil
		}
	}
}

func (c *JoinedConn) leaveWrite() {
	c.mu.Lock()
	if c.activeWriteOps > 0 {
		c.activeWriteOps--
	}
	c.broadcastWriteLocked()
	c.mu.Unlock()
}

func waitJoinedState(ctx context.Context, notifyCh <-chan struct{}, closedCh <-chan struct{}, deadline time.Time) error {
	ctx = contextOrBackground(ctx)

	if deadline.IsZero() {
		select {
		case <-notifyCh:
			return nil
		case <-closedCh:
			return ErrSessionClosed
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	delay := time.Until(deadline)
	if delay <= 0 {
		return os.ErrDeadlineExceeded
	}

	timer := time.NewTimer(delay)
	defer stopTimer(timer)

	select {
	case <-notifyCh:
		return nil
	case <-closedCh:
		return ErrSessionClosed
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return os.ErrDeadlineExceeded
	}
}

func waitJoinedStateLocal(notifyCh <-chan struct{}, closedCh <-chan struct{}, deadline time.Time) error {
	if deadline.IsZero() {
		select {
		case <-notifyCh:
			return nil
		case <-closedCh:
			return ErrSessionClosed
		}
	}

	delay := time.Until(deadline)
	if delay <= 0 {
		return os.ErrDeadlineExceeded
	}

	timer := time.NewTimer(delay)
	defer stopTimer(timer)

	select {
	case <-notifyCh:
		return nil
	case <-closedCh:
		return ErrSessionClosed
	case <-timer.C:
		return os.ErrDeadlineExceeded
	}
}

func (c *JoinedConn) pauseReadHalf(ctx context.Context) (ReadHalf, error) {
	ctx = contextOrBackground(ctx)
	ownedPause := false

	for {
		c.mu.Lock()
		c.initLocked()
		switch {
		case c.closed:
			c.mu.Unlock()
			return nil, ErrSessionClosed
		case !ownedPause && c.readPaused:
			notifyCh, closedCh := c.readNotify, c.closedCh
			c.mu.Unlock()
			if err := waitJoinedState(ctx, notifyCh, closedCh, time.Time{}); err != nil {
				return nil, err
			}
		case !ownedPause:
			c.readPaused = true
			ownedPause = true
			c.broadcastReadLocked()
			c.mu.Unlock()
		case c.activeReadOps == 0 && c.activeReadDeadlineOps == 0:
			current := c.readHalf
			c.readHalf = nil
			c.broadcastReadLocked()
			c.mu.Unlock()
			return current, nil
		default:
			notifyCh, closedCh := c.readNotify, c.closedCh
			c.mu.Unlock()
			if err := waitJoinedState(ctx, notifyCh, closedCh, time.Time{}); err != nil {
				c.mu.Lock()
				if ownedPause && !c.closed {
					c.readPaused = false
					c.broadcastReadLocked()
				}
				c.mu.Unlock()
				return nil, err
			}
		}
	}
}

func (c *JoinedConn) pauseWriteHalf(ctx context.Context) (WriteHalf, error) {
	ctx = contextOrBackground(ctx)
	ownedPause := false

	for {
		c.mu.Lock()
		c.initLocked()
		switch {
		case c.closed:
			c.mu.Unlock()
			return nil, ErrSessionClosed
		case !ownedPause && c.writePaused:
			notifyCh, closedCh := c.writeNotify, c.closedCh
			c.mu.Unlock()
			if err := waitJoinedState(ctx, notifyCh, closedCh, time.Time{}); err != nil {
				return nil, err
			}
		case !ownedPause:
			c.writePaused = true
			ownedPause = true
			c.broadcastWriteLocked()
			c.mu.Unlock()
		case c.activeWriteOps == 0 && c.activeWriteDeadlineOps == 0:
			current := c.writeHalf
			c.writeHalf = nil
			c.broadcastWriteLocked()
			c.mu.Unlock()
			return current, nil
		default:
			notifyCh, closedCh := c.writeNotify, c.closedCh
			c.mu.Unlock()
			if err := waitJoinedState(ctx, notifyCh, closedCh, time.Time{}); err != nil {
				c.mu.Lock()
				if ownedPause && !c.closed {
					c.writePaused = false
					c.broadcastWriteLocked()
				}
				c.mu.Unlock()
				return nil, err
			}
		}
	}
}
