package zmux

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"reflect"
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

// JoinedConn adapts independent read and write halves into a net.Conn.
//
// Either half may be nil. PauseRead and PauseWrite detach ownership until the
// returned handle resumes the half.
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
	readWaiters            int
	writeWaiters           int

	readDeadline     time.Time
	writeDeadline    time.Time
	readDeadlineGen  uint64
	writeDeadlineGen uint64

	closed bool
}

// PausedReadHalf owns a detached read half until Resume reattaches it.
type PausedReadHalf struct {
	handle joinedPausedHalf
}

// PausedWriteHalf owns a detached write half until Resume reattaches it.
type PausedWriteHalf struct {
	handle joinedPausedHalf
}

type joinedPausedHalf struct {
	mu      sync.Mutex
	conn    *JoinedConn
	current any
	side    joinedHalfSide
	resumed bool
}

type joinedCloseIdentity interface {
	joinedCloseIdentity() any
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

func sameJoinedHalf(first, second any) bool {
	if sameComparableValue(joinedCloseIdentityOf(first), joinedCloseIdentityOf(second)) {
		return true
	}
	return sameComparableValue(first, second)
}

func joinedCloseIdentityOf(v any) any {
	if v == nil {
		return nil
	}
	identity, ok := v.(joinedCloseIdentity)
	if !ok {
		return nil
	}
	return identity.joinedCloseIdentity()
}

func sameComparableValue(first, second any) bool {
	if first == nil || second == nil {
		return false
	}
	firstValue := reflect.ValueOf(first)
	secondValue := reflect.ValueOf(second)
	if !firstValue.IsValid() ||
		!secondValue.IsValid() ||
		firstValue.Type() != secondValue.Type() ||
		!firstValue.Type().Comparable() {
		return false
	}
	return firstValue.Interface() == secondValue.Interface()
}

func closeJoinedReadHalf(readHalf ReadHalf) (bool, error) {
	if closer, ok := readHalf.(io.Closer); ok {
		return true, closer.Close()
	}
	return false, readHalf.CloseRead()
}

func closeJoinedWriteHalf(writeHalf WriteHalf) error {
	if closer, ok := writeHalf.(io.Closer); ok {
		return closer.Close()
	}
	return writeHalf.CloseWrite()
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
	if c.readNotify == nil {
		c.readNotify = make(chan struct{})
		return
	}
	if c.readWaiters == 0 {
		return
	}
	close(c.readNotify)
	c.readNotify = make(chan struct{})
}

func (c *JoinedConn) broadcastWriteLocked() {
	if c.writeNotify == nil {
		c.writeNotify = make(chan struct{})
		return
	}
	if c.writeWaiters == 0 {
		return
	}
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

func (c *JoinedConn) beginReadWaitLocked() (<-chan struct{}, <-chan struct{}, time.Time) {
	c.initLocked()
	c.readWaiters++
	return c.readNotify, c.closedCh, c.readDeadline
}

func (c *JoinedConn) endReadWaitLocked() {
	if c.readWaiters > 0 {
		c.readWaiters--
	}
}

func (c *JoinedConn) beginWriteWaitLocked() (<-chan struct{}, <-chan struct{}, time.Time) {
	c.initLocked()
	c.writeWaiters++
	return c.writeNotify, c.closedCh, c.writeDeadline
}

func (c *JoinedConn) endWriteWaitLocked() {
	if c.writeWaiters > 0 {
		c.writeWaiters--
	}
}

type joinedHalfSide uint8

const (
	joinedReadSide joinedHalfSide = iota
	joinedWriteSide
)

func (side joinedHalfSide) beginWaitLocked(c *JoinedConn) (<-chan struct{}, <-chan struct{}, time.Time) {
	if side == joinedReadSide {
		return c.beginReadWaitLocked()
	}
	return c.beginWriteWaitLocked()
}

func (side joinedHalfSide) endWaitLocked(c *JoinedConn) {
	if side == joinedReadSide {
		c.endReadWaitLocked()
		return
	}
	c.endWriteWaitLocked()
}

func (side joinedHalfSide) broadcastLocked(c *JoinedConn) {
	if side == joinedReadSide {
		c.broadcastReadLocked()
		return
	}
	c.broadcastWriteLocked()
}

func (side joinedHalfSide) halfLocked(c *JoinedConn) any {
	if side == joinedReadSide {
		return c.readHalf
	}
	return c.writeHalf
}

func (side joinedHalfSide) setHalfLocked(c *JoinedConn, half any) {
	if side == joinedReadSide {
		if half == nil {
			c.readHalf = nil
			return
		}
		c.readHalf = half.(ReadHalf)
		return
	}
	if half == nil {
		c.writeHalf = nil
		return
	}
	c.writeHalf = half.(WriteHalf)
}

func (side joinedHalfSide) pausedLocked(c *JoinedConn) bool {
	if side == joinedReadSide {
		return c.readPaused
	}
	return c.writePaused
}

func (side joinedHalfSide) setPausedLocked(c *JoinedConn, paused bool) {
	if side == joinedReadSide {
		c.readPaused = paused
		return
	}
	c.writePaused = paused
}

func (side joinedHalfSide) activeOpsLocked(c *JoinedConn) int {
	if side == joinedReadSide {
		return c.activeReadOps
	}
	return c.activeWriteOps
}

func (side joinedHalfSide) addActiveOpLocked(c *JoinedConn) {
	if side == joinedReadSide {
		c.activeReadOps++
		return
	}
	c.activeWriteOps++
}

func (side joinedHalfSide) doneActiveOpLocked(c *JoinedConn) {
	if side == joinedReadSide {
		if c.activeReadOps > 0 {
			c.activeReadOps--
		}
		return
	}
	if c.activeWriteOps > 0 {
		c.activeWriteOps--
	}
}

func (side joinedHalfSide) activeDeadlineOpsLocked(c *JoinedConn) int {
	if side == joinedReadSide {
		return c.activeReadDeadlineOps
	}
	return c.activeWriteDeadlineOps
}

func (side joinedHalfSide) addActiveDeadlineOpLocked(c *JoinedConn) {
	if side == joinedReadSide {
		c.activeReadDeadlineOps++
		return
	}
	c.activeWriteDeadlineOps++
}

func (side joinedHalfSide) doneActiveDeadlineOpLocked(c *JoinedConn) {
	if side == joinedReadSide {
		if c.activeReadDeadlineOps > 0 {
			c.activeReadDeadlineOps--
		}
		return
	}
	if c.activeWriteDeadlineOps > 0 {
		c.activeWriteDeadlineOps--
	}
}

func (side joinedHalfSide) deadlineLocked(c *JoinedConn) time.Time {
	if side == joinedReadSide {
		return c.readDeadline
	}
	return c.writeDeadline
}

func (side joinedHalfSide) deadlineGenLocked(c *JoinedConn) uint64 {
	if side == joinedReadSide {
		return c.readDeadlineGen
	}
	return c.writeDeadlineGen
}

func (side joinedHalfSide) setDeadlineLocked(c *JoinedConn, t time.Time) {
	if side == joinedReadSide {
		c.readDeadline = t
		return
	}
	c.writeDeadline = t
}

func (side joinedHalfSide) bumpDeadlineGenLocked(c *JoinedConn) uint64 {
	if side == joinedReadSide {
		c.readDeadlineGen++
		return c.readDeadlineGen
	}
	c.writeDeadlineGen++
	return c.writeDeadlineGen
}

func (side joinedHalfSide) applyDeadline(half any, t time.Time) error {
	if side == joinedReadSide {
		return half.(ReadHalf).SetReadDeadline(t)
	}
	return half.(WriteHalf).SetWriteDeadline(t)
}

func (side joinedHalfSide) missingHalfErr() error {
	if side == joinedReadSide {
		return ErrStreamNotReadable
	}
	return ErrStreamNotWritable
}

func (side joinedHalfSide) invalidProgressErr() error {
	if side == joinedReadSide {
		return io.ErrShortBuffer
	}
	return io.ErrShortWrite
}

func (side joinedHalfSide) transfer(half any, p []byte) (int, error) {
	if side == joinedReadSide {
		return half.(ReadHalf).Read(p)
	}
	return half.(WriteHalf).Write(p)
}

func (side joinedHalfSide) close(half any) error {
	if side == joinedReadSide {
		return half.(ReadHalf).CloseRead()
	}
	return half.(WriteHalf).CloseWrite()
}

func (c *JoinedConn) Read(p []byte) (int, error) {
	return c.transferHalf(joinedReadSide, p)
}

func (c *JoinedConn) Write(p []byte) (int, error) {
	return c.transferHalf(joinedWriteSide, p)
}

func (c *JoinedConn) transferHalf(side joinedHalfSide, p []byte) (int, error) {
	if c == nil {
		return 0, ErrSessionClosed
	}
	half, err := c.enterHalf(side)
	if err != nil {
		return 0, err
	}
	defer c.leaveHalf(side)

	if half == nil {
		return 0, side.missingHalfErr()
	}
	n, err := side.transfer(half, p)
	if n < 0 || n > len(p) {
		return 0, side.invalidProgressErr()
	}
	return n, err
}

// CloseRead closes the currently attached read half. If no read half is
// attached, CloseRead is a no-op.
func (c *JoinedConn) CloseRead() error {
	return c.closeHalf(joinedReadSide)
}

// CloseWrite closes the currently attached write half. If no write half is
// attached, CloseWrite is a no-op.
func (c *JoinedConn) CloseWrite() error {
	return c.closeHalf(joinedWriteSide)
}

func (c *JoinedConn) closeHalf(side joinedHalfSide) error {
	if c == nil {
		return ErrSessionClosed
	}

	half, err := c.enterHalf(side)
	if err != nil {
		if errors.Is(err, ErrSessionClosed) {
			return nil
		}
		return err
	}
	defer c.leaveHalf(side)

	if half == nil {
		return nil
	}
	return side.close(half)
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

	var errs []error
	readClosedFully := false
	if readHalf != nil {
		var err error
		readClosedFully, err = closeJoinedReadHalf(readHalf)
		if err != nil {
			errs = append(errs, err)
		}
	}
	if writeHalf != nil && (!readClosedFully || !sameJoinedHalf(readHalf, writeHalf)) {
		if err := closeJoinedWriteHalf(writeHalf); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
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
	return c.setHalfDeadline(joinedReadSide, t)
}

func (c *JoinedConn) SetWriteDeadline(t time.Time) error {
	return c.setHalfDeadline(joinedWriteSide, t)
}

func (c *JoinedConn) setHalfDeadline(side joinedHalfSide, t time.Time) error {
	if c == nil {
		return ErrSessionClosed
	}

	c.mu.Lock()
	c.initLocked()
	if c.closed {
		c.mu.Unlock()
		return ErrSessionClosed
	}
	prevDeadline := side.deadlineLocked(c)
	side.setDeadlineLocked(c, t)
	deadlineGen := side.bumpDeadlineGenLocked(c)
	half := side.halfLocked(c)
	if half != nil {
		side.addActiveDeadlineOpLocked(c)
	}
	side.broadcastLocked(c)
	c.mu.Unlock()

	if half == nil {
		return nil
	}
	err := side.applyDeadline(half, t)
	c.mu.Lock()
	side.doneActiveDeadlineOpLocked(c)
	if err != nil && side.deadlineGenLocked(c) == deadlineGen {
		side.setDeadlineLocked(c, prevDeadline)
		side.bumpDeadlineGenLocked(c)
	}
	side.broadcastLocked(c)
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
	return &PausedReadHalf{
		handle: joinedPausedHalf{conn: c, current: current, side: joinedReadSide},
	}, nil
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
	return &PausedWriteHalf{
		handle: joinedPausedHalf{conn: c, current: current, side: joinedWriteSide},
	}, nil
}

// Current returns the read half currently owned by the pause handle.
func (p *PausedReadHalf) Current() ReadHalf {
	if p == nil {
		return nil
	}
	current := p.handle.currentHalf()
	if current == nil {
		return nil
	}
	return current.(ReadHalf)
}

// Set stages next as the read half to attach on Resume and returns the
// previously staged half. Passing nil detaches the read side.
func (p *PausedReadHalf) Set(next ReadHalf) ReadHalf {
	if p == nil {
		return nil
	}
	prev := p.handle.setHalf(next)
	if prev == nil {
		return nil
	}
	return prev.(ReadHalf)
}

// Resume reattaches the staged read half and re-enables upper-layer reads.
func (p *PausedReadHalf) Resume() error {
	if p == nil {
		return ErrSessionClosed
	}
	return resumePausedHalf(&p.handle)
}

// Current returns the write half currently owned by the pause handle.
func (p *PausedWriteHalf) Current() WriteHalf {
	if p == nil {
		return nil
	}
	current := p.handle.currentHalf()
	if current == nil {
		return nil
	}
	return current.(WriteHalf)
}

// Set stages next as the write half to attach on Resume and returns the
// previously staged half. Passing nil detaches the write side.
func (p *PausedWriteHalf) Set(next WriteHalf) WriteHalf {
	if p == nil {
		return nil
	}
	prev := p.handle.setHalf(next)
	if prev == nil {
		return nil
	}
	return prev.(WriteHalf)
}

// Resume reattaches the staged write half and re-enables upper-layer writes.
func (p *PausedWriteHalf) Resume() error {
	if p == nil {
		return ErrSessionClosed
	}
	return resumePausedHalf(&p.handle)
}

func (p *joinedPausedHalf) currentHalf() any {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.current
}

func (p *joinedPausedHalf) setHalf(next any) any {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	prev := p.current
	p.current = next
	return prev
}

func resumePausedHalf(p *joinedPausedHalf) error {
	if p == nil || p.conn == nil {
		return ErrSessionClosed
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.resumed {
		return nil
	}

	current := p.current
	side := p.side
	for {
		p.conn.mu.Lock()
		if p.conn.closed {
			p.conn.mu.Unlock()
			p.resumed = true
			return ErrSessionClosed
		}
		deadline := side.deadlineLocked(p.conn)
		gen := side.deadlineGenLocked(p.conn)
		p.conn.mu.Unlock()
		if current != nil {
			if err := side.applyDeadline(current, deadline); err != nil {
				return err
			}
		}
		p.conn.mu.Lock()
		if p.conn.closed {
			p.conn.mu.Unlock()
			p.resumed = true
			return ErrSessionClosed
		}
		if current != nil && side.deadlineGenLocked(p.conn) != gen {
			p.conn.mu.Unlock()
			continue
		}
		side.setHalfLocked(p.conn, current)
		side.setPausedLocked(p.conn, false)
		side.broadcastLocked(p.conn)
		p.conn.mu.Unlock()
		p.resumed = true
		return nil
	}
}

func (c *JoinedConn) enterHalf(side joinedHalfSide) (any, error) {
	for {
		c.mu.Lock()
		c.initLocked()
		switch {
		case c.closed:
			c.mu.Unlock()
			return nil, ErrSessionClosed
		case side.pausedLocked(c):
			notifyCh, closedCh, deadline := side.beginWaitLocked(c)
			c.mu.Unlock()
			err := waitJoinedStateLocal(notifyCh, closedCh, deadline)
			c.mu.Lock()
			side.endWaitLocked(c)
			c.mu.Unlock()
			if err != nil {
				return nil, err
			}
		default:
			half := side.halfLocked(c)
			side.addActiveOpLocked(c)
			c.mu.Unlock()
			return half, nil
		}
	}
}

func (c *JoinedConn) leaveHalf(side joinedHalfSide) {
	c.mu.Lock()
	side.doneActiveOpLocked(c)
	side.broadcastLocked(c)
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
	half, err := c.pauseHalf(ctx, joinedReadSide)
	if half == nil || err != nil {
		return nil, err
	}
	return half.(ReadHalf), nil
}

func (c *JoinedConn) pauseWriteHalf(ctx context.Context) (WriteHalf, error) {
	half, err := c.pauseHalf(ctx, joinedWriteSide)
	if half == nil || err != nil {
		return nil, err
	}
	return half.(WriteHalf), nil
}

func (c *JoinedConn) pauseHalf(ctx context.Context, side joinedHalfSide) (any, error) {
	ctx = contextOrBackground(ctx)
	ownedPause := false

	for {
		c.mu.Lock()
		c.initLocked()
		switch {
		case c.closed:
			c.mu.Unlock()
			return nil, ErrSessionClosed
		case !ownedPause && side.pausedLocked(c):
			notifyCh, closedCh, _ := side.beginWaitLocked(c)
			c.mu.Unlock()
			err := waitJoinedState(ctx, notifyCh, closedCh, time.Time{})
			c.mu.Lock()
			side.endWaitLocked(c)
			c.mu.Unlock()
			if err != nil {
				return nil, err
			}
		case !ownedPause:
			side.setPausedLocked(c, true)
			ownedPause = true
			side.broadcastLocked(c)
			c.mu.Unlock()
		case side.activeOpsLocked(c) == 0 && side.activeDeadlineOpsLocked(c) == 0:
			current := side.halfLocked(c)
			side.setHalfLocked(c, nil)
			side.broadcastLocked(c)
			c.mu.Unlock()
			return current, nil
		default:
			notifyCh, closedCh, _ := side.beginWaitLocked(c)
			c.mu.Unlock()
			err := waitJoinedState(ctx, notifyCh, closedCh, time.Time{})
			c.mu.Lock()
			side.endWaitLocked(c)
			if err != nil {
				if ownedPause && !c.closed {
					side.setPausedLocked(c, false)
					side.broadcastLocked(c)
				}
				c.mu.Unlock()
				return nil, err
			}
			c.mu.Unlock()
		}
	}
}
