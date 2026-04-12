package zmux

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync/atomic"
	"time"

	"github.com/zmuxio/zmux-go/internal/state"
	"github.com/zmuxio/zmux-go/internal/wire"
)

type streamAddrProvider interface {
	LocalAddr() net.Addr
	RemoteAddr() net.Addr
}

type streamAddr struct {
	endpoint    string
	streamID    uint64
	streamIDSet bool
}

func (a streamAddr) Network() string {
	return "zmux"
}

func (a streamAddr) String() string {
	if a.streamIDSet {
		return fmt.Sprintf("%s/stream/%d", a.endpoint, a.streamID)
	}
	return fmt.Sprintf("%s/stream/pending", a.endpoint)
}

func (s *Stream) LocalAddr() net.Addr {
	return s.streamAddr(streamAddrEndpointLocal)
}

func (s *Stream) RemoteAddr() net.Addr {
	return s.streamAddr(streamAddrEndpointRemote)
}

type streamAddrEndpoint uint8

const (
	streamAddrEndpointRemote streamAddrEndpoint = iota
	streamAddrEndpointLocal
)

func (e streamAddrEndpoint) endpointName() string {
	if e == streamAddrEndpointLocal {
		return "local"
	}
	return "remote"
}

func (e streamAddrEndpoint) providerAddr(provider streamAddrProvider) net.Addr {
	if provider == nil {
		return nil
	}
	if e == streamAddrEndpointLocal {
		return provider.LocalAddr()
	}
	return provider.RemoteAddr()
}

func (s *Stream) streamAddr(endpoint streamAddrEndpoint) net.Addr {
	var (
		provider streamAddrProvider
		streamID uint64
		idSet    bool
	)
	if s != nil && s.conn != nil {
		s.conn.mu.Lock()
		streamID = s.id
		idSet = s.idSet
		if conn := s.conn.io.conn; conn != nil {
			provider, _ = conn.(streamAddrProvider)
		}
		s.conn.mu.Unlock()
		if provider != nil {
			if addr := endpoint.providerAddr(provider); addr != nil {
				return addr
			}
		}
	}

	if s == nil {
		return streamAddr{endpoint: endpoint.endpointName()}
	}
	return streamAddr{
		endpoint:    endpoint.endpointName(),
		streamID:    streamID,
		streamIDSet: idSet,
	}
}

type SendStream struct {
	stream *Stream
}

func (s *SendStream) StreamID() uint64 {
	if s == nil || s.stream == nil {
		return 0
	}
	return s.stream.StreamID()
}

func (s *SendStream) ID() uint64 {
	return s.StreamID()
}

func (s *SendStream) OpenInfo() []byte {
	if s == nil || s.stream == nil {
		return nil
	}
	return s.stream.OpenInfo()
}

func (s *SendStream) LocalAddr() net.Addr {
	if s == nil || s.stream == nil {
		return streamAddr{endpoint: "local"}
	}
	return s.stream.LocalAddr()
}

func (s *SendStream) RemoteAddr() net.Addr {
	if s == nil || s.stream == nil {
		return streamAddr{endpoint: "remote"}
	}
	return s.stream.RemoteAddr()
}

func (s *SendStream) Metadata() StreamMetadata {
	if s == nil || s.stream == nil {
		return StreamMetadata{}
	}
	return s.stream.Metadata()
}

func (s *SendStream) UpdateMetadata(update MetadataUpdate) error {
	if s == nil || s.stream == nil {
		return ErrSessionClosed
	}
	return s.stream.UpdateMetadata(update)
}

func (s *SendStream) Write(p []byte) (int, error) {
	if s == nil || s.stream == nil {
		return 0, ErrSessionClosed
	}
	return s.stream.Write(p)
}

func (s *SendStream) WriteFinal(p []byte) (int, error) {
	if s == nil || s.stream == nil {
		return 0, ErrSessionClosed
	}
	return s.stream.WriteFinal(p)
}

func (s *SendStream) WritevFinal(parts ...[]byte) (int, error) {
	if s == nil || s.stream == nil {
		return 0, ErrSessionClosed
	}
	return s.stream.WritevFinal(parts...)
}

func (s *SendStream) CloseWrite() error {
	if s == nil || s.stream == nil {
		return ErrSessionClosed
	}
	return s.stream.CloseWrite()
}

func (s *SendStream) Reset(code uint64) error {
	if s == nil || s.stream == nil {
		return ErrSessionClosed
	}
	return s.stream.Reset(code)
}

func (s *SendStream) CancelWrite() error {
	if s == nil || s.stream == nil {
		return ErrSessionClosed
	}
	return s.stream.CancelWrite()
}

func (s *SendStream) CancelWriteWithCode(code uint64) error {
	if s == nil || s.stream == nil {
		return ErrSessionClosed
	}
	return s.stream.CancelWriteWithCode(code)
}

func (s *SendStream) ResetWrite() error {
	if s == nil || s.stream == nil {
		return ErrSessionClosed
	}
	return s.stream.ResetWrite()
}

func (s *SendStream) ResetWriteWithCode(code uint64) error {
	if s == nil || s.stream == nil {
		return ErrSessionClosed
	}
	return s.stream.ResetWriteWithCode(code)
}

func (s *SendStream) ResetWithReason(code uint64, reason string) error {
	if s == nil || s.stream == nil {
		return ErrSessionClosed
	}
	return s.stream.ResetWithReason(code, reason)
}

func (s *SendStream) Abort() error {
	if s == nil || s.stream == nil {
		return ErrSessionClosed
	}
	return s.stream.Abort()
}

func (s *SendStream) AbortWithError(err error) error {
	if s == nil || s.stream == nil {
		return ErrSessionClosed
	}
	return s.stream.AbortWithError(err)
}

func (s *SendStream) AbortWithErrorCode(code uint64, reason string) error {
	if s == nil || s.stream == nil {
		return ErrSessionClosed
	}
	return s.stream.AbortWithErrorCode(code, reason)
}

func (s *SendStream) CloseWithError(err error) error {
	if s == nil || s.stream == nil {
		return ErrSessionClosed
	}
	return s.stream.CloseWithError(err)
}

func (s *SendStream) CloseWithErrorCode(code uint64, reason string) error {
	if s == nil || s.stream == nil {
		return ErrSessionClosed
	}
	return s.stream.CloseWithErrorCode(code, reason)
}

func (s *SendStream) Close() error {
	if s == nil || s.stream == nil {
		return ErrSessionClosed
	}
	return s.stream.Close()
}

func (s *SendStream) SetDeadline(t time.Time) error {
	if s == nil || s.stream == nil {
		return ErrSessionClosed
	}
	return s.stream.SetDeadline(t)
}

func (s *SendStream) SetWriteDeadline(t time.Time) error {
	if s == nil || s.stream == nil {
		return ErrSessionClosed
	}
	return s.stream.SetWriteDeadline(t)
}

type RecvStream struct {
	stream *Stream
}

func (s *RecvStream) StreamID() uint64 {
	if s == nil || s.stream == nil {
		return 0
	}
	return s.stream.StreamID()
}

func (s *RecvStream) ID() uint64 {
	return s.StreamID()
}

func (s *RecvStream) OpenInfo() []byte {
	if s == nil || s.stream == nil {
		return nil
	}
	return s.stream.OpenInfo()
}

func (s *RecvStream) LocalAddr() net.Addr {
	if s == nil || s.stream == nil {
		return streamAddr{endpoint: "local"}
	}
	return s.stream.LocalAddr()
}

func (s *RecvStream) RemoteAddr() net.Addr {
	if s == nil || s.stream == nil {
		return streamAddr{endpoint: "remote"}
	}
	return s.stream.RemoteAddr()
}

func (s *RecvStream) Metadata() StreamMetadata {
	if s == nil || s.stream == nil {
		return StreamMetadata{}
	}
	return s.stream.Metadata()
}

func (s *RecvStream) Read(p []byte) (int, error) {
	if s == nil || s.stream == nil {
		return 0, ErrSessionClosed
	}
	return s.stream.Read(p)
}

func (s *RecvStream) CloseRead() error {
	if s == nil || s.stream == nil {
		return ErrSessionClosed
	}
	return s.stream.CloseRead()
}

func (s *RecvStream) CancelRead() error {
	if s == nil || s.stream == nil {
		return ErrSessionClosed
	}
	return s.stream.CancelRead()
}

func (s *RecvStream) CloseReadWithCode(code uint64) error {
	if s == nil || s.stream == nil {
		return ErrSessionClosed
	}
	return s.stream.CloseReadWithCode(code)
}

func (s *RecvStream) CancelReadWithCode(code uint64) error {
	if s == nil || s.stream == nil {
		return ErrSessionClosed
	}
	return s.stream.CancelReadWithCode(code)
}

func (s *RecvStream) Abort() error {
	if s == nil || s.stream == nil {
		return ErrSessionClosed
	}
	return s.stream.Abort()
}

func (s *RecvStream) AbortWithError(err error) error {
	if s == nil || s.stream == nil {
		return ErrSessionClosed
	}
	return s.stream.AbortWithError(err)
}

func (s *RecvStream) AbortWithErrorCode(code uint64, reason string) error {
	if s == nil || s.stream == nil {
		return ErrSessionClosed
	}
	return s.stream.AbortWithErrorCode(code, reason)
}

func (s *RecvStream) CloseWithError(err error) error {
	if s == nil || s.stream == nil {
		return ErrSessionClosed
	}
	return s.stream.CloseWithError(err)
}

func (s *RecvStream) CloseWithErrorCode(code uint64, reason string) error {
	if s == nil || s.stream == nil {
		return ErrSessionClosed
	}
	return s.stream.CloseWithErrorCode(code, reason)
}

func (s *RecvStream) Close() error {
	if s == nil || s.stream == nil {
		return ErrSessionClosed
	}
	return s.stream.Close()
}

func (s *RecvStream) SetDeadline(t time.Time) error {
	if s == nil || s.stream == nil {
		return ErrSessionClosed
	}
	return s.stream.SetDeadline(t)
}

func (s *RecvStream) SetReadDeadline(t time.Time) error {
	if s == nil || s.stream == nil {
		return ErrSessionClosed
	}
	return s.stream.SetReadDeadline(t)
}

const (
	readBufShrinkMinCap  = 256 << 10
	readBufShrinkMaxTail = 64 << 10
)

func shouldTightenReadBufAfterConsume(buf []byte) bool {
	if len(buf) == 0 {
		return true
	}
	if cap(buf) < readBufShrinkMinCap {
		return false
	}
	if len(buf) > readBufShrinkMaxTail {
		return false
	}
	return len(buf) <= cap(buf)/4
}

func readChunkRetainedBytes(data, backing []byte) uint64 {
	switch {
	case len(backing) > 0:
		return uint64(cap(backing))
	case len(data) > 0:
		return uint64(cap(data))
	default:
		return 0
	}
}

func readChunkOverheadBytes(retained uint64, dataLen int) uint64 {
	if retained <= uint64(dataLen) {
		return 0
	}
	return retained - uint64(dataLen)
}

func (c *Conn) replaceReadChunkOverheadLocked(chunk *streamReadChunk, overhead uint64) {
	if c == nil || chunk == nil || chunk.overheadBytes == overhead {
		if chunk != nil {
			chunk.overheadBytes = overhead
		}
		return
	}
	c.ingress.readBufferOverhead = csub(c.ingress.readBufferOverhead, chunk.overheadBytes)
	c.ingress.readBufferOverhead = saturatingAdd(c.ingress.readBufferOverhead, overhead)
	chunk.overheadBytes = overhead
}

func (c *Conn) releaseReadChunkLocked(chunk *streamReadChunk) {
	if chunk == nil {
		return
	}
	if c != nil {
		c.replaceReadChunkOverheadLocked(chunk, 0)
	}
	releaseReadFrameBuffer(chunk.backing, chunk.handle)
	releaseStreamReadChunk(chunk)
}

func (s *Stream) syncReadBufLocked() {
	if s == nil {
		return
	}
	if s.readHead == nil {
		if len(s.readBuf) == 0 {
			s.readBuf = nil
		}
		return
	}
	s.readBuf = s.readHead.data
}

func (s *Stream) ensureReadQueueHeadLocked(c *Conn) {
	if s == nil || s.readHead != nil || len(s.readBuf) == 0 {
		return
	}
	chunk := acquireStreamReadChunk()
	chunk.data = s.readBuf
	chunk.retainedBytes = readChunkRetainedBytes(s.readBuf, nil)
	chunk.overheadBytes = readChunkOverheadBytes(chunk.retainedBytes, len(chunk.data))
	if c != nil {
		c.ingress.readBufferOverhead = saturatingAdd(c.ingress.readBufferOverhead, chunk.overheadBytes)
	}
	s.readHead = chunk
	s.readTail = chunk
	s.syncReadBufLocked()
}

func (s *Stream) appendReadChunkLocked(c *Conn, data, backing []byte, handle *wire.FrameReadBufferHandle) {
	if s == nil || len(data) == 0 {
		releaseReadFrameBuffer(backing, handle)
		return
	}
	s.ensureReadQueueHeadLocked(c)

	chunk := acquireStreamReadChunk()
	chunk.data = data
	chunk.backing = backing
	chunk.handle = handle
	chunk.retainedBytes = readChunkRetainedBytes(data, backing)
	chunk.overheadBytes = readChunkOverheadBytes(chunk.retainedBytes, len(data))
	if c != nil {
		c.ingress.readBufferOverhead = saturatingAdd(c.ingress.readBufferOverhead, chunk.overheadBytes)
	}

	if s.readTail == nil {
		s.readHead = chunk
		s.readTail = chunk
		s.syncReadBufLocked()
		return
	}
	s.readTail.next = chunk
	s.readTail = chunk
}

func (c *Conn) clearReadChunksLocked(stream *Stream) {
	if stream == nil {
		return
	}
	for chunk := stream.readHead; chunk != nil; {
		next := chunk.next
		c.releaseReadChunkLocked(chunk)
		chunk = next
	}
	stream.readHead = nil
	stream.readTail = nil
	stream.readBuf = nil
}

func (s *Stream) bufferedReadLenLocked() int {
	if s == nil {
		return 0
	}
	if s.readHead == nil {
		return len(s.readBuf)
	}
	total := 0
	for chunk := s.readHead; chunk != nil; chunk = chunk.next {
		total += len(chunk.data)
	}
	return total
}

func (s *Stream) consumeReadChunkLocked(c *Conn, n int) {
	if s == nil || n <= 0 {
		return
	}
	s.ensureReadQueueHeadLocked(c)
	if s.readHead == nil {
		return
	}
	chunk := s.readHead
	if n >= len(chunk.data) {
		s.readHead = chunk.next
		if s.readHead == nil {
			s.readTail = nil
		}
		s.readBuf = nil
		c.releaseReadChunkLocked(chunk)
		s.syncReadBufLocked()
		return
	}

	if shouldTightenReadBufAfterConsume(chunk.data[n:]) {
		chunk.data = clonePayloadBytes(chunk.data[n:])
		chunk.backing = nil
		chunk.handle = nil
		chunk.retainedBytes = uint64(len(chunk.data))
		c.replaceReadChunkOverheadLocked(chunk, 0)
		s.syncReadBufLocked()
		return
	}

	chunk.data = chunk.data[n:]
	c.replaceReadChunkOverheadLocked(chunk, readChunkOverheadBytes(chunk.retainedBytes, len(chunk.data)))
	s.syncReadBufLocked()
}

func (s *Stream) Read(p []byte) (int, error) {
	if s == nil || s.conn == nil {
		return 0, ErrSessionClosed
	}
	if len(p) == 0 {
		return 0, nil
	}
	for {
		s.conn.mu.Lock()
		s.ensureReadQueueHeadLocked(s.conn)
		if len(s.readBuf) > 0 {
			n := copy(p, s.readBuf)
			s.consumeReadChunkLocked(s.conn, n)
			s.conn.consumeReceiveLocked(s, uint64(n))
			now := time.Now()
			s.conn.noteStreamProgressLocked(now)
			s.conn.noteAppProgressLocked(now)
			s.conn.maybeFinalizePeerActiveLocked(s)
			s.conn.mu.Unlock()
			return n, nil
		}
		if err := s.readErrLocked(); err != nil {
			s.conn.maybeFinalizePeerActiveLocked(s)
			s.conn.mu.Unlock()
			return 0, s.readSurfaceErrLocked(err)
		}
		notifyCh, deadline := s.readWaitSnapshotLocked()
		s.conn.mu.Unlock()

		if err := s.waitWithDeadline(notifyCh, deadline, OperationRead); err != nil {
			return 0, err
		}
	}
}

func (s *Stream) Write(p []byte) (int, error) {
	if s == nil || s.conn == nil {
		return 0, ErrSessionClosed
	}
	if len(p) == 0 {
		return 0, nil
	}

	written := 0
	for written < len(p) {
		progress, stop, err := s.writeBurst(p[written:])
		written += progress
		if err != nil {
			return written, err
		}
		if stop {
			break
		}
	}
	return written, nil
}

func (s *Stream) WriteFinal(p []byte) (int, error) {
	return s.writevFinal([][]byte{p})
}

func (s *Stream) WritevFinal(parts ...[]byte) (int, error) {
	return s.writevFinal(parts)
}

func (s *Stream) writevFinal(parts [][]byte) (int, error) {
	if s == nil || s.conn == nil {
		return 0, ErrSessionClosed
	}

	total := totalPartLen(parts)
	if total == 0 {
		return 0, s.CloseWrite()
	}

	written := 0
	partIdx := 0
	partOff := 0
	for written < total {
		progress, finalState, stop, err := s.writeFinalBurst(parts, partIdx, partOff, total-written)
		written += progress
		partIdx, partOff = advanceParts(parts, partIdx, partOff, progress)
		if err != nil {
			return written, err
		}
		if finalState.finalized() {
			return written, nil
		}
		if stop {
			break
		}
	}
	return written, nil
}

func (s *Stream) SetDeadline(t time.Time) error {
	if err := s.SetReadDeadline(t); err != nil {
		return err
	}
	return s.SetWriteDeadline(t)
}

func (s *Stream) SetReadDeadline(t time.Time) error {
	return s.setWaitDeadline(t, OperationRead)
}

func (s *Stream) SetWriteDeadline(t time.Time) error {
	return s.setWaitDeadline(t, OperationWrite)
}

type streamWaitState struct {
	readDeadline  time.Time
	writeDeadline time.Time
	readWaiters   uint32
	writeWaiters  uint32
}

func (s *Stream) ensureReadNotifyLocked() chan struct{} {
	if s == nil {
		return nil
	}
	return ensureNotifyChan(&s.readNotify)
}

func (s *Stream) ensureWriteNotifyLocked() chan struct{} {
	if s == nil {
		return nil
	}
	return ensureNotifyChan(&s.writeNotify)
}

func (s *Stream) readNotifyChan() <-chan struct{} {
	if s == nil || s.conn == nil {
		return nil
	}
	s.conn.mu.Lock()
	ch := s.ensureReadNotifyLocked()
	s.conn.mu.Unlock()
	return ch
}

func (s *Stream) writeNotifyChan() <-chan struct{} {
	if s == nil || s.conn == nil {
		return nil
	}
	s.conn.mu.Lock()
	ch := s.ensureWriteNotifyLocked()
	s.conn.mu.Unlock()
	return ch
}

func (s *Stream) readWaitSnapshotLocked() (<-chan struct{}, time.Time) {
	if s == nil {
		return nil, time.Time{}
	}
	deadline := time.Time{}
	if ws := s.waitStateLocked(); ws != nil {
		deadline = ws.readDeadline
	}
	return s.ensureReadNotifyLocked(), deadline
}

func (s *Stream) writeWaitSnapshotLocked() (<-chan struct{}, time.Time) {
	if s == nil {
		return nil, time.Time{}
	}
	deadline := time.Time{}
	if ws := s.waitStateLocked(); ws != nil {
		deadline = ws.writeDeadline
	}
	return s.ensureWriteNotifyLocked(), deadline
}

type streamNotifyMask uint8

const (
	streamNotifyRead streamNotifyMask = 1 << iota
	streamNotifyWrite
	streamNotifyBoth = streamNotifyRead | streamNotifyWrite
)

func (m streamNotifyMask) includesRead() bool {
	return m&streamNotifyRead != 0
}

func (m streamNotifyMask) includesWrite() bool {
	return m&streamNotifyWrite != 0
}

func notifyStreamLocked(stream *Stream, mask streamNotifyMask) {
	if stream == nil {
		return
	}
	if mask.includesRead() {
		notify(stream.readNotify)
	}
	if mask.includesWrite() {
		notify(stream.writeNotify)
	}
}

type streamReceiveReleaseMode uint8

const (
	streamReceiveRetain streamReceiveReleaseMode = iota
	streamReceiveClearReadBufOnly
	streamReceiveReleaseBudget
	streamReceiveReleaseAndClearReadBuf
)

type streamReceiveReleaseTraits uint8

const (
	streamReceiveReleaseTraitNone   streamReceiveReleaseTraits = 0
	streamReceiveReleaseTraitBudget streamReceiveReleaseTraits = 1 << iota
	streamReceiveReleaseTraitClearReadBuf
)

func (t streamReceiveReleaseTraits) releaseMode() streamReceiveReleaseMode {
	switch {
	case t&streamReceiveReleaseTraitBudget != 0 && t&streamReceiveReleaseTraitClearReadBuf != 0:
		return streamReceiveReleaseAndClearReadBuf
	case t&streamReceiveReleaseTraitBudget != 0:
		return streamReceiveReleaseBudget
	case t&streamReceiveReleaseTraitClearReadBuf != 0:
		return streamReceiveClearReadBufOnly
	default:
		return streamReceiveRetain
	}
}

func (m streamReceiveReleaseMode) releasesBudget() bool {
	return m == streamReceiveReleaseBudget || m == streamReceiveReleaseAndClearReadBuf
}

func (m streamReceiveReleaseMode) clearsReadBuf() bool {
	return m == streamReceiveClearReadBufOnly || m == streamReceiveReleaseAndClearReadBuf
}

func (c *Conn) clearStreamReceiveBufferStateLocked(stream *Stream, mode streamReceiveReleaseMode) {
	if c == nil || stream == nil {
		return
	}
	stream.recvPending = 0
	stream.recvBuffer = 0
	if mode.clearsReadBuf() {
		c.clearReadChunksLocked(stream)
	} else {
		stream.syncReadBufLocked()
	}
}

func (c *Conn) releaseStreamReceiveStateLocked(stream *Stream, mode streamReceiveReleaseMode) {
	if c == nil || stream == nil {
		return
	}
	c.releaseReceiveLocked(stream, stream.recvBuffer)
	c.clearStreamReceiveBufferStateLocked(stream, mode)
}

func (c *Conn) applyReceiveReleasePlanLocked(stream *Stream, mode streamReceiveReleaseMode) {
	if c == nil || stream == nil {
		return
	}
	if mode.releasesBudget() {
		c.releaseStreamReceiveStateLocked(stream, mode)
		return
	}
	if mode.clearsReadBuf() {
		c.clearReadChunksLocked(stream)
	} else {
		stream.syncReadBufLocked()
	}
}

func (s *Stream) ensureWaitStateLocked() *streamWaitState {
	if s == nil {
		return nil
	}
	if s.waitState == nil {
		s.waitState = &streamWaitState{}
	}
	return s.waitState
}

func (s *Stream) waitStateLocked() *streamWaitState {
	if s == nil {
		return nil
	}
	return s.waitState
}

func (s *Stream) loadReadWaiters() uint32 {
	if s == nil || s.conn == nil {
		return 0
	}
	s.conn.mu.Lock()
	ws := s.waitState
	s.conn.mu.Unlock()
	if ws == nil {
		return 0
	}
	return atomic.LoadUint32(&ws.readWaiters)
}

func (s *Stream) loadWriteWaiters() uint32 {
	if s == nil || s.conn == nil {
		return 0
	}
	s.conn.mu.Lock()
	ws := s.waitState
	s.conn.mu.Unlock()
	if ws == nil {
		return 0
	}
	return atomic.LoadUint32(&ws.writeWaiters)
}

func (s *Stream) setWaitDeadline(t time.Time, op Operation) error {
	if s == nil || s.conn == nil {
		return ErrSessionClosed
	}
	s.conn.mu.Lock()
	ws := s.ensureWaitStateLocked()
	switch op {
	case OperationRead:
		ws.readDeadline = t
		notify(s.ensureReadNotifyLocked())
	case OperationWrite:
		ws.writeDeadline = t
		notify(s.ensureWriteNotifyLocked())
	default:
	}
	s.conn.mu.Unlock()
	return nil
}

func (s *Stream) waitRead(deadline time.Time) error {
	return s.waitWithDeadline(s.readNotifyChan(), deadline, OperationRead)
}

func (s *Stream) waitWrite(deadline time.Time) error {
	return s.waitWriteWithWakeTracked(deadline, nil)
}

func (s *Stream) waitWriteWithWakeTracked(deadline time.Time, wakeCh <-chan struct{}) error {
	start := time.Now()
	err := s.waitWriteWithWake(deadline, wakeCh)
	s.conn.noteBlockedWrite(time.Since(start))
	return err
}

func (s *Stream) waitWithDeadlineAndWakeTracked(notifyCh <-chan struct{}, wakeCh <-chan struct{}, deadline time.Time, op Operation) error {
	start := time.Now()
	err := s.waitWithDeadlineAndWake(notifyCh, wakeCh, deadline, op)
	s.conn.noteBlockedWrite(time.Since(start))
	return err
}

func (s *Stream) waitWriteWithWake(deadline time.Time, wakeCh <-chan struct{}) error {
	return s.waitWithDeadlineAndWake(s.writeNotifyChan(), wakeCh, deadline, OperationWrite)
}

func (s *Stream) waitWithDeadline(notifyCh <-chan struct{}, deadline time.Time, op Operation) error {
	return s.waitWithDeadlineAndWake(notifyCh, nil, deadline, op)
}

func (s *Stream) waitWithDeadlineAndWake(notifyCh <-chan struct{}, wakeCh <-chan struct{}, deadline time.Time, op Operation) error {
	var (
		timer   *time.Timer
		timeout <-chan time.Time
	)
	endWait := s.beginWait(op)
	defer endWait()
	if !deadline.IsZero() {
		delay := time.Until(deadline)
		if delay <= 0 {
			return os.ErrDeadlineExceeded
		}
		timer = time.NewTimer(delay)
		timeout = timer.C
	}
	defer stopTimer(timer)

	select {
	case <-s.conn.lifecycle.closedCh:
		return s.sessionWaitErr(op)
	case <-notifyCh:
		return s.waitReadyErr(op)
	case <-wakeCh:
		return s.waitReadyErr(op)
	case <-timeout:
		return os.ErrDeadlineExceeded
	}
}

func (s *Stream) beginWait(op Operation) func() {
	if s == nil {
		return func() {}
	}
	var ws *streamWaitState
	if s.conn != nil {
		s.conn.mu.Lock()
		ws = s.ensureWaitStateLocked()
		s.conn.mu.Unlock()
	}
	switch op {
	case OperationRead:
		if ws == nil {
			return func() {}
		}
		atomic.AddUint32(&ws.readWaiters, 1)
		return func() {
			atomic.AddUint32(&ws.readWaiters, ^uint32(0))
		}
	case OperationWrite:
		if ws == nil {
			return func() {}
		}
		atomic.AddUint32(&ws.writeWaiters, 1)
		return func() {
			atomic.AddUint32(&ws.writeWaiters, ^uint32(0))
		}
	default:
		return func() {}
	}
}

func (s *Stream) sessionCloseErr() error {
	if s == nil || s.conn == nil {
		return ErrSessionClosed
	}
	if err := s.conn.err(); err != nil {
		s.conn.mu.Lock()
		current := s.conn.lifecycle.sessionState
		s.conn.mu.Unlock()
		return state.VisibleSessionError(current, err, ErrSessionClosed)
	}
	return nil
}

func (s *Stream) sessionWaitErr(op Operation) error {
	if err := s.sessionCloseErr(); err != nil {
		return sessionOperationErr(s.conn, op, err)
	}
	return sessionOperationErr(s.conn, op, ErrSessionClosed)
}

func (s *Stream) waitReadyErr(op Operation) error {
	if err := s.sessionCloseErr(); err != nil {
		return sessionOperationErr(s.conn, op, err)
	}
	return nil
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

func resetTimer(timer *time.Timer, delay time.Duration) *time.Timer {
	if timer == nil {
		return time.NewTimer(delay)
	}
	stopTimer(timer)
	timer.Reset(delay)
	return timer
}

func (s *Stream) CloseRead() error {
	return s.closeReadWithCode(uint64(CodeCancelled))
}

type terminalLocalOpenerDisposition uint8

const terminalLocalOpenerFinished terminalLocalOpenerDisposition = 1

type terminalLocalOpenerResult struct {
	visibility  openerVisibilityMark
	disposition terminalLocalOpenerDisposition
}

func (r terminalLocalOpenerResult) finished() bool {
	return r.disposition == terminalLocalOpenerFinished
}

func (s *Stream) prepareTerminalLocalOpenerLocked(appErr *ApplicationError, policy terminalOpenerPolicy) terminalLocalOpenerResult {
	if s == nil || s.conn == nil || !s.needsLocalOpenerLocked() {
		return terminalLocalOpenerResult{}
	}
	if !s.idSet {
		s.conn.failProvisionalLocked(s, appErr)
		return terminalLocalOpenerResult{disposition: terminalLocalOpenerFinished}
	}
	if policy == terminalOpenerRejectUnopened {
		s.conn.failUnopenedLocalStreamLocked(s, appErr)
		return terminalLocalOpenerResult{disposition: terminalLocalOpenerFinished}
	}
	result := terminalLocalOpenerResult{visibility: openerVisibilityPeerVisible}
	s.markSendCommittedAndMaybeBarrierLocked(result.visibility)
	return result
}

func (s *Stream) CancelRead() error {
	return s.CancelReadWithCode(uint64(CodeCancelled))
}

func (s *Stream) CancelReadWithCode(code uint64) error {
	if s == nil || s.conn == nil {
		return ErrSessionClosed
	}
	s.conn.mu.Lock()
	alreadyStopped := s.readStopSentLocked()
	s.conn.mu.Unlock()
	if alreadyStopped {
		return nil
	}
	return s.CloseReadWithCode(code)
}

func (s *Stream) CloseReadWithCode(code uint64) error {
	return s.closeReadWithCode(code)
}

type terminalSignalKind uint8

const (
	terminalSignalReset terminalSignalKind = iota
	terminalSignalAbort
)

type terminalOpenerPolicy uint8

const (
	terminalOpenerAllow terminalOpenerPolicy = iota
	terminalOpenerRejectUnopened
)

type terminalResetSource uint8

const (
	terminalResetDirect terminalResetSource = iota
	terminalResetFromStopSending
)

type terminalSignalOptions struct {
	openerPolicy terminalOpenerPolicy
	resetSource  terminalResetSource
}

type terminalDataIntent uint8

const (
	terminalDataCloseRead terminalDataIntent = iota
	terminalDataCloseWrite
)

func (i terminalDataIntent) requiresLocalSend() bool {
	return i == terminalDataCloseRead
}

func (i terminalDataIntent) includesPriority() bool {
	return i == terminalDataCloseWrite
}

func (i terminalDataIntent) sendsFIN() bool {
	return i == terminalDataCloseWrite
}

type terminalDataPrepareSpec struct {
	intent terminalDataIntent
	wrap   func(error) error
}

type terminalFrameRollbackKind uint8

const (
	terminalFrameRollbackNone terminalFrameRollbackKind = iota
	terminalFrameRollbackCloseWrite
)

type terminalFramePlanStatus uint8

const terminalFramePlanRetry terminalFramePlanStatus = 1

type terminalFramePlan struct {
	frames           []txFrame
	openerVisibility openerVisibilityMark
	status           terminalFramePlanStatus
}

func (p terminalFramePlan) shouldRetry() bool {
	return p.status == terminalFramePlanRetry
}

type terminalSignalDisposition uint8

const terminalSignalFinished terminalSignalDisposition = 1

type terminalWriteWakePolicy uint8

const (
	terminalWriteWakeSkip terminalWriteWakePolicy = iota
	terminalWriteWakeNotify
)

type terminalSignalPlan struct {
	frameType        FrameType
	payload          []byte
	openerVisibility openerVisibilityMark
	disposition      terminalSignalDisposition
	writeWake        terminalWriteWakePolicy
}

func (p terminalSignalPlan) finished() bool {
	return p.disposition == terminalSignalFinished
}

func (p terminalSignalPlan) shouldNotifyWrite() bool {
	return p.writeWake == terminalWriteWakeNotify
}

type terminalQueueHooks struct {
	rollback              terminalFrameRollbackKind
	clearPeerVisibleOnErr bool
	notifyWrite           bool
}

type terminalQueueExecution struct {
	frames      []txFrame
	opts        queuedWriteOptions
	commit      queuedWriteCommit
	errorCommit queuedWriteCommit
	hooks       terminalQueueHooks
	errWrap     func(error) error
}

func (e terminalQueueExecution) wrapErr(err error) error {
	if err == nil {
		return nil
	}
	if e.errWrap != nil {
		return e.errWrap(err)
	}
	return err
}

func (e terminalQueueExecution) handleLockedPostQueue(s *Stream, err error) {
	if s == nil || s.conn == nil {
		return
	}
	if err != nil {
		switch e.hooks.rollback {
		case terminalFrameRollbackCloseWrite:
			s.clearSendFin()
			if priority := preparedPriorityUpdateFromFrames(e.frames); priority.hasFrame() {
				s.conn.queuePriorityUpdateAsync(priority.streamID, priority.payload, retainedBytesBorrowed)
			}
			if e.opts.openerVisibility.marksPeerVisible() {
				s.clearOpeningBarrierLocked()
			}
		default:
		}
		if e.hooks.clearPeerVisibleOnErr && e.opts.openerVisibility.marksPeerVisible() {
			s.conn.releaseStreamOpenMetadataPrefixLocked(s)
			s.clearOpeningBarrierLocked()
		}
	}
	if e.hooks.notifyWrite {
		notify(s.writeNotify)
	}
}

func (e terminalQueueExecution) queue(s *Stream) error {
	if s == nil || s.conn == nil || len(e.frames) == 0 {
		return nil
	}
	err := s.queueFramesUntilDeadlineAndOptionsOwned(e.frames, e.opts)
	if err != nil || e.hooks.notifyWrite {
		s.conn.mu.Lock()
		e.handleLockedPostQueue(s, err)
		s.conn.mu.Unlock()
	}
	if err != nil {
		if !e.errorCommit.empty() {
			s.commitQueuedWrite(e.errorCommit)
		}
		return e.wrapErr(err)
	}
	s.commitQueuedWrite(e.commit)
	return nil
}

type closeReadPlan struct {
	opener    terminalFramePlan
	stopFrame txFrame
}

func (p closeReadPlan) shouldRetry() bool {
	return p.opener.shouldRetry()
}

type localCloseReadCommitState uint8

const (
	localCloseReadPending localCloseReadCommitState = iota
	localCloseReadCommitted
)

func (s *Stream) commitLocalCloseReadLocked() {
	if s == nil || s.conn == nil {
		return
	}
	s.setRecvStopSent()
	s.conn.releaseStreamReceiveStateLocked(s, streamReceiveReleaseAndClearReadBuf)
	notify(s.readNotify)
	s.conn.maybeFinalizePeerActiveLocked(s)
	s.markLocalReadSignalPending()
}

func (s *Stream) ensureLocalCloseReadCommittedLocked(commitState localCloseReadCommitState) (localCloseReadCommitState, error) {
	if s == nil {
		return localCloseReadPending, ErrSessionClosed
	}
	if commitState == localCloseReadCommitted || s.localReadSignalPendingFlag() {
		return localCloseReadCommitted, nil
	}
	if err := s.localRecvActionErrLocked(state.LocalCloseReadAction(s.localReceive, s.effectiveRecvHalfStateLocked())); err != nil {
		return localCloseReadPending, err
	}
	s.commitLocalCloseReadLocked()
	return localCloseReadCommitted, nil
}

func (p closeReadPlan) queue(s *Stream) error {
	if s == nil || s.conn == nil {
		return ErrSessionClosed
	}
	if p.opener.openerVisibility.marksPeerVisible() {
		if err := p.opener.queueCloseReadOpener(s); err != nil {
			return err
		}
	}
	var stopFrameBuf [1]txFrame
	stopFrames := stopFrameBuf[:1]
	stopFrames[0] = p.stopFrame
	if err := s.queueFramesUntilDeadlineAndOptionsOwned(stopFrames, queuedWriteOptions{
		ownership: frameOwned,
	}); err != nil {
		return s.closeOperationErr(err)
	}
	s.conn.mu.Lock()
	s.clearLocalReadSignalPending()
	s.conn.mu.Unlock()
	return nil
}

func (s *Stream) prepareCloseReadPlanLocked(code uint64) (plan closeReadPlan, err error) {
	if s == nil || s.conn == nil {
		return closeReadPlan{}, ErrSessionClosed
	}
	plan.opener, err = s.prepareTerminalFramePlanLocked(terminalDataPrepareSpec{
		intent: terminalDataCloseRead,
		wrap:   s.closeOperationErr,
	})
	if err != nil {
		return closeReadPlan{}, err
	}
	if plan.shouldRetry() {
		return plan, nil
	}

	payload, payloadErr := buildCodePayload(code, "", 0)
	if payloadErr != nil {
		return closeReadPlan{}, s.closeOperationErr(payloadErr)
	}
	plan.stopFrame = flatTxFrame(Frame{
		Type:     FrameTypeStopSending,
		StreamID: s.id,
		Payload:  payload,
	})
	return plan, nil
}

func (s *Stream) closeReadWithCode(code uint64) error {
	if s == nil || s.conn == nil {
		return ErrSessionClosed
	}
	commitState := localCloseReadPending
	for {
		s.conn.mu.Lock()
		if s.conn.lifecycle.closeErr != nil {
			err := visibleSessionErrLocked(s.conn, s.conn.lifecycle.closeErr)
			s.conn.mu.Unlock()
			return sessionOperationErrLocked(s.conn, OperationClose, err)
		}
		var err error
		commitState, err = s.ensureLocalCloseReadCommittedLocked(commitState)
		if err != nil {
			s.conn.mu.Unlock()
			return s.closeOperationErr(err)
		}

		plan, err := s.prepareCloseReadPlanLocked(code)
		if err != nil {
			return err
		}
		if plan.shouldRetry() {
			continue
		}
		s.conn.mu.Unlock()
		return plan.queue(s)
	}
}

func (s *Stream) CloseWrite() error {
	return s.closeWriteUntil(time.Time{})
}

func (s *Stream) appendCloseWritePriorityFrameLocked(frames []txFrame, visibility openerVisibilityMark) []txFrame {
	if s == nil || s.conn == nil || visibility.marksPeerVisible() {
		return frames
	}
	priority := s.conn.takePendingPriorityUpdateFrameLocked(s.id)
	if !priority.hasFrame() {
		return frames
	}
	return priority.append(frames)
}

func (s *Stream) closeWriteUntil(deadlineOverride time.Time) error {
	if s == nil || s.conn == nil {
		return ErrSessionClosed
	}

	for {
		s.conn.mu.Lock()
		if s.conn.lifecycle.closeErr != nil {
			err := visibleSessionErrLocked(s.conn, s.conn.lifecycle.closeErr)
			s.conn.mu.Unlock()
			return sessionOperationErrLocked(s.conn, OperationClose, err)
		}
		if s.allowsCloseWriteNoOpAfterStopResetLocked() {
			s.conn.mu.Unlock()
			return nil
		}
		if err := s.localSendActionErrLocked(state.LocalCloseWriteAction(s.localSend, s.effectiveSendHalfStateLocked())); err != nil {
			s.conn.mu.Unlock()
			return s.closeOperationErr(err)
		}
		plan, err := s.prepareTerminalFramePlanLocked(terminalDataPrepareSpec{
			intent: terminalDataCloseWrite,
			wrap:   s.closeOperationErr,
		})
		if err != nil {
			return err
		}
		if plan.shouldRetry() {
			continue
		}
		s.conn.mu.Unlock()
		return plan.queueCloseWrite(s, deadlineOverride)
	}
}

func (s *Stream) CancelWrite() error {
	return s.ResetWrite()
}

func (s *Stream) CancelWriteWithCode(code uint64) error {
	return s.resetWriteWithCodeIfNeeded(code)
}

func (s *Stream) ResetWrite() error {
	return s.resetWriteWithCodeIfNeeded(uint64(CodeCancelled))
}

func (s *Stream) ResetWriteWithCode(code uint64) error {
	return s.resetWriteWithCodeIfNeeded(code)
}

func (s *Stream) resetWriteWithCodeIfNeeded(code uint64) error {
	if s == nil || s.conn == nil {
		return ErrSessionClosed
	}
	s.conn.mu.Lock()
	alreadyReset := s.sendResetMatchesCodeLocked(code)
	s.conn.mu.Unlock()
	if alreadyReset {
		return nil
	}
	return s.executeTerminalSignal(terminalSignalReset, code, "", terminalSignalOptions{
		openerPolicy: terminalOpenerAllow,
		resetSource:  terminalResetDirect,
	})
}

func (s *Stream) Reset(code uint64) error {
	return s.executeTerminalSignal(terminalSignalReset, code, "", terminalSignalOptions{
		openerPolicy: terminalOpenerRejectUnopened,
		resetSource:  terminalResetDirect,
	})
}

func (s *Stream) ResetWithReason(code uint64, reason string) error {
	return s.executeTerminalSignal(terminalSignalReset, code, reason, terminalSignalOptions{
		openerPolicy: terminalOpenerRejectUnopened,
		resetSource:  terminalResetDirect,
	})
}

func (s *Stream) resetAfterStopSending(code uint64) error {
	return s.executeTerminalSignal(terminalSignalReset, code, "", terminalSignalOptions{
		openerPolicy: terminalOpenerAllow,
		resetSource:  terminalResetFromStopSending,
	})
}

func (c *Conn) failUnopenedLocalStreamLocked(stream *Stream, appErr *ApplicationError) {
	if c == nil || stream == nil || !stream.isLocalOpenedLocked() || !stream.idSet || stream.isSendCommittedLocked() {
		return
	}
	stream.setAbortedWithSource(appErr, terminalAbortLocal)
	c.finalizeTerminalStreamLocked(stream, transientStreamReleaseOptions{
		send:    true,
		receive: streamReceiveReleaseAndClearReadBuf,
	}, streamNotifyBoth, false)
	c.dropLiveStreamLocked(stream.id)
	c.removeUnseenLocalLocked(stream)
	notify(c.signals.livenessCh)
}

func (c *Conn) abortWithCode(streamID uint64, code ErrorCode) error {
	payload, err := buildCodePayload(uint64(code), "", 0)
	if err != nil {
		return wireError(CodeInternal, "queue ABORT", err)
	}
	return c.queueImmutableFrame(flatTxFrame(Frame{Type: FrameTypeABORT, StreamID: streamID, Payload: payload}))
}

func (c *Conn) abortStreamState(streamID uint64) error {
	return c.abortWithCode(streamID, CodeStreamState)
}

func (s *Stream) Close() error {
	if s == nil || s.conn == nil {
		return ErrSessionClosed
	}
	s.conn.mu.Lock()
	if s.conn.lifecycle.closeErr != nil {
		err := visibleSessionErrLocked(s.conn, s.conn.lifecycle.closeErr)
		s.conn.mu.Unlock()
		return sessionOperationErrLocked(s.conn, OperationClose, err)
	}
	s.conn.mu.Unlock()
	if s.localSend && !state.SendTerminal(s.effectiveSendHalfStateLocked()) {
		if err := s.CloseWrite(); err != nil &&
			!errors.Is(err, ErrStreamNotWritable) &&
			!errors.Is(err, ErrWriteClosed) &&
			!errors.Is(err, ErrReadClosed) {
			return err
		}
	}
	if s.localReceive && !state.RecvTerminal(s.effectiveRecvHalfStateLocked()) && !s.readStopSentLocked() {
		if err := s.CloseRead(); err != nil &&
			!errors.Is(err, ErrStreamNotReadable) &&
			!errors.Is(err, ErrReadClosed) &&
			!errors.Is(err, ErrWriteClosed) {
			return err
		}
	}
	return nil
}

func (s *Stream) CloseWithError(err error) error {
	if err == nil {
		return s.CloseWithErrorCode(uint64(CodeNoError), "")
	}
	var appErr *ApplicationError
	if errors.As(err, &appErr) {
		return s.CloseWithErrorCode(appErr.Code, appErr.Reason)
	}
	return s.CloseWithErrorCode(uint64(CodeInternal), err.Error())
}

func (s *Stream) Abort() error {
	return s.CloseWithErrorCode(uint64(CodeCancelled), "")
}

func (s *Stream) AbortWithError(err error) error {
	return s.CloseWithError(err)
}

func (s *Stream) AbortWithErrorCode(code uint64, reason string) error {
	return s.CloseWithErrorCode(code, reason)
}

func (s *Stream) CloseWithErrorCode(code uint64, reason string) error {
	return s.executeTerminalSignal(terminalSignalAbort, code, reason, terminalSignalOptions{
		openerPolicy: terminalOpenerAllow,
	})
}

func (p terminalFramePlan) queue(s *Stream, opts queuedWriteOptions, commit queuedWriteCommit, rollback terminalFrameRollbackKind) error {
	return terminalQueueExecution{
		frames: p.frames,
		opts:   opts,
		commit: commit,
		hooks: terminalQueueHooks{
			rollback: rollback,
		},
		errWrap: s.closeOperationErr,
	}.queue(s)
}

func (p terminalFramePlan) queueCloseReadOpener(s *Stream) error {
	return p.queue(s, queuedWriteOptions{
		ownership:        frameOwned,
		openerVisibility: p.openerVisibility,
	}, queuedWriteCommit{
		openerVisibility: p.openerVisibility,
	}, terminalFrameRollbackNone)
}

func (p terminalFramePlan) queueCloseWrite(s *Stream, deadlineOverride time.Time) error {
	return p.queue(s, queuedWriteOptions{
		terminalPolicy:   terminalWriteAllow,
		deadlineOverride: deadlineOverride,
		ownership:        frameOwned,
		openerVisibility: p.openerVisibility,
	}, queuedWriteCommit{
		openerVisibility: p.openerVisibility,
		finalize:         true,
	}, terminalFrameRollbackCloseWrite)
}

func (s *Stream) prepareTerminalFramePlanLocked(spec terminalDataPrepareSpec) (plan terminalFramePlan, err error) {
	if s == nil || s.conn == nil {
		return terminalFramePlan{}, ErrSessionClosed
	}
	if spec.intent.requiresLocalSend() && !s.localSend {
		return terminalFramePlan{}, nil
	}

	openerResult, err := s.prepareRetriableLocalOpenerLocked(spec.wrap)
	if err != nil {
		return plan, err
	}
	if openerResult.shouldRetry() {
		plan.openerVisibility = openerResult.visibility
		plan.status = terminalFramePlanRetry
		return plan, nil
	}
	if spec.intent.requiresLocalSend() && !openerResult.visibility.marksPeerVisible() {
		return terminalFramePlan{}, nil
	}

	var frameBuf [2]txFrame
	frames := frameBuf[:0]
	if spec.intent.includesPriority() {
		frames = s.appendCloseWritePriorityFrameLocked(frames, openerResult.visibility)
	}
	traits := dataFrameTraitNone
	if spec.intent.sendsFIN() {
		traits |= dataFrameTraitFIN
	}
	if openerResult.visibility.marksPeerVisible() {
		traits |= dataFrameTraitOpenMetadata
	}
	frames = append(frames, s.dataFrameLocked(nil, traits))
	if err := s.validateOpenedFramesLocked(frames, openerResult.visibility); err != nil {
		s.conn.mu.Unlock()
		return terminalFramePlan{}, spec.wrap(err)
	}
	s.markSendCommittedAndMaybeBarrierLocked(openerResult.visibility)
	if spec.intent.sendsFIN() {
		s.setSendFin()
	}
	plan.frames = frames
	plan.openerVisibility = openerResult.visibility
	return plan, nil
}

func (s *Stream) prepareTerminalSignalPlanLocked(kind terminalSignalKind, code uint64, reason string, appErr *ApplicationError, opts terminalSignalOptions) (plan terminalSignalPlan, err error) {
	if s == nil || s.conn == nil {
		return terminalSignalPlan{}, ErrSessionClosed
	}

	payload, err := buildCodePayload(code, reason, s.conn.config.peer.Settings.MaxControlPayloadBytes)
	if err != nil {
		return terminalSignalPlan{}, err
	}
	plan.payload = payload
	if s.needsLocalOpenerLocked() {
		openerResult := s.prepareTerminalLocalOpenerLocked(appErr, opts.openerPolicy)
		plan.openerVisibility = openerResult.visibility
		if openerResult.finished() {
			plan.disposition = terminalSignalFinished
		}
	}
	if plan.finished() {
		return plan, nil
	}
	plan.frameType, plan.writeWake = s.applyPreparedTerminalSignalLocked(kind, code, appErr, opts)
	return plan, nil
}

func (s *Stream) applyPreparedTerminalSignalLocked(kind terminalSignalKind, code uint64, appErr *ApplicationError, opts terminalSignalOptions) (frameType FrameType, writeWake terminalWriteWakePolicy) {
	if s == nil || s.conn == nil {
		return 0, terminalWriteWakeSkip
	}
	switch kind {
	case terminalSignalReset:
		if opts.resetSource == terminalResetFromStopSending {
			s.setSendResetWithSource(appErr, terminalResetFromStopSending)
		} else {
			s.setSendResetWithSource(appErr, terminalResetDirect)
		}
		s.conn.noteResetReasonLocked(code)
		s.conn.releaseSendLocked(s)
		return FrameTypeRESET, terminalWriteWakeNotify
	case terminalSignalAbort:
		s.setAbortedWithSource(appErr, terminalAbortLocal)
		s.conn.noteAbortReasonLocked(code)
		s.conn.finalizeTerminalStreamLocked(s, transientStreamReleaseOptions{
			send:    true,
			receive: streamReceiveReleaseAndClearReadBuf,
		}, streamNotifyBoth, false)
		return FrameTypeABORT, terminalWriteWakeSkip
	default:
		return 0, terminalWriteWakeSkip
	}
}

func (p terminalSignalPlan) queue(s *Stream) error {
	if s == nil || s.conn == nil {
		return ErrSessionClosed
	}
	if p.finished() {
		s.conn.mu.Unlock()
		return nil
	}
	s.conn.mu.Unlock()
	var frameBuf [1]txFrame
	frames := frameBuf[:1]
	frames[0] = flatTxFrame(Frame{
		Type:     p.frameType,
		StreamID: s.id,
		Payload:  p.payload,
	})
	return terminalQueueExecution{
		frames: frames,
		opts: queuedWriteOptions{
			terminalPolicy:   terminalWriteAllow,
			ownership:        frameOwned,
			openerVisibility: p.openerVisibility,
		},
		commit: queuedWriteCommit{
			openerVisibility: p.openerVisibility,
			finalize:         true,
		},
		errorCommit: queuedWriteCommit{finalize: true},
		hooks: terminalQueueHooks{
			clearPeerVisibleOnErr: true,
			notifyWrite:           p.shouldNotifyWrite(),
		},
		errWrap: s.closeOperationErr,
	}.queue(s)
}

func (s *Stream) executeTerminalSignal(kind terminalSignalKind, code uint64, reason string, opts terminalSignalOptions) error {
	if s == nil || s.conn == nil {
		return ErrSessionClosed
	}
	s.conn.mu.Lock()
	if s.conn.lifecycle.closeErr != nil {
		err := visibleSessionErrLocked(s.conn, s.conn.lifecycle.closeErr)
		s.conn.mu.Unlock()
		return sessionOperationErrLocked(s.conn, OperationClose, err)
	}
	switch kind {
	case terminalSignalReset:
		if err := s.localSendActionErrLocked(state.LocalResetAction(s.localSend, s.effectiveSendHalfStateLocked())); err != nil {
			s.conn.mu.Unlock()
			return s.closeOperationErr(err)
		}
	case terminalSignalAbort:
		if state.LocalAbortActionForStream(s.effectiveSendHalfStateLocked(), s.effectiveRecvHalfStateLocked()) == state.LocalAbortActionNoOp {
			s.conn.mu.Unlock()
			return nil
		}
	}
	appErr := applicationErr(code, reason)
	plan, err := s.prepareTerminalSignalPlanLocked(kind, code, reason, appErr, opts)
	if err != nil {
		s.conn.mu.Unlock()
		return s.closeOperationErr(err)
	}
	return plan.queue(s)
}

var (
	_ net.Conn  = (*Stream)(nil)
	_ net.Conn  = (*JoinedConn)(nil)
	_ io.Reader = (*Stream)(nil)
	_ io.Writer = (*Stream)(nil)
	_ io.Reader = (*RecvStream)(nil)
	_ io.Writer = (*SendStream)(nil)
	_ ReadHalf  = (*Stream)(nil)
	_ WriteHalf = (*Stream)(nil)
	_ ReadHalf  = (*RecvStream)(nil)
	_ WriteHalf = (*SendStream)(nil)
)
