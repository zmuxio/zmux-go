package zmux

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/state"
)

type addrOnlyReadHalf struct {
	local  net.Addr
	remote net.Addr
}

func (h *addrOnlyReadHalf) Read(_ []byte) (int, error)      { return 0, io.EOF }
func (h *addrOnlyReadHalf) CloseRead() error                { return nil }
func (h *addrOnlyReadHalf) SetReadDeadline(time.Time) error { return nil }
func (h *addrOnlyReadHalf) LocalAddr() net.Addr             { return h.local }
func (h *addrOnlyReadHalf) RemoteAddr() net.Addr            { return h.remote }

type addrOnlyWriteHalf struct {
	local  net.Addr
	remote net.Addr
}

func (h *addrOnlyWriteHalf) Write(p []byte) (int, error)      { return len(p), nil }
func (h *addrOnlyWriteHalf) CloseWrite() error                { return nil }
func (h *addrOnlyWriteHalf) SetWriteDeadline(time.Time) error { return nil }
func (h *addrOnlyWriteHalf) LocalAddr() net.Addr              { return h.local }
func (h *addrOnlyWriteHalf) RemoteAddr() net.Addr             { return h.remote }

type blockingCloseReadHalf struct {
	closeStarted chan struct{}
	releaseClose chan struct{}
}

func (h *blockingCloseReadHalf) Read(_ []byte) (int, error)      { return 0, io.EOF }
func (h *blockingCloseReadHalf) SetReadDeadline(time.Time) error { return nil }
func (h *blockingCloseReadHalf) LocalAddr() net.Addr             { return nil }
func (h *blockingCloseReadHalf) RemoteAddr() net.Addr            { return nil }
func (h *blockingCloseReadHalf) CloseRead() error {
	select {
	case h.closeStarted <- struct{}{}:
	default:
	}
	<-h.releaseClose
	return nil
}

type blockingCloseWriteHalf struct {
	closeStarted chan struct{}
	releaseClose chan struct{}
}

func (h *blockingCloseWriteHalf) Write(p []byte) (int, error)      { return len(p), nil }
func (h *blockingCloseWriteHalf) SetWriteDeadline(time.Time) error { return nil }
func (h *blockingCloseWriteHalf) LocalAddr() net.Addr              { return nil }
func (h *blockingCloseWriteHalf) RemoteAddr() net.Addr             { return nil }
func (h *blockingCloseWriteHalf) CloseWrite() error {
	select {
	case h.closeStarted <- struct{}{}:
	default:
	}
	<-h.releaseClose
	return nil
}

func TestJoinedConnImplementsNetConnAtRuntime(t *testing.T) {
	t.Parallel()

	c := JoinConn(nil, nil)
	var v any = c
	conn, ok := v.(net.Conn)
	if !ok {
		t.Fatal("runtime type assertion to net.Conn failed")
	}
	if conn == nil {
		t.Fatal("asserted net.Conn is nil")
	}
}

func TestJoinedConnZeroValueSurface(t *testing.T) {
	t.Parallel()

	var conn JoinedConn
	if conn.ReadHalf() != nil {
		t.Fatal("zero-value ReadHalf should be nil")
	}
	if conn.WriteHalf() != nil {
		t.Fatal("zero-value WriteHalf should be nil")
	}
	if _, err := conn.Read(make([]byte, 1)); !errors.Is(err, ErrStreamNotReadable) {
		t.Fatalf("zero-value Read err = %v, want %v", err, ErrStreamNotReadable)
	}
	if _, err := conn.Write([]byte("x")); !errors.Is(err, ErrStreamNotWritable) {
		t.Fatalf("zero-value Write err = %v, want %v", err, ErrStreamNotWritable)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("zero-value Close err = %v, want nil", err)
	}
}

func TestJoinedConnHalfAccessors(t *testing.T) {
	t.Parallel()

	c, _, stop := newHandlerTestConn(t)
	defer stop()

	c.mu.Lock()
	readStream := c.newPeerStreamLocked(state.FirstPeerStreamID(c.config.negotiated.LocalRole, false))
	writeStream := c.newLocalStreamWithIDLocked(state.FirstLocalStreamID(c.config.negotiated.LocalRole, false), streamArityUni, OpenOptions{}, nil)
	c.registry.streams[readStream.id] = readStream
	c.registry.streams[writeStream.id] = writeStream
	readHalf := &nativeRecvStream{stream: readStream}
	writeHalf := &nativeSendStream{stream: writeStream}
	c.mu.Unlock()

	conn := JoinConn(readHalf, writeHalf)
	if conn.ReadHalf() != readHalf {
		t.Fatal("ReadHalf accessor mismatch")
	}
	if conn.WriteHalf() != writeHalf {
		t.Fatal("WriteHalf accessor mismatch")
	}
}

func TestJoinedConnPauseHandlesCanSwapAndDetachHalves(t *testing.T) {
	t.Parallel()

	c, _, stop := newHandlerTestConn(t)
	defer stop()

	c.mu.Lock()
	readAStream := c.newPeerStreamLocked(state.FirstPeerStreamID(c.config.negotiated.LocalRole, false))
	readBStream := c.newPeerStreamLocked(state.FirstPeerStreamID(c.config.negotiated.LocalRole, false) + 4)
	writeAStream := c.newLocalStreamWithIDLocked(state.FirstLocalStreamID(c.config.negotiated.LocalRole, false), streamArityUni, OpenOptions{}, nil)
	writeBStream := c.newLocalStreamWithIDLocked(state.FirstLocalStreamID(c.config.negotiated.LocalRole, false)+4, streamArityUni, OpenOptions{}, nil)
	c.registry.streams[readAStream.id] = readAStream
	c.registry.streams[readBStream.id] = readBStream
	c.registry.streams[writeAStream.id] = writeAStream
	c.registry.streams[writeBStream.id] = writeBStream
	readA := &nativeRecvStream{stream: readAStream}
	readB := &nativeRecvStream{stream: readBStream}
	writeA := &nativeSendStream{stream: writeAStream}
	writeB := &nativeSendStream{stream: writeBStream}
	c.mu.Unlock()

	conn := JoinConn(readA, writeA)

	readPause, err := conn.PauseRead(context.Background())
	if err != nil {
		t.Fatalf("PauseRead: %v", err)
	}
	if readPause.Current() != readA {
		t.Fatal("PauseRead current mismatch")
	}
	if prev := readPause.Set(readB); prev != readA {
		t.Fatal("PauseRead Set did not return previous read half")
	}
	if err := readPause.Resume(); err != nil {
		t.Fatalf("PauseRead Resume: %v", err)
	}

	writePause, err := conn.PauseWrite(context.Background())
	if err != nil {
		t.Fatalf("PauseWrite: %v", err)
	}
	if writePause.Current() != writeA {
		t.Fatal("PauseWrite current mismatch")
	}
	if prev := writePause.Set(writeB); prev != writeA {
		t.Fatal("PauseWrite Set did not return previous write half")
	}
	if err := writePause.Resume(); err != nil {
		t.Fatalf("PauseWrite Resume: %v", err)
	}

	if conn.ReadHalf() != readB || conn.WriteHalf() != writeB {
		t.Fatal("paused replacement did not update attached halves")
	}

	readPause, err = conn.PauseRead(context.Background())
	if err != nil {
		t.Fatalf("PauseRead detach: %v", err)
	}
	if prev := readPause.Set(nil); prev != readB {
		t.Fatal("PauseRead Set(nil) did not detach current read half")
	}
	if err := readPause.Resume(); err != nil {
		t.Fatalf("PauseRead Resume(nil): %v", err)
	}

	writePause, err = conn.PauseWrite(context.Background())
	if err != nil {
		t.Fatalf("PauseWrite detach: %v", err)
	}
	if prev := writePause.Set(nil); prev != writeB {
		t.Fatal("PauseWrite Set(nil) did not detach current write half")
	}
	if err := writePause.Resume(); err != nil {
		t.Fatalf("PauseWrite Resume(nil): %v", err)
	}

	if conn.ReadHalf() != nil || conn.WriteHalf() != nil {
		t.Fatal("detach did not clear attached halves")
	}
}

func TestJoinedConnBridgesUniStreamsAsNetConn(t *testing.T) {
	t.Parallel()

	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	clientSend, err := client.OpenUniStream(ctx)
	if err != nil {
		t.Fatalf("client OpenUniStream: %v", err)
	}
	serverSend, err := server.OpenUniStream(ctx)
	if err != nil {
		t.Fatalf("server OpenUniStream: %v", err)
	}

	if _, err := clientSend.Write([]byte("hi")); err != nil {
		t.Fatalf("client initial Write: %v", err)
	}
	if _, err := serverSend.Write([]byte("yo")); err != nil {
		t.Fatalf("server initial Write: %v", err)
	}

	clientRecv, err := client.AcceptUniStream(ctx)
	if err != nil {
		t.Fatalf("client AcceptUniStream: %v", err)
	}
	serverRecv, err := server.AcceptUniStream(ctx)
	if err != nil {
		t.Fatalf("server AcceptUniStream: %v", err)
	}

	clientConn := JoinConn(clientRecv, clientSend)
	serverConn := JoinConn(serverRecv, serverSend)

	var cnet net.Conn = clientConn
	var snet net.Conn = serverConn

	buf := make([]byte, 8)
	n, err := snet.Read(buf)
	if err != nil {
		t.Fatalf("server Read: %v", err)
	}
	if got := string(buf[:n]); got != "hi" {
		t.Fatalf("server Read payload = %q, want %q", got, "hi")
	}

	n, err = cnet.Read(buf)
	if err != nil {
		t.Fatalf("client Read: %v", err)
	}
	if got := string(buf[:n]); got != "yo" {
		t.Fatalf("client Read payload = %q, want %q", got, "yo")
	}

	if n, err := cnet.Write([]byte("ping")); err != nil || n != 4 {
		t.Fatalf("client Write = (%d, %v), want (4, nil)", n, err)
	}
	n, err = snet.Read(buf)
	if err != nil {
		t.Fatalf("server second Read: %v", err)
	}
	if got := string(buf[:n]); got != "ping" {
		t.Fatalf("server second Read payload = %q, want %q", got, "ping")
	}

	if n, err := snet.Write([]byte("pong")); err != nil || n != 4 {
		t.Fatalf("server Write = (%d, %v), want (4, nil)", n, err)
	}
	n, err = cnet.Read(buf)
	if err != nil {
		t.Fatalf("client second Read: %v", err)
	}
	if got := string(buf[:n]); got != "pong" {
		t.Fatalf("client second Read payload = %q, want %q", got, "pong")
	}
}

func TestJoinedConnNilReadHalf(t *testing.T) {
	t.Parallel()

	c, _, stop := newHandlerTestConn(t)
	defer stop()

	c.mu.Lock()
	writeStream := c.newLocalStreamWithIDLocked(state.FirstLocalStreamID(c.config.negotiated.LocalRole, false), streamArityUni, OpenOptions{}, nil)
	c.registry.streams[writeStream.id] = writeStream
	writeHalf := &nativeSendStream{stream: writeStream}
	c.mu.Unlock()

	conn := JoinConn(nil, writeHalf)
	if _, err := conn.Read(make([]byte, 1)); !errors.Is(err, ErrStreamNotReadable) {
		t.Fatalf("Read err = %v, want %v", err, ErrStreamNotReadable)
	}
	if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("SetReadDeadline err = %v, want nil", err)
	}
	if err := conn.CloseRead(); err != nil {
		t.Fatalf("CloseRead err = %v, want nil", err)
	}
}

func TestJoinedConnNilWriteHalf(t *testing.T) {
	t.Parallel()

	c, _, stop := newHandlerTestConn(t)
	defer stop()

	c.mu.Lock()
	readStream := c.newPeerStreamLocked(state.FirstPeerStreamID(c.config.negotiated.LocalRole, false))
	c.registry.streams[readStream.id] = readStream
	readHalf := &nativeRecvStream{stream: readStream}
	c.mu.Unlock()

	conn := JoinConn(readHalf, nil)
	if _, err := conn.Write([]byte("x")); !errors.Is(err, ErrStreamNotWritable) {
		t.Fatalf("Write err = %v, want %v", err, ErrStreamNotWritable)
	}
	if err := conn.SetWriteDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("SetWriteDeadline err = %v, want nil", err)
	}
	if err := conn.CloseWrite(); err != nil {
		t.Fatalf("CloseWrite err = %v, want nil", err)
	}
}

func TestJoinedConnAddrsFallbackToOtherHalfWhenPrimaryHalfReturnsNil(t *testing.T) {
	t.Parallel()

	writeAddrLocal := streamAddr{endpoint: "write-local", streamID: 7, streamIDSet: true}
	writeAddrRemote := streamAddr{endpoint: "write-remote", streamID: 7, streamIDSet: true}
	conn := JoinConn(
		&addrOnlyReadHalf{},
		&addrOnlyWriteHalf{local: writeAddrLocal, remote: writeAddrRemote},
	)

	local := conn.LocalAddr()
	remote := conn.RemoteAddr()

	if local == nil {
		t.Fatal("LocalAddr = nil, want fallback write-half addr")
	}
	if remote == nil {
		t.Fatal("RemoteAddr = nil, want fallback write-half addr")
	}
	if local.String() != writeAddrLocal.String() {
		t.Fatalf("LocalAddr string = %q, want %q", local.String(), writeAddrLocal.String())
	}
	if remote.String() != writeAddrRemote.String() {
		t.Fatalf("RemoteAddr string = %q, want %q", remote.String(), writeAddrRemote.String())
	}
}

func TestJoinedConnAddrsFallbackToSyntheticWhenAttachedHalvesReturnNil(t *testing.T) {
	t.Parallel()

	conn := JoinConn(&addrOnlyReadHalf{}, &addrOnlyWriteHalf{})

	local := conn.LocalAddr()
	remote := conn.RemoteAddr()

	if local == nil || remote == nil {
		t.Fatal("fallback synthetic addrs should be non-nil")
	}
	if local.Network() != "zmux" || local.String() != "local/stream/pending" {
		t.Fatalf("LocalAddr = (%q, %q), want (\"zmux\", \"local/stream/pending\")", local.Network(), local.String())
	}
	if remote.Network() != "zmux" || remote.String() != "remote/stream/pending" {
		t.Fatalf("RemoteAddr = (%q, %q), want (\"zmux\", \"remote/stream/pending\")", remote.Network(), remote.String())
	}
}

func TestJoinedConnCloseClosesPresentHalves(t *testing.T) {
	t.Parallel()

	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	clientSend, err := client.OpenUniStream(ctx)
	if err != nil {
		t.Fatalf("client OpenUniStream: %v", err)
	}
	if _, err := clientSend.Write([]byte("x")); err != nil {
		t.Fatalf("client initial Write: %v", err)
	}
	serverRecv, err := server.AcceptUniStream(ctx)
	if err != nil {
		t.Fatalf("server AcceptUniStream: %v", err)
	}

	conn := JoinConn(serverRecv, nil)
	if err := conn.Close(); err != nil {
		t.Fatalf("Close err = %v, want nil", err)
	}

	awaitStreamWriteState(t, clientSend.stream, testSignalTimeout, func(s *nativeStream) bool {
		return s.localReadStop || s.sendStop != nil || s.sendAbort != nil || s.sendReset != nil || s.sendFinReached()
	}, "timed out waiting for sender to observe JoinedConn CloseRead")

	if _, err := clientSend.Write([]byte("x")); err == nil {
		t.Fatal("expected Write to fail after peer JoinedConn Close")
	}
}

func TestJoinedConnDeadlineViaNetConn(t *testing.T) {
	t.Parallel()

	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	clientSend, err := client.OpenUniStream(ctx)
	if err != nil {
		t.Fatalf("client OpenUniStream: %v", err)
	}
	serverSend, err := server.OpenUniStream(ctx)
	if err != nil {
		t.Fatalf("server OpenUniStream: %v", err)
	}

	if _, err := clientSend.Write([]byte("a")); err != nil {
		t.Fatalf("client initial Write: %v", err)
	}
	if _, err := serverSend.Write([]byte("b")); err != nil {
		t.Fatalf("server initial Write: %v", err)
	}

	clientRecv, err := client.AcceptUniStream(ctx)
	if err != nil {
		t.Fatalf("client AcceptUniStream: %v", err)
	}

	conn := net.Conn(JoinConn(clientRecv, clientSend))

	buf := make([]byte, 1)
	if _, err := conn.Read(buf); err != nil {
		t.Fatalf("initial Read: %v", err)
	}
	if string(buf) != "b" {
		t.Fatalf("initial Read payload = %q, want %q", string(buf), "b")
	}

	if err := conn.SetReadDeadline(time.Now().Add(30 * time.Millisecond)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	if _, err := conn.Read(buf); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("Read err = %v, want deadline exceeded", err)
	}
}

func TestJoinedConnCloseWriteProducesEOFOnPeerRead(t *testing.T) {
	t.Parallel()

	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	clientSend, err := client.OpenUniStream(ctx)
	if err != nil {
		t.Fatalf("client OpenUniStream: %v", err)
	}
	type acceptResult struct {
		stream *nativeRecvStream
		err    error
	}
	acceptCh := make(chan acceptResult, 1)
	go func() {
		s, err := server.AcceptUniStream(ctx)
		acceptCh <- acceptResult{stream: s, err: err}
	}()

	conn := JoinConn(nil, clientSend)
	if err := conn.CloseWrite(); err != nil {
		t.Fatalf("CloseWrite: %v", err)
	}

	accepted := <-acceptCh
	if accepted.err != nil {
		t.Fatalf("server AcceptUniStream: %v", accepted.err)
	}
	serverRecv := accepted.stream

	buf := make([]byte, 1)
	if _, err := serverRecv.Read(buf); !errors.Is(err, io.EOF) {
		t.Fatalf("peer Read err = %v, want EOF", err)
	}
}

func TestJoinedConnPauseReadBlocksUpperReadUntilResume(t *testing.T) {
	t.Parallel()

	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	clientSend, err := client.OpenUniStream(ctx)
	if err != nil {
		t.Fatalf("client OpenUniStream: %v", err)
	}
	serverSend, err := server.OpenUniStream(ctx)
	if err != nil {
		t.Fatalf("server OpenUniStream: %v", err)
	}
	if _, err := clientSend.Write([]byte("a")); err != nil {
		t.Fatalf("client initial Write: %v", err)
	}
	if _, err := serverSend.Write([]byte("b")); err != nil {
		t.Fatalf("server initial Write: %v", err)
	}

	clientRecv, err := client.AcceptUniStream(ctx)
	if err != nil {
		t.Fatalf("client AcceptUniStream: %v", err)
	}
	serverRecv, err := server.AcceptUniStream(ctx)
	if err != nil {
		t.Fatalf("server AcceptUniStream: %v", err)
	}

	clientConn := JoinConn(clientRecv, clientSend)
	serverConn := JoinConn(serverRecv, serverSend)

	buf := make([]byte, 1)
	if _, err := clientConn.Read(buf); err != nil {
		t.Fatalf("initial client Read: %v", err)
	}

	readPause, err := clientConn.PauseRead(context.Background())
	if err != nil {
		t.Fatalf("PauseRead: %v", err)
	}
	if readPause.Current() != clientRecv {
		t.Fatal("PauseRead returned wrong read half")
	}

	readDone := make(chan struct {
		n   int
		err error
	}, 1)
	go func() {
		waitBuf := make([]byte, 1)
		n, err := clientConn.Read(waitBuf)
		readDone <- struct {
			n   int
			err error
		}{n: n, err: err}
	}()

	select {
	case result := <-readDone:
		t.Fatalf("Read completed while paused: %+v", result)
	case <-time.After(50 * time.Millisecond):
	}

	if _, err := serverConn.Write([]byte("x")); err != nil {
		t.Fatalf("server Write: %v", err)
	}

	select {
	case result := <-readDone:
		t.Fatalf("Read completed before resume: %+v", result)
	case <-time.After(50 * time.Millisecond):
	}

	if err := readPause.Resume(); err != nil {
		t.Fatalf("PauseRead Resume: %v", err)
	}

	select {
	case result := <-readDone:
		if result.err != nil || result.n != 1 {
			t.Fatalf("Read result = (%d, %v), want (1, nil)", result.n, result.err)
		}
	case <-time.After(testSignalTimeout):
		t.Fatal("paused Read did not resume")
	}
}

func TestJoinedConnPauseWriteBlocksUpperWriteUntilResume(t *testing.T) {
	t.Parallel()

	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	clientSend, err := client.OpenUniStream(ctx)
	if err != nil {
		t.Fatalf("client OpenUniStream: %v", err)
	}
	serverSend, err := server.OpenUniStream(ctx)
	if err != nil {
		t.Fatalf("server OpenUniStream: %v", err)
	}
	if _, err := clientSend.Write([]byte("a")); err != nil {
		t.Fatalf("client initial Write: %v", err)
	}
	if _, err := serverSend.Write([]byte("b")); err != nil {
		t.Fatalf("server initial Write: %v", err)
	}

	clientRecv, err := client.AcceptUniStream(ctx)
	if err != nil {
		t.Fatalf("client AcceptUniStream: %v", err)
	}
	serverRecv, err := server.AcceptUniStream(ctx)
	if err != nil {
		t.Fatalf("server AcceptUniStream: %v", err)
	}

	clientConn := JoinConn(clientRecv, clientSend)
	serverConn := JoinConn(serverRecv, serverSend)

	buf := make([]byte, 1)
	if _, err := serverConn.Read(buf); err != nil {
		t.Fatalf("initial server Read: %v", err)
	}

	writePause, err := clientConn.PauseWrite(context.Background())
	if err != nil {
		t.Fatalf("PauseWrite: %v", err)
	}
	if writePause.Current() != clientSend {
		t.Fatal("PauseWrite returned wrong write half")
	}

	writeDone := make(chan struct {
		n   int
		err error
	}, 1)
	go func() {
		n, err := clientConn.Write([]byte("x"))
		writeDone <- struct {
			n   int
			err error
		}{n: n, err: err}
	}()

	select {
	case result := <-writeDone:
		t.Fatalf("Write completed while paused: %+v", result)
	case <-time.After(50 * time.Millisecond):
	}

	if err := writePause.Resume(); err != nil {
		t.Fatalf("PauseWrite Resume: %v", err)
	}

	select {
	case result := <-writeDone:
		if result.err != nil || result.n != 1 {
			t.Fatalf("Write result = (%d, %v), want (1, nil)", result.n, result.err)
		}
	case <-time.After(testSignalTimeout):
		t.Fatal("paused Write did not resume")
	}

	if _, err := serverConn.Read(buf); err != nil {
		t.Fatalf("server Read after resumed Write: %v", err)
	}
	if string(buf) != "x" {
		t.Fatalf("server Read payload = %q, want %q", string(buf), "x")
	}
}

func TestJoinedConnPauseReadWaitsForInflightReadToDrain(t *testing.T) {
	t.Parallel()

	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	serverSend, err := server.OpenUniStream(ctx)
	if err != nil {
		t.Fatalf("server OpenUniStream: %v", err)
	}
	if _, err := serverSend.Write([]byte("a")); err != nil {
		t.Fatalf("server initial Write: %v", err)
	}

	clientRecv, err := client.AcceptUniStream(ctx)
	if err != nil {
		t.Fatalf("client AcceptUniStream: %v", err)
	}
	clientConn := JoinConn(clientRecv, nil)

	buf := make([]byte, 1)
	if _, err := clientConn.Read(buf); err != nil {
		t.Fatalf("initial Read: %v", err)
	}

	readDone := make(chan struct {
		n   int
		err error
	}, 1)
	go func() {
		waitBuf := make([]byte, 1)
		n, err := clientConn.Read(waitBuf)
		readDone <- struct {
			n   int
			err error
		}{n: n, err: err}
	}()
	awaitStreamReadWaiter(t, clientRecv.stream, testSignalTimeout, "timed out waiting for in-flight JoinedConn Read")

	pauseDone := make(chan *PausedReadHalf, 1)
	go func() {
		pause, err := clientConn.PauseRead(context.Background())
		if err != nil {
			t.Errorf("PauseRead: %v", err)
			return
		}
		pauseDone <- pause
	}()

	select {
	case <-pauseDone:
		t.Fatal("PauseRead returned before in-flight Read drained")
	case <-time.After(50 * time.Millisecond):
	}

	if _, err := serverSend.Write([]byte("x")); err != nil {
		t.Fatalf("server Write: %v", err)
	}
	select {
	case result := <-readDone:
		if result.err != nil || result.n != 1 {
			t.Fatalf("in-flight Read result = (%d, %v), want (1, nil)", result.n, result.err)
		}
	case <-time.After(testSignalTimeout):
		t.Fatal("in-flight Read did not finish")
	}

	var pause *PausedReadHalf
	select {
	case pause = <-pauseDone:
	case <-time.After(testSignalTimeout):
		t.Fatal("PauseRead did not return after in-flight Read drained")
	}
	if pause.Current() != clientRecv {
		t.Fatal("PauseRead returned wrong read half after drain")
	}
	if err := pause.Resume(); err != nil {
		t.Fatalf("PauseRead Resume after drain: %v", err)
	}
}

func TestJoinedConnPausedReadStillHonorsDeadline(t *testing.T) {
	t.Parallel()

	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	serverSend, err := server.OpenUniStream(ctx)
	if err != nil {
		t.Fatalf("server OpenUniStream: %v", err)
	}
	if _, err := serverSend.Write([]byte("a")); err != nil {
		t.Fatalf("server initial Write: %v", err)
	}

	clientRecv, err := client.AcceptUniStream(ctx)
	if err != nil {
		t.Fatalf("client AcceptUniStream: %v", err)
	}
	clientConn := JoinConn(clientRecv, nil)

	buf := make([]byte, 1)
	if _, err := clientConn.Read(buf); err != nil {
		t.Fatalf("initial Read: %v", err)
	}

	readPause, err := clientConn.PauseRead(context.Background())
	if err != nil {
		t.Fatalf("PauseRead: %v", err)
	}
	if err := clientConn.SetReadDeadline(time.Now().Add(30 * time.Millisecond)); err != nil {
		t.Fatalf("SetReadDeadline while paused: %v", err)
	}

	errCh := make(chan error, 1)
	go func() {
		_, err := clientConn.Read(make([]byte, 1))
		errCh <- err
	}()

	select {
	case err := <-errCh:
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("paused Read err = %v, want deadline exceeded", err)
		}
	case <-time.After(testSignalTimeout):
		t.Fatal("paused Read did not honor deadline")
	}

	if err := readPause.Resume(); err != nil {
		t.Fatalf("PauseRead Resume after deadline: %v", err)
	}
}

func TestJoinedConnPauseReadContextCanCancel(t *testing.T) {
	t.Parallel()

	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	serverSend, err := server.OpenUniStream(ctx)
	if err != nil {
		t.Fatalf("server OpenUniStream: %v", err)
	}
	if _, err := serverSend.Write([]byte("a")); err != nil {
		t.Fatalf("server initial Write: %v", err)
	}

	clientRecv, err := client.AcceptUniStream(ctx)
	if err != nil {
		t.Fatalf("client AcceptUniStream: %v", err)
	}
	clientConn := JoinConn(clientRecv, nil)

	buf := make([]byte, 1)
	if _, err := clientConn.Read(buf); err != nil {
		t.Fatalf("initial Read: %v", err)
	}

	readDone := make([]byte, 1)
	doneCh := make(chan error, 1)
	go func() {
		_, err := clientConn.Read(readDone)
		doneCh <- err
	}()
	awaitStreamReadWaiter(t, clientRecv.stream, testSignalTimeout, "timed out waiting for in-flight read before PauseRead cancel")

	pauseCtx, pauseCancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer pauseCancel()
	if _, err := clientConn.PauseRead(pauseCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("PauseRead err = %v, want context deadline exceeded", err)
	}

	if _, err := serverSend.Write([]byte("x")); err != nil {
		t.Fatalf("server Write: %v", err)
	}
	select {
	case err := <-doneCh:
		if err != nil {
			t.Fatalf("Read after PauseRead cancel err = %v, want nil", err)
		}
	case <-time.After(testSignalTimeout):
		t.Fatal("Read did not continue after PauseRead cancel")
	}
}

func TestJoinedConnPauseReadBlocksCloseReadUntilDeadline(t *testing.T) {
	t.Parallel()

	readHalf := &blockingCloseReadHalf{
		closeStarted: make(chan struct{}, 1),
		releaseClose: make(chan struct{}),
	}
	conn := JoinConn(readHalf, nil)

	pause, err := conn.PauseRead(context.Background())
	if err != nil {
		t.Fatalf("PauseRead: %v", err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(30 * time.Millisecond)); err != nil {
		t.Fatalf("SetReadDeadline while paused: %v", err)
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- conn.CloseRead()
	}()

	select {
	case err := <-errCh:
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("paused CloseRead err = %v, want deadline exceeded", err)
		}
	case <-time.After(testSignalTimeout):
		t.Fatal("paused CloseRead did not honor deadline")
	}

	select {
	case <-readHalf.closeStarted:
		t.Fatal("CloseRead reached detached read half while paused under deadline")
	default:
	}

	if err := pause.Resume(); err != nil {
		t.Fatalf("PauseRead Resume after CloseRead deadline: %v", err)
	}
}

func TestJoinedConnPauseReadWaitsForInflightCloseReadToDrain(t *testing.T) {
	t.Parallel()

	readHalf := &blockingCloseReadHalf{
		closeStarted: make(chan struct{}, 1),
		releaseClose: make(chan struct{}),
	}
	conn := JoinConn(readHalf, nil)

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- conn.CloseRead()
	}()

	select {
	case <-readHalf.closeStarted:
	case <-time.After(testSignalTimeout):
		t.Fatal("CloseRead did not reach underlying read half")
	}

	pauseDone := make(chan struct {
		pause *PausedReadHalf
		err   error
	}, 1)
	go func() {
		pause, err := conn.PauseRead(context.Background())
		pauseDone <- struct {
			pause *PausedReadHalf
			err   error
		}{pause: pause, err: err}
	}()

	select {
	case result := <-pauseDone:
		t.Fatalf("PauseRead returned before in-flight CloseRead drained: %+v", result)
	case <-time.After(50 * time.Millisecond):
	}

	close(readHalf.releaseClose)

	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("CloseRead err = %v, want nil", err)
		}
	case <-time.After(testSignalTimeout):
		t.Fatal("CloseRead did not finish")
	}

	select {
	case result := <-pauseDone:
		if result.err != nil {
			t.Fatalf("PauseRead err = %v, want nil", result.err)
		}
		if result.pause == nil || result.pause.Current() != readHalf {
			t.Fatal("PauseRead did not detach the expected read half after CloseRead drained")
		}
		if err := result.pause.Resume(); err != nil {
			t.Fatalf("PauseRead Resume: %v", err)
		}
	case <-time.After(testSignalTimeout):
		t.Fatal("PauseRead did not resume after in-flight CloseRead drained")
	}
}

func TestJoinedConnPauseWriteWaitsForInflightCloseWriteToDrain(t *testing.T) {
	t.Parallel()

	writeHalf := &blockingCloseWriteHalf{
		closeStarted: make(chan struct{}, 1),
		releaseClose: make(chan struct{}),
	}
	conn := JoinConn(nil, writeHalf)

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- conn.CloseWrite()
	}()

	select {
	case <-writeHalf.closeStarted:
	case <-time.After(testSignalTimeout):
		t.Fatal("CloseWrite did not reach underlying write half")
	}

	pauseDone := make(chan struct {
		pause *PausedWriteHalf
		err   error
	}, 1)
	go func() {
		pause, err := conn.PauseWrite(context.Background())
		pauseDone <- struct {
			pause *PausedWriteHalf
			err   error
		}{pause: pause, err: err}
	}()

	select {
	case result := <-pauseDone:
		t.Fatalf("PauseWrite returned before in-flight CloseWrite drained: %+v", result)
	case <-time.After(50 * time.Millisecond):
	}

	close(writeHalf.releaseClose)

	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("CloseWrite err = %v, want nil", err)
		}
	case <-time.After(testSignalTimeout):
		t.Fatal("CloseWrite did not finish")
	}

	select {
	case result := <-pauseDone:
		if result.err != nil {
			t.Fatalf("PauseWrite err = %v, want nil", result.err)
		}
		if result.pause == nil || result.pause.Current() != writeHalf {
			t.Fatal("PauseWrite did not detach the expected write half after CloseWrite drained")
		}
		if err := result.pause.Resume(); err != nil {
			t.Fatalf("PauseWrite Resume: %v", err)
		}
	case <-time.After(testSignalTimeout):
		t.Fatal("PauseWrite did not resume after in-flight CloseWrite drained")
	}
}

func TestJoinedConnPauseReadBlocksCloseReadUntilResume(t *testing.T) {
	t.Parallel()

	readHalf := &blockingCloseReadHalf{
		closeStarted: make(chan struct{}, 1),
		releaseClose: make(chan struct{}),
	}
	conn := JoinConn(readHalf, nil)

	pause, err := conn.PauseRead(context.Background())
	if err != nil {
		t.Fatalf("PauseRead: %v", err)
	}
	if pause.Current() != readHalf {
		t.Fatal("PauseRead returned wrong detached read half")
	}

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- conn.CloseRead()
	}()

	select {
	case err := <-closeDone:
		t.Fatalf("CloseRead returned while paused: %v", err)
	case <-readHalf.closeStarted:
		t.Fatal("CloseRead reached detached read half while paused")
	case <-time.After(50 * time.Millisecond):
	}

	if err := pause.Resume(); err != nil {
		t.Fatalf("PauseRead Resume: %v", err)
	}

	select {
	case <-readHalf.closeStarted:
	case <-time.After(testSignalTimeout):
		t.Fatal("CloseRead did not reach underlying read half after resume")
	}

	close(readHalf.releaseClose)

	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("CloseRead err = %v, want nil", err)
		}
	case <-time.After(testSignalTimeout):
		t.Fatal("CloseRead did not finish after resume")
	}
}

func TestJoinedConnPauseWriteBlocksCloseWriteUntilResume(t *testing.T) {
	t.Parallel()

	writeHalf := &blockingCloseWriteHalf{
		closeStarted: make(chan struct{}, 1),
		releaseClose: make(chan struct{}),
	}
	conn := JoinConn(nil, writeHalf)

	pause, err := conn.PauseWrite(context.Background())
	if err != nil {
		t.Fatalf("PauseWrite: %v", err)
	}
	if pause.Current() != writeHalf {
		t.Fatal("PauseWrite returned wrong detached write half")
	}

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- conn.CloseWrite()
	}()

	select {
	case err := <-closeDone:
		t.Fatalf("CloseWrite returned while paused: %v", err)
	case <-writeHalf.closeStarted:
		t.Fatal("CloseWrite reached detached write half while paused")
	case <-time.After(50 * time.Millisecond):
	}

	if err := pause.Resume(); err != nil {
		t.Fatalf("PauseWrite Resume: %v", err)
	}

	select {
	case <-writeHalf.closeStarted:
	case <-time.After(testSignalTimeout):
		t.Fatal("CloseWrite did not reach underlying write half after resume")
	}

	close(writeHalf.releaseClose)

	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("CloseWrite err = %v, want nil", err)
		}
	case <-time.After(testSignalTimeout):
		t.Fatal("CloseWrite did not finish after resume")
	}
}

func TestJoinedConnPauseWriteBlocksCloseWriteUntilDeadline(t *testing.T) {
	t.Parallel()

	writeHalf := &blockingCloseWriteHalf{
		closeStarted: make(chan struct{}, 1),
		releaseClose: make(chan struct{}),
	}
	conn := JoinConn(nil, writeHalf)

	pause, err := conn.PauseWrite(context.Background())
	if err != nil {
		t.Fatalf("PauseWrite: %v", err)
	}
	if err := conn.SetWriteDeadline(time.Now().Add(30 * time.Millisecond)); err != nil {
		t.Fatalf("SetWriteDeadline while paused: %v", err)
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- conn.CloseWrite()
	}()

	select {
	case err := <-errCh:
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("paused CloseWrite err = %v, want deadline exceeded", err)
		}
	case <-time.After(testSignalTimeout):
		t.Fatal("paused CloseWrite did not honor deadline")
	}

	select {
	case <-writeHalf.closeStarted:
		t.Fatal("CloseWrite reached detached write half while paused under deadline")
	default:
	}

	if err := pause.Resume(); err != nil {
		t.Fatalf("PauseWrite Resume after CloseWrite deadline: %v", err)
	}
}

func TestJoinedConnCloseKeepsDetachedHalfCallerOwned(t *testing.T) {
	t.Parallel()

	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	serverSend, err := server.OpenUniStream(ctx)
	if err != nil {
		t.Fatalf("server OpenUniStream: %v", err)
	}
	if _, err := serverSend.Write([]byte("a")); err != nil {
		t.Fatalf("server initial Write: %v", err)
	}

	clientRecv, err := client.AcceptUniStream(ctx)
	if err != nil {
		t.Fatalf("client AcceptUniStream: %v", err)
	}
	clientConn := JoinConn(clientRecv, nil)

	buf := make([]byte, 1)
	if _, err := clientConn.Read(buf); err != nil {
		t.Fatalf("initial Read: %v", err)
	}

	readPause, err := clientConn.PauseRead(context.Background())
	if err != nil {
		t.Fatalf("PauseRead: %v", err)
	}
	detached := readPause.Current()
	if detached != clientRecv {
		t.Fatal("PauseRead returned wrong detached half")
	}

	if err := clientConn.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := readPause.Resume(); !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("Resume after Close err = %v, want %v", err, ErrSessionClosed)
	}

	if _, err := serverSend.Write([]byte("x")); err != nil {
		t.Fatalf("server Write after JoinedConn Close: %v", err)
	}
	if _, err := detached.Read(buf); err != nil {
		t.Fatalf("detached Read after JoinedConn Close: %v", err)
	}
	if string(buf) != "x" {
		t.Fatalf("detached Read payload = %q, want %q", string(buf), "x")
	}
}

func TestStreamImplementsNetConnAtRuntime(t *testing.T) {
	t.Parallel()

	c, _, stop := newHandlerTestConn(t)
	defer stop()

	c.mu.Lock()
	stream := c.newLocalStreamWithIDLocked(state.FirstLocalStreamID(c.config.negotiated.LocalRole, true), streamArityBidi, OpenOptions{}, nil)
	c.registry.streams[stream.id] = stream
	c.mu.Unlock()

	var v any = stream
	conn, ok := v.(net.Conn)
	if !ok {
		t.Fatal("runtime type assertion to net.Conn failed")
	}
	if conn == nil {
		t.Fatal("asserted net.Conn is nil")
	}
}

func TestStreamNetConnAddrsDelegateToSessionTransport(t *testing.T) {
	t.Parallel()

	client, _ := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	stream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}

	transport, ok := client.io.conn.(streamAddrProvider)
	if !ok {
		t.Fatal("client transport does not expose net.Addr")
	}

	assertSameAddr(t, stream.LocalAddr(), transport.LocalAddr(), "LocalAddr")
	assertSameAddr(t, stream.RemoteAddr(), transport.RemoteAddr(), "RemoteAddr")
}

func TestStreamNetConnReadWriteAndClose(t *testing.T) {
	t.Parallel()

	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	type acceptResult struct {
		stream *nativeStream
		err    error
	}
	acceptCh := make(chan acceptResult, 1)
	go func() {
		s, err := server.AcceptStream(ctx)
		acceptCh <- acceptResult{stream: s, err: err}
	}()

	stream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}

	clientConn := net.Conn(stream)
	if n, err := clientConn.Write([]byte("hi")); err != nil || n != 2 {
		t.Fatalf("net.Conn Write = (%d, %v), want (2, nil)", n, err)
	}
	if err := clientConn.Close(); err != nil {
		t.Fatalf("net.Conn Close: %v", err)
	}

	accepted := <-acceptCh
	if accepted.err != nil {
		t.Fatalf("accept: %v", accepted.err)
	}
	serverConn := net.Conn(accepted.stream)

	buf := make([]byte, 8)
	n, err := serverConn.Read(buf)
	if err != nil {
		t.Fatalf("net.Conn Read: %v", err)
	}
	if got := string(buf[:n]); got != "hi" {
		t.Fatalf("net.Conn Read payload = %q, want %q", got, "hi")
	}
	if _, err := serverConn.Read(buf); !errors.Is(err, io.EOF) {
		t.Fatalf("second net.Conn Read err = %v, want EOF", err)
	}
}

func TestStreamNetConnDeadlineViaInterface(t *testing.T) {
	t.Parallel()

	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	type acceptResult struct {
		stream *nativeStream
		err    error
	}
	acceptCh := make(chan acceptResult, 1)
	go func() {
		s, err := server.AcceptStream(ctx)
		acceptCh <- acceptResult{stream: s, err: err}
	}()

	stream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if _, err := stream.Write([]byte("x")); err != nil {
		t.Fatalf("write opener: %v", err)
	}

	accepted := <-acceptCh
	if accepted.err != nil {
		t.Fatalf("accept: %v", accepted.err)
	}
	conn := net.Conn(accepted.stream)

	buf := make([]byte, 1)
	if _, err := conn.Read(buf); err != nil {
		t.Fatalf("initial net.Conn Read: %v", err)
	}

	if err := conn.SetReadDeadline(time.Now().Add(30 * time.Millisecond)); err != nil {
		t.Fatalf("net.Conn SetReadDeadline: %v", err)
	}
	if _, err := conn.Read(buf); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("net.Conn Read err = %v, want deadline exceeded", err)
	}
}

func TestStreamNetConnAddrsFallbackWithoutTransportAddrs(t *testing.T) {
	t.Parallel()

	c, _, stop := newHandlerTestConn(t)
	defer stop()

	c.mu.Lock()
	stream := c.newLocalStreamWithIDLocked(state.FirstLocalStreamID(c.config.negotiated.LocalRole, true), streamArityBidi, OpenOptions{}, nil)
	c.registry.streams[stream.id] = stream
	c.mu.Unlock()

	local := stream.LocalAddr()
	remote := stream.RemoteAddr()

	if local == nil {
		t.Fatal("LocalAddr = nil, want synthetic addr")
	}
	if remote == nil {
		t.Fatal("RemoteAddr = nil, want synthetic addr")
	}
	if local.Network() != "zmux" {
		t.Fatalf("LocalAddr network = %q, want %q", local.Network(), "zmux")
	}
	if remote.Network() != "zmux" {
		t.Fatalf("RemoteAddr network = %q, want %q", remote.Network(), "zmux")
	}
	if local.String() != "local/stream/4" {
		t.Fatalf("LocalAddr string = %q, want %q", local.String(), "local/stream/4")
	}
	if remote.String() != "remote/stream/4" {
		t.Fatalf("RemoteAddr string = %q, want %q", remote.String(), "remote/stream/4")
	}
}

func TestStreamNetConnAddrsFallbackPendingStreamID(t *testing.T) {
	t.Parallel()

	c, _, stop := newHandlerTestConn(t)
	defer stop()

	c.mu.Lock()
	stream := c.newProvisionalLocalStreamLocked(streamArityBidi, OpenOptions{}, nil)
	c.mu.Unlock()

	if got := stream.LocalAddr().String(); got != "local/stream/pending" {
		t.Fatalf("LocalAddr string = %q, want %q", got, "local/stream/pending")
	}
	if got := stream.RemoteAddr().String(); got != "remote/stream/pending" {
		t.Fatalf("RemoteAddr string = %q, want %q", got, "remote/stream/pending")
	}
}

func assertSameAddr(t *testing.T, got, want net.Addr, name string) {
	t.Helper()
	if got == nil {
		t.Fatalf("%s = nil, want non-nil", name)
	}
	if want == nil {
		t.Fatalf("%s want addr = nil", name)
	}
	if got.Network() != want.Network() {
		t.Fatalf("%s network = %q, want %q", name, got.Network(), want.Network())
	}
	if got.String() != want.String() {
		t.Fatalf("%s string = %q, want %q", name, got.String(), want.String())
	}
}

func TestSendStreamImplementsWriterAtRuntime(t *testing.T) {
	t.Parallel()

	c, _, stop := newHandlerTestConn(t)
	defer stop()

	c.mu.Lock()
	stream := c.newLocalStreamWithIDLocked(state.FirstLocalStreamID(c.config.negotiated.LocalRole, false), streamArityUni, OpenOptions{}, nil)
	c.registry.streams[stream.id] = stream
	send := &nativeSendStream{stream: stream}
	c.mu.Unlock()

	var v any = send
	writer, ok := v.(io.Writer)
	if !ok {
		t.Fatal("runtime type assertion to io.Writer failed")
	}
	if writer == nil {
		t.Fatal("asserted io.Writer is nil")
	}
}

func TestRecvStreamImplementsReaderAtRuntime(t *testing.T) {
	t.Parallel()

	c, _, stop := newHandlerTestConn(t)
	defer stop()

	c.mu.Lock()
	stream := c.newPeerStreamLocked(state.FirstPeerStreamID(c.config.negotiated.LocalRole, false))
	c.registry.streams[stream.id] = stream
	recv := &nativeRecvStream{stream: stream}
	c.mu.Unlock()

	var v any = recv
	reader, ok := v.(io.Reader)
	if !ok {
		t.Fatal("runtime type assertion to io.Reader failed")
	}
	if reader == nil {
		t.Fatal("asserted io.Reader is nil")
	}
}

func TestUniStreamsReadWriteViaIOInterfaces(t *testing.T) {
	t.Parallel()

	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	type acceptResult struct {
		stream *nativeRecvStream
		err    error
	}
	acceptCh := make(chan acceptResult, 1)
	go func() {
		s, err := server.AcceptUniStream(ctx)
		acceptCh <- acceptResult{stream: s, err: err}
	}()

	send, err := client.OpenUniStream(ctx)
	if err != nil {
		t.Fatalf("open uni stream: %v", err)
	}

	var writer io.Writer = send
	if n, err := writer.Write([]byte("hi")); err != nil || n != 2 {
		t.Fatalf("io.Writer Write = (%d, %v), want (2, nil)", n, err)
	}
	if err := send.CloseWrite(); err != nil {
		t.Fatalf("CloseWrite: %v", err)
	}

	accepted := <-acceptCh
	if accepted.err != nil {
		t.Fatalf("accept uni: %v", accepted.err)
	}

	var reader io.Reader = accepted.stream
	buf := make([]byte, 8)
	n, err := reader.Read(buf)
	if err != nil {
		t.Fatalf("io.Reader Read: %v", err)
	}
	if got := string(buf[:n]); got != "hi" {
		t.Fatalf("io.Reader Read payload = %q, want %q", got, "hi")
	}
	if _, err := reader.Read(buf); !errors.Is(err, io.EOF) {
		t.Fatalf("second io.Reader Read err = %v, want EOF", err)
	}
}

func TestZeroValueStreamSurfaceReturnsSessionClosed(t *testing.T) {
	t.Parallel()

	var s Stream = (*nativeStream)(nil)

	if got := s.Metadata(); got.Priority != 0 || got.Group != nil || got.OpenInfo != nil {
		t.Fatalf("Metadata() = %#v, want zero-value fields", got)
	}
	if got := s.OpenInfo(); got != nil {
		t.Fatalf("OpenInfo() = %v, want nil", got)
	}

	if err := s.UpdateMetadata(MetadataUpdate{Priority: uint64ptr(7)}); !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("UpdateMetadata() err = %v, want %v", err, ErrSessionClosed)
	}
	if _, err := s.Read(make([]byte, 1)); !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("Read() err = %v, want %v", err, ErrSessionClosed)
	}
	if _, err := s.Write([]byte("x")); !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("Write() err = %v, want %v", err, ErrSessionClosed)
	}
	if _, err := s.Write(nil); !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("Write(nil) err = %v, want %v", err, ErrSessionClosed)
	}
	if _, err := s.WriteFinal([]byte("x")); !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("WriteFinal() err = %v, want %v", err, ErrSessionClosed)
	}
	if _, err := s.WritevFinal([][]byte{[]byte("x")}...); !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("WritevFinal() err = %v, want %v", err, ErrSessionClosed)
	}

	if err := s.CloseRead(); !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("CloseRead() err = %v, want %v", err, ErrSessionClosed)
	}
	if err := s.CloseReadWithCode(uint64(CodeCancelled)); !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("CloseReadWithCode() err = %v, want %v", err, ErrSessionClosed)
	}
	if err := s.CloseWrite(); !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("CloseWrite() err = %v, want %v", err, ErrSessionClosed)
	}
	if err := s.Reset(uint64(CodeCancelled)); !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("Reset() err = %v, want %v", err, ErrSessionClosed)
	}
	if err := s.CloseWithErrorCode(uint64(CodeCancelled), ""); !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("CloseWithErrorCode() err = %v, want %v", err, ErrSessionClosed)
	}
	if err := s.Close(); !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("Close() err = %v, want %v", err, ErrSessionClosed)
	}

	deadline := time.Now().Add(time.Second)
	if err := s.SetReadDeadline(deadline); !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("SetReadDeadline() err = %v, want %v", err, ErrSessionClosed)
	}
	if err := s.SetWriteDeadline(deadline); !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("SetWriteDeadline() err = %v, want %v", err, ErrSessionClosed)
	}
	if err := s.SetDeadline(deadline); !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("SetDeadline() err = %v, want %v", err, ErrSessionClosed)
	}
}

func TestStreamAddrsConcurrentWithIDCommit(t *testing.T) {
	t.Parallel()

	conn := &Conn{}
	stream := testBuildDetachedStream(conn, 0)

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
				_ = stream.LocalAddr()
				_ = stream.RemoteAddr()
			}
		}
	}()

	conn.mu.Lock()
	stream.id = 4
	stream.idSet = true
	conn.mu.Unlock()

	close(stop)
	<-done

	if got := stream.LocalAddr().String(); got != "local/stream/4" {
		t.Fatalf("LocalAddr().String() = %q, want %q", got, "local/stream/4")
	}
	if got := stream.RemoteAddr().String(); got != "remote/stream/4" {
		t.Fatalf("RemoteAddr().String() = %q, want %q", got, "remote/stream/4")
	}
}

func TestWaitWriteDoesNotConsumeControlNotify(t *testing.T) {
	t.Parallel()

	c := &Conn{
		pending:   connPendingControlState{controlNotify: make(chan struct{}, 1)},
		signals:   connRuntimeSignalState{livenessCh: make(chan struct{}, 1)},
		lifecycle: connLifecycleState{closedCh: make(chan struct{})},
	}
	stream := testBuildDetachedStream(c, 0, testWithWriteNotify())

	c.pending.controlNotify <- struct{}{}

	if err := stream.waitWrite(time.Now().Add(10 * time.Millisecond)); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("waitWrite err = %v, want %v", err, os.ErrDeadlineExceeded)
	}

	select {
	case <-c.pending.controlNotify:
	default:
		t.Fatal("waitWrite consumed conn.controlNotify, want stream wait to leave control-flush signal intact")
	}
}

func TestWaitReadDoesNotConsumeControlNotify(t *testing.T) {
	t.Parallel()

	c := &Conn{
		pending:   connPendingControlState{controlNotify: make(chan struct{}, 1)},
		lifecycle: connLifecycleState{closedCh: make(chan struct{})},
	}
	stream := testBuildDetachedStream(c, 0, testWithReadNotify())

	c.pending.controlNotify <- struct{}{}

	if err := stream.waitRead(time.Now().Add(10 * time.Millisecond)); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("waitRead err = %v, want %v", err, os.ErrDeadlineExceeded)
	}

	select {
	case <-c.pending.controlNotify:
	default:
		t.Fatal("waitRead consumed conn.controlNotify, want stream wait to leave control-flush signal intact")
	}
}

func TestStreamGettersConcurrentWithCommit(t *testing.T) {
	t.Parallel()

	clientCfg := &Config{Capabilities: CapabilityOpenMetadata}
	serverCfg := &Config{Capabilities: CapabilityOpenMetadata}
	client, server := newConnPairWithConfig(t, clientCfg, serverCfg)
	ctx, cancel := testContext(t)
	defer cancel()

	type acceptResult struct {
		stream *nativeStream
		err    error
	}
	acceptCh := make(chan acceptResult, 1)
	go func() {
		stream, err := server.AcceptStream(ctx)
		acceptCh <- acceptResult{stream: stream, err: err}
	}()

	stream, err := client.OpenStreamWithOptions(ctx, OpenOptions{OpenInfo: []byte("ssh")})
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
				_ = stream.StreamID()
				_ = stream.ID()
				_ = stream.OpenInfo()
			}
		}
	}()

	if _, err := stream.Write([]byte("x")); err != nil {
		close(stop)
		<-done
		t.Fatalf("write: %v", err)
	}

	close(stop)
	<-done

	accepted := <-acceptCh
	if accepted.err != nil {
		t.Fatalf("accept stream: %v", accepted.err)
	}
	if accepted.stream == nil {
		t.Fatal("expected accepted stream")
	}

	if got := stream.StreamID(); got == 0 {
		t.Fatal("stream StreamID() = 0, want committed id")
	}
	if got := stream.ID(); got != stream.StreamID() {
		t.Fatalf("stream ID() = %d, want %d", got, stream.StreamID())
	}
	if got := stream.OpenInfo(); !bytes.Equal(got, []byte("ssh")) {
		t.Fatalf("stream OpenInfo() = %q, want %q", got, []byte("ssh"))
	}
}

func isWriteStoppedErr(err error) bool {
	if errors.Is(err, ErrWriteClosed) {
		return true
	}
	var appErr *ApplicationError
	return errors.As(err, &appErr) && appErr.Code == uint64(CodeCancelled)
}

func TestCloseWriteFromSendStopSeenQueuesFin(t *testing.T) {
	t.Parallel()

	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()

	stream := seedStateFixtureStream(t, c, state.FirstLocalStreamID(c.config.negotiated.LocalRole, true), "bidi", "local_owned", stateHalfExpect{
		SendHalf: "send_stop_seen",
		RecvHalf: "recv_open",
	})
	testMarkLocalOpenCommitted(stream)

	if err := stream.CloseWrite(); err != nil {
		t.Fatalf("CloseWrite err = %v, want nil", err)
	}

	frame := awaitQueuedFrame(t, frames)
	if frame.Type != FrameTypeDATA {
		t.Fatalf("queued frame type = %v, want %v", frame.Type, FrameTypeDATA)
	}
	if frame.StreamID != stream.id {
		t.Fatalf("queued frame stream = %d, want %d", frame.StreamID, stream.id)
	}
	if frame.Flags&FrameFlagFIN == 0 {
		t.Fatalf("queued DATA frame should include FIN flag in send-stop-seen close")
	}
	if len(frame.Payload) != 0 {
		t.Fatalf("queued DATA|FIN payload len = %d, want 0", len(frame.Payload))
	}
	if got := stream.sendHalfState(); got != state.SendHalfFin {
		t.Fatalf("sendHalf = %v, want %v", got, state.SendHalfFin)
	}
}

func TestCloseWriteAfterStopDrivenResetReturnsNil(t *testing.T) {
	t.Parallel()

	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()

	stream := seedStateFixtureStream(t, c, state.FirstLocalStreamID(c.config.negotiated.LocalRole, true), "bidi", "local_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})
	testMarkLocalOpenCommitted(stream)
	stream.setSendStopSeen(&ApplicationError{Code: uint64(CodeCancelled)})
	stream.setSendResetWithSource(&ApplicationError{Code: uint64(CodeCancelled)}, terminalResetFromStopSending)

	if err := stream.CloseWrite(); err != nil {
		t.Fatalf("CloseWrite err = %v, want nil", err)
	}

	assertNoQueuedFrame(t, frames)
	if got := stream.sendHalfState(); got != state.SendHalfReset {
		t.Fatalf("sendHalf = %v, want %v", got, state.SendHalfReset)
	}
}

func TestCloseWriteWithZeroWindowQueuesOnlyOpeningFin(t *testing.T) {
	t.Parallel()

	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()

	c.mu.Lock()
	c.flow.sendSessionMax = 0
	c.config.peer.Settings.InitialMaxStreamDataBidiPeerOpened = 0
	stream := c.newProvisionalLocalStreamLocked(streamArityBidi, OpenOptions{}, nil)
	c.mu.Unlock()

	if err := stream.CloseWrite(); err != nil {
		t.Fatalf("CloseWrite err = %v, want nil", err)
	}

	frame := awaitQueuedFrame(t, frames)
	if frame.Type != FrameTypeDATA {
		t.Fatalf("queued frame type = %v, want %v", frame.Type, FrameTypeDATA)
	}
	if frame.StreamID != state.FirstLocalStreamID(c.config.negotiated.LocalRole, true) {
		t.Fatalf("queued frame stream = %d, want %d", frame.StreamID, state.FirstLocalStreamID(c.config.negotiated.LocalRole, true))
	}
	if frame.Flags&FrameFlagFIN == 0 {
		t.Fatal("queued DATA frame should include FIN flag")
	}
	if len(frame.Payload) != 0 {
		t.Fatalf("queued DATA|FIN payload len = %d, want 0", len(frame.Payload))
	}
	assertNoQueuedFrame(t, frames)

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.pending.hasSessionBlocked || testPendingStreamBlockedCount(c) != 0 {
		t.Fatalf("pending blocked state = %t/%d, want false/0", c.pending.hasSessionBlocked, testPendingStreamBlockedCount(c))
	}
}

func TestCloseReadWithZeroWindowQueuesOpeningDataBeforeStopSending(t *testing.T) {
	t.Parallel()

	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()

	c.mu.Lock()
	c.flow.sendSessionMax = 0
	c.config.peer.Settings.InitialMaxStreamDataBidiPeerOpened = 0
	stream := c.newProvisionalLocalStreamLocked(streamArityBidi, OpenOptions{}, nil)
	c.mu.Unlock()

	if err := stream.CloseRead(); err != nil {
		t.Fatalf("CloseRead err = %v, want nil", err)
	}

	first := awaitQueuedFrame(t, frames)
	if first.Type != FrameTypeDATA {
		t.Fatalf("first queued frame type = %v, want %v", first.Type, FrameTypeDATA)
	}
	if first.StreamID != state.FirstLocalStreamID(c.config.negotiated.LocalRole, true) {
		t.Fatalf("first queued frame stream = %d, want %d", first.StreamID, state.FirstLocalStreamID(c.config.negotiated.LocalRole, true))
	}
	if first.Flags&FrameFlagFIN != 0 {
		t.Fatal("opening DATA for CloseRead should not include FIN")
	}
	if len(first.Payload) != 0 {
		t.Fatalf("opening DATA payload len = %d, want 0", len(first.Payload))
	}

	second := awaitQueuedFrame(t, frames)
	if second.Type != FrameTypeStopSending {
		t.Fatalf("second queued frame type = %v, want %v", second.Type, FrameTypeStopSending)
	}
	if second.StreamID != first.StreamID {
		t.Fatalf("STOP_SENDING stream = %d, want %d", second.StreamID, first.StreamID)
	}
	code, _, err := parseErrorPayload(second.Payload)
	if err != nil {
		t.Fatalf("decode STOP_SENDING payload err = %v", err)
	}
	if code != uint64(CodeCancelled) {
		t.Fatalf("queued STOP_SENDING code = %d, want %d", code, uint64(CodeCancelled))
	}
	assertNoQueuedFrame(t, frames)
}

func TestCloseWithErrorOnConcreteLocalIDQueuesOpeningAbort(t *testing.T) {
	t.Parallel()

	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()

	c.mu.Lock()
	stream := c.newLocalStreamWithIDLocked(state.FirstLocalStreamID(c.config.negotiated.LocalRole, true), streamArityBidi, OpenOptions{}, nil)
	c.registry.streams[stream.id] = stream
	c.appendUnseenLocalLocked(stream)
	c.mu.Unlock()

	if err := stream.CloseWithErrorCode(uint64(CodeInternal), "bye"); err != nil {
		t.Fatalf("CloseWithErrorCode err = %v, want nil", err)
	}

	frame := awaitQueuedFrame(t, frames)
	if frame.Type != FrameTypeABORT {
		t.Fatalf("queued frame type = %v, want %v", frame.Type, FrameTypeABORT)
	}
	if frame.StreamID != stream.id {
		t.Fatalf("queued frame stream = %d, want %d", frame.StreamID, stream.id)
	}
	code, reason, err := parseErrorPayload(frame.Payload)
	if err != nil {
		t.Fatalf("decode ABORT payload err = %v", err)
	}
	if code != uint64(CodeInternal) || reason != "bye" {
		t.Fatalf("queued ABORT payload = (%d, %q), want (%d, %q)", code, reason, uint64(CodeInternal), "bye")
	}
	assertNoQueuedFrame(t, frames)

	c.mu.Lock()
	defer c.mu.Unlock()
	if !stream.localOpen.committed {
		t.Fatal("sendCommitted = false, want true after opening ABORT")
	}
	if !stream.isPeerVisibleLocked() {
		t.Fatal("peerVisible = false, want true after queued opening ABORT")
	}
	if stream.sendAbort == nil || stream.sendAbort.Code != uint64(CodeInternal) {
		t.Fatalf("sendAbort = %v, want code %d", stream.sendAbort, uint64(CodeInternal))
	}
}

func TestCloseReadOnConcreteLocalIDQueuesOpeningDataBeforeStopSending(t *testing.T) {
	t.Parallel()

	c, frames, stop := newInvalidFrameConn(t, CapabilityOpenMetadata)
	defer stop()

	prefix, err := buildOpenMetadataPrefix(
		CapabilityOpenMetadata,
		OpenOptions{OpenInfo: []byte("ssh")},
		c.config.peer.Settings.MaxFramePayload,
	)
	if err != nil {
		t.Fatalf("buildOpenMetadataPrefix err = %v", err)
	}

	c.mu.Lock()
	stream := c.newLocalStreamWithIDLocked(
		state.FirstLocalStreamID(c.config.negotiated.LocalRole, true), streamArityBidi, OpenOptions{OpenInfo: []byte("ssh")},
		prefix,
	)
	c.registry.streams[stream.id] = stream
	c.appendUnseenLocalLocked(stream)
	c.mu.Unlock()

	if err := stream.CloseRead(); err != nil {
		t.Fatalf("CloseRead err = %v, want nil", err)
	}

	first := awaitQueuedFrame(t, frames)
	if first.Type != FrameTypeDATA {
		t.Fatalf("first queued frame type = %v, want %v", first.Type, FrameTypeDATA)
	}
	if first.StreamID != stream.id {
		t.Fatalf("first queued frame stream = %d, want %d", first.StreamID, stream.id)
	}
	if first.Flags&FrameFlagOpenMetadata == 0 {
		t.Fatal("opening DATA for concrete-id CloseRead should include OPEN_METADATA")
	}
	if first.Flags&FrameFlagFIN != 0 {
		t.Fatal("opening DATA for CloseRead should not include FIN")
	}
	if string(first.Payload) != string(prefix) {
		t.Fatalf("opening DATA payload = %x, want %x", first.Payload, prefix)
	}

	second := awaitQueuedFrame(t, frames)
	if second.Type != FrameTypeStopSending {
		t.Fatalf("second queued frame type = %v, want %v", second.Type, FrameTypeStopSending)
	}
	if second.StreamID != stream.id {
		t.Fatalf("STOP_SENDING stream = %d, want %d", second.StreamID, stream.id)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if !stream.localOpen.committed {
		t.Fatal("sendCommitted = false, want true after opening DATA")
	}
	if !stream.isPeerVisibleLocked() {
		t.Fatal("peerVisible = false, want true after concrete-id CloseRead opener")
	}
}

func TestCloseReadWithOversizedOpenMetadataKeepsReadStopButDoesNotCommitOpenerState(t *testing.T) {
	t.Parallel()

	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()

	c.mu.Lock()
	c.config.peer.Settings.MaxFramePayload = 4
	stream := c.newLocalStreamWithIDLocked(
		state.FirstLocalStreamID(c.config.negotiated.LocalRole, true), streamArityBidi, OpenOptions{},
		[]byte("oversized"),
	)
	c.registry.streams[stream.id] = stream
	c.appendUnseenLocalLocked(stream)
	c.mu.Unlock()

	err := stream.CloseRead()
	if !errors.Is(err, ErrOpenMetadataTooLarge) {
		t.Fatalf("CloseRead err = %v, want %v", err, ErrOpenMetadataTooLarge)
	}
	assertNoQueuedFrame(t, frames)

	c.mu.Lock()
	defer c.mu.Unlock()
	if !stream.readStopSentLocked() {
		t.Fatal("readStopped = false, want true after local CloseRead commit")
	}
	if stream.localOpen.committed {
		t.Fatal("sendCommitted = true, want false when opener validation fails before queueing")
	}
	if stream.visibilityPhaseLocked() == state.LocalOpenPhaseQueued {
		t.Fatal("openingBarrier = true, want false when opener validation fails before queueing")
	}
	if stream.isPeerVisibleLocked() {
		t.Fatal("peerVisible = true, want false when opener never enters writer path")
	}
}

func TestCloseReadWithMalformedOpenMetadataDoesNotCommitOpenerState(t *testing.T) {
	t.Parallel()

	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()

	c.mu.Lock()
	c.config.peer.Settings.MaxFramePayload = 16
	stream := c.newLocalStreamWithIDLocked(
		state.FirstLocalStreamID(c.config.negotiated.LocalRole, true), streamArityBidi, OpenOptions{},
		[]byte("bad"),
	)
	c.registry.streams[stream.id] = stream
	c.appendUnseenLocalLocked(stream)
	c.mu.Unlock()

	err := stream.CloseRead()
	if err == nil {
		t.Fatal("CloseRead err = nil, want malformed OPEN_METADATA error")
	}
	assertNoQueuedFrame(t, frames)

	c.mu.Lock()
	defer c.mu.Unlock()
	if !stream.readStopSentLocked() {
		t.Fatal("readStopped = false, want true after local CloseRead commit")
	}
	if stream.localOpen.committed {
		t.Fatal("sendCommitted = true, want false when opener validation fails before queueing")
	}
	if stream.visibilityPhaseLocked() == state.LocalOpenPhaseQueued {
		t.Fatal("openingBarrier = true, want false when opener validation fails before queueing")
	}
	if stream.isPeerVisibleLocked() {
		t.Fatal("peerVisible = true, want false when opener never enters writer path")
	}
}

func TestCloseWriteOnConcreteLocalIDQueuesOpeningFinWithMetadata(t *testing.T) {
	t.Parallel()

	c, frames, stop := newInvalidFrameConn(t, CapabilityOpenMetadata)
	defer stop()

	prefix, err := buildOpenMetadataPrefix(
		CapabilityOpenMetadata,
		OpenOptions{OpenInfo: []byte("ssh")},
		c.config.peer.Settings.MaxFramePayload,
	)
	if err != nil {
		t.Fatalf("buildOpenMetadataPrefix err = %v", err)
	}

	c.mu.Lock()
	stream := c.newLocalStreamWithIDLocked(
		state.FirstLocalStreamID(c.config.negotiated.LocalRole, true), streamArityBidi, OpenOptions{OpenInfo: []byte("ssh")},
		prefix,
	)
	c.registry.streams[stream.id] = stream
	c.appendUnseenLocalLocked(stream)
	c.mu.Unlock()

	if err := stream.CloseWrite(); err != nil {
		t.Fatalf("CloseWrite err = %v, want nil", err)
	}

	frame := awaitQueuedFrame(t, frames)
	if frame.Type != FrameTypeDATA {
		t.Fatalf("queued frame type = %v, want %v", frame.Type, FrameTypeDATA)
	}
	if frame.StreamID != stream.id {
		t.Fatalf("queued frame stream = %d, want %d", frame.StreamID, stream.id)
	}
	if frame.Flags&FrameFlagOpenMetadata == 0 {
		t.Fatal("opening DATA|FIN for concrete-id CloseWrite should include OPEN_METADATA")
	}
	if frame.Flags&FrameFlagFIN == 0 {
		t.Fatal("opening DATA|FIN for concrete-id CloseWrite should include FIN")
	}
	if string(frame.Payload) != string(prefix) {
		t.Fatalf("opening DATA|FIN payload = %x, want %x", frame.Payload, prefix)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if !stream.localOpen.committed {
		t.Fatal("sendCommitted = false, want true after concrete-id CloseWrite opener")
	}
	if !stream.isPeerVisibleLocked() {
		t.Fatal("peerVisible = false, want true after concrete-id CloseWrite opener")
	}
	if !stream.sendFinReached() {
		t.Fatal("sendFinReached() = false, want true after concrete-id CloseWrite opener")
	}
}

func TestCloseWriteWithOversizedOpenMetadataDoesNotCommitOpenerState(t *testing.T) {
	t.Parallel()

	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()

	c.mu.Lock()
	c.config.peer.Settings.MaxFramePayload = 4
	stream := c.newLocalStreamWithIDLocked(
		state.FirstLocalStreamID(c.config.negotiated.LocalRole, true), streamArityBidi, OpenOptions{},
		[]byte("oversized"),
	)
	c.registry.streams[stream.id] = stream
	c.appendUnseenLocalLocked(stream)
	c.mu.Unlock()

	err := stream.CloseWrite()
	if !errors.Is(err, ErrOpenMetadataTooLarge) {
		t.Fatalf("CloseWrite err = %v, want %v", err, ErrOpenMetadataTooLarge)
	}
	assertNoQueuedFrame(t, frames)

	c.mu.Lock()
	defer c.mu.Unlock()
	if stream.localOpen.committed {
		t.Fatal("sendCommitted = true, want false when opener validation fails before queueing")
	}
	if stream.sendFinReached() {
		t.Fatal("sendFinReached() = true, want false when opener validation fails before queueing")
	}
	if stream.visibilityPhaseLocked() == state.LocalOpenPhaseQueued {
		t.Fatal("openingBarrier = true, want false when opener validation fails before queueing")
	}
	if stream.isPeerVisibleLocked() {
		t.Fatal("peerVisible = true, want false when opener never enters writer path")
	}
}

func TestCloseWriteWithMalformedOpenMetadataDoesNotCommitOpenerState(t *testing.T) {
	t.Parallel()

	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()

	c.mu.Lock()
	c.config.peer.Settings.MaxFramePayload = 16
	stream := c.newLocalStreamWithIDLocked(
		state.FirstLocalStreamID(c.config.negotiated.LocalRole, true), streamArityBidi, OpenOptions{},
		[]byte("bad"),
	)
	c.registry.streams[stream.id] = stream
	c.appendUnseenLocalLocked(stream)
	c.mu.Unlock()

	err := stream.CloseWrite()
	if err == nil {
		t.Fatal("CloseWrite err = nil, want malformed OPEN_METADATA error")
	}
	assertNoQueuedFrame(t, frames)

	c.mu.Lock()
	defer c.mu.Unlock()
	if stream.localOpen.committed {
		t.Fatal("sendCommitted = true, want false when opener validation fails before queueing")
	}
	if stream.sendFinReached() {
		t.Fatal("sendFinReached() = true, want false when opener validation fails before queueing")
	}
	if stream.visibilityPhaseLocked() == state.LocalOpenPhaseQueued {
		t.Fatal("openingBarrier = true, want false when opener validation fails before queueing")
	}
	if stream.isPeerVisibleLocked() {
		t.Fatal("peerVisible = true, want false when opener never enters writer path")
	}
}

func TestResetAfterSendFinReturnsWriteClosedWithoutQueueingReset(t *testing.T) {
	t.Parallel()

	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()

	stream := seedStateFixtureStream(t, c, state.FirstLocalStreamID(c.config.negotiated.LocalRole, true), "bidi", "local_owned", stateHalfExpect{
		SendHalf: "send_fin",
		RecvHalf: "recv_open",
	})
	testMarkLocalOpenCommitted(stream)

	err := stream.Reset(uint64(CodeCancelled))
	if !isWriteStoppedErr(err) {
		t.Fatalf("Reset err = %v, want write-stopped error", err)
	}
	if got := stream.sendHalfState(); got != state.SendHalfFin {
		t.Fatalf("sendHalf = %v, want %v", got, state.SendHalfFin)
	}
	if stream.sendReset != nil {
		t.Fatalf("sendReset = %#v, want nil", stream.sendReset)
	}
	assertNoQueuedFrame(t, frames)
}

func TestResetOnConcreteLocalIDBeforeCommitFailsLocallyWithoutQueueingReset(t *testing.T) {
	t.Parallel()

	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()

	c.mu.Lock()
	stream := c.newLocalStreamWithIDLocked(
		state.FirstLocalStreamID(c.config.negotiated.LocalRole, true), streamArityBidi, OpenOptions{OpenInfo: []byte("ssh")},
		nil,
	)
	stream.sendSent = 9
	stream.recvBuffer = 4
	stream.readBuf = []byte("read")
	c.flow.sendSessionUsed = 9
	c.flow.recvSessionUsed = 4
	c.registry.streams[stream.id] = stream
	c.appendUnseenLocalLocked(stream)
	c.mu.Unlock()

	if err := stream.Reset(uint64(CodeCancelled)); err != nil {
		t.Fatalf("Reset err = %v, want nil", err)
	}
	assertNoQueuedFrame(t, frames)

	_, err := stream.Write([]byte("x"))
	var appErr *ApplicationError
	if !errors.As(err, &appErr) {
		t.Fatalf("Write after local-only unopened reset err = %v, want ApplicationError", err)
	}
	if appErr.Code != uint64(CodeCancelled) {
		t.Fatalf("Write after local-only unopened reset code = %d, want %d", appErr.Code, uint64(CodeCancelled))
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if stream.localOpen.committed {
		t.Fatal("sendCommitted = true, want false for local-only unopened reset")
	}
	if stream.sendReset != nil {
		t.Fatalf("sendReset = %v, want nil for local-only unopened reset", stream.sendReset)
	}
	if stream.sendAbort == nil || stream.sendAbort.Code != uint64(CodeCancelled) {
		t.Fatalf("sendAbort = %v, want code %d", stream.sendAbort, uint64(CodeCancelled))
	}
	if stream.recvAbort == nil || stream.recvAbort.Code != uint64(CodeCancelled) {
		t.Fatalf("recvAbort = %v, want code %d", stream.recvAbort, uint64(CodeCancelled))
	}
	if _, ok := c.registry.streams[stream.id]; ok {
		t.Fatalf("stream %d remained live in conn.streams after local-only unopened reset", stream.id)
	}
	if c.flow.sendSessionUsed != 0 {
		t.Fatalf("sendSessionUsed = %d, want 0", c.flow.sendSessionUsed)
	}
	if c.flow.recvSessionUsed != 0 {
		t.Fatalf("recvSessionUsed = %d, want 0", c.flow.recvSessionUsed)
	}
	if c.retention.retainedOpenInfoBytes != 0 {
		t.Fatalf("retainedOpenInfoBytes = %d, want 0", c.retention.retainedOpenInfoBytes)
	}
	if stream.recvBuffer != 0 {
		t.Fatalf("recvBuffer = %d, want 0", stream.recvBuffer)
	}
	if len(stream.readBuf) != 0 {
		t.Fatalf("readBuf len = %d, want 0", len(stream.readBuf))
	}
}

func TestFailUnopenedLocalStreamClearsPendingRuntimeState(t *testing.T) {
	t.Parallel()

	c := newSessionMemoryTestConn()

	c.mu.Lock()
	stream := c.newLocalStreamLocked(
		state.FirstLocalStreamID(c.config.negotiated.LocalRole, true), streamArityBidi, OpenOptions{OpenInfo: []byte("ssh")},
		[]byte("meta"),
	)
	stream.readNotify = make(chan struct{}, 1)
	stream.writeNotify = make(chan struct{}, 1)
	c.registry.streams[stream.id] = stream
	c.appendUnseenLocalLocked(stream)
	testSetPendingStreamMaxData(c, stream.id, 64)
	testSetPendingStreamBlocked(c, stream.id, 32)
	testSetPendingPriorityUpdate(c, stream.id, []byte{1, 2, 3})
	c.failUnopenedLocalStreamLocked(stream, &ApplicationError{Code: uint64(CodeCancelled)})
	hasMaxData := testHasPendingStreamMaxData(c, stream.id)
	hasBlocked := testHasPendingStreamBlocked(c, stream.id)
	hasPriority := testHasPendingPriorityUpdate(c, stream.id)
	pendingControlBytes := c.pending.controlBytes
	pendingPriorityBytes := c.pending.priorityBytes
	c.mu.Unlock()

	if hasMaxData {
		t.Fatal("pending MAX_DATA retained after failUnopenedLocalStreamLocked")
	}
	if hasBlocked {
		t.Fatal("pending BLOCKED retained after failUnopenedLocalStreamLocked")
	}
	if hasPriority {
		t.Fatal("pending PRIORITY_UPDATE retained after failUnopenedLocalStreamLocked")
	}
	if pendingControlBytes != 0 {
		t.Fatalf("pendingControlBytes = %d, want 0 after failUnopenedLocalStreamLocked", pendingControlBytes)
	}
	if pendingPriorityBytes != 0 {
		t.Fatalf("pendingPriorityBytes = %d, want 0 after failUnopenedLocalStreamLocked", pendingPriorityBytes)
	}
}

func TestResetAfterSendTerminalReturnsLocalErrorWithoutQueueingReset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		sendHalf      string
		wantWriteStop bool
		wantCode      uint64
	}{
		{
			name:          "send_fin",
			sendHalf:      "send_fin",
			wantWriteStop: true,
		},
		{
			name:     "send_reset",
			sendHalf: "send_reset",
			wantCode: uint64(CodeCancelled),
		},
		{
			name:     "send_aborted",
			sendHalf: "send_aborted",
			wantCode: uint64(CodeCancelled),
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c, frames, stop := newInvalidFrameConn(t, 0)
			defer stop()

			stream := seedStateFixtureStream(t, c, state.FirstLocalStreamID(c.config.negotiated.LocalRole, true), "bidi", "local_owned", stateHalfExpect{
				SendHalf: tc.sendHalf,
				RecvHalf: "recv_open",
			})
			testMarkLocalOpenCommitted(stream)

			err := stream.Reset(uint64(CodeCancelled))
			if tc.wantWriteStop {
				if !isWriteStoppedErr(err) {
					t.Fatalf("Reset err = %v, want write-stopped error", err)
				}
			} else {
				var appErr *ApplicationError
				if !errors.As(err, &appErr) {
					t.Fatalf("Reset err = %v, want ApplicationError", err)
				}
				if appErr.Code != tc.wantCode {
					t.Fatalf("Reset code = %d, want %d", appErr.Code, tc.wantCode)
				}
			}

			switch tc.sendHalf {
			case "send_fin":
				if got := stream.sendHalfState(); got != state.SendHalfFin {
					t.Fatalf("sendHalf = %v, want %v", got, state.SendHalfFin)
				}
			case "send_reset":
				if got := stream.sendHalfState(); got != state.SendHalfReset {
					t.Fatalf("sendHalf = %v, want %v", got, state.SendHalfReset)
				}
			case "send_aborted":
				if got := stream.sendHalfState(); got != state.SendHalfAborted {
					t.Fatalf("sendHalf = %v, want %v", got, state.SendHalfAborted)
				}
			}

			if tc.sendHalf != "send_reset" && stream.sendReset != nil {
				t.Fatalf("sendReset = %#v, want nil", stream.sendReset)
			}
			assertNoQueuedFrame(t, frames)
		})
	}
}

func TestCloseWriteAfterSendTerminalReturnsLocalErrorWithoutQueueingFrame(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		sendHalf      string
		wantWriteStop bool
		wantCode      uint64
	}{
		{
			name:          "send_fin",
			sendHalf:      "send_fin",
			wantWriteStop: true,
		},
		{
			name:     "send_reset",
			sendHalf: "send_reset",
			wantCode: uint64(CodeCancelled),
		},
		{
			name:     "send_aborted",
			sendHalf: "send_aborted",
			wantCode: uint64(CodeCancelled),
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c, frames, stop := newInvalidFrameConn(t, 0)
			defer stop()

			stream := seedStateFixtureStream(t, c, state.FirstLocalStreamID(c.config.negotiated.LocalRole, true), "bidi", "local_owned", stateHalfExpect{
				SendHalf: tc.sendHalf,
				RecvHalf: "recv_open",
			})
			testMarkLocalOpenCommitted(stream)

			err := stream.CloseWrite()
			if tc.wantWriteStop {
				if !isWriteStoppedErr(err) {
					t.Fatalf("CloseWrite err = %v, want write-stopped error", err)
				}
			} else {
				var appErr *ApplicationError
				if !errors.As(err, &appErr) {
					t.Fatalf("CloseWrite err = %v, want ApplicationError", err)
				}
				if appErr.Code != tc.wantCode {
					t.Fatalf("CloseWrite code = %d, want %d", appErr.Code, tc.wantCode)
				}
			}

			switch tc.sendHalf {
			case "send_fin":
				if got := stream.sendHalfState(); got != state.SendHalfFin {
					t.Fatalf("sendHalf = %v, want %v", got, state.SendHalfFin)
				}
			case "send_reset":
				if got := stream.sendHalfState(); got != state.SendHalfReset {
					t.Fatalf("sendHalf = %v, want %v", got, state.SendHalfReset)
				}
			case "send_aborted":
				if got := stream.sendHalfState(); got != state.SendHalfAborted {
					t.Fatalf("sendHalf = %v, want %v", got, state.SendHalfAborted)
				}
			}
			assertNoQueuedFrame(t, frames)
		})
	}
}

func TestCloseReadStopsPeerWritesButPreservesReverseRead(t *testing.T) {
	t.Parallel()
	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	acceptCh := make(chan *nativeStream, 1)
	acceptErrCh := make(chan error, 1)
	go func() {
		accepted, err := server.AcceptStream(ctx)
		if err != nil {
			acceptErrCh <- err
			return
		}
		acceptCh <- accepted
	}()

	clientStream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if _, err := clientStream.Write([]byte("hi")); err != nil {
		t.Fatalf("initial write: %v", err)
	}

	var serverStream *nativeStream
	select {
	case err := <-acceptErrCh:
		t.Fatalf("accept failed: %v", err)
	case serverStream = <-acceptCh:
	}

	buf := make([]byte, 8)
	n, err := serverStream.Read(buf)
	if err != nil {
		t.Fatalf("server initial read: %v", err)
	}
	if got := string(buf[:n]); got != "hi" {
		t.Fatalf("server initial read = %q, want %q", got, "hi")
	}

	if err := serverStream.CloseRead(); err != nil {
		t.Fatalf("server CloseRead: %v", err)
	}

	if _, err := serverStream.Write([]byte("ok")); err != nil {
		t.Fatalf("server reverse write after CloseRead: %v", err)
	}

	awaitStreamWriteState(t, clientStream, testSignalTimeout, func(s *nativeStream) bool {
		return s.localReadStop || s.sendStop != nil || s.sendAbort != nil || s.sendFinReached() || s.sendReset != nil
	}, "timed out waiting for peer CloseRead visibility")

	_, err = clientStream.Write([]byte("x"))
	if !isWriteStoppedErr(err) {
		t.Fatalf("client write err = %v, want write-stopped error", err)
	}

	n, err = clientStream.Read(buf)
	if err != nil {
		t.Fatalf("client reverse read after peer CloseRead: %v", err)
	}
	if got := string(buf[:n]); got != "ok" {
		t.Fatalf("client reverse read = %q, want %q", got, "ok")
	}
}

func TestCancelReadAlias(t *testing.T) {
	t.Parallel()
	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	acceptCh := make(chan *nativeStream, 1)
	go func() {
		accepted, err := server.AcceptStream(ctx)
		if err != nil {
			t.Errorf("accept stream: %v", err)
			return
		}
		acceptCh <- accepted
	}()

	clientStream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if _, err := clientStream.Write([]byte("x")); err != nil {
		t.Fatalf("initial write: %v", err)
	}

	serverStream := <-acceptCh
	buf := make([]byte, 8)
	if _, err := serverStream.Read(buf); err != nil {
		t.Fatalf("server initial read: %v", err)
	}

	if err := serverStream.CancelRead(); err != nil {
		t.Fatalf("cancel read: %v", err)
	}
	if err := serverStream.CancelRead(); err != nil {
		t.Fatalf("second cancel read: %v", err)
	}

	if _, err := serverStream.Read(buf); !errors.Is(err, ErrReadClosed) {
		t.Fatalf("read after cancel read err = %v, want %v", err, ErrReadClosed)
	}
}

func TestCancelWriteAlias(t *testing.T) {
	t.Parallel()
	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	acceptCh := make(chan *nativeStream, 1)
	go func() {
		accepted, err := server.AcceptStream(ctx)
		if err != nil {
			t.Errorf("accept stream: %v", err)
			return
		}
		acceptCh <- accepted
	}()

	clientStream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if _, err := clientStream.Write([]byte("x")); err != nil {
		t.Fatalf("initial write: %v", err)
	}

	serverStream := <-acceptCh
	buf := make([]byte, 8)
	if _, err := serverStream.Read(buf); err != nil {
		t.Fatalf("server initial read: %v", err)
	}

	if err := clientStream.CancelWrite(); err != nil {
		t.Fatalf("cancel write: %v", err)
	}
	if _, err := clientStream.Write([]byte("y")); !isWriteStoppedErr(err) {
		t.Fatalf("write after cancel write err = %v, want write-stopped error", err)
	}
	if err := clientStream.ResetWrite(); err != nil {
		t.Fatalf("reset-write alias: %v", err)
	}
}

func TestCloseReadDoesNotBlockExplicitCloseWrite(t *testing.T) {
	t.Parallel()
	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	acceptCh := make(chan *nativeStream, 1)
	acceptErrCh := make(chan error, 1)
	go func() {
		accepted, err := server.AcceptStream(ctx)
		if err != nil {
			acceptErrCh <- err
			return
		}
		acceptCh <- accepted
	}()

	clientStream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if _, err := clientStream.Write([]byte("hi")); err != nil {
		t.Fatalf("initial write: %v", err)
	}

	var serverStream *nativeStream
	select {
	case err := <-acceptErrCh:
		t.Fatalf("accept failed: %v", err)
	case serverStream = <-acceptCh:
	}

	buf := make([]byte, 8)
	if _, err := serverStream.Read(buf); err != nil {
		t.Fatalf("server initial read: %v", err)
	}

	if err := serverStream.CloseRead(); err != nil {
		t.Fatalf("server CloseRead: %v", err)
	}

	awaitStreamWriteState(t, clientStream, testSignalTimeout, func(s *nativeStream) bool {
		return s.localReadStop || s.sendStop != nil || s.sendAbort != nil || s.sendFinReached() || s.sendReset != nil
	}, "timed out waiting for peer CloseRead visibility")

	err = clientStream.CloseWrite()
	if err != nil {
		t.Fatalf("client CloseWrite: %v", err)
	}
}

func TestPeerResetDoesNotOverrideLocalReadStopError(t *testing.T) {
	t.Parallel()
	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	streamID := state.FirstLocalStreamID(c.config.negotiated.LocalRole, true)
	stream := seedStateFixtureStream(t, c, streamID, "bidi", "local_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})
	if err := stream.CloseRead(); err != nil {
		t.Fatalf("local CloseRead: %v", err)
	}
	if !stream.localReadStop {
		t.Fatal("localReadStop = false, want true")
	}

	if err := c.handleResetFrame(Frame{
		Type:     FrameTypeRESET,
		StreamID: streamID,
		Payload:  mustEncodeVarint(uint64(CodeCancelled)),
	}); err != nil {
		t.Fatalf("handle RESET err = %v", err)
	}
	if stream.recvReset == nil {
		t.Fatal("recvReset = nil, want peer RESET to be recorded")
	}
	if stream.recvReset.Code != uint64(CodeCancelled) {
		t.Fatalf("recvReset code = %d, want %d", stream.recvReset.Code, uint64(CodeCancelled))
	}
	if !stream.readStopSentLocked() {
		t.Fatal("readStopped = false, want true after CloseRead + peer RESET")
	}

	_, err := stream.Read(make([]byte, 8))
	if !errors.Is(err, ErrReadClosed) {
		t.Fatalf("read after CloseRead + peer RESET err = %v, want %v", err, ErrReadClosed)
	}
}

func TestPeerFinDoesNotOverrideLocalReadStopError(t *testing.T) {
	t.Parallel()
	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	streamID := state.FirstLocalStreamID(c.config.negotiated.LocalRole, true)
	stream := seedStateFixtureStream(t, c, streamID, "bidi", "local_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})
	if err := stream.CloseRead(); err != nil {
		t.Fatalf("local CloseRead: %v", err)
	}
	if !stream.localReadStop {
		t.Fatal("localReadStop = false, want true")
	}

	c.mu.Lock()
	stream.setRecvFin()
	c.mu.Unlock()

	if !stream.readStopSentLocked() {
		t.Fatal("readStopped = false, want true after CloseRead + peer FIN")
	}

	_, err := stream.Read(make([]byte, 8))
	if !errors.Is(err, ErrReadClosed) {
		t.Fatalf("read after CloseRead + peer FIN err = %v, want %v", err, ErrReadClosed)
	}
}

func TestSessionMaxDataIncreaseBroadcastsConnWriteWake(t *testing.T) {
	c := &Conn{

		lifecycle: connLifecycleState{closedCh: make(chan struct{})}, registry: connRegistryState{streams: make(map[uint64]*nativeStream)},
	}
	streamA := testLocalSendStream(c, 4)
	streamB := testLocalSendStream(c, 8)

	c.mu.Lock()
	c.flow.sendSessionMax = 0
	wake := c.currentWriteWakeLocked()
	c.mu.Unlock()

	if err := c.handleMaxDataFrame(Frame{Type: FrameTypeMAXDATA, Payload: mustEncodeVarint(32)}); err != nil {
		t.Fatalf("handle session MAX_DATA err = %v, want nil", err)
	}

	select {
	case <-wake:
	default:
		t.Fatal("expected session MAX_DATA increase to close the connection-level write wake channel")
	}

	select {
	case <-streamA.writeNotify:
		t.Fatal("session MAX_DATA increase should not need per-stream writeNotify for streamA")
	default:
	}
	select {
	case <-streamB.writeNotify:
		t.Fatal("session MAX_DATA increase should not need per-stream writeNotify for streamB")
	default:
	}
}

func TestPrepareWriteWakesOnSessionMaxDataIncrease(t *testing.T) {
	c := newSessionMemoryTestConn()

	c.mu.Lock()
	c.flow.sendSessionMax = 0
	stream := c.newLocalStreamWithIDLocked(state.FirstLocalStreamID(RoleInitiator, true), streamArityBidi, OpenOptions{}, nil)
	testMarkLocalOpenVisible(stream)
	c.registry.streams[stream.id] = stream
	c.mu.Unlock()

	resultCh := make(chan struct {
		step writeStep
		err  error
	}, 1)
	go func() {
		var parts [1][]byte
		parts[0] = []byte("x")
		step, err := stream.prepareWritePartsLocked(parts[:], 0, 0, 1, writeChunkStreaming)
		resultCh <- struct {
			step writeStep
			err  error
		}{step: step, err: err}
	}()

	if err := c.handleMaxDataFrame(Frame{Type: FrameTypeMAXDATA, Payload: mustEncodeVarint(1)}); err != nil {
		t.Fatalf("handle session MAX_DATA err = %v, want nil", err)
	}

	result := <-resultCh
	if result.err != nil {
		t.Fatalf("prepareWriteLocked err = %v, want nil", result.err)
	}
	if result.step.frame.Type != FrameTypeDATA || result.step.frame.StreamID != stream.id || string(result.step.frame.Payload) != "x" {
		t.Fatalf("prepareWriteLocked frame = %+v, want DATA for stream %d with payload x", result.step.frame, stream.id)
	}
	if result.step.appN != 1 {
		t.Fatalf("prepareWriteLocked appN = %d, want 1", result.step.appN)
	}
}

func TestPrepareWriteFinalWakesOnSessionMaxDataIncrease(t *testing.T) {
	c := newSessionMemoryTestConn()

	c.mu.Lock()
	c.flow.sendSessionMax = 0
	stream := c.newLocalStreamWithIDLocked(state.FirstLocalStreamID(RoleInitiator, true), streamArityBidi, OpenOptions{}, nil)
	testMarkLocalOpenVisible(stream)
	c.registry.streams[stream.id] = stream
	c.mu.Unlock()

	resultCh := make(chan struct {
		step writeStep
		err  error
	}, 1)
	go func() {
		var parts [1][]byte
		parts[0] = []byte("x")
		step, err := stream.prepareWritePartsLocked(parts[:], 0, 0, 1, writeChunkFinal)
		resultCh <- struct {
			step writeStep
			err  error
		}{step: step, err: err}
	}()

	if err := c.handleMaxDataFrame(Frame{Type: FrameTypeMAXDATA, Payload: mustEncodeVarint(1)}); err != nil {
		t.Fatalf("handle session MAX_DATA err = %v, want nil", err)
	}

	result := <-resultCh
	if result.err != nil {
		t.Fatalf("prepareWriteFinalLocked err = %v, want nil", result.err)
	}
	if result.step.frame.Type != FrameTypeDATA || result.step.frame.StreamID != stream.id || string(result.step.frame.Payload) != "x" || result.step.frame.Flags&FrameFlagFIN == 0 {
		t.Fatalf("prepareWriteFinalLocked frame = %+v, want DATA|FIN for stream %d with payload x", result.step.frame, stream.id)
	}
	if result.step.appN != 1 {
		t.Fatalf("prepareWriteFinalLocked appN = %d, want 1", result.step.appN)
	}
}

func TestPrepareWriteFragmentsToCurrentSessionCredit(t *testing.T) {
	c := newSessionMemoryTestConn()

	c.mu.Lock()
	c.config.peer.Settings.MaxFramePayload = 16384
	c.flow.sendSessionMax = 5
	c.flow.sendSessionUsed = 3
	stream := c.newLocalStreamWithIDLocked(state.FirstLocalStreamID(RoleInitiator, true), streamArityBidi, OpenOptions{}, nil)
	testMarkLocalOpenVisible(stream)
	stream.sendMax = 8
	stream.sendSent = 4
	c.registry.streams[stream.id] = stream
	c.mu.Unlock()

	var parts [1][]byte
	parts[0] = []byte("hello")
	step, err := stream.prepareWritePartsLocked(parts[:], 0, 0, len(parts[0]), writeChunkStreaming)
	if err != nil {
		t.Fatalf("prepareWriteLocked err = %v, want nil", err)
	}
	if step.frame.Type != FrameTypeDATA || step.frame.StreamID != stream.id || string(step.frame.Payload) != "he" {
		t.Fatalf("prepareWriteLocked frame = %+v, want DATA for stream %d with payload %q", step.frame, stream.id, "he")
	}
	if step.appN != 2 {
		t.Fatalf("prepareWriteLocked appN = %d, want 2 from current session credit", step.appN)
	}
	if step.frame.Flags&FrameFlagFIN != 0 {
		t.Fatalf("prepareWriteLocked frame flags = %v, want no FIN", step.frame.Flags)
	}
}

func TestPrepareWriteFinalFragmentsToCurrentStreamCredit(t *testing.T) {
	c := newSessionMemoryTestConn()

	c.mu.Lock()
	c.config.peer.Settings.MaxFramePayload = 16384
	c.flow.sendSessionMax = 16
	stream := c.newLocalStreamWithIDLocked(state.FirstLocalStreamID(RoleInitiator, true), streamArityBidi, OpenOptions{}, nil)
	testMarkLocalOpenVisible(stream)
	stream.sendMax = 5
	stream.sendSent = 3
	c.registry.streams[stream.id] = stream
	c.mu.Unlock()

	var finalParts [1][]byte
	finalParts[0] = []byte("hello")
	step, err := stream.prepareWritePartsLocked(finalParts[:], 0, 0, len(finalParts[0]), writeChunkFinal)
	if err != nil {
		t.Fatalf("prepareWriteFinalLocked err = %v, want nil", err)
	}
	if step.frame.Type != FrameTypeDATA || step.frame.StreamID != stream.id || string(step.frame.Payload) != "he" {
		t.Fatalf("prepareWriteFinalLocked frame = %+v, want DATA for stream %d with payload %q", step.frame, stream.id, "he")
	}
	if step.appN != 2 {
		t.Fatalf("prepareWriteFinalLocked appN = %d, want 2 from current stream credit", step.appN)
	}
	if step.frame.Flags&FrameFlagFIN != 0 {
		t.Fatalf("prepareWriteFinalLocked flags = %v, want no FIN when credit only covers part of final write", step.frame.Flags)
	}
}

func TestPrepareWriteReusesSinglePartPayloadBacking(t *testing.T) {
	c := newSessionMemoryTestConn()

	c.mu.Lock()
	c.config.peer.Settings.MaxFramePayload = 16384
	c.flow.sendSessionMax = 16
	stream := c.newLocalStreamWithIDLocked(state.FirstLocalStreamID(RoleInitiator, true), streamArityBidi, OpenOptions{}, nil)
	testMarkLocalOpenVisible(stream)
	stream.sendMax = 16
	c.registry.streams[stream.id] = stream
	c.mu.Unlock()

	payload := []byte("hello")
	var parts [1][]byte
	parts[0] = payload
	step, err := stream.prepareWritePartsLocked(parts[:], 0, 0, len(payload), writeChunkStreaming)
	if err != nil {
		t.Fatalf("prepareWriteLocked err = %v, want nil", err)
	}
	if string(step.frame.Payload) != string(payload) {
		t.Fatalf("prepareWriteLocked payload = %q, want %q", step.frame.Payload, payload)
	}
	if len(step.frame.Payload) == 0 || &step.frame.Payload[0] != &payload[0] {
		t.Fatal("prepareWriteLocked copied single-part payload backing")
	}
}

func TestPrepareWriteKeepsMultipartPayloadViewAcrossPartBoundary(t *testing.T) {
	c := newSessionMemoryTestConn()

	c.mu.Lock()
	c.config.peer.Settings.MaxFramePayload = 16384
	c.flow.sendSessionMax = 16
	stream := c.newLocalStreamWithIDLocked(state.FirstLocalStreamID(RoleInitiator, true), streamArityBidi, OpenOptions{}, nil)
	testMarkLocalOpenVisible(stream)
	stream.sendMax = 16
	c.registry.streams[stream.id] = stream
	c.mu.Unlock()

	first := []byte("he")
	second := []byte("llo")
	parts := [][]byte{first, second}
	step, err := stream.prepareWritePartsLocked(parts, 0, 0, len(first)+len(second), writeChunkStreaming)
	if err != nil {
		t.Fatalf("prepareWriteLocked err = %v, want nil", err)
	}
	if got := string(step.frame.clonedPayload()); got != "hello" {
		t.Fatalf("prepareWriteLocked payload = %q, want %q", got, "hello")
	}
	if len(step.frame.Payload) != 0 {
		t.Fatalf("prepareWriteLocked flat payload len = %d, want 0 for multipart view", len(step.frame.Payload))
	}
	if !step.frame.hasPayloadParts() {
		t.Fatal("prepareWriteLocked did not retain multipart payload view")
	}
}

func TestSessionMaxDataIncreaseClearsPendingSessionBlocked(t *testing.T) {
	c := newSessionMemoryTestConn()

	c.mu.Lock()
	c.flow.sendSessionMax = 0
	c.queuePendingSessionControlAsync(sessionControlBlocked, 0)
	if !c.pending.hasSessionBlocked {
		c.mu.Unlock()
		t.Fatal("expected pending session BLOCKED before MAX_DATA increase")
	}
	c.mu.Unlock()

	if err := c.handleMaxDataFrame(Frame{Type: FrameTypeMAXDATA, Payload: mustEncodeVarint(1)}); err != nil {
		t.Fatalf("handle session MAX_DATA err = %v, want nil", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.pending.hasSessionBlocked {
		t.Fatal("pending session BLOCKED retained after session MAX_DATA increase")
	}
}

func TestStreamMaxDataIncreaseClearsPendingStreamBlocked(t *testing.T) {
	c := newSessionMemoryTestConn()
	stream := c.newLocalStreamWithIDLocked(state.FirstLocalStreamID(RoleInitiator, true), streamArityBidi, OpenOptions{}, nil)
	testMarkLocalOpenVisible(stream)

	c.mu.Lock()
	c.registry.streams[stream.id] = stream
	stream.sendMax = 0
	c.queueStreamBlockedAsync(stream, 0)
	if !testHasPendingStreamBlocked(c, stream.id) {
		c.mu.Unlock()
		t.Fatal("expected pending stream BLOCKED before stream MAX_DATA increase")
	}
	c.mu.Unlock()

	if err := c.handleMaxDataFrame(Frame{Type: FrameTypeMAXDATA, StreamID: stream.id, Payload: mustEncodeVarint(1)}); err != nil {
		t.Fatalf("handle stream MAX_DATA err = %v, want nil", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if testHasPendingStreamBlocked(c, stream.id) {
		t.Fatal("pending stream BLOCKED retained after stream MAX_DATA increase")
	}
}

func TestNewStreamsDeferNotifyChannelAllocation(t *testing.T) {
	t.Parallel()

	client, _ := newConnPair(t)

	client.mu.Lock()
	local := client.newLocalStreamLocked(client.registry.nextLocalBidi, streamArityBidi, OpenOptions{}, nil)
	peer := client.newPeerStreamLocked(client.registry.nextPeerBidi)
	client.mu.Unlock()

	for _, stream := range []*nativeStream{local, peer} {
		if stream.readNotify != nil {
			t.Fatal("new stream should defer read notify channel allocation")
		}
		if stream.writeNotify != nil {
			t.Fatal("new stream should defer write notify channel allocation")
		}
	}
}

func TestWaitPathsAllocateNotifyChannelsOnDemand(t *testing.T) {
	t.Parallel()

	conn := &Conn{lifecycle: connLifecycleState{closedCh: make(chan struct{})}}
	stream := testBuildDetachedStream(conn, 0)

	if err := stream.waitRead(time.Now().Add(5 * time.Millisecond)); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("waitRead err = %v, want deadline exceeded", err)
	}
	if err := stream.waitWrite(time.Now().Add(5 * time.Millisecond)); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("waitWrite err = %v, want deadline exceeded", err)
	}

	conn.mu.Lock()
	defer conn.mu.Unlock()
	if stream.readNotify == nil {
		t.Fatal("waitRead should allocate read notify channel")
	}
	if stream.writeNotify == nil {
		t.Fatal("waitWrite should allocate write notify channel")
	}
}

func TestReadWaitSnapshotPreservesDeadlineWake(t *testing.T) {
	t.Parallel()

	conn := &Conn{lifecycle: connLifecycleState{closedCh: make(chan struct{})}}
	stream := testBuildDetachedStream(conn, 0)

	conn.mu.Lock()
	notifyCh, deadline := stream.readWaitSnapshotLocked()
	conn.mu.Unlock()

	done := make(chan error, 1)
	go func() {
		done <- stream.waitWithDeadline(notifyCh, deadline, OperationRead)
	}()

	if err := stream.SetReadDeadline(time.Now().Add(10 * time.Millisecond)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("wait err = %v, want nil wake", err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("read wait did not wake after deadline update")
	}
}

func TestWriteWaitSnapshotPreservesDeadlineWake(t *testing.T) {
	t.Parallel()

	conn := &Conn{lifecycle: connLifecycleState{closedCh: make(chan struct{})}}
	stream := testBuildDetachedStream(conn, 0)

	conn.mu.Lock()
	notifyCh, deadline := stream.writeWaitSnapshotLocked()
	conn.mu.Unlock()

	done := make(chan error, 1)
	go func() {
		done <- stream.waitWithDeadlineAndWake(notifyCh, nil, deadline, OperationWrite)
	}()

	if err := stream.SetWriteDeadline(time.Now().Add(10 * time.Millisecond)); err != nil {
		t.Fatalf("SetWriteDeadline: %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("wait err = %v, want nil wake", err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("write wait did not wake after deadline update")
	}
}

func TestEstablishedConnStartsWithEmptyPendingState(t *testing.T) {
	t.Parallel()

	client, _ := newConnPair(t)

	client.mu.Lock()
	defer client.mu.Unlock()

	if client.registry.streams != nil {
		t.Fatal("streams map should stay nil until first live stream")
	}
	if client.registry.tombstones != nil {
		t.Fatal("tombstones map should stay nil until first compaction")
	}
	if client.registry.usedStreamData != nil {
		t.Fatal("used-stream marker map should stay nil until first marker")
	}
	if testPendingStreamMaxDataCount(client) != 0 {
		t.Fatal("pending stream MAX_DATA should stay empty until first queued MAX_DATA")
	}
	if testPendingStreamBlockedCount(client) != 0 {
		t.Fatal("pending stream BLOCKED should stay empty until first queued BLOCKED")
	}
	if testPendingPriorityUpdateCount(client) != 0 {
		t.Fatal("pending PRIORITY_UPDATE should stay empty until first queued update")
	}
	if client.pending.controlBytes != 0 || client.pending.priorityBytes != 0 {
		t.Fatalf("pending byte accounting = %d/%d, want 0/0", client.pending.controlBytes, client.pending.priorityBytes)
	}
	if client.retention.resetReasonCount != nil {
		t.Fatal("resetReasonCount map should stay nil until first reset")
	}
	if client.retention.abortReasonCount != nil {
		t.Fatal("abortReasonCount map should stay nil until first abort")
	}
}

func BenchmarkNewLocalStreamLocked(b *testing.B) {
	conn := &Conn{config: connConfigState{local: Preface{Settings: DefaultSettings()},
		peer: Preface{Settings: DefaultSettings()}},
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = conn.newLocalStreamWithIDLocked(4, streamArityBidi, OpenOptions{}, nil)
	}
}

func BenchmarkNewPeerStreamLocked(b *testing.B) {
	conn := &Conn{config: connConfigState{local: Preface{Settings: DefaultSettings()},
		peer:       Preface{Settings: DefaultSettings()},
		negotiated: Negotiated{LocalRole: RoleInitiator}},
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = conn.newPeerStreamLocked(1)
	}
}

type stallingWriteCloser struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func newStallingWriteCloser() *stallingWriteCloser {
	return &stallingWriteCloser{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func awaitWriterBlocked(t *testing.T, writer *stallingWriteCloser) {
	t.Helper()

	select {
	case <-writer.started:
	case <-time.After(2 * testSignalTimeout):
		t.Fatal("writer never became blocked")
	}
}

func (c *stallingWriteCloser) Read(_ []byte) (int, error) {
	return 0, io.EOF
}

func (c *stallingWriteCloser) Write(p []byte) (int, error) {
	c.once.Do(func() { close(c.started) })
	<-c.release
	return len(p), nil
}

func (c *stallingWriteCloser) Close() error {
	select {
	case <-c.release:
	default:
		close(c.release)
	}
	return nil
}

func TestStreamReadDeadlineExpires(t *testing.T) {
	t.Parallel()
	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	stream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}

	acceptCh := make(chan *nativeStream, 1)
	errCh := make(chan error, 1)
	go func() {
		accepted, err := server.AcceptStream(ctx)
		if err != nil {
			errCh <- err
			return
		}
		acceptCh <- accepted
	}()

	if _, err := stream.Write([]byte("x")); err != nil {
		t.Fatalf("write opener: %v", err)
	}

	var peer *nativeStream
	select {
	case err := <-errCh:
		t.Fatalf("accept failed: %v", err)
	case peer = <-acceptCh:
	case <-time.After(testSignalTimeout):
		t.Fatal("accept timed out")
	}

	buf := make([]byte, 1)
	if _, err := peer.Read(buf); err != nil {
		t.Fatalf("initial read: %v", err)
	}

	if err := peer.SetReadDeadline(time.Now().Add(30 * time.Millisecond)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}

	_, err = peer.Read(buf)
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("Read err = %v, want deadline exceeded", err)
	}
}

func TestStreamWriteDeadlineExpiresOnFlowControlWait(t *testing.T) {
	t.Parallel()
	serverCfg := DefaultConfig()
	serverCfg.Settings.InitialMaxStreamDataBidiPeerOpened = 0

	client, server := newConnPairWithConfig(t, nil, serverCfg)
	ctx, cancel := testContext(t)
	defer cancel()

	stream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}

	acceptDone := make(chan error, 1)
	go func() {
		accepted, err := server.AcceptStream(ctx)
		if err != nil {
			acceptDone <- err
			return
		}
		if accepted == nil {
			acceptDone <- errors.New("nil accepted stream")
			return
		}
		acceptDone <- nil
	}()

	if err := stream.SetWriteDeadline(time.Now().Add(testSignalTimeout)); err != nil {
		t.Fatalf("SetWriteDeadline: %v", err)
	}

	_, err = stream.Write([]byte("hello"))
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("Write err = %v, want deadline exceeded", err)
	}

	select {
	case err := <-acceptDone:
		if err != nil {
			t.Fatalf("accept failed: %v", err)
		}
	case <-time.After(2 * testSignalTimeout):
		t.Fatal("accept timed out")
	}
}

func TestSetReadDeadlineWakesBlockedRead(t *testing.T) {
	t.Parallel()
	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	stream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}

	acceptCh := make(chan *nativeStream, 1)
	acceptErrCh := make(chan error, 1)
	go func() {
		accepted, err := server.AcceptStream(ctx)
		if err != nil {
			acceptErrCh <- err
			return
		}
		acceptCh <- accepted
	}()

	if _, err := stream.Write([]byte("x")); err != nil {
		t.Fatalf("write opener: %v", err)
	}

	var peer *nativeStream
	select {
	case err := <-acceptErrCh:
		t.Fatalf("accept failed: %v", err)
	case peer = <-acceptCh:
	case <-time.After(testSignalTimeout):
		t.Fatal("accept timed out")
	}

	buf := make([]byte, 1)
	if _, err := peer.Read(buf); err != nil {
		t.Fatalf("initial read: %v", err)
	}

	errCh := make(chan error, 1)
	go func() {
		waitBuf := make([]byte, 1)
		_, err := peer.Read(waitBuf)
		errCh <- err
	}()

	if err := peer.SetReadDeadline(time.Now().Add(20 * time.Millisecond)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}

	select {
	case err := <-errCh:
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("Read err = %v, want deadline exceeded", err)
		}
	case <-time.After(testSignalTimeout):
		t.Fatal("blocked read did not wake on deadline update")
	}
}

func TestSetWriteDeadlineWakesBlockedWrite(t *testing.T) {
	t.Parallel()
	client, _, stop := newHandlerTestConn(t)
	defer stop()

	client.mu.Lock()
	stream := client.newLocalStreamWithIDLocked(state.FirstLocalStreamID(client.config.negotiated.LocalRole, true), streamArityBidi, OpenOptions{}, nil)
	testMarkLocalOpenVisible(stream)
	stream.sendMax = 0
	client.registry.streams[stream.id] = stream
	client.mu.Unlock()

	errCh := make(chan error, 1)
	go func() {
		_, err := stream.Write([]byte("hello"))
		errCh <- err
	}()

	if err := stream.SetWriteDeadline(time.Now().Add(30 * time.Millisecond)); err != nil {
		t.Fatalf("SetWriteDeadline: %v", err)
	}

	select {
	case err := <-errCh:
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("Write err = %v, want deadline exceeded", err)
		}
	case <-time.After(2 * testSignalTimeout):
		t.Fatal("blocked write did not wake on deadline update")
	}
}

func TestStreamWriteDeadlineExpiresWhileWriterIsBlocked(t *testing.T) {
	t.Parallel()

	writer := newStallingWriteCloser()
	c := &Conn{
		io: connIOState{conn: writer},

		lifecycle: connLifecycleState{closedCh: make(chan struct{})},

		writer: connWriterRuntimeState{writeCh: make(chan writeRequest), urgentWriteCh: make(chan writeRequest)}, config: connConfigState{peer: Preface{Settings: Settings{MaxFramePayload: 16}}}, flow: connFlowState{sendSessionMax: 1024}, registry: connRegistryState{streams: make(map[uint64]*nativeStream)},
	}
	stream := testVisibleBidiStream(c, 4, testWithSendMax(1024))

	go c.writeLoop()
	defer func() {
		_ = writer.Close()
		close(c.lifecycle.closedCh)
	}()

	if err := stream.SetWriteDeadline(time.Now().Add(30 * time.Millisecond)); err != nil {
		t.Fatalf("SetWriteDeadline: %v", err)
	}

	errCh := make(chan error, 1)
	go func() {
		_, err := stream.Write([]byte("hello"))
		errCh <- err
	}()

	awaitWriterBlocked(t, writer)

	select {
	case err := <-errCh:
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("Write err = %v, want deadline exceeded", err)
		}
	case <-time.After(2 * testSignalTimeout):
		t.Fatal("blocked queued write did not hit deadline")
	}
}

func TestSetWriteDeadlineWakesBlockedQueuedWrite(t *testing.T) {
	t.Parallel()

	writer := newStallingWriteCloser()
	c := &Conn{
		io: connIOState{conn: writer},

		lifecycle: connLifecycleState{closedCh: make(chan struct{})},

		writer: connWriterRuntimeState{writeCh: make(chan writeRequest), urgentWriteCh: make(chan writeRequest)}, config: connConfigState{peer: Preface{Settings: Settings{MaxFramePayload: 16}}}, flow: connFlowState{sendSessionMax: 1024}, registry: connRegistryState{streams: make(map[uint64]*nativeStream)},
	}
	stream := testVisibleBidiStream(c, 4, testWithSendMax(1024))

	go c.writeLoop()
	defer func() {
		_ = writer.Close()
		close(c.lifecycle.closedCh)
	}()

	errCh := make(chan error, 1)
	go func() {
		_, err := stream.Write([]byte("hello"))
		errCh <- err
	}()

	awaitWriterBlocked(t, writer)

	if err := stream.SetWriteDeadline(time.Now().Add(30 * time.Millisecond)); err != nil {
		t.Fatalf("SetWriteDeadline: %v", err)
	}

	select {
	case err := <-errCh:
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("Write err = %v, want deadline exceeded", err)
		}
	case <-time.After(2 * testSignalTimeout):
		t.Fatal("blocked queued write did not wake on deadline update")
	}
}

func TestCloseSessionFailsBlockedQueuedWrite(t *testing.T) {
	t.Parallel()

	writer := newStallingWriteCloser()
	c := &Conn{
		io: connIOState{conn: writer},

		lifecycle: connLifecycleState{closedCh: make(chan struct{})},

		writer: connWriterRuntimeState{writeCh: make(chan writeRequest), urgentWriteCh: make(chan writeRequest)}, config: connConfigState{peer: Preface{Settings: Settings{MaxFramePayload: 16}}}, flow: connFlowState{sendSessionMax: 1024}, registry: connRegistryState{streams: make(map[uint64]*nativeStream)},
	}
	stream := testVisibleBidiStream(c, 4, testWithSendMax(1024))

	go c.writeLoop()
	defer func() {
		_ = writer.Close()
		select {
		case <-c.lifecycle.closedCh:
		default:
			close(c.lifecycle.closedCh)
		}
	}()

	errCh := make(chan error, 1)
	go func() {
		_, err := stream.Write([]byte("hello"))
		errCh <- err
	}()

	awaitWriterBlocked(t, writer)
	c.closeSession(nil)

	select {
	case err := <-errCh:
		if !errors.Is(err, ErrSessionClosed) {
			t.Fatalf("Write err = %v, want %v", err, ErrSessionClosed)
		}
	case <-time.After(2 * testSignalTimeout):
		t.Fatal("blocked queued write did not wake on session close")
	}
}

func TestStreamWriteZeroWindowQueuesOpenerBeforeBlocked(t *testing.T) {
	t.Parallel()

	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	loopDone := make(chan struct{})
	go func() {
		c.controlFlushLoop()
		close(loopDone)
	}()
	defer func() {
		close(c.lifecycle.closedCh)
		<-loopDone
	}()

	c.mu.Lock()
	c.flow.sendSessionMax = 0
	c.config.peer.Settings.InitialMaxStreamDataBidiPeerOpened = 0
	stream := c.newProvisionalLocalStreamLocked(streamArityBidi, OpenOptions{}, nil)
	c.mu.Unlock()

	if err := stream.SetWriteDeadline(time.Now().Add(testSignalTimeout / 2)); err != nil {
		t.Fatalf("SetWriteDeadline: %v", err)
	}

	resultCh := make(chan struct {
		n   int
		err error
	}, 1)
	go func() {
		n, err := stream.Write([]byte("x"))
		resultCh <- struct {
			n   int
			err error
		}{n: n, err: err}
	}()

	first := awaitQueuedFrame(t, frames)
	if first.Type != FrameTypeDATA {
		t.Fatalf("first queued frame type = %v, want %v", first.Type, FrameTypeDATA)
	}
	if first.StreamID != state.FirstLocalStreamID(c.config.negotiated.LocalRole, true) {
		t.Fatalf("first queued frame stream = %d, want %d", first.StreamID, state.FirstLocalStreamID(c.config.negotiated.LocalRole, true))
	}
	if len(first.Payload) != 0 {
		t.Fatalf("opening DATA payload len = %d, want 0", len(first.Payload))
	}

	second := awaitQueuedFrame(t, frames)
	if second.Type != FrameTypeBLOCKED || second.StreamID != 0 || string(second.Payload) != string(mustEncodeVarint(0)) {
		t.Fatalf("second queued frame = %+v, want session BLOCKED(0)", second)
	}

	third := awaitQueuedFrame(t, frames)
	if third.Type != FrameTypeBLOCKED || third.StreamID != first.StreamID || string(third.Payload) != string(mustEncodeVarint(0)) {
		t.Fatalf("third queued frame = %+v, want stream BLOCKED(%d,0)", third, first.StreamID)
	}

	if err := stream.SetWriteDeadline(time.Now().Add(30 * time.Millisecond)); err != nil {
		t.Fatalf("SetWriteDeadline: %v", err)
	}

	select {
	case result := <-resultCh:
		if result.n != 0 {
			t.Fatalf("Write n = %d, want 0", result.n)
		}
		if !errors.Is(result.err, os.ErrDeadlineExceeded) {
			t.Fatalf("Write err = %v, want deadline exceeded", result.err)
		}
	case <-time.After(2 * testSignalTimeout):
		t.Fatal("zero-window write did not hit deadline")
	}
}

func TestStreamWriteZeroWindowWithOpenMetadataQueuesMetadataOpenerBeforeBlocked(t *testing.T) {
	t.Parallel()

	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	loopDone := make(chan struct{})
	go func() {
		c.controlFlushLoop()
		close(loopDone)
	}()
	defer func() {
		close(c.lifecycle.closedCh)
		<-loopDone
	}()

	prefix, err := buildOpenMetadataPrefix(
		CapabilityOpenMetadata,
		OpenOptions{OpenInfo: []byte("ssh")},
		c.config.peer.Settings.MaxFramePayload,
	)
	if err != nil {
		t.Fatalf("buildOpenMetadataPrefix: %v", err)
	}

	c.mu.Lock()
	c.config.negotiated.Capabilities = CapabilityOpenMetadata
	c.flow.sendSessionMax = 0
	c.config.peer.Settings.InitialMaxStreamDataBidiPeerOpened = 0
	stream := c.newProvisionalLocalStreamLocked(streamArityBidi, OpenOptions{OpenInfo: []byte("ssh")}, prefix)
	c.mu.Unlock()

	resultCh := make(chan struct {
		n   int
		err error
	}, 1)
	go func() {
		n, err := stream.Write([]byte("x"))
		resultCh <- struct {
			n   int
			err error
		}{n: n, err: err}
	}()

	first := awaitQueuedFrame(t, frames)
	if first.Type != FrameTypeDATA {
		t.Fatalf("first queued frame type = %v, want %v", first.Type, FrameTypeDATA)
	}
	if first.Flags&FrameFlagOpenMetadata == 0 {
		t.Fatalf("first queued frame flags = %v, want OPEN_METADATA", first.Flags)
	}
	if first.Flags&FrameFlagFIN != 0 {
		t.Fatalf("first queued frame unexpectedly carried FIN: %v", first.Flags)
	}
	if first.StreamID != state.FirstLocalStreamID(c.config.negotiated.LocalRole, true) {
		t.Fatalf("first queued frame stream = %d, want %d", first.StreamID, state.FirstLocalStreamID(c.config.negotiated.LocalRole, true))
	}
	if string(first.Payload) != string(prefix) {
		t.Fatalf("opening DATA payload = %x, want %x", first.Payload, prefix)
	}

	second := awaitQueuedFrame(t, frames)
	if second.Type != FrameTypeBLOCKED || second.StreamID != 0 || string(second.Payload) != string(mustEncodeVarint(0)) {
		t.Fatalf("second queued frame = %+v, want session BLOCKED(0)", second)
	}

	third := awaitQueuedFrame(t, frames)
	if third.Type != FrameTypeBLOCKED || third.StreamID != first.StreamID || string(third.Payload) != string(mustEncodeVarint(0)) {
		t.Fatalf("third queued frame = %+v, want stream BLOCKED(%d,0)", third, first.StreamID)
	}

	if err := stream.SetWriteDeadline(time.Now().Add(30 * time.Millisecond)); err != nil {
		t.Fatalf("SetWriteDeadline: %v", err)
	}

	select {
	case result := <-resultCh:
		if result.n != 0 {
			t.Fatalf("Write n = %d, want 0", result.n)
		}
		if !errors.Is(result.err, os.ErrDeadlineExceeded) {
			t.Fatalf("Write err = %v, want deadline exceeded", result.err)
		}
	case <-time.After(2 * testSignalTimeout):
		t.Fatal("zero-window metadata write did not hit deadline")
	}
}

func TestStreamWriteFinalZeroWindowQueuesOpenerBeforeBlocked(t *testing.T) {
	t.Parallel()

	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	loopDone := make(chan struct{})
	go func() {
		c.controlFlushLoop()
		close(loopDone)
	}()
	defer func() {
		close(c.lifecycle.closedCh)
		<-loopDone
	}()

	c.mu.Lock()
	c.flow.sendSessionMax = 0
	c.config.peer.Settings.InitialMaxStreamDataBidiPeerOpened = 0
	stream := c.newProvisionalLocalStreamLocked(streamArityBidi, OpenOptions{}, nil)
	c.mu.Unlock()

	resultCh := make(chan struct {
		n   int
		err error
	}, 1)
	go func() {
		n, err := stream.WriteFinal([]byte("x"))
		resultCh <- struct {
			n   int
			err error
		}{n: n, err: err}
	}()

	first := awaitQueuedFrame(t, frames)
	if first.Type != FrameTypeDATA {
		t.Fatalf("first queued frame type = %v, want %v", first.Type, FrameTypeDATA)
	}
	if first.Flags&FrameFlagFIN != 0 {
		t.Fatalf("first queued frame unexpectedly carried FIN: %v", first.Flags)
	}
	if first.StreamID != state.FirstLocalStreamID(c.config.negotiated.LocalRole, true) {
		t.Fatalf("first queued frame stream = %d, want %d", first.StreamID, state.FirstLocalStreamID(c.config.negotiated.LocalRole, true))
	}
	if len(first.Payload) != 0 {
		t.Fatalf("opening DATA payload len = %d, want 0", len(first.Payload))
	}

	second := awaitQueuedFrame(t, frames)
	if second.Type != FrameTypeBLOCKED || second.StreamID != 0 || string(second.Payload) != string(mustEncodeVarint(0)) {
		t.Fatalf("second queued frame = %+v, want session BLOCKED(0)", second)
	}

	third := awaitQueuedFrame(t, frames)
	if third.Type != FrameTypeBLOCKED || third.StreamID != first.StreamID || string(third.Payload) != string(mustEncodeVarint(0)) {
		t.Fatalf("third queued frame = %+v, want stream BLOCKED(%d,0)", third, first.StreamID)
	}

	if err := stream.SetWriteDeadline(time.Now().Add(30 * time.Millisecond)); err != nil {
		t.Fatalf("SetWriteDeadline: %v", err)
	}

	select {
	case result := <-resultCh:
		if result.n != 0 {
			t.Fatalf("WriteFinal n = %d, want 0", result.n)
		}
		if !errors.Is(result.err, os.ErrDeadlineExceeded) {
			t.Fatalf("WriteFinal err = %v, want deadline exceeded", result.err)
		}
	case <-time.After(2 * testSignalTimeout):
		t.Fatal("zero-window WriteFinal did not hit deadline")
	}
}

func TestStreamWriteFinalZeroWindowWithOpenMetadataQueuesMetadataOpenerBeforeBlocked(t *testing.T) {
	t.Parallel()

	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	loopDone := make(chan struct{})
	go func() {
		c.controlFlushLoop()
		close(loopDone)
	}()
	defer func() {
		close(c.lifecycle.closedCh)
		<-loopDone
	}()

	prefix, err := buildOpenMetadataPrefix(
		CapabilityOpenMetadata,
		OpenOptions{OpenInfo: []byte("ssh")},
		c.config.peer.Settings.MaxFramePayload,
	)
	if err != nil {
		t.Fatalf("buildOpenMetadataPrefix: %v", err)
	}

	c.mu.Lock()
	c.config.negotiated.Capabilities = CapabilityOpenMetadata
	c.flow.sendSessionMax = 0
	c.config.peer.Settings.InitialMaxStreamDataBidiPeerOpened = 0
	stream := c.newProvisionalLocalStreamLocked(streamArityBidi, OpenOptions{OpenInfo: []byte("ssh")}, prefix)
	c.mu.Unlock()

	resultCh := make(chan struct {
		n   int
		err error
	}, 1)
	go func() {
		n, err := stream.WriteFinal([]byte("x"))
		resultCh <- struct {
			n   int
			err error
		}{n: n, err: err}
	}()

	first := awaitQueuedFrame(t, frames)
	if first.Type != FrameTypeDATA {
		t.Fatalf("first queued frame type = %v, want %v", first.Type, FrameTypeDATA)
	}
	if first.Flags&FrameFlagOpenMetadata == 0 {
		t.Fatalf("first queued frame flags = %v, want OPEN_METADATA", first.Flags)
	}
	if first.Flags&FrameFlagFIN != 0 {
		t.Fatalf("first queued frame unexpectedly carried FIN: %v", first.Flags)
	}
	if first.StreamID != state.FirstLocalStreamID(c.config.negotiated.LocalRole, true) {
		t.Fatalf("first queued frame stream = %d, want %d", first.StreamID, state.FirstLocalStreamID(c.config.negotiated.LocalRole, true))
	}
	if string(first.Payload) != string(prefix) {
		t.Fatalf("opening DATA payload = %x, want %x", first.Payload, prefix)
	}

	second := awaitQueuedFrame(t, frames)
	if second.Type != FrameTypeBLOCKED || second.StreamID != 0 || string(second.Payload) != string(mustEncodeVarint(0)) {
		t.Fatalf("second queued frame = %+v, want session BLOCKED(0)", second)
	}

	third := awaitQueuedFrame(t, frames)
	if third.Type != FrameTypeBLOCKED || third.StreamID != first.StreamID || string(third.Payload) != string(mustEncodeVarint(0)) {
		t.Fatalf("third queued frame = %+v, want stream BLOCKED(%d,0)", third, first.StreamID)
	}

	if err := stream.SetWriteDeadline(time.Now().Add(30 * time.Millisecond)); err != nil {
		t.Fatalf("SetWriteDeadline: %v", err)
	}

	select {
	case result := <-resultCh:
		if result.n != 0 {
			t.Fatalf("WriteFinal n = %d, want 0", result.n)
		}
		if !errors.Is(result.err, os.ErrDeadlineExceeded) {
			t.Fatalf("WriteFinal err = %v, want deadline exceeded", result.err)
		}
	case <-time.After(2 * testSignalTimeout):
		t.Fatal("zero-window metadata WriteFinal did not hit deadline")
	}
}

func TestStreamWriteCurrentCreditReturnsShortWriteWithDeadlineAndQueuesBlocked(t *testing.T) {
	t.Parallel()

	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	loopDone := make(chan struct{})
	go func() {
		c.controlFlushLoop()
		close(loopDone)
	}()
	defer func() {
		close(c.lifecycle.closedCh)
		<-loopDone
	}()

	c.mu.Lock()
	c.flow.sendSessionMax = 1
	c.config.peer.Settings.InitialMaxStreamDataBidiPeerOpened = 1
	stream := c.newProvisionalLocalStreamLocked(streamArityBidi, OpenOptions{}, nil)
	c.mu.Unlock()

	if err := stream.SetWriteDeadline(time.Now().Add(testSignalTimeout / 2)); err != nil {
		t.Fatalf("SetWriteDeadline: %v", err)
	}

	resultCh := make(chan struct {
		n   int
		err error
	}, 1)
	go func() {
		n, err := stream.Write([]byte("xy"))
		resultCh <- struct {
			n   int
			err error
		}{n: n, err: err}
	}()

	streamID := state.FirstLocalStreamID(c.config.negotiated.LocalRole, true)
	assertCurrentCreditWriteFrames(t, frames, streamID)

	select {
	case result := <-resultCh:
		if result.n != 1 {
			t.Fatalf("Write n = %d, want 1", result.n)
		}
		if !errors.Is(result.err, os.ErrDeadlineExceeded) {
			t.Fatalf("Write err = %v, want deadline exceeded", result.err)
		}
	case <-time.After(2 * testSignalTimeout):
		t.Fatal("current-credit write did not hit deadline")
	}
}

func TestStreamWriteFinalCurrentCreditReturnsShortWriteWithDeadlineAndQueuesBlocked(t *testing.T) {
	t.Parallel()

	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	loopDone := make(chan struct{})
	go func() {
		c.controlFlushLoop()
		close(loopDone)
	}()
	defer func() {
		close(c.lifecycle.closedCh)
		<-loopDone
	}()

	c.mu.Lock()
	c.flow.sendSessionMax = 1
	c.config.peer.Settings.InitialMaxStreamDataBidiPeerOpened = 1
	stream := c.newProvisionalLocalStreamLocked(streamArityBidi, OpenOptions{}, nil)
	c.mu.Unlock()

	if err := stream.SetWriteDeadline(time.Now().Add(testSignalTimeout / 2)); err != nil {
		t.Fatalf("SetWriteDeadline: %v", err)
	}

	resultCh := make(chan struct {
		n   int
		err error
	}, 1)
	go func() {
		n, err := stream.WriteFinal([]byte("xy"))
		resultCh <- struct {
			n   int
			err error
		}{n: n, err: err}
	}()

	streamID := state.FirstLocalStreamID(c.config.negotiated.LocalRole, true)
	assertCurrentCreditWriteFrames(t, frames, streamID)

	select {
	case result := <-resultCh:
		if result.n != 1 {
			t.Fatalf("WriteFinal n = %d, want 1", result.n)
		}
		if !errors.Is(result.err, os.ErrDeadlineExceeded) {
			t.Fatalf("WriteFinal err = %v, want deadline exceeded", result.err)
		}
	case <-time.After(2 * testSignalTimeout):
		t.Fatal("current-credit WriteFinal did not hit deadline")
	}
}

func TestStreamCloseWriteZeroWindowWithOpenMetadataQueuesMetadataFinOpener(t *testing.T) {
	t.Parallel()

	c, frames, stop := newHandlerTestConn(t)
	defer stop()

	prefix, err := buildOpenMetadataPrefix(
		CapabilityOpenMetadata,
		OpenOptions{OpenInfo: []byte("ssh")},
		c.config.peer.Settings.MaxFramePayload,
	)
	if err != nil {
		t.Fatalf("buildOpenMetadataPrefix: %v", err)
	}

	c.mu.Lock()
	c.config.negotiated.Capabilities = CapabilityOpenMetadata
	c.flow.sendSessionMax = 0
	c.config.peer.Settings.InitialMaxStreamDataBidiPeerOpened = 0
	stream := c.newProvisionalLocalStreamLocked(streamArityBidi, OpenOptions{OpenInfo: []byte("ssh")}, prefix)
	c.mu.Unlock()

	if err := stream.CloseWrite(); err != nil {
		t.Fatalf("CloseWrite err = %v, want nil", err)
	}

	first := awaitQueuedFrame(t, frames)
	if first.Type != FrameTypeDATA {
		t.Fatalf("queued frame type = %v, want %v", first.Type, FrameTypeDATA)
	}
	if first.Flags&FrameFlagOpenMetadata == 0 || first.Flags&FrameFlagFIN == 0 {
		t.Fatalf("queued frame flags = %v, want OPEN_METADATA|FIN", first.Flags)
	}
	if string(first.Payload) != string(prefix) {
		t.Fatalf("queued payload = %x, want %x", first.Payload, prefix)
	}

	assertNoQueuedFrame(t, frames)

	c.mu.Lock()
	defer c.mu.Unlock()
	if !stream.localOpen.committed {
		t.Fatal("sendCommitted = false, want true after zero-window CloseWrite opener")
	}
	if stream.sendHalfState() != state.SendHalfFin {
		t.Fatalf("sendHalf = %v, want %v", stream.sendHalfState(), state.SendHalfFin)
	}
}

func assertCurrentCreditWriteFrames(t *testing.T, frames <-chan Frame, streamID uint64) {
	t.Helper()

	sawData := false
	sawSessionBlocked := false
	deadline := time.After(testSignalTimeout)

	for seen := 0; seen < 3; seen++ {
		select {
		case frame := <-frames:
			switch {
			case frame.Type == FrameTypeDATA:
				if frame.Flags&FrameFlagFIN != 0 || frame.StreamID != streamID || string(frame.Payload) != "x" {
					t.Fatalf("queued DATA frame = %+v, want DATA stream %d payload x without FIN", frame, streamID)
				}
				sawData = true
			case frame.Type == FrameTypeBLOCKED && frame.StreamID == 0:
				if string(frame.Payload) != string(mustEncodeVarint(1)) {
					t.Fatalf("session BLOCKED payload = %x, want %x", frame.Payload, mustEncodeVarint(1))
				}
				sawSessionBlocked = true
			case frame.Type == FrameTypeBLOCKED && frame.StreamID == streamID:
				if string(frame.Payload) != string(mustEncodeVarint(1)) {
					t.Fatalf("stream BLOCKED payload = %x, want %x", frame.Payload, mustEncodeVarint(1))
				}
			default:
				t.Fatalf("unexpected queued frame = %+v", frame)
			}
			if sawData && sawSessionBlocked {
				return
			}
		case <-deadline:
			t.Fatalf("queued frames missing expected current-credit set: data=%t session_blocked=%t", sawData, sawSessionBlocked)
		}
	}

	t.Fatalf("queued frames missing expected current-credit set: data=%t session_blocked=%t", sawData, sawSessionBlocked)
}

func TestOpenInfoRequiresOpenMetadataCapability(t *testing.T) {
	t.Parallel()

	client, server := newConnPairWithConfig(t, nil, nil)
	_ = server

	_, err := client.OpenStreamWithOptions(context.Background(), OpenOptions{OpenInfo: []byte("need-metadata")})
	if !errors.Is(err, ErrOpenInfoUnavailable) {
		t.Fatalf("OpenStreamWithOptions err = %v, want %v", err, ErrOpenInfoUnavailable)
	}
}

func TestOpenInfoOverflowsOpenMetadataPrefix(t *testing.T) {
	t.Parallel()

	clientCfg := &Config{
		Capabilities: CapabilityOpenMetadata,
	}
	serverCfg := &Config{
		Capabilities: CapabilityOpenMetadata,
	}
	client, _ := newConnPairWithConfig(t, clientCfg, serverCfg)

	openInfo := make([]byte, DefaultSettings().MaxFramePayload+1)
	_, err := client.OpenStreamWithOptions(context.Background(), OpenOptions{OpenInfo: openInfo})
	if !errors.Is(err, ErrOpenMetadataTooLarge) {
		t.Fatalf("OpenStreamWithOptions err = %v, want %v", err, ErrOpenMetadataTooLarge)
	}
}

func TestStreamCancelWriteWithCodeAliasesReset(t *testing.T) {
	t.Parallel()

	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()

	stream := seedStateFixtureStream(t, c, state.FirstLocalStreamID(c.config.negotiated.LocalRole, true), "bidi", "local_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})
	testMarkLocalOpenVisible(stream)

	if err := stream.CancelWriteWithCode(99); err != nil {
		t.Fatalf("CancelWriteWithCode err = %v", err)
	}

	assertQueuedResetCode(t, frames, stream.id, ErrorCode(99))
	if got := stream.sendHalfState(); got != state.SendHalfReset {
		t.Fatalf("sendHalf = %v, want %v", got, state.SendHalfReset)
	}
}

func TestSendStreamResetWriteWithCodeAliasesReset(t *testing.T) {
	t.Parallel()

	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()

	stream := seedStateFixtureStream(t, c, state.FirstLocalStreamID(c.config.negotiated.LocalRole, false), "uni_local_send_only", "local_owned", stateHalfExpect{
		SendHalf: "send_open",
	})
	testMarkLocalOpenVisible(stream)

	send := &nativeSendStream{stream: stream}
	if err := send.ResetWriteWithCode(77); err != nil {
		t.Fatalf("ResetWriteWithCode err = %v", err)
	}

	assertQueuedResetCode(t, frames, stream.id, ErrorCode(77))
}

func TestStreamAbortAliasUsesCancelledCode(t *testing.T) {
	t.Parallel()

	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()

	stream := seedStateFixtureStream(t, c, state.FirstLocalStreamID(c.config.negotiated.LocalRole, true), "bidi", "local_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})

	if err := stream.Abort(); err != nil {
		t.Fatalf("Abort err = %v", err)
	}

	frame := awaitQueuedFrame(t, frames)
	if frame.Type != FrameTypeABORT {
		t.Fatalf("queued frame type = %v, want %v", frame.Type, FrameTypeABORT)
	}
	if frame.StreamID != stream.id {
		t.Fatalf("queued frame stream-id = %d, want %d", frame.StreamID, stream.id)
	}
	code, reason, err := parseErrorPayload(frame.Payload)
	if err != nil {
		t.Fatalf("parse ABORT payload: %v", err)
	}
	if code != uint64(CodeCancelled) {
		t.Fatalf("abort code = %d, want %d", code, uint64(CodeCancelled))
	}
	if reason != "" {
		t.Fatalf("abort reason = %q, want empty", reason)
	}
}

func TestRecvStreamAbortWithErrorCodeAliasesWholeStreamAbort(t *testing.T) {
	t.Parallel()

	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()

	stream := seedStateFixtureStream(t, c, state.FirstPeerStreamID(c.config.negotiated.LocalRole, false), "uni_local_send_only", "peer_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})

	recv := &nativeRecvStream{stream: stream}
	if err := recv.AbortWithErrorCode(55, "bye"); err != nil {
		t.Fatalf("AbortWithErrorCode err = %v", err)
	}

	frame := awaitQueuedFrame(t, frames)
	if frame.Type != FrameTypeABORT {
		t.Fatalf("queued frame type = %v, want %v", frame.Type, FrameTypeABORT)
	}
	if frame.StreamID != stream.id {
		t.Fatalf("queued frame stream-id = %d, want %d", frame.StreamID, stream.id)
	}
	code, reason, err := parseErrorPayload(frame.Payload)
	if err != nil {
		t.Fatalf("parse ABORT payload: %v", err)
	}
	if code != 55 {
		t.Fatalf("abort code = %d, want %d", code, 55)
	}
	if reason != "bye" {
		t.Fatalf("abort reason = %q, want %q", reason, "bye")
	}
}

func TestWriteFinalBidi(t *testing.T) {
	t.Parallel()

	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	acceptCh := acceptStreamAsync(ctx, server)

	stream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	n, err := stream.WriteFinal([]byte("hello"))
	if err != nil || n != 5 {
		t.Fatalf("WriteFinal = (%d, %v), want (5, nil)", n, err)
	}

	accepted := requireAcceptedStream(t, acceptCh)

	got, err := io.ReadAll(accepted)
	if err != nil {
		t.Fatalf("ReadAll err = %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("ReadAll = %q, want %q", string(got), "hello")
	}
	if _, err := stream.Write([]byte("x")); !errors.Is(err, ErrWriteClosed) {
		t.Fatalf("post-final write err = %v, want %v", err, ErrWriteClosed)
	}
}

func TestWritevFinalUni(t *testing.T) {
	t.Parallel()

	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	acceptCh := acceptUniStreamAsync(ctx, server)

	stream, err := client.OpenUniStream(ctx)
	if err != nil {
		t.Fatalf("open uni stream: %v", err)
	}
	n, err := stream.WritevFinal([]byte("he"), []byte("llo"))
	if err != nil || n != 5 {
		t.Fatalf("WritevFinal = (%d, %v), want (5, nil)", n, err)
	}

	accepted := requireAcceptedUniStream(t, acceptCh)
	got, err := io.ReadAll(accepted)
	if err != nil {
		t.Fatalf("ReadAll err = %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("ReadAll = %q, want %q", string(got), "hello")
	}
	if _, err := stream.Write([]byte("x")); !errors.Is(err, ErrWriteClosed) {
		t.Fatalf("post-final write err = %v, want %v", err, ErrWriteClosed)
	}
}

func TestOpenAndSend(t *testing.T) {
	t.Parallel()

	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	acceptCh := acceptStreamAsync(ctx, server)

	stream, n, err := client.OpenAndSend(ctx, []byte("hello"))
	if err != nil || n != 5 {
		t.Fatalf("OpenAndSend = (%v, %d, %v), want (stream, 5, nil)", stream, n, err)
	}
	if stream == nil || stream.StreamID() == 0 {
		t.Fatalf("OpenAndSend stream = %#v, want committed stream", stream)
	}

	accepted := requireAcceptedStream(t, acceptCh)
	buf := make([]byte, 5)
	if _, err := io.ReadFull(accepted, buf); err != nil {
		t.Fatalf("ReadFull err = %v", err)
	}
	if string(buf) != "hello" {
		t.Fatalf("payload = %q, want %q", string(buf), "hello")
	}
}

func TestOpenUniAndSendWithOptionsPreservesOpenInfo(t *testing.T) {
	t.Parallel()

	clientCfg := DefaultConfig()
	serverCfg := DefaultConfig()
	clientCfg.Capabilities |= CapabilityOpenMetadata
	serverCfg.Capabilities |= CapabilityOpenMetadata
	client, server := newConnPairWithConfig(t, clientCfg, serverCfg)
	ctx, cancel := testContext(t)
	defer cancel()

	acceptCh := acceptUniStreamAsync(ctx, server)

	opts := OpenOptions{OpenInfo: []byte("ssh")}
	stream, n, err := client.OpenUniAndSendWithOptions(ctx, opts, []byte("hello"))
	if err != nil || n != 5 {
		t.Fatalf("OpenUniAndSendWithOptions = (%v, %d, %v), want (stream, 5, nil)", stream, n, err)
	}
	if stream == nil || stream.StreamID() == 0 {
		t.Fatalf("OpenUniAndSendWithOptions stream = %#v, want committed stream", stream)
	}

	accepted := requireAcceptedUniStream(t, acceptCh)
	if got := accepted.OpenInfo(); !bytes.Equal(got, []byte("ssh")) {
		t.Fatalf("accepted OpenInfo = %q, want %q", string(got), "ssh")
	}
	got, err := io.ReadAll(accepted)
	if err != nil {
		t.Fatalf("ReadAll err = %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("ReadAll = %q, want %q", string(got), "hello")
	}
}

func TestOpenAndSendHonorsContextCancellationBeforeOpen(t *testing.T) {
	t.Parallel()

	client, _ := newConnPair(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	stream, n, err := client.OpenAndSend(ctx, []byte("hello"))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("OpenAndSend err = %v, want context.Canceled", err)
	}
	if stream != nil || n != 0 {
		t.Fatalf("OpenAndSend = (%v, %d, %v), want (nil, 0, canceled)", stream, n, err)
	}
}

func BenchmarkStreamUpdateMetadataOpenerReuse(b *testing.B) {
	conn := newMetadataBenchConn()
	priority := uint64(7)
	group := uint64(9)
	stream := conn.newProvisionalLocalStreamOwnedLocked(streamArityBidi, OpenOptions{
		InitialPriority: uint64ptr(priority),
		InitialGroup:    uint64ptr(group),
		OpenInfo:        []byte("ssh"),
	}, nil)

	updatePriority := uint64(11)
	updateGroup := uint64(13)
	update := MetadataUpdate{Priority: &updatePriority, Group: &updateGroup}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		updatePriority++
		updateGroup++
		if err := stream.UpdateMetadata(update); err != nil {
			b.Fatalf("UpdateMetadata: %v", err)
		}
	}
}

func BenchmarkStreamUpdateMetadataPendingReuse(b *testing.B) {
	conn := newMetadataBenchConn()
	stream := conn.newLocalStreamWithIDLocked(state.FirstLocalStreamID(RoleInitiator, true), streamArityBidi, OpenOptions{}, nil)
	testMarkLocalOpenCommitted(stream)
	conn.registry.streams[stream.id] = stream

	updatePriority := uint64(7)
	updateGroup := uint64(9)
	update := MetadataUpdate{Priority: &updatePriority, Group: &updateGroup}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		updatePriority++
		updateGroup++
		if err := stream.UpdateMetadata(update); err != nil {
			b.Fatalf("UpdateMetadata: %v", err)
		}
	}
}

func newMetadataBenchConn() *Conn {
	settings := DefaultSettings()
	conn := &Conn{

		pending: connPendingControlState{controlNotify: make(chan struct{}, 1)},
		sessionControl: connSessionControlState{
			peerGoAwayBidi:  MaxVarint62,
			peerGoAwayUni:   MaxVarint62,
			localGoAwayBidi: MaxVarint62,
			localGoAwayUni:  MaxVarint62,
		},
		lifecycle: connLifecycleState{sessionState: connStateReady, closedCh: make(chan struct{})}, config: connConfigState{local: Preface{Settings: settings},
			peer:       Preface{Settings: settings},
			negotiated: Negotiated{Capabilities: CapabilityOpenMetadata | CapabilityPriorityUpdate | CapabilityPriorityHints | CapabilityStreamGroups, LocalRole: RoleInitiator, PeerRole: RoleResponder}}, registry: connRegistryState{streams: make(map[uint64]*nativeStream)},
	}
	conn.bootstrapRuntimeQueuesLocked()
	return conn
}

func TestTransportWrongSideBlockedOnLocalSendOnlyUniAbortsStreamState(t *testing.T) {
	t.Parallel()
	client, server, clientStream, serverStream := openServerToClientUniPair(t)

	if err := testQueueFrame(client, Frame{
		Type:     FrameTypeBLOCKED,
		StreamID: serverStream.StreamID(),
		Payload:  mustEncodeVarint(64),
	}); err != nil {
		t.Fatalf("queue BLOCKED on server-owned uni: %v", err)
	}

	waitForAbortCode(t, client, clientStream, CodeStreamState)
	assertSessionStillOpen(t, client)
	assertSessionStillOpen(t, server)
}

func TestTransportWrongSideResetOnLocalSendOnlyUniAbortsStreamState(t *testing.T) {
	t.Parallel()
	client, server, clientStream, serverStream := openServerToClientUniPair(t)

	if err := testQueueFrame(client, Frame{
		Type:     FrameTypeRESET,
		StreamID: serverStream.StreamID(),
		Payload:  mustEncodeVarint(uint64(CodeCancelled)),
	}); err != nil {
		t.Fatalf("queue RESET on server-owned uni: %v", err)
	}

	waitForAbortCode(t, client, clientStream, CodeStreamState)
	assertSessionStillOpen(t, client)
	assertSessionStillOpen(t, server)
}

func TestTransportWrongSideMaxDataOnLocalRecvOnlyUniAbortsStreamState(t *testing.T) {
	t.Parallel()
	client, server, clientStream, _ := openClientToServerUniPair(t)

	if err := testQueueFrame(client, Frame{
		Type:     FrameTypeMAXDATA,
		StreamID: clientStream.StreamID(),
		Payload:  mustEncodeVarint(64),
	}); err != nil {
		t.Fatalf("queue MAX_DATA on client-owned uni: %v", err)
	}

	waitForAbortCode(t, client, clientStream, CodeStreamState)
	assertSessionStillOpen(t, client)
	assertSessionStillOpen(t, server)
}

func TestTransportWrongSideStopSendingOnLocalRecvOnlyUniAbortsStreamState(t *testing.T) {
	t.Parallel()
	client, server, clientStream, _ := openClientToServerUniPair(t)

	if err := testQueueFrame(client, Frame{
		Type:     FrameTypeStopSending,
		StreamID: clientStream.StreamID(),
		Payload:  mustEncodeVarint(uint64(CodeCancelled)),
	}); err != nil {
		t.Fatalf("queue STOP_SENDING on client-owned uni: %v", err)
	}

	waitForAbortCode(t, client, clientStream, CodeStreamState)
	assertSessionStillOpen(t, client)
	assertSessionStillOpen(t, server)
}

func openServerToClientUniPair(t *testing.T) (*Conn, *Conn, *nativeRecvStream, *nativeSendStream) {
	t.Helper()

	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	t.Cleanup(cancel)

	acceptCh := make(chan *nativeRecvStream, 1)
	acceptErrCh := make(chan error, 1)
	go func() {
		stream, err := client.AcceptUniStream(ctx)
		if err != nil {
			acceptErrCh <- err
			return
		}
		acceptCh <- stream
	}()

	serverStream, err := server.OpenUniStream(ctx)
	if err != nil {
		t.Fatalf("server open uni stream: %v", err)
	}
	if _, err := serverStream.Write([]byte("hi")); err != nil {
		t.Fatalf("server write uni stream: %v", err)
	}

	select {
	case err := <-acceptErrCh:
		t.Fatalf("client accept uni failed: %v", err)
	case clientStream := <-acceptCh:
		return client, server, clientStream, serverStream
	case <-ctx.Done():
		t.Fatal("timed out waiting for client uni accept")
		return nil, nil, nil, nil
	}

	return nil, nil, nil, nil
}

func openClientToServerUniPair(t *testing.T) (*Conn, *Conn, *nativeSendStream, *nativeRecvStream) {
	t.Helper()

	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	t.Cleanup(cancel)

	acceptCh := make(chan *nativeRecvStream, 1)
	acceptErrCh := make(chan error, 1)
	go func() {
		stream, err := server.AcceptUniStream(ctx)
		if err != nil {
			acceptErrCh <- err
			return
		}
		acceptCh <- stream
	}()

	clientStream, err := client.OpenUniStream(ctx)
	if err != nil {
		t.Fatalf("client open uni stream: %v", err)
	}
	if _, err := clientStream.Write([]byte("hi")); err != nil {
		t.Fatalf("client write uni stream: %v", err)
	}

	select {
	case err := <-acceptErrCh:
		t.Fatalf("server accept uni failed: %v", err)
	case serverStream := <-acceptCh:
		return client, server, clientStream, serverStream
	case <-ctx.Done():
		t.Fatal("timed out waiting for server uni accept")
		return nil, nil, nil, nil
	}

	return nil, nil, nil, nil
}

func waitForAbortCode(t *testing.T, conn *Conn, stream any, want ErrorCode) {
	t.Helper()

	deadline := time.Now().Add(testSignalTimeout)
	switch s := stream.(type) {
	case *nativeRecvStream:
		awaitStreamReadState(t, s.stream, time.Until(deadline), func(stream *nativeStream) bool {
			return stream.recvAbort != nil
		}, "timed out waiting for expected recv abort")

		conn.mu.Lock()
		recvAbort := s.stream.recvAbort
		conn.mu.Unlock()
		if recvAbort == nil {
			t.Fatal("recv abort missing after wait")
		}
		if recvAbort.Code != uint64(want) {
			t.Fatalf("recv abort code = %d, want %d", recvAbort.Code, uint64(want))
		}
	case *nativeSendStream:
		awaitStreamWriteState(t, s.stream, time.Until(deadline), func(stream *nativeStream) bool {
			return stream.sendAbort != nil
		}, "timed out waiting for expected send abort")

		conn.mu.Lock()
		sendAbort := s.stream.sendAbort
		conn.mu.Unlock()
		if sendAbort == nil {
			t.Fatal("send abort missing after wait")
		}
		if sendAbort.Code != uint64(want) {
			t.Fatalf("send abort code = %d, want %d", sendAbort.Code, uint64(want))
		}
	default:
		t.Fatalf("unsupported stream view type %T", stream)
	}
}

func assertSessionStillOpen(t *testing.T, conn *Conn) {
	t.Helper()

	select {
	case <-conn.lifecycle.closedCh:
		if err := conn.err(); err != nil {
			t.Fatalf("session unexpectedly closed: %v", err)
		}
		t.Fatal("session unexpectedly closed")
	default:
	}
}

func requireStructuredError(t *testing.T, err error) *Error {
	t.Helper()
	se, ok := AsStructuredError(err)
	if !ok {
		t.Fatalf("err = %v, want structured *Error", err)
	}
	return se
}

func TestStructuredErrorAfterLocalCloseRead(t *testing.T) {
	t.Parallel()

	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	acceptCh := make(chan *nativeStream, 1)
	go func() {
		stream, err := server.AcceptStream(ctx)
		if err != nil {
			t.Errorf("accept stream: %v", err)
			return
		}
		acceptCh <- stream
	}()

	clientStream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if _, err := clientStream.Write([]byte("x")); err != nil {
		t.Fatalf("initial write: %v", err)
	}

	serverStream := <-acceptCh
	buf := make([]byte, 8)
	if _, err := serverStream.Read(buf); err != nil {
		t.Fatalf("server initial read: %v", err)
	}

	if err := serverStream.CloseRead(); err != nil {
		t.Fatalf("CloseRead: %v", err)
	}

	_, err = serverStream.Read(buf)
	if !errors.Is(err, ErrReadClosed) {
		t.Fatalf("read after CloseRead err = %v, want %v", err, ErrReadClosed)
	}

	se := requireStructuredError(t, err)
	if se.Scope != ScopeStream || se.Operation != OperationRead || se.Source != SourceLocal ||
		se.Direction != DirectionRead || se.TerminationKind != TerminationStopped {
		t.Fatalf("structured error = %+v", *se)
	}
}

func TestStructuredErrorAfterPeerReset(t *testing.T) {
	t.Parallel()

	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	acceptCh := make(chan *nativeStream, 1)
	go func() {
		stream, err := server.AcceptStream(ctx)
		if err != nil {
			t.Errorf("accept stream: %v", err)
			return
		}
		acceptCh <- stream
	}()

	clientStream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if _, err := clientStream.Write([]byte("x")); err != nil {
		t.Fatalf("initial write: %v", err)
	}

	serverStream := <-acceptCh
	buf := make([]byte, 8)
	if _, err := serverStream.Read(buf); err != nil {
		t.Fatalf("server initial read: %v", err)
	}

	if err := serverStream.Reset(uint64(CodeCancelled)); err != nil {
		t.Fatalf("Reset: %v", err)
	}

	_, err = clientStream.Read(buf)
	var appErr *ApplicationError
	if !errors.As(err, &appErr) {
		t.Fatalf("read err = %v, want ApplicationError", err)
	}
	if appErr.Code != uint64(CodeCancelled) {
		t.Fatalf("read code = %d, want %d", appErr.Code, uint64(CodeCancelled))
	}

	se := requireStructuredError(t, err)
	if se.Scope != ScopeStream || se.Operation != OperationRead || se.Source != SourceRemote ||
		se.Direction != DirectionRead || se.TerminationKind != TerminationReset ||
		se.WireCode != uint64(CodeCancelled) {
		t.Fatalf("structured error = %+v", *se)
	}
}

func TestStructuredSessionErrorAfterPeerCloseOpen(t *testing.T) {
	t.Parallel()

	client, _, stop := newHandlerTestConn(t)
	defer stop()

	if err := client.handleCloseFrame(Frame{
		Type:    FrameTypeCLOSE,
		Payload: mustClosePayload(t, uint64(CodeInternal), "peer closing"),
	}); err == nil {
		t.Fatal("handleCloseFrame = nil, want non-nil session-close error")
	}

	ctx, cancel := testContext(t)
	defer cancel()
	_, err := client.OpenStream(ctx)
	if err == nil {
		t.Fatal("OpenStream after peer close = nil, want error")
	}

	var appErr *ApplicationError
	if !errors.As(err, &appErr) {
		t.Fatalf("open err = %v, want ApplicationError", err)
	}
	if appErr.Code != uint64(CodeInternal) || appErr.Reason != "peer closing" {
		t.Fatalf("open application error = %+v, want code=%d reason=%q", *appErr, uint64(CodeInternal), "peer closing")
	}

	se := requireStructuredError(t, err)
	if se.Scope != ScopeSession || se.Operation != OperationOpen || se.Source != SourceRemote ||
		se.Direction != DirectionBoth || se.TerminationKind != TerminationSessionTermination ||
		se.WireCode != uint64(CodeInternal) || se.ReasonText != "peer closing" {
		t.Fatalf("structured error = %+v", *se)
	}
}

func TestStructuredOpenErrorForOpenInfoUnavailable(t *testing.T) {
	t.Parallel()

	client, _ := newConnPairWithConfig(t, nil, nil)

	_, err := client.OpenStreamWithOptions(context.Background(), OpenOptions{OpenInfo: []byte("need-metadata")})
	if !errors.Is(err, ErrOpenInfoUnavailable) {
		t.Fatalf("OpenStreamWithOptions err = %v, want %v", err, ErrOpenInfoUnavailable)
	}

	se := requireStructuredError(t, err)
	if se.Scope != ScopeSession || se.Operation != OperationOpen || se.Source != SourceLocal ||
		se.Direction != DirectionBoth || se.TerminationKind != TerminationUnknown {
		t.Fatalf("structured error = %+v", *se)
	}
}

func TestStructuredOpenErrorAfterPeerGoAway(t *testing.T) {
	t.Parallel()

	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	if err := server.GoAway(0, 0); err != nil {
		t.Fatalf("server GoAway: %v", err)
	}

	awaitPeerGoAwayBidi(t, client, 0)

	_, err := client.OpenStream(ctx)
	var appErr *ApplicationError
	if !errors.As(err, &appErr) {
		t.Fatalf("open after GOAWAY err = %v, want ApplicationError", err)
	}
	if appErr.Code != uint64(CodeRefusedStream) {
		t.Fatalf("open after GOAWAY code = %d, want %d", appErr.Code, uint64(CodeRefusedStream))
	}

	se := requireStructuredError(t, err)
	if se.Scope != ScopeSession || se.Operation != OperationOpen || se.Source != SourceRemote ||
		se.Direction != DirectionBoth || se.TerminationKind != TerminationUnknown ||
		se.WireCode != uint64(CodeRefusedStream) {
		t.Fatalf("structured error = %+v", *se)
	}
}

func TestStructuredErrorAfterLocalCloseWrite(t *testing.T) {
	t.Parallel()

	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	acceptCh := make(chan *nativeStream, 1)
	go func() {
		stream, err := server.AcceptStream(ctx)
		if err != nil {
			t.Errorf("accept stream: %v", err)
			return
		}
		acceptCh <- stream
	}()

	clientStream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if _, err := clientStream.Write([]byte("x")); err != nil {
		t.Fatalf("initial write: %v", err)
	}

	serverStream := <-acceptCh
	buf := make([]byte, 8)
	if _, err := serverStream.Read(buf); err != nil {
		t.Fatalf("server initial read: %v", err)
	}

	if err := clientStream.CloseWrite(); err != nil {
		t.Fatalf("client CloseWrite: %v", err)
	}

	_, err = clientStream.Write([]byte("y"))
	if !errors.Is(err, ErrWriteClosed) {
		t.Fatalf("write after CloseWrite err = %v, want %v", err, ErrWriteClosed)
	}

	se := requireStructuredError(t, err)
	if se.Scope != ScopeStream || se.Operation != OperationWrite || se.Source != SourceLocal ||
		se.Direction != DirectionWrite || se.TerminationKind != TerminationGraceful {
		t.Fatalf("structured error = %+v", *se)
	}
}

func TestStructuredErrorAfterPeerStopWrite(t *testing.T) {
	t.Parallel()

	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	stream := seedStateFixtureStream(t, c, state.FirstLocalStreamID(c.config.negotiated.LocalRole, true), "bidi", "local_owned", stateHalfExpect{
		SendHalf: "send_stop_seen",
		RecvHalf: "recv_open",
	})
	testMarkLocalOpenCommitted(stream)

	_, err := stream.Write([]byte("x"))
	if !errors.Is(err, ErrWriteClosed) {
		t.Fatalf("write on send_stop_seen err = %v, want %v", err, ErrWriteClosed)
	}

	se := requireStructuredError(t, err)
	if se.Scope != ScopeStream || se.Operation != OperationWrite || se.Source != SourceRemote ||
		se.Direction != DirectionWrite || se.TerminationKind != TerminationStopped {
		t.Fatalf("structured error = %+v", *se)
	}
}

func TestStructuredErrorAfterUpdateMetadataSendFin(t *testing.T) {
	t.Parallel()

	caps := CapabilityPriorityUpdate | CapabilityPriorityHints
	c := &Conn{

		pending: connPendingControlState{controlNotify: make(chan struct{}, 1)}, config: connConfigState{negotiated: Negotiated{Capabilities: caps},
			peer: Preface{Settings: Settings{
				MaxExtensionPayloadBytes: 4096,
				MaxFramePayload:          4096,
			}}},
	}
	s := testBuildDetachedStream(c, 11,
		testWithBidi(),
		testWithLocalOpen(testLocalOpenOpenedCommittedState()),
		testWithLocalSend(),
		testWithLocalReceive(),
	)
	s.setSendFin()

	err := s.UpdateMetadata(MetadataUpdate{Priority: uint64ptr(7)})
	if !errors.Is(err, ErrWriteClosed) {
		t.Fatalf("UpdateMetadata err = %v, want %v", err, ErrWriteClosed)
	}

	se := requireStructuredError(t, err)
	if se.Scope != ScopeStream || se.Operation != OperationWrite || se.Source != SourceLocal ||
		se.Direction != DirectionWrite || se.TerminationKind != TerminationGraceful {
		t.Fatalf("structured error = %+v", *se)
	}
}

func TestStructuredErrorAfterUpdateMetadataSendStopSeen(t *testing.T) {
	t.Parallel()

	caps := CapabilityPriorityUpdate | CapabilityPriorityHints
	c := &Conn{

		pending: connPendingControlState{controlNotify: make(chan struct{}, 1)}, config: connConfigState{negotiated: Negotiated{Capabilities: caps},
			peer: Preface{Settings: Settings{
				MaxExtensionPayloadBytes: 4096,
				MaxFramePayload:          4096,
			}}},
	}
	s := testBuildDetachedStream(c, 11,
		testWithBidi(),
		testWithLocalOpen(testLocalOpenOpenedCommittedState()),
		testWithLocalSend(),
		testWithLocalReceive(),
	)
	s.setSendStopSeen(&ApplicationError{Code: uint64(CodeCancelled)})

	err := s.UpdateMetadata(MetadataUpdate{Priority: uint64ptr(7)})
	if !errors.Is(err, ErrWriteClosed) {
		t.Fatalf("UpdateMetadata err = %v, want %v", err, ErrWriteClosed)
	}

	se := requireStructuredError(t, err)
	if se.Scope != ScopeStream || se.Operation != OperationWrite || se.Source != SourceRemote ||
		se.Direction != DirectionWrite || se.TerminationKind != TerminationStopped {
		t.Fatalf("structured error = %+v", *se)
	}
}

func TestStructuredSessionErrorAfterPeerCloseUpdateMetadata(t *testing.T) {
	t.Parallel()

	peerClose := &ApplicationError{Code: uint64(CodeInternal), Reason: "peer closing"}
	c := &Conn{
		lifecycle: connLifecycleState{closeErr: peerClose},
		sessionControl: connSessionControlState{
			peerCloseErr: &ApplicationError{Code: peerClose.Code, Reason: peerClose.Reason},
		},
	}
	c.sessionControl.peerCloseView.Store(cloneApplicationError(peerClose))
	s := testBuildDetachedStream(c, 0, testWithLocalSend(), testWithLocalReceive())

	err := s.UpdateMetadata(MetadataUpdate{Priority: uint64ptr(5)})
	appErr := testApplicationError(err)
	if appErr == nil {
		t.Fatalf("UpdateMetadata err = %v, want ApplicationError", err)
	}
	if appErr.Code != peerClose.Code || appErr.Reason != peerClose.Reason {
		t.Fatalf("UpdateMetadata app error = %+v, want code=%d reason=%q", *appErr, peerClose.Code, peerClose.Reason)
	}

	se := requireStructuredError(t, err)
	if se.Scope != ScopeSession || se.Operation != OperationWrite || se.Source != SourceRemote ||
		se.Direction != DirectionBoth || se.TerminationKind != TerminationSessionTermination ||
		se.WireCode != peerClose.Code || se.ReasonText != peerClose.Reason {
		t.Fatalf("structured error = %+v", *se)
	}
}

func TestStructuredErrorUpdateMetadataUnavailable(t *testing.T) {
	t.Parallel()

	c := &Conn{

		pending: connPendingControlState{controlNotify: make(chan struct{}, 1)}, config: connConfigState{negotiated: Negotiated{Capabilities: CapabilityPriorityHints},
			peer: Preface{Settings: Settings{
				MaxExtensionPayloadBytes: 4096,
			}}},
	}
	s := testBuildDetachedStream(c, 11,
		testWithBidi(),
		testWithLocalOpen(testLocalOpenOpenedCommittedState()),
		testWithLocalSend(),
		testWithLocalReceive(),
	)

	err := s.UpdateMetadata(MetadataUpdate{Priority: uint64ptr(3)})
	if !errors.Is(err, ErrPriorityUpdateUnavailable) {
		t.Fatalf("UpdateMetadata err = %v, want %v", err, ErrPriorityUpdateUnavailable)
	}

	se := requireStructuredError(t, err)
	if se.Scope != ScopeStream || se.Operation != OperationWrite || se.Source != SourceLocal ||
		se.Direction != DirectionWrite || se.TerminationKind != TerminationUnknown {
		t.Fatalf("structured error = %+v", *se)
	}
}

func TestStructuredErrorAfterExpiredProvisionalWrite(t *testing.T) {
	t.Parallel()

	client, _ := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	first, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open provisional stream: %v", err)
	}
	client.mu.Lock()
	first.setProvisionalCreated(time.Now().Add(-provisionalOpenMaxAge - time.Second))
	client.mu.Unlock()

	_, err = first.Write([]byte("x"))
	var appErr *ApplicationError
	if !errors.As(err, &appErr) {
		t.Fatalf("expired provisional write err = %v, want ApplicationError", err)
	}
	if appErr.Code != uint64(CodeCancelled) || appErr.Reason != ErrOpenExpired.Error() {
		t.Fatalf("expired provisional write app error = %+v, want code=%d reason=%q", *appErr, uint64(CodeCancelled), ErrOpenExpired.Error())
	}

	se := requireStructuredError(t, err)
	if se.Scope != ScopeStream || se.Operation != OperationWrite || se.Source != SourceLocal ||
		se.Direction != DirectionBoth || se.TerminationKind != TerminationAbort ||
		se.WireCode != uint64(CodeCancelled) || se.ReasonText != ErrOpenExpired.Error() {
		t.Fatalf("structured error = %+v", *se)
	}
}

func TestStructuredErrorAfterPeerGoAwayOnProvisionalWrite(t *testing.T) {
	t.Parallel()

	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	stream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open provisional stream: %v", err)
	}

	if err := server.GoAway(0, 0); err != nil {
		t.Fatalf("server GoAway: %v", err)
	}

	awaitPeerGoAwayBidi(t, client, 0)

	_, err = stream.Write([]byte("x"))
	var appErr *ApplicationError
	if !errors.As(err, &appErr) {
		t.Fatalf("provisional write after GOAWAY err = %v, want ApplicationError", err)
	}
	if appErr.Code != uint64(CodeRefusedStream) {
		t.Fatalf("provisional write after GOAWAY code = %d, want %d", appErr.Code, uint64(CodeRefusedStream))
	}
	if got := stream.StreamID(); got != 0 {
		t.Fatalf("provisional stream id after GOAWAY write = %d, want 0", got)
	}

	se := requireStructuredError(t, err)
	if se.Scope != ScopeStream || se.Operation != OperationWrite || se.Source != SourceRemote ||
		se.Direction != DirectionBoth || se.TerminationKind != TerminationAbort ||
		se.WireCode != uint64(CodeRefusedStream) {
		t.Fatalf("structured error = %+v", *se)
	}
}

func TestStructuredErrorAfterPeerGoAwayReclaimsCommittedUnseenLocalWrite(t *testing.T) {
	t.Parallel()

	c, _, stop := newHandlerTestConn(t)
	defer stop()

	c.mu.Lock()
	stream := c.newLocalStreamWithIDLocked(state.FirstLocalStreamID(c.config.negotiated.LocalRole, true), streamArityBidi, OpenOptions{}, nil)
	testMarkLocalOpenCommitted(stream)
	c.registry.streams[stream.id] = stream
	c.sessionControl.peerGoAwayBidi = 0
	c.sessionControl.peerGoAwayUni = 0
	c.reclaimUnseenLocalStreamsLocked()
	c.mu.Unlock()

	_, err := stream.Write([]byte("x"))
	var appErr *ApplicationError
	if !errors.As(err, &appErr) {
		t.Fatalf("write after GOAWAY reclaim err = %v, want ApplicationError", err)
	}
	if appErr.Code != uint64(CodeRefusedStream) {
		t.Fatalf("write after GOAWAY reclaim code = %d, want %d", appErr.Code, uint64(CodeRefusedStream))
	}

	se := requireStructuredError(t, err)
	if se.Scope != ScopeStream || se.Operation != OperationWrite || se.Source != SourceRemote ||
		se.Direction != DirectionBoth || se.TerminationKind != TerminationAbort ||
		se.WireCode != uint64(CodeRefusedStream) {
		t.Fatalf("structured error = %+v", *se)
	}
}

func TestStructuredErrorAfterWriteOpenMetadataTooLarge(t *testing.T) {
	t.Parallel()

	c := &Conn{

		pending: connPendingControlState{controlNotify: make(chan struct{}, 1)}, config: connConfigState{peer: Preface{Settings: Settings{
			MaxFramePayload: 4,
		}}}, registry: connRegistryState{streams: make(map[uint64]*nativeStream)},
	}
	s := testOpenedBidiStream(
		c,
		state.FirstLocalStreamID(c.config.negotiated.LocalRole, true),
		func(stream *nativeStream) {
			stream.openMetadataPrefix = []byte("oversized")
		},
	)

	_, err := s.Write([]byte("x"))
	if !errors.Is(err, ErrOpenMetadataTooLarge) {
		t.Fatalf("Write err = %v, want %v", err, ErrOpenMetadataTooLarge)
	}

	se := requireStructuredError(t, err)
	if se.Scope != ScopeStream || se.Operation != OperationWrite || se.Source != SourceLocal ||
		se.Direction != DirectionWrite || se.TerminationKind != TerminationUnknown {
		t.Fatalf("structured error = %+v", *se)
	}
}

func TestStructuredErrorAfterLocalAbortRead(t *testing.T) {
	t.Parallel()

	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	acceptCh := make(chan *nativeStream, 1)
	go func() {
		stream, err := server.AcceptStream(ctx)
		if err != nil {
			t.Errorf("accept stream: %v", err)
			return
		}
		acceptCh <- stream
	}()

	clientStream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if _, err := clientStream.Write([]byte("x")); err != nil {
		t.Fatalf("initial write: %v", err)
	}

	serverStream := <-acceptCh
	buf := make([]byte, 8)
	if _, err := serverStream.Read(buf); err != nil {
		t.Fatalf("server initial read: %v", err)
	}

	if err := clientStream.CloseWithErrorCode(uint64(CodeInternal), "local abort"); err != nil {
		t.Fatalf("client abort: %v", err)
	}

	_, err = clientStream.Read(buf)
	var appErr *ApplicationError
	if !errors.As(err, &appErr) {
		t.Fatalf("read err = %v, want ApplicationError", err)
	}
	if appErr.Code != uint64(CodeInternal) || appErr.Reason != "local abort" {
		t.Fatalf("read app error = (%d, %q), want (%d, %q)", appErr.Code, appErr.Reason, uint64(CodeInternal), "local abort")
	}

	se := requireStructuredError(t, err)
	if se.Scope != ScopeStream || se.Operation != OperationRead || se.Source != SourceLocal ||
		se.Direction != DirectionBoth || se.TerminationKind != TerminationAbort ||
		se.WireCode != uint64(CodeInternal) || se.ReasonText != "local abort" {
		t.Fatalf("structured error = %+v", *se)
	}
}

func TestStructuredErrorAfterPeerAbortWrite(t *testing.T) {
	t.Parallel()

	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	acceptCh := make(chan *nativeStream, 1)
	go func() {
		stream, err := server.AcceptStream(ctx)
		if err != nil {
			t.Errorf("accept stream: %v", err)
			return
		}
		acceptCh <- stream
	}()

	clientStream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if _, err := clientStream.Write([]byte("x")); err != nil {
		t.Fatalf("initial write: %v", err)
	}

	serverStream := <-acceptCh
	buf := make([]byte, 8)
	if _, err := serverStream.Read(buf); err != nil {
		t.Fatalf("server initial read: %v", err)
	}

	if err := serverStream.CloseWithErrorCode(uint64(CodeRefusedStream), "peer abort"); err != nil {
		t.Fatalf("server abort: %v", err)
	}

	awaitStreamWriteState(t, clientStream, testSignalTimeout, func(s *nativeStream) bool {
		return s.sendAbort != nil || s.recvAbort != nil
	}, "timed out waiting for peer abort to be observed before Write")

	_, err = clientStream.Write([]byte("y"))
	var appErr *ApplicationError
	if !errors.As(err, &appErr) {
		t.Fatalf("write err = %v, want ApplicationError", err)
	}
	if appErr.Code != uint64(CodeRefusedStream) || appErr.Reason != "peer abort" {
		t.Fatalf("write app error = (%d, %q), want (%d, %q)", appErr.Code, appErr.Reason, uint64(CodeRefusedStream), "peer abort")
	}

	se := requireStructuredError(t, err)
	if se.Scope != ScopeStream || se.Operation != OperationWrite || se.Source != SourceRemote ||
		se.Direction != DirectionBoth || se.TerminationKind != TerminationAbort ||
		se.WireCode != uint64(CodeRefusedStream) || se.ReasonText != "peer abort" {
		t.Fatalf("structured error = %+v", *se)
	}
}

func TestStructuredErrorAfterRepeatedLocalCloseRead(t *testing.T) {
	t.Parallel()

	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	stream := seedStateFixtureStream(t, c, state.FirstLocalStreamID(c.config.negotiated.LocalRole, true), "bidi", "local_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_stop_sent",
	})
	testMarkLocalOpenCommitted(stream)

	err := stream.CloseRead()
	if !errors.Is(err, ErrReadClosed) {
		t.Fatalf("CloseRead on recv_stop_sent err = %v, want %v", err, ErrReadClosed)
	}

	se := requireStructuredError(t, err)
	if se.Scope != ScopeStream || se.Operation != OperationClose || se.Source != SourceLocal ||
		se.Direction != DirectionRead || se.TerminationKind != TerminationStopped {
		t.Fatalf("structured error = %+v", *se)
	}
}

func TestStructuredErrorAfterPeerFinCloseRead(t *testing.T) {
	t.Parallel()

	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	acceptCh := make(chan *nativeStream, 1)
	go func() {
		stream, err := server.AcceptStream(ctx)
		if err != nil {
			t.Errorf("accept stream: %v", err)
			return
		}
		acceptCh <- stream
	}()

	clientStream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if _, err := clientStream.Write([]byte("x")); err != nil {
		t.Fatalf("initial write: %v", err)
	}
	if err := clientStream.CloseWrite(); err != nil {
		t.Fatalf("client CloseWrite: %v", err)
	}

	serverStream := <-acceptCh
	buf := make([]byte, 8)
	if _, err := serverStream.Read(buf); err != nil {
		t.Fatalf("server initial read: %v", err)
	}
	if _, err := serverStream.Read(buf); !errors.Is(err, io.EOF) {
		t.Fatalf("server EOF read err = %v, want %v", err, io.EOF)
	}

	err = serverStream.CloseRead()
	if !errors.Is(err, ErrReadClosed) {
		t.Fatalf("CloseRead after peer FIN err = %v, want %v", err, ErrReadClosed)
	}

	se := requireStructuredError(t, err)
	if se.Scope != ScopeStream || se.Operation != OperationClose || se.Source != SourceRemote ||
		se.Direction != DirectionRead || se.TerminationKind != TerminationGraceful {
		t.Fatalf("structured error = %+v", *se)
	}
}

func TestStructuredErrorAfterRepeatedLocalCloseWrite(t *testing.T) {
	t.Parallel()

	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	acceptCh := make(chan *nativeStream, 1)
	go func() {
		stream, err := server.AcceptStream(ctx)
		if err != nil {
			t.Errorf("accept stream: %v", err)
			return
		}
		acceptCh <- stream
	}()

	clientStream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if _, err := clientStream.Write([]byte("x")); err != nil {
		t.Fatalf("initial write: %v", err)
	}

	serverStream := <-acceptCh
	buf := make([]byte, 8)
	if _, err := serverStream.Read(buf); err != nil {
		t.Fatalf("server initial read: %v", err)
	}

	if err := clientStream.CloseWrite(); err != nil {
		t.Fatalf("first CloseWrite: %v", err)
	}

	err = clientStream.CloseWrite()
	if !errors.Is(err, ErrWriteClosed) {
		t.Fatalf("second CloseWrite err = %v, want %v", err, ErrWriteClosed)
	}

	se := requireStructuredError(t, err)
	if se.Scope != ScopeStream || se.Operation != OperationClose || se.Source != SourceLocal ||
		se.Direction != DirectionWrite || se.TerminationKind != TerminationGraceful {
		t.Fatalf("structured error = %+v", *se)
	}
}

func TestStructuredErrorAfterPeerAbortCloseWrite(t *testing.T) {
	t.Parallel()

	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()

	acceptCh := make(chan *nativeStream, 1)
	go func() {
		stream, err := server.AcceptStream(ctx)
		if err != nil {
			t.Errorf("accept stream: %v", err)
			return
		}
		acceptCh <- stream
	}()

	clientStream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if _, err := clientStream.Write([]byte("x")); err != nil {
		t.Fatalf("initial write: %v", err)
	}

	serverStream := <-acceptCh
	buf := make([]byte, 8)
	if _, err := serverStream.Read(buf); err != nil {
		t.Fatalf("server initial read: %v", err)
	}

	if err := serverStream.CloseWithErrorCode(uint64(CodeRefusedStream), "peer abort"); err != nil {
		t.Fatalf("server abort: %v", err)
	}

	awaitStreamWriteState(t, clientStream, testSignalTimeout, func(s *nativeStream) bool {
		return s.sendAbort != nil || s.recvAbort != nil
	}, "timed out waiting for peer abort to be observed before CloseWrite")

	err = clientStream.CloseWrite()
	var appErr *ApplicationError
	if !errors.As(err, &appErr) {
		t.Fatalf("CloseWrite err = %v, want ApplicationError", err)
	}
	if appErr.Code != uint64(CodeRefusedStream) || appErr.Reason != "peer abort" {
		t.Fatalf("CloseWrite app error = (%d, %q), want (%d, %q)", appErr.Code, appErr.Reason, uint64(CodeRefusedStream), "peer abort")
	}

	se := requireStructuredError(t, err)
	if se.Scope != ScopeStream || se.Operation != OperationClose || se.Source != SourceRemote ||
		se.Direction != DirectionBoth || se.TerminationKind != TerminationAbort ||
		se.WireCode != uint64(CodeRefusedStream) || se.ReasonText != "peer abort" {
		t.Fatalf("structured error = %+v", *se)
	}
}

func TestStreamCloseWithErrorCodeTruncatesReasonTextToControlLimit(t *testing.T) {
	t.Parallel()

	c, frames, stop := newHandlerTestConn(t)
	defer stop()
	c.config.peer.Settings.MaxControlPayloadBytes = 16

	stream := testBuildStream(c, 4,
		testWithLocalOpen(testLocalOpenOpenedCommittedState()),
		testWithLocalSend(),
		testWithLocalReceive(),
	)
	c.registry.streams[4] = stream

	reason := "abcdefghijklmnopqrst"
	if err := stream.CloseWithErrorCode(uint64(CodeRefusedStream), reason); err != nil {
		t.Fatalf("close with long reason: %v", err)
	}

	frame := awaitQueuedFrame(t, frames)
	if frame.Type != FrameTypeABORT {
		t.Fatalf("frame type = %s, want ABORT", frame.Type)
	}
	if got := len(frame.Payload); got > 16 {
		t.Fatalf("payload size = %d, want <= 16", got)
	}
	code, parsedReason, err := parseErrorPayload(frame.Payload)
	if err != nil {
		t.Fatalf("parse error payload: %v", err)
	}
	if code != uint64(CodeRefusedStream) {
		t.Fatalf("error code = %d, want %d", code, uint64(CodeRefusedStream))
	}
	if parsedReason != reason[:13] {
		t.Fatalf("reason = %q, want UTF-8-safe truncated %q", parsedReason, reason[:13])
	}
}

func TestStreamResetWithReasonOmitsInvalidUTF8Text(t *testing.T) {
	t.Parallel()

	c, frames, stop := newHandlerTestConn(t)
	defer stop()
	c.config.peer.Settings.MaxControlPayloadBytes = 16

	stream := testBuildStream(c, 4,
		testWithLocalOpen(testLocalOpenOpenedCommittedState()),
		testWithLocalSend(),
		testWithLocalReceive(),
	)
	c.registry.streams[4] = stream

	if err := stream.ResetWithReason(uint64(CodeCancelled), string([]byte{0xe2, 0x82})); err != nil {
		t.Fatalf("reset with invalid reason: %v", err)
	}

	frame := awaitQueuedFrame(t, frames)
	if frame.Type != FrameTypeRESET {
		t.Fatalf("frame type = %s, want RESET", frame.Type)
	}
	if got := len(frame.Payload); got != 1 {
		t.Fatalf("payload size = %d, want 1", got)
	}
	code, parsedReason, err := parseErrorPayload(frame.Payload)
	if err != nil {
		t.Fatalf("parse error payload: %v", err)
	}
	if code != uint64(CodeCancelled) {
		t.Fatalf("error code = %d, want %d", code, uint64(CodeCancelled))
	}
	if parsedReason != "" {
		t.Fatalf("reason = %q, want empty string for invalid utf8", parsedReason)
	}
}

func TestStreamCloseWithErrorCodePreservesUTF8BoundaryWhenTruncating(t *testing.T) {
	t.Parallel()

	c, frames, stop := newHandlerTestConn(t)
	defer stop()
	c.config.peer.Settings.MaxControlPayloadBytes = 11

	stream := testBuildStream(c, 4,
		testWithLocalOpen(testLocalOpenOpenedCommittedState()),
		testWithLocalSend(),
		testWithLocalReceive(),
	)
	c.registry.streams[4] = stream

	reason := "😀😀😀"
	if err := stream.CloseWithErrorCode(uint64(CodeInternal), reason); err != nil {
		t.Fatalf("close with utf8 reason: %v", err)
	}

	frame := awaitQueuedFrame(t, frames)
	if frame.Type != FrameTypeABORT {
		t.Fatalf("frame type = %s, want ABORT", frame.Type)
	}
	code, parsedReason, err := parseErrorPayload(frame.Payload)
	if err != nil {
		t.Fatalf("parse error payload: %v", err)
	}
	if code != uint64(CodeInternal) {
		t.Fatalf("error code = %d, want %d", code, uint64(CodeInternal))
	}
	if parsedReason != "😀😀" {
		t.Fatalf("reason = %q, want %q", parsedReason, "😀😀")
	}
	if len(parsedReason) != 8 {
		t.Fatalf("parsed reason byte length = %d, want 8", len(parsedReason))
	}
}

func TestSessionAbortPayloadTruncatesReasonTextToControlLimit(t *testing.T) {
	t.Parallel()

	c, frames, stop := newHandlerTestConn(t)
	defer stop()
	c.config.peer.Settings.MaxControlPayloadBytes = 16

	reason := "abcdefghijklmnopqrst"
	c.Abort(&ApplicationError{Code: uint64(CodeInternal), Reason: reason})

	frame := awaitQueuedFrame(t, frames)
	if frame.Type != FrameTypeCLOSE {
		t.Fatalf("frame type = %s, want CLOSE", frame.Type)
	}
	if got := len(frame.Payload); got > 16 {
		t.Fatalf("payload size = %d, want <= 16", got)
	}
	code, parsedReason, err := parseErrorPayload(frame.Payload)
	if err != nil {
		t.Fatalf("parse error payload: %v", err)
	}
	if code != uint64(CodeInternal) {
		t.Fatalf("error code = %d, want %d", code, uint64(CodeInternal))
	}
	if parsedReason != reason[:13] {
		t.Fatalf("reason = %q, want UTF-8-safe truncated %q", parsedReason, reason[:13])
	}
}

func TestSessionGoAwayPayloadTruncatesReasonTextToControlLimit(t *testing.T) {
	t.Parallel()

	c, frames, stop := newHandlerTestConn(t)
	defer stop()
	c.config.peer.Settings.MaxControlPayloadBytes = 16

	reason := "abcdefghijklmnopqrst"
	if err := c.GoAwayWithError(0, 0, uint64(CodeInternal), reason); err != nil {
		t.Fatalf("go away with long reason: %v", err)
	}

	frame := awaitQueuedFrame(t, frames)
	if frame.Type != FrameTypeGOAWAY {
		t.Fatalf("frame type = %s, want GOAWAY", frame.Type)
	}
	if got := len(frame.Payload); got > 16 {
		t.Fatalf("payload size = %d, want <= 16", got)
	}
	parsed, err := parseGOAWAYPayload(frame.Payload)
	if err != nil {
		t.Fatalf("parse goaway payload: %v", err)
	}
	if parsed.Code != uint64(CodeInternal) {
		t.Fatalf("goaway code = %d, want %d", parsed.Code, uint64(CodeInternal))
	}
	if parsed.Reason != reason[:11] {
		t.Fatalf("reason = %q, want UTF-8-safe truncated %q", parsed.Reason, reason[:11])
	}
}

func TestSessionGoAwayPayloadOmitsInvalidUTF8ReasonText(t *testing.T) {
	t.Parallel()

	c, frames, stop := newHandlerTestConn(t)
	defer stop()
	c.config.peer.Settings.MaxControlPayloadBytes = 16

	if err := c.GoAwayWithError(0, 0, uint64(CodeInternal), string([]byte{0xe2, 0x82})); err != nil {
		t.Fatalf("go away with invalid reason: %v", err)
	}

	frame := awaitQueuedFrame(t, frames)
	if frame.Type != FrameTypeGOAWAY {
		t.Fatalf("frame type = %s, want GOAWAY", frame.Type)
	}
	if got := len(frame.Payload); got != 1+1+1 {
		t.Fatalf("payload size = %d, want 3", got)
	}
	parsed, err := parseGOAWAYPayload(frame.Payload)
	if err != nil {
		t.Fatalf("parse goaway payload: %v", err)
	}
	if parsed.Reason != "" {
		t.Fatalf("reason = %q, want empty string for invalid utf8", parsed.Reason)
	}
}

func TestSessionAbortPayloadPreservesWireErrorCode(t *testing.T) {
	t.Parallel()

	c, frames, stop := newHandlerTestConn(t)
	defer stop()
	c.config.peer.Settings.MaxControlPayloadBytes = 32

	c.Abort(frameSizeError("close", errors.New("bad frame")))

	frame := awaitQueuedFrame(t, frames)
	if frame.Type != FrameTypeCLOSE {
		t.Fatalf("frame type = %s, want CLOSE", frame.Type)
	}
	code, parsedReason, err := parseErrorPayload(frame.Payload)
	if err != nil {
		t.Fatalf("parse error payload: %v", err)
	}
	if code != uint64(CodeFrameSize) {
		t.Fatalf("error code = %d, want %d", code, uint64(CodeFrameSize))
	}
	if parsedReason == "" {
		t.Fatalf("reason should be preserved from wrapped wire error")
	}
}

func TestSessionAbortPayloadMapsKeepaliveTimeoutToIdleTimeout(t *testing.T) {
	t.Parallel()

	c, frames, stop := newHandlerTestConn(t)
	defer stop()
	c.config.peer.Settings.MaxControlPayloadBytes = 32

	c.Abort(ErrKeepaliveTimeout)

	frame := awaitQueuedFrame(t, frames)
	if frame.Type != FrameTypeCLOSE {
		t.Fatalf("frame type = %s, want CLOSE", frame.Type)
	}
	code, parsedReason, err := parseErrorPayload(frame.Payload)
	if err != nil {
		t.Fatalf("parse error payload: %v", err)
	}
	if code != uint64(CodeIdleTimeout) {
		t.Fatalf("error code = %d, want %d", code, uint64(CodeIdleTimeout))
	}
	if parsedReason != ErrKeepaliveTimeout.Error() {
		t.Fatalf("reason = %q, want %q", parsedReason, ErrKeepaliveTimeout.Error())
	}
}

func TestSessionAbortDoesNotEmitDuplicateCloseFrame(t *testing.T) {
	t.Parallel()

	c, frames, stop := newHandlerTestConn(t)
	defer stop()
	c.io.conn = &countingWriteCloser{}

	c.Abort(&ApplicationError{Code: uint64(CodeInternal), Reason: "fatal"})

	frame := awaitQueuedFrame(t, frames)
	if frame.Type != FrameTypeCLOSE {
		t.Fatalf("first queued frame type = %s, want CLOSE", frame.Type)
	}
	assertNoQueuedFrame(t, frames)
}

func mustAppendTestTLV(t *testing.T, dst []byte, typ uint64, value []byte) []byte {
	t.Helper()

	out, err := AppendTLV(dst, typ, value)
	if err != nil {
		t.Fatalf("append tlv type=%d: %v", typ, err)
	}
	return out
}

func duplicateStandardDiagPayload(t *testing.T, payload []byte, reason string) []byte {
	t.Helper()

	payload = mustAppendTestTLV(t, payload, uint64(DIAGRetryAfterMillis), mustEncodeVarint(1))
	payload = mustAppendTestTLV(t, payload, uint64(DIAGRetryAfterMillis), mustEncodeVarint(2))
	if reason != "" {
		payload = mustAppendTestTLV(t, payload, uint64(DIAGDebugText), []byte(reason))
	}
	return payload
}

func TestParseErrorPayloadDuplicateDebugTextDropsReason(t *testing.T) {
	t.Parallel()

	payload := mustEncodeVarint(uint64(CodeProtocol))
	payload = mustAppendTestTLV(t, payload, uint64(DIAGDebugText), []byte("first"))
	payload = mustAppendTestTLV(t, payload, uint64(DIAGDebugText), []byte("second"))

	code, reason, err := parseErrorPayload(payload)
	if err != nil {
		t.Fatalf("parse error payload: %v", err)
	}
	if code != uint64(CodeProtocol) {
		t.Fatalf("code = %d, want %d", code, uint64(CodeProtocol))
	}
	if reason != "" {
		t.Fatalf("reason = %q, want empty string when duplicate singleton DIAG invalidates the block", reason)
	}
}

func TestParseGOAWAYPayloadDuplicateStandardDIAGDropsReason(t *testing.T) {
	t.Parallel()

	payload := mustEncodeVarint(8)
	payload = append(payload, mustEncodeVarint(12)...)
	payload = append(payload, mustEncodeVarint(uint64(CodeInternal))...)
	payload = duplicateStandardDiagPayload(t, payload, "maintenance")

	parsed, err := parseGOAWAYPayload(payload)
	if err != nil {
		t.Fatalf("parse goaway payload: %v", err)
	}
	if parsed.LastAcceptedBidi != 8 || parsed.LastAcceptedUni != 12 {
		t.Fatalf("watermarks = (%d,%d), want (8,12)", parsed.LastAcceptedBidi, parsed.LastAcceptedUni)
	}
	if parsed.Code != uint64(CodeInternal) {
		t.Fatalf("code = %d, want %d", parsed.Code, uint64(CodeInternal))
	}
	if parsed.Reason != "" {
		t.Fatalf("reason = %q, want empty string when duplicate singleton DIAG invalidates the block", parsed.Reason)
	}
}

func TestHandleResetFrameDuplicateStandardDIAGDropsReasonKeepsReset(t *testing.T) {
	t.Parallel()

	c, _, stop := newHandlerTestConn(t)
	defer stop()

	streamID := state.FirstPeerStreamID(c.config.negotiated.LocalRole, true)
	stream := testBuildStream(c, streamID,
		testWithBidi(),
		testWithLocalOpen(testLocalOpenClosedState()),
		testWithLocalSend(),
		testWithLocalReceive(),
		testWithApplicationVisible(),
		testWithNotifications(),
	)
	stream.recvBuffer = 3
	stream.readBuf = []byte("abc")

	c.mu.Lock()
	c.registry.streams[streamID] = stream
	c.flow.recvSessionUsed = 3
	c.mu.Unlock()

	payload := duplicateStandardDiagPayload(t, mustEncodeVarint(uint64(CodeCancelled)), "peer reset")
	if err := c.handleResetFrame(Frame{Type: FrameTypeRESET, StreamID: streamID, Payload: payload}); err != nil {
		t.Fatalf("handleResetFrame: %v", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if stream.recvReset == nil {
		t.Fatal("recvReset = nil, want recorded reset")
	}
	if stream.recvReset.Code != uint64(CodeCancelled) {
		t.Fatalf("recvReset code = %d, want %d", stream.recvReset.Code, uint64(CodeCancelled))
	}
	if stream.recvReset.Reason != "" {
		t.Fatalf("recvReset reason = %q, want empty string after duplicate singleton DIAG", stream.recvReset.Reason)
	}
	if stream.recvBuffer != 0 {
		t.Fatalf("recvBuffer = %d, want 0 after RESET", stream.recvBuffer)
	}
	if len(stream.readBuf) != 0 {
		t.Fatalf("readBuf len = %d, want 0 after RESET", len(stream.readBuf))
	}
}

func TestHandleAbortFrameDuplicateStandardDIAGDropsReasonKeepsAbort(t *testing.T) {
	t.Parallel()

	c, _, stop := newHandlerTestConn(t)
	defer stop()

	streamID := state.FirstPeerStreamID(c.config.negotiated.LocalRole, true)
	stream := testBuildStream(c, streamID,
		testWithBidi(),
		testWithLocalOpen(testLocalOpenClosedState()),
		testWithLocalSend(),
		testWithLocalReceive(),
		testWithApplicationVisible(),
		testWithNotifications(),
	)
	stream.recvBuffer = 2
	stream.readBuf = []byte("ab")

	c.mu.Lock()
	c.registry.streams[streamID] = stream
	c.flow.recvSessionUsed = 2
	c.mu.Unlock()

	payload := duplicateStandardDiagPayload(t, mustEncodeVarint(uint64(CodeRefusedStream)), "peer abort")
	if err := c.handleAbortFrame(Frame{Type: FrameTypeABORT, StreamID: streamID, Payload: payload}); err != nil {
		t.Fatalf("handleAbortFrame: %v", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if stream.recvAbort == nil {
		t.Fatal("recvAbort = nil, want recorded abort")
	}
	if stream.recvAbort.Code != uint64(CodeRefusedStream) {
		t.Fatalf("recvAbort code = %d, want %d", stream.recvAbort.Code, uint64(CodeRefusedStream))
	}
	if stream.recvAbort.Reason != "" {
		t.Fatalf("recvAbort reason = %q, want empty string after duplicate singleton DIAG", stream.recvAbort.Reason)
	}
	if stream.recvBuffer != 0 {
		t.Fatalf("recvBuffer = %d, want 0 after ABORT", stream.recvBuffer)
	}
	if len(stream.readBuf) != 0 {
		t.Fatalf("readBuf len = %d, want 0 after ABORT", len(stream.readBuf))
	}
}

func TestHandleGoAwayFrameDuplicateStandardDIAGDropsReasonKeepsPrimarySemantics(t *testing.T) {
	t.Parallel()

	c, _, stop := newHandlerTestConn(t)
	defer stop()

	payload := mustEncodeVarint(8)
	payload = append(payload, mustEncodeVarint(0)...)
	payload = append(payload, mustEncodeVarint(uint64(CodeProtocol))...)
	payload = duplicateStandardDiagPayload(t, payload, "maintenance")

	if err := c.handleGoAwayFrame(Frame{Type: FrameTypeGOAWAY, Payload: payload}); err != nil {
		t.Fatalf("handleGoAwayFrame: %v", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lifecycle.sessionState != connStateDraining {
		t.Fatalf("sessionState = %d, want %d", c.lifecycle.sessionState, connStateDraining)
	}
	if c.sessionControl.peerGoAwayErr == nil {
		t.Fatal("peerGoAwayErr = nil, want recorded GOAWAY")
	}
	if c.sessionControl.peerGoAwayErr.Code != uint64(CodeProtocol) {
		t.Fatalf("peerGoAwayErr code = %d, want %d", c.sessionControl.peerGoAwayErr.Code, uint64(CodeProtocol))
	}
	if c.sessionControl.peerGoAwayErr.Reason != "" {
		t.Fatalf("peerGoAwayErr reason = %q, want empty string after duplicate singleton DIAG", c.sessionControl.peerGoAwayErr.Reason)
	}
	if c.sessionControl.peerGoAwayBidi != 8 || c.sessionControl.peerGoAwayUni != 0 {
		t.Fatalf("peer GOAWAY watermarks = (%d,%d), want (8,0)", c.sessionControl.peerGoAwayBidi, c.sessionControl.peerGoAwayUni)
	}
}

func TestHandleCloseFrameDuplicateStandardDIAGDropsReasonKeepsPrimarySemantics(t *testing.T) {
	t.Parallel()

	c, _, stop := newHandlerTestConn(t)
	defer stop()

	payload := duplicateStandardDiagPayload(t, mustEncodeVarint(uint64(CodeProtocol)), "peer close")
	err := c.handleCloseFrame(Frame{Type: FrameTypeCLOSE, Payload: payload})
	if err == nil {
		t.Fatal("handleCloseFrame err = nil, want peer close error")
	}

	appErr := testApplicationError(err)
	if appErr == nil {
		t.Fatalf("handleCloseFrame err = %T, want *ApplicationError", err)
	}
	if appErr.Code != uint64(CodeProtocol) {
		t.Fatalf("returned code = %d, want %d", appErr.Code, uint64(CodeProtocol))
	}
	if appErr.Reason != "" {
		t.Fatalf("returned reason = %q, want empty string after duplicate singleton DIAG", appErr.Reason)
	}

	peerCloseErr := c.PeerCloseError()
	if peerCloseErr == nil {
		t.Fatal("PeerCloseError = nil, want recorded peer close")
	}
	if peerCloseErr.Code != uint64(CodeProtocol) {
		t.Fatalf("peer close code = %d, want %d", peerCloseErr.Code, uint64(CodeProtocol))
	}
	if peerCloseErr.Reason != "" {
		t.Fatalf("peer close reason = %q, want empty string after duplicate singleton DIAG", peerCloseErr.Reason)
	}
}
