package zmux

import (
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/wire"
)

var _ Stream = (*nativeStream)(nil)
var _ NativeStream = (*nativeStream)(nil)
var _ SendStream = (*nativeSendStream)(nil)
var _ NativeSendStream = (*nativeSendStream)(nil)
var _ RecvStream = (*nativeRecvStream)(nil)
var _ NativeRecvStream = (*nativeRecvStream)(nil)
var _ Session = nativeSession{}
var _ NativeSession = (*Conn)(nil)

func TestPublicProtocolAliasesRemainPinned(t *testing.T) {
	t.Parallel()

	_ = New

	checks := []struct {
		name string
		got  func() uint64
		want func() uint64
	}{
		{
			name: "MaxPrefaceSettingsBytes",
			got:  func() uint64 { return MaxPrefaceSettingsBytes },
			want: func() uint64 { return wire.MaxPrefaceSettingsBytes },
		},
		{
			name: "SchedulerUnspecifiedOrBalanced",
			got:  func() uint64 { return uint64(SchedulerUnspecifiedOrBalanced) },
			want: func() uint64 { return uint64(wire.SchedulerUnspecifiedOrBalanced) },
		},
		{
			name: "SettingInitialMaxStreamDataBidiLocallyOpened",
			got:  func() uint64 { return uint64(SettingInitialMaxStreamDataBidiLocallyOpened) },
			want: func() uint64 { return uint64(wire.SettingInitialMaxStreamDataBidiLocallyOpened) },
		},
		{
			name: "SettingInitialMaxStreamDataBidiPeerOpened",
			got:  func() uint64 { return uint64(SettingInitialMaxStreamDataBidiPeerOpened) },
			want: func() uint64 { return uint64(wire.SettingInitialMaxStreamDataBidiPeerOpened) },
		},
		{
			name: "SettingInitialMaxStreamDataUni",
			got:  func() uint64 { return uint64(SettingInitialMaxStreamDataUni) },
			want: func() uint64 { return uint64(wire.SettingInitialMaxStreamDataUni) },
		},
		{
			name: "SettingInitialMaxData",
			got:  func() uint64 { return uint64(SettingInitialMaxData) },
			want: func() uint64 { return uint64(wire.SettingInitialMaxData) },
		},
		{
			name: "SettingMaxIncomingStreamsUni",
			got:  func() uint64 { return uint64(SettingMaxIncomingStreamsUni) },
			want: func() uint64 { return uint64(wire.SettingMaxIncomingStreamsUni) },
		},
		{
			name: "SettingSchedulerHints",
			got:  func() uint64 { return uint64(SettingSchedulerHints) },
			want: func() uint64 { return uint64(wire.SettingSchedulerHints) },
		},
		{
			name: "SettingPingPaddingKey",
			got:  func() uint64 { return uint64(SettingPingPaddingKey) },
			want: func() uint64 { return uint64(wire.SettingPingPaddingKey) },
		},
		{
			name: "SettingPrefacePadding",
			got:  func() uint64 { return uint64(SettingPrefacePadding) },
			want: func() uint64 { return uint64(wire.SettingPrefacePadding) },
		},
		{
			name: "DIAGOffendingStreamID",
			got:  func() uint64 { return uint64(DIAGOffendingStreamID) },
			want: func() uint64 { return uint64(wire.DIAGOffendingStreamID) },
		},
		{
			name: "DIAGOffendingFrameType",
			got:  func() uint64 { return uint64(DIAGOffendingFrameType) },
			want: func() uint64 { return uint64(wire.DIAGOffendingFrameType) },
		},
		{
			name: "CodeStreamLimit",
			got:  func() uint64 { return uint64(CodeStreamLimit) },
			want: func() uint64 { return uint64(wire.CodeStreamLimit) },
		},
		{
			name: "CodeSessionClosing",
			got:  func() uint64 { return uint64(CodeSessionClosing) },
			want: func() uint64 { return uint64(wire.CodeSessionClosing) },
		},
	}

	for _, tc := range checks {
		if got, want := tc.got(), tc.want(); got != want {
			t.Fatalf("%s = %d, want %d", tc.name, got, want)
		}
	}

	if got := SchedulerHintFromCode(uint64(wire.SchedulerGroupFair)); got != SchedulerGroupFair {
		t.Fatalf("SchedulerHintFromCode(group_fair) = %d, want %d", got, SchedulerGroupFair)
	}
	if got := SchedulerHintFromCode(99); got != SchedulerUnspecifiedOrBalanced {
		t.Fatalf("SchedulerHintFromCode(unknown) = %d, want %d", got, SchedulerUnspecifiedOrBalanced)
	}
}

func TestSessionConstructorsRejectNilConn(t *testing.T) {
	t.Parallel()

	t.Run("native", func(t *testing.T) {
		t.Parallel()

		cases := []struct {
			name string
			call func() (*Conn, error)
		}{
			{name: "New", call: func() (*Conn, error) { return New(nil, nil) }},
			{name: "Client", call: func() (*Conn, error) { return Client(nil, nil) }},
			{name: "Server", call: func() (*Conn, error) { return Server(nil, nil) }},
		}

		for _, tc := range cases {
			conn, err := tc.call()
			if conn != nil {
				t.Fatalf("%s returned non-nil conn for nil transport", tc.name)
			}
			if !errors.Is(err, ErrNilConn) {
				t.Fatalf("%s err = %v, want %v", tc.name, err, ErrNilConn)
			}
		}
	})

	t.Run("session", func(t *testing.T) {
		t.Parallel()

		cases := []struct {
			name string
			call func() (Session, error)
		}{
			{name: "NewSession", call: func() (Session, error) { return NewSession(nil, nil) }},
			{name: "ClientSession", call: func() (Session, error) { return ClientSession(nil, nil) }},
			{name: "ServerSession", call: func() (Session, error) { return ServerSession(nil, nil) }},
		}

		for _, tc := range cases {
			session, err := tc.call()
			if session != nil {
				t.Fatalf("%s returned non-nil session for nil transport", tc.name)
			}
			if !errors.Is(err, ErrNilConn) {
				t.Fatalf("%s err = %v, want %v", tc.name, err, ErrNilConn)
			}
		}
	})
}

func TestIOConstructorsRejectNilHalves(t *testing.T) {
	t.Parallel()

	conn := &countingCloseConn{}

	t.Run("native", func(t *testing.T) {
		t.Parallel()

		cases := []struct {
			name string
			call func() (*Conn, error)
		}{
			{name: "NewIO nil reader", call: func() (*Conn, error) { return NewIO(nil, conn, nil) }},
			{name: "NewIO nil writer", call: func() (*Conn, error) { return NewIO(conn, nil, nil) }},
			{name: "ClientIO nil reader", call: func() (*Conn, error) { return ClientIO(nil, conn, nil) }},
			{name: "ServerIO nil writer", call: func() (*Conn, error) { return ServerIO(conn, nil, nil) }},
		}

		for _, tc := range cases {
			session, err := tc.call()
			if session != nil {
				t.Fatalf("%s returned non-nil conn for nil split transport", tc.name)
			}
			if !errors.Is(err, ErrNilConn) {
				t.Fatalf("%s err = %v, want %v", tc.name, err, ErrNilConn)
			}
		}
	})

	t.Run("session", func(t *testing.T) {
		t.Parallel()

		cases := []struct {
			name string
			call func() (Session, error)
		}{
			{name: "NewIOSession nil reader", call: func() (Session, error) { return NewIOSession(nil, conn, nil) }},
			{name: "NewIOSession nil writer", call: func() (Session, error) { return NewIOSession(conn, nil, nil) }},
			{name: "ClientIOSession nil reader", call: func() (Session, error) { return ClientIOSession(nil, conn, nil) }},
			{name: "ServerIOSession nil writer", call: func() (Session, error) { return ServerIOSession(conn, nil, nil) }},
		}

		for _, tc := range cases {
			session, err := tc.call()
			if session != nil {
				t.Fatalf("%s returned non-nil session for nil split transport", tc.name)
			}
			if !errors.Is(err, ErrNilConn) {
				t.Fatalf("%s err = %v, want %v", tc.name, err, ErrNilConn)
			}
		}
	})
}

type countingCloseConn struct {
	closeCount atomic.Int32
}

func (c *countingCloseConn) Read(_ []byte) (int, error) {
	return 0, io.EOF
}

func (c *countingCloseConn) Write(p []byte) (int, error) {
	return len(p), nil
}

func (c *countingCloseConn) Close() error {
	c.closeCount.Add(1)
	return nil
}

func TestSessionConstructorsDoNotDoubleCloseTransportOnEstablishFailure(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		call func(io.ReadWriteCloser, *Config) (Session, error)
	}{
		{name: "NewSession", call: NewSession},
		{name: "ClientSession", call: ClientSession},
		{name: "ServerSession", call: ServerSession},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			conn := &countingCloseConn{}
			session, err := tc.call(conn, nil)
			if session != nil {
				t.Fatalf("%s returned non-nil session on establish failure", tc.name)
			}
			if err == nil {
				t.Fatalf("%s err = nil, want establishment failure", tc.name)
			}
			if got := conn.closeCount.Load(); got != 1 {
				t.Fatalf("%s close count = %d, want 1", tc.name, got)
			}
		})
	}
}

func TestIOSessionConstructorsUseSplitReaderWriter(t *testing.T) {
	t.Parallel()

	left, right := net.Pipe()
	type result struct {
		session Session
		err     error
	}
	clientCh := make(chan result, 1)
	serverCh := make(chan result, 1)

	go func() {
		session, err := ClientIOSession(left, left, nil)
		clientCh <- result{session: session, err: err}
	}()
	go func() {
		session, err := ServerIOSession(right, right, nil)
		serverCh <- result{session: session, err: err}
	}()

	client := <-clientCh
	server := <-serverCh

	if client.err != nil {
		t.Fatalf("ClientIOSession establish: %v", client.err)
	}
	if server.err != nil {
		t.Fatalf("ServerIOSession establish: %v", server.err)
	}
	defer func() { _ = client.session.Close() }()
	defer func() { _ = server.session.Close() }()

	ctx, cancel := testContext(t)
	defer cancel()

	stream, n, err := client.session.OpenAndSend(ctx, []byte("ping"))
	if err != nil {
		t.Fatalf("OpenAndSend: %v", err)
	}
	defer func() { _ = stream.Close() }()
	if n != len("ping") {
		t.Fatalf("OpenAndSend n = %d, want %d", n, len("ping"))
	}

	accepted, err := server.session.AcceptStream(ctx)
	if err != nil {
		t.Fatalf("AcceptStream: %v", err)
	}
	defer func() { _ = accepted.Close() }()

	buf := make([]byte, len("ping"))
	if _, err := io.ReadFull(accepted, buf); err != nil {
		t.Fatalf("ReadFull accepted stream: %v", err)
	}
	if string(buf) != "ping" {
		t.Fatalf("accepted payload = %q, want %q", buf, "ping")
	}
}

func TestJoinIOCloseDeduplicatesSharedCloser(t *testing.T) {
	t.Parallel()

	conn := &countingCloseConn{}
	joined := JoinIO(conn, conn)

	if err := joined.Close(); err != nil {
		t.Fatalf("JoinIO(shared).Close err = %v", err)
	}
	if got := conn.closeCount.Load(); got != 1 {
		t.Fatalf("shared split transport close count = %d, want 1", got)
	}
}

func TestJoinIODirectionalCloseDeduplicatesSharedFullCloser(t *testing.T) {
	t.Parallel()

	conn := &countingCloseConn{}
	joined := JoinIO(conn, conn)

	if err := joined.CloseRead(); err != nil {
		t.Fatalf("JoinIO(shared).CloseRead err = %v", err)
	}
	if got := conn.closeCount.Load(); got != 1 {
		t.Fatalf("shared split transport close count after CloseRead = %d, want 1", got)
	}
	if err := joined.CloseWrite(); err != nil {
		t.Fatalf("JoinIO(shared).CloseWrite err = %v", err)
	}
	if got := conn.closeCount.Load(); got != 1 {
		t.Fatalf("shared split transport close count after CloseWrite = %d, want 1", got)
	}
}

func TestJoinIOPlainHalvesReportUnsupportedDeadlines(t *testing.T) {
	t.Parallel()

	joined := JoinIO(&countingCloseConn{}, &countingCloseConn{})
	deadline := time.Now().Add(time.Second)

	if err := joined.SetReadDeadline(deadline); !errors.Is(err, errors.ErrUnsupported) {
		t.Fatalf("SetReadDeadline err = %v, want %v", err, errors.ErrUnsupported)
	}
	if err := joined.SetWriteDeadline(deadline); !errors.Is(err, errors.ErrUnsupported) {
		t.Fatalf("SetWriteDeadline err = %v, want %v", err, errors.ErrUnsupported)
	}
}

func TestAsSessionNilLifecycleSurfaceMatchesConnNoopSemantics(t *testing.T) {
	t.Parallel()

	session := AsSession(nil)
	var nilCtx context.Context

	if err := session.Close(); err != nil {
		t.Fatalf("AsSession(nil).Close() err = %v, want nil", err)
	}
	if err := session.Wait(nilCtx); err != nil {
		t.Fatalf("AsSession(nil).Wait(nil) err = %v, want nil", err)
	}
	if got := session.State(); got != SessionStateInvalid {
		t.Fatalf("AsSession(nil).State() = %v, want %v", got, SessionStateInvalid)
	}
	if got := session.Stats().State; got != SessionStateInvalid {
		t.Fatalf("AsSession(nil).Stats().State = %v, want %v", got, SessionStateInvalid)
	}
	if !session.Closed() {
		t.Fatal("AsSession(nil).Closed() = false, want true")
	}
}

func TestStableSessionInterfacesExposeDocumentedSurface(t *testing.T) {
	t.Parallel()

	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()
	defer func() { _ = client.Close() }()
	defer func() { _ = server.Close() }()

	clientSession := AsSession(client)
	serverSession := AsSession(server)

	type bidiResult struct {
		stream Stream
		err    error
	}
	bidiAcceptCh := make(chan bidiResult, 1)
	go func() {
		stream, err := serverSession.AcceptStream(ctx)
		bidiAcceptCh <- bidiResult{stream: stream, err: err}
	}()

	bidi, _, err := clientSession.OpenAndSend(ctx, []byte("x"))
	if err != nil {
		t.Fatalf("OpenAndSend err = %v", err)
	}
	defer func() { _ = bidi.Close() }()

	acceptedBidi := <-bidiAcceptCh
	if acceptedBidi.err != nil {
		t.Fatalf("AcceptStream err = %v", acceptedBidi.err)
	}
	defer func() { _ = acceptedBidi.stream.Close() }()

	type recvResult struct {
		stream RecvStream
		err    error
	}
	recvAcceptCh := make(chan recvResult, 1)
	go func() {
		stream, err := serverSession.AcceptUniStream(ctx)
		recvAcceptCh <- recvResult{stream: stream, err: err}
	}()

	send, _, err := clientSession.OpenUniAndSend(ctx, []byte("y"))
	if err != nil {
		t.Fatalf("OpenUniAndSend err = %v", err)
	}
	defer func() { _ = send.Close() }()

	acceptedRecv := <-recvAcceptCh
	if acceptedRecv.err != nil {
		t.Fatalf("AcceptUniStream err = %v", acceptedRecv.err)
	}
	defer func() { _ = acceptedRecv.stream.Close() }()

	type sessionSurface interface{ CloseWithError(err error) }
	type streamSurface interface {
		CancelRead(code uint64) error
		CancelWrite(code uint64) error
		CloseWithError(code uint64, reason string) error
	}
	type sendSurface interface {
		CancelWrite(code uint64) error
		CloseWithError(code uint64, reason string) error
	}
	type recvSurface interface {
		CancelRead(code uint64) error
		CloseWithError(code uint64, reason string) error
	}

	var _ sessionSurface = clientSession
	var _ sessionSurface = serverSession
	var _ streamSurface = bidi
	var _ streamSurface = acceptedBidi.stream
	var _ sendSurface = send
	var _ recvSurface = acceptedRecv.stream
}

func TestNativeInterfacesExposeNativeQueries(t *testing.T) {
	t.Parallel()

	client, server := newConnPair(t)
	ctx, cancel := testContext(t)
	defer cancel()
	defer func() { _ = client.Close() }()
	defer func() { _ = server.Close() }()

	type bidiResult struct {
		stream NativeStream
		err    error
	}
	bidiAcceptCh := make(chan bidiResult, 1)
	go func() {
		stream, err := server.AcceptStream(ctx)
		bidiAcceptCh <- bidiResult{stream: stream, err: err}
	}()

	bidi, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream err = %v", err)
	}
	if !bidi.OpenedLocally() {
		t.Fatal("opened bidi stream OpenedLocally() = false, want true")
	}
	if !bidi.Bidirectional() {
		t.Fatal("opened bidi stream Bidirectional() = false, want true")
	}
	if bidi.ReadClosed() {
		t.Fatal("opened bidi stream ReadClosed() = true, want false")
	}
	if bidi.WriteClosed() {
		t.Fatal("opened bidi stream WriteClosed() = true, want false")
	}
	if _, err := bidi.Write([]byte("x")); err != nil {
		t.Fatalf("bidi Write err = %v", err)
	}

	acceptedBidi := <-bidiAcceptCh
	if acceptedBidi.err != nil {
		t.Fatalf("AcceptStream err = %v", acceptedBidi.err)
	}
	if acceptedBidi.stream.OpenedLocally() {
		t.Fatal("accepted bidi stream OpenedLocally() = true, want false")
	}
	if !acceptedBidi.stream.Bidirectional() {
		t.Fatal("accepted bidi stream Bidirectional() = false, want true")
	}
	if acceptedBidi.stream.ReadClosed() {
		t.Fatal("accepted bidi stream ReadClosed() = true, want false")
	}
	if acceptedBidi.stream.WriteClosed() {
		t.Fatal("accepted bidi stream WriteClosed() = true, want false")
	}

	send, err := client.OpenUniStream(ctx)
	if err != nil {
		t.Fatalf("OpenUniStream err = %v", err)
	}
	if !send.OpenedLocally() {
		t.Fatal("send-only stream OpenedLocally() = false, want true")
	}
	if send.Bidirectional() {
		t.Fatal("send-only stream Bidirectional() = true, want false")
	}
	if send.WriteClosed() {
		t.Fatal("send-only stream WriteClosed() = true, want false")
	}
	if _, err := send.Write([]byte("y")); err != nil {
		t.Fatalf("send-only Write err = %v", err)
	}

	recv, err := server.AcceptUniStream(ctx)
	if err != nil {
		t.Fatalf("AcceptUniStream err = %v", err)
	}
	if recv.OpenedLocally() {
		t.Fatal("recv-only stream OpenedLocally() = true, want false")
	}
	if recv.Bidirectional() {
		t.Fatal("recv-only stream Bidirectional() = true, want false")
	}
	if recv.ReadClosed() {
		t.Fatal("recv-only stream ReadClosed() = true, want false")
	}

	if err := bidi.CloseRead(); err != nil {
		t.Fatalf("bidi CloseRead err = %v", err)
	}
	if !bidi.ReadClosed() {
		t.Fatal("bidi ReadClosed() = false after CloseRead, want true")
	}

	if err := send.CancelWrite(uint64(CodeCancelled)); err != nil {
		t.Fatalf("send CancelWrite err = %v", err)
	}
	if !send.WriteClosed() {
		t.Fatal("send WriteClosed() = false after CancelWrite, want true")
	}
}
