package quicmux

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/internal/adaptertest"
)

func TestQUICSessionContract(t *testing.T) {
	adaptertest.RunSessionContract(t, newWrappedPair)
}

func TestWrapSessionNilIsClosedSafeSession(t *testing.T) {
	session := WrapSession(nil)
	if session == nil {
		t.Fatal("WrapSession(nil) = nil, want closed session wrapper")
	}
	if !session.Closed() {
		t.Fatal("WrapSession(nil).Closed() = false, want true")
	}
	if session.State() != zmux.SessionStateInvalid {
		t.Fatalf("WrapSession(nil).State() = %v, want %v", session.State(), zmux.SessionStateInvalid)
	}
}

func TestWrapSessionWaitReturnsNilAfterGracefulClose(t *testing.T) {
	client, _ := newWrappedPair(t)

	if err := client.Close(); err != nil {
		t.Fatalf("Close err = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Wait(ctx); err != nil {
		t.Fatalf("Wait err = %v, want nil", err)
	}
}

func TestWrapSessionCloseReadUsesCancelledCode(t *testing.T) {
	client, server := newWrappedPair(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	type acceptResult struct {
		stream zmux.Stream
		err    error
	}
	acceptCh := make(chan acceptResult, 1)
	go func() {
		stream, err := server.AcceptStream(ctx)
		acceptCh <- acceptResult{stream: stream, err: err}
	}()

	clientStream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream err = %v", err)
	}
	if _, err := clientStream.Write([]byte("p")); err != nil {
		t.Fatalf("initial Write err = %v", err)
	}
	accepted := <-acceptCh
	if accepted.err != nil {
		t.Fatalf("AcceptStream err = %v", accepted.err)
	}

	if err := accepted.stream.CloseRead(); err != nil {
		t.Fatalf("CloseRead err = %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	_ = clientStream.SetWriteDeadline(deadline)
	for time.Now().Before(deadline) {
		_, err := clientStream.Write([]byte("x"))
		if err == nil {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		appErr, ok := findError[*zmux.ApplicationError](err)
		if !ok {
			t.Fatalf("Write err = %v, want ApplicationError(CodeCancelled)", err)
		}
		if appErr.Code != uint64(zmux.CodeCancelled) {
			t.Fatalf("Write code = %d, want %d", appErr.Code, uint64(zmux.CodeCancelled))
		}
		return
	}
	t.Fatal("client write did not observe CANCELLED after peer CloseRead")
}

func TestWrapSessionUniCloseMatchesAvailableDirection(t *testing.T) {
	client, server := newWrappedPair(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	type acceptResult struct {
		stream zmux.RecvStream
		err    error
	}
	acceptCh := make(chan acceptResult, 1)
	go func() {
		stream, err := server.AcceptUniStream(ctx)
		acceptCh <- acceptResult{stream: stream, err: err}
	}()

	send, err := client.OpenUniStream(ctx)
	if err != nil {
		t.Fatalf("OpenUniStream err = %v", err)
	}
	payload := []byte("uni-close")
	if _, err := send.Write(payload[:1]); err != nil {
		t.Fatalf("initial Write err = %v", err)
	}
	recv := <-acceptCh
	if recv.err != nil {
		t.Fatalf("AcceptUniStream err = %v", recv.err)
	}
	if _, err := send.Write(payload[1:]); err != nil {
		t.Fatalf("Write err = %v", err)
	}
	if err := send.Close(); err != nil {
		t.Fatalf("send Close err = %v", err)
	}

	got, err := io.ReadAll(recv.stream)
	if err != nil {
		t.Fatalf("ReadAll err = %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("ReadAll = %q, want %q", got, payload)
	}

	if err := recv.stream.Close(); err != nil {
		t.Fatalf("recv Close err = %v", err)
	}
}

func TestWrapSessionOpenMetadataVisibleOnAccept(t *testing.T) {
	client, server := newWrappedPair(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	priority := uint64(7)
	group := uint64(11)

	type acceptResult struct {
		stream zmux.Stream
		err    error
	}
	acceptCh := make(chan acceptResult, 1)
	go func() {
		stream, err := server.AcceptStream(ctx)
		acceptCh <- acceptResult{stream: stream, err: err}
	}()

	stream, err := client.OpenStreamWithOptions(ctx, zmux.OpenOptions{
		InitialPriority: &priority,
		InitialGroup:    &group,
		OpenInfo:        []byte("ssh"),
	})
	if err != nil {
		t.Fatalf("OpenStreamWithOptions err = %v", err)
	}

	accepted := <-acceptCh
	if accepted.err != nil {
		t.Fatalf("AcceptStream err = %v", accepted.err)
	}

	meta := accepted.stream.Metadata()
	if meta.Priority != priority {
		t.Fatalf("accepted Priority = %d, want %d", meta.Priority, priority)
	}
	if meta.Group == nil || *meta.Group != group {
		t.Fatalf("accepted Group = %v, want %d", meta.Group, group)
	}
	if got := string(meta.OpenInfo); got != "ssh" {
		t.Fatalf("accepted OpenInfo = %q, want %q", got, "ssh")
	}
	if got := string(accepted.stream.OpenInfo()); got != "ssh" {
		t.Fatalf("accepted OpenInfo() = %q, want %q", got, "ssh")
	}

	_ = accepted.stream.Close()
	_ = stream.Close()
}

func TestWrapSessionUpdateMetadataBeforeVisibilityUsesPrelude(t *testing.T) {
	client, server := newWrappedPair(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	priority := uint64(5)
	group := uint64(9)

	type acceptResult struct {
		stream zmux.Stream
		err    error
	}
	acceptCh := make(chan acceptResult, 1)
	go func() {
		stream, err := server.AcceptStream(ctx)
		acceptCh <- acceptResult{stream: stream, err: err}
	}()

	stream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream err = %v", err)
	}
	if err := stream.UpdateMetadata(zmux.MetadataUpdate{
		Priority: &priority,
		Group:    &group,
	}); err != nil {
		t.Fatalf("UpdateMetadata err = %v", err)
	}

	accepted := <-acceptCh
	if accepted.err != nil {
		t.Fatalf("AcceptStream err = %v", accepted.err)
	}

	meta := accepted.stream.Metadata()
	if meta.Priority != priority {
		t.Fatalf("accepted Priority = %d, want %d", meta.Priority, priority)
	}
	if meta.Group == nil || *meta.Group != group {
		t.Fatalf("accepted Group = %v, want %d", meta.Group, group)
	}
	if meta.OpenInfo != nil {
		t.Fatalf("accepted OpenInfo = %v, want nil", meta.OpenInfo)
	}

	_ = accepted.stream.Close()
	_ = stream.Close()
}

func TestWrapSessionUpdateMetadataAfterVisibilityUnavailable(t *testing.T) {
	client, server := newWrappedPair(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	type acceptResult struct {
		stream zmux.Stream
		err    error
	}
	acceptCh := make(chan acceptResult, 1)
	go func() {
		stream, err := server.AcceptStream(ctx)
		acceptCh <- acceptResult{stream: stream, err: err}
	}()

	stream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream err = %v", err)
	}
	if _, err := stream.Write([]byte("x")); err != nil {
		t.Fatalf("Write err = %v", err)
	}

	accepted := <-acceptCh
	if accepted.err != nil {
		t.Fatalf("AcceptStream err = %v", accepted.err)
	}

	priority := uint64(13)
	err = stream.UpdateMetadata(zmux.MetadataUpdate{Priority: &priority})
	if !errors.Is(err, zmux.ErrPriorityUpdateUnavailable) {
		t.Fatalf("UpdateMetadata err = %v, want %v", err, zmux.ErrPriorityUpdateUnavailable)
	}
	if !errors.Is(err, zmux.ErrAdapterUnsupported) {
		t.Fatalf("UpdateMetadata err = %v, want %v", err, zmux.ErrAdapterUnsupported)
	}

	_ = accepted.stream.Close()
	_ = stream.Close()
}

func TestWrapSessionAcceptStreamPreservesOrderAcrossStalledPreludeTimeout(t *testing.T) {
	clientConn, serverConn := newQUICConnPair(t)
	client := WrapSession(clientConn)
	server := WrapSession(serverConn)

	prevTimeout := acceptedPreludeReadTimeout
	acceptedPreludeReadTimeout = 100 * time.Millisecond
	t.Cleanup(func() {
		acceptedPreludeReadTimeout = prevTimeout
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stalled, err := clientConn.OpenStreamSync(ctx)
	if err != nil {
		t.Fatalf("raw OpenStreamSync stalled err = %v", err)
	}
	t.Cleanup(func() {
		stalled.CancelRead(0)
		stalled.CancelWrite(0)
		_ = stalled.Close()
	})

	ready, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream ready err = %v", err)
	}
	t.Cleanup(func() {
		_ = ready.Close()
	})
	if _, err := ready.Write([]byte("x")); err != nil {
		t.Fatalf("ready Write err = %v", err)
	}

	firstAccepted, err := server.AcceptStream(ctx)
	if err == nil {
		t.Fatalf("first AcceptStream = (%v, nil), want protocol error", firstAccepted)
	}
	if !zmux.IsErrorCode(err, zmux.CodeProtocol) {
		t.Fatalf("first AcceptStream err = %v, want protocol error", err)
	}

	accepted, err := server.AcceptStream(ctx)
	if err != nil {
		t.Fatalf("second AcceptStream err = %v", err)
	}
	if accepted.StreamID() != ready.StreamID() {
		t.Fatalf("accepted StreamID = %d, want %d", accepted.StreamID(), ready.StreamID())
	}

	buf := make([]byte, 1)
	if _, err := io.ReadFull(accepted, buf); err != nil {
		t.Fatalf("ReadFull err = %v", err)
	}
	if !bytes.Equal(buf, []byte("x")) {
		t.Fatalf("ReadFull = %q, want %q", buf, []byte("x"))
	}

	_ = accepted.Close()
}

func TestWrapSessionConcurrentAcceptStreamDoesNotOvertakeStalledPrelude(t *testing.T) {
	clientConn, serverConn := newQUICConnPair(t)
	client := WrapSession(clientConn)
	server := WrapSession(serverConn)

	prevTimeout := acceptedPreludeReadTimeout
	acceptedPreludeReadTimeout = 100 * time.Millisecond
	t.Cleanup(func() {
		acceptedPreludeReadTimeout = prevTimeout
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stalled, err := clientConn.OpenStreamSync(ctx)
	if err != nil {
		t.Fatalf("raw OpenStreamSync stalled err = %v", err)
	}
	t.Cleanup(func() {
		stalled.CancelRead(0)
		stalled.CancelWrite(0)
		_ = stalled.Close()
	})

	ready, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream ready err = %v", err)
	}
	t.Cleanup(func() {
		_ = ready.Close()
	})
	if _, err := ready.Write([]byte("x")); err != nil {
		t.Fatalf("ready Write err = %v", err)
	}

	type acceptResult struct {
		stream zmux.Stream
		err    error
	}
	acceptCh := make(chan acceptResult, 2)
	for range 2 {
		go func() {
			stream, err := server.AcceptStream(ctx)
			acceptCh <- acceptResult{stream: stream, err: err}
		}()
	}

	first := <-acceptCh
	if first.err == nil {
		t.Fatalf("first concurrent AcceptStream = (%v, nil), want protocol error", first.stream)
	}
	if !zmux.IsErrorCode(first.err, zmux.CodeProtocol) {
		t.Fatalf("first concurrent AcceptStream err = %v, want protocol error", first.err)
	}

	second := <-acceptCh
	if second.err != nil {
		t.Fatalf("second concurrent AcceptStream err = %v", second.err)
	}
	if second.stream == nil {
		t.Fatal("second concurrent AcceptStream stream = nil, want ready stream")
	}
	if second.stream.StreamID() != ready.StreamID() {
		t.Fatalf("accepted StreamID = %d, want %d", second.stream.StreamID(), ready.StreamID())
	}

	buf := make([]byte, 1)
	if _, err := io.ReadFull(second.stream, buf); err != nil {
		t.Fatalf("ReadFull err = %v", err)
	}
	if !bytes.Equal(buf, []byte("x")) {
		t.Fatalf("ReadFull = %q, want %q", buf, []byte("x"))
	}

	_ = second.stream.Close()
}

func TestWrapSessionResetMakesLocalWriteFailImmediately(t *testing.T) {
	client, _ := newWrappedPair(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream err = %v", err)
	}
	if err := stream.Reset(44); err != nil {
		t.Fatalf("Reset err = %v", err)
	}
	if _, err := stream.Write([]byte("x")); err == nil {
		t.Fatal("Write after Reset err = nil, want application error")
	} else {
		appErr, ok := findError[*zmux.ApplicationError](err)
		if !ok || appErr.Code != 44 {
			t.Fatalf("Write after Reset err = %v, want ApplicationError(44)", err)
		}
	}
}

func TestWrapSessionCloseWithErrorMakesLocalBidiOpsFailImmediately(t *testing.T) {
	client, _ := newWrappedPair(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream err = %v", err)
	}
	if err := stream.CloseWithErrorCode(55, "bye"); err != nil {
		t.Fatalf("CloseWithErrorCode err = %v", err)
	}
	if _, err := stream.Read(make([]byte, 1)); err == nil {
		t.Fatal("Read after CloseWithErrorCode err = nil, want application error")
	} else {
		appErr, ok := findError[*zmux.ApplicationError](err)
		if !ok || appErr.Code != 55 || appErr.Reason != "bye" {
			t.Fatalf("Read after CloseWithErrorCode err = %v, want ApplicationError(55, \"bye\")", err)
		}
	}
	if _, err := stream.Write([]byte("x")); err == nil {
		t.Fatal("Write after CloseWithErrorCode err = nil, want application error")
	} else {
		appErr, ok := findError[*zmux.ApplicationError](err)
		if !ok || appErr.Code != 55 || appErr.Reason != "bye" {
			t.Fatalf("Write after CloseWithErrorCode err = %v, want ApplicationError(55, \"bye\")", err)
		}
	}
}

func TestWrapSessionCloseWithErrorMakesLocalSendWriteFailImmediately(t *testing.T) {
	client, _ := newWrappedPair(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := client.OpenUniStream(ctx)
	if err != nil {
		t.Fatalf("OpenUniStream err = %v", err)
	}
	if err := stream.CloseWithErrorCode(66, "bye"); err != nil {
		t.Fatalf("CloseWithErrorCode err = %v", err)
	}
	if _, err := stream.Write([]byte("x")); err == nil {
		t.Fatal("Write after CloseWithErrorCode err = nil, want application error")
	} else {
		appErr, ok := findError[*zmux.ApplicationError](err)
		if !ok || appErr.Code != 66 || appErr.Reason != "bye" {
			t.Fatalf("Write after CloseWithErrorCode err = %v, want ApplicationError(66, \"bye\")", err)
		}
	}
}

func TestEnsureOpenPreludeResumesAfterPartialWriteError(t *testing.T) {
	writer := &partialPreludeWriter{
		failAfter: 3,
		failErr:   io.ErrUnexpectedEOF,
	}
	base := &quicStreamBase{}
	priority := uint64(7)
	group := uint64(9)
	initLocalStreamBase(base, nil, nil, writer, zmux.OpenOptions{
		InitialPriority: &priority,
		InitialGroup:    &group,
		OpenInfo:        []byte("ssh"),
	})

	prelude, err := base.prepareOpenPrelude()
	if err != nil {
		t.Fatalf("prepareOpenPrelude err = %v", err)
	}
	if err := base.ensureOpenPrelude(); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("first ensureOpenPrelude err = %v, want %v", err, io.ErrUnexpectedEOF)
	}
	if err := base.ensureOpenPrelude(); err != nil {
		t.Fatalf("second ensureOpenPrelude err = %v", err)
	}
	if got := writer.Bytes(); !bytes.Equal(got, prelude) {
		t.Fatalf("written prelude = %x, want %x", got, prelude)
	}
	if base.preludeOffset != len(prelude) {
		t.Fatalf("preludeOffset = %d, want %d", base.preludeOffset, len(prelude))
	}
	if !base.preludeSent {
		t.Fatal("preludeSent = false, want true")
	}
}

func TestWrapSessionCloseReadReturnsErrReadClosedLocally(t *testing.T) {
	client, server := newWrappedPair(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	type acceptResult struct {
		stream zmux.Stream
		err    error
	}
	acceptCh := make(chan acceptResult, 1)
	go func() {
		stream, err := server.AcceptStream(ctx)
		acceptCh <- acceptResult{stream: stream, err: err}
	}()

	stream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream err = %v", err)
	}
	if _, err := stream.Write([]byte("x")); err != nil {
		t.Fatalf("Write err = %v", err)
	}

	accepted := <-acceptCh
	if accepted.err != nil {
		t.Fatalf("AcceptStream err = %v", accepted.err)
	}
	if err := accepted.stream.CloseRead(); err != nil {
		t.Fatalf("CloseRead err = %v", err)
	}
	if _, err := accepted.stream.Read(make([]byte, 1)); !errors.Is(err, zmux.ErrReadClosed) {
		t.Fatalf("Read err = %v, want %v", err, zmux.ErrReadClosed)
	}

	_ = accepted.stream.Close()
	_ = stream.Close()
}

func TestWrapSessionCloseWriteReturnsErrWriteClosedLocally(t *testing.T) {
	client, server := newWrappedPair(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	type acceptResult struct {
		stream zmux.Stream
		err    error
	}
	acceptCh := make(chan acceptResult, 1)
	go func() {
		stream, err := server.AcceptStream(ctx)
		acceptCh <- acceptResult{stream: stream, err: err}
	}()

	stream, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream err = %v", err)
	}
	if _, err := stream.Write([]byte("x")); err != nil {
		t.Fatalf("Write err = %v", err)
	}

	accepted := <-acceptCh
	if accepted.err != nil {
		t.Fatalf("AcceptStream err = %v", accepted.err)
	}
	if err := stream.CloseWrite(); err != nil {
		t.Fatalf("CloseWrite err = %v", err)
	}
	if _, err := stream.Write([]byte("y")); !errors.Is(err, zmux.ErrWriteClosed) {
		t.Fatalf("Write err = %v, want %v", err, zmux.ErrWriteClosed)
	}

	_ = accepted.stream.Close()
	_ = stream.Close()
}

func TestWrapSessionUniOpenMetadataVisibleOnAccept(t *testing.T) {
	client, server := newWrappedPair(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	priority := uint64(3)
	group := uint64(21)

	type acceptResult struct {
		stream zmux.RecvStream
		err    error
	}
	acceptCh := make(chan acceptResult, 1)
	go func() {
		stream, err := server.AcceptUniStream(ctx)
		acceptCh <- acceptResult{stream: stream, err: err}
	}()

	stream, err := client.OpenUniStreamWithOptions(ctx, zmux.OpenOptions{
		InitialPriority: &priority,
		InitialGroup:    &group,
		OpenInfo:        []byte("rpc"),
	})
	if err != nil {
		t.Fatalf("OpenUniStreamWithOptions err = %v", err)
	}

	accepted := <-acceptCh
	if accepted.err != nil {
		t.Fatalf("AcceptUniStream err = %v", accepted.err)
	}

	meta := accepted.stream.Metadata()
	if meta.Priority != priority {
		t.Fatalf("accepted Priority = %d, want %d", meta.Priority, priority)
	}
	if meta.Group == nil || *meta.Group != group {
		t.Fatalf("accepted Group = %v, want %d", meta.Group, group)
	}
	if got := string(meta.OpenInfo); got != "rpc" {
		t.Fatalf("accepted OpenInfo = %q, want %q", got, "rpc")
	}

	_ = accepted.stream.Close()
	_ = stream.Close()
}

func newWrappedPair(t *testing.T) (zmux.Session, zmux.Session) {
	t.Helper()

	clientConn, serverConn := newQUICConnPair(t)
	return WrapSession(clientConn), WrapSession(serverConn)
}

func newQUICConnPair(t *testing.T) (*quic.Conn, *quic.Conn) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	serverTLS, clientTLS := testTLSConfigs(t)
	listener, err := quic.ListenAddr("127.0.0.1:0", serverTLS, nil)
	if err != nil {
		t.Fatalf("ListenAddr err = %v", err)
	}
	t.Cleanup(func() {
		_ = listener.Close()
	})

	type acceptResult struct {
		conn *quic.Conn
		err  error
	}
	acceptCh := make(chan acceptResult, 1)
	go func() {
		conn, err := listener.Accept(ctx)
		acceptCh <- acceptResult{conn: conn, err: err}
	}()

	clientConn, err := quic.DialAddr(ctx, listener.Addr().String(), clientTLS, nil)
	if err != nil {
		t.Fatalf("DialAddr err = %v", err)
	}
	t.Cleanup(func() {
		_ = clientConn.CloseWithError(0, "")
	})

	serverResult := <-acceptCh
	if serverResult.err != nil {
		t.Fatalf("Accept err = %v", serverResult.err)
	}
	t.Cleanup(func() {
		_ = serverResult.conn.CloseWithError(0, "")
	})

	return clientConn, serverResult.conn
}

type partialPreludeWriter struct {
	bytes.Buffer
	failAfter int
	failErr   error
	failed    bool
}

func (w *partialPreludeWriter) Write(p []byte) (int, error) {
	if !w.failed && w.failAfter > 0 {
		n := w.failAfter
		if n > len(p) {
			n = len(p)
		}
		if n > 0 {
			_, _ = w.Buffer.Write(p[:n])
		}
		w.failed = true
		return n, w.failErr
	}
	return w.Buffer.Write(p)
}

func testTLSConfigs(t *testing.T) (*tls.Config, *tls.Config) {
	t.Helper()

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("GenerateKey err = %v", err)
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: "localhost",
		},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{"localhost"},
		IPAddresses:           nil,
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, &privateKey.PublicKey, privateKey)
	if err != nil {
		t.Fatalf("CreateCertificate err = %v", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privateKey)})
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("X509KeyPair err = %v", err)
	}

	serverTLS := &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{"zmux-quicmux-test"},
	}
	clientTLS := &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{"zmux-quicmux-test"},
	}
	return serverTLS, clientTLS
}
