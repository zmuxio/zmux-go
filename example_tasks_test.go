package zmux_test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"sync"
	"time"

	"github.com/zmuxio/zmux-go"
)

// echo answers every stream of conn with what it read, until conn ends.
func echo(ctx context.Context, conn *zmux.Conn) {
	for {
		st, err := conn.AcceptStream(ctx)
		if err != nil {
			return
		}
		go func() {
			_, _ = io.Copy(st, st)
			_ = st.Close()
		}()
	}
}

// roundTrip opens a stream, sends msg, finishes the stream and returns the
// answer.
func roundTrip(ctx context.Context, conn *zmux.Conn, msg string) (string, error) {
	st, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return "", err
	}
	if _, err := st.Write([]byte(msg)); err != nil {
		return "", err
	}
	if err := st.Close(); err != nil {
		return "", err
	}
	b, err := io.ReadAll(st)
	return string(b), err
}

// Example_tcp serves zmux on a TCP listener and dials it. The Listener
// is the server for every kind of client, including those that later join
// more pipes to their session.
func Example_tcp() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Server.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		fmt.Println(err)
		return
	}
	l, err := zmux.NewListener(nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() { _ = l.Close() }()
	go func() { _ = l.Serve(ctx, ln) }()
	go func() {
		for {
			conn, err := l.Accept(ctx)
			if err != nil {
				return
			}
			go echo(ctx, conn)
		}
	}()

	// Client.
	conn, err := zmux.Dial(ctx, "tcp", ln.Addr().String(), nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	reply, err := roundTrip(ctx, conn, "hello over TCP")
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(reply)

	// Graceful end: the client, then every session of the listener.
	_ = conn.Shutdown(ctx, 0, "")
	_ = l.Shutdown(ctx, 0, "")
	// Output: hello over TCP
}

// ExampleDialTLS serves zmux over TLS (tls.NewListener) and dials it with
// DialTLS.
func ExampleDialTLS() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	serverTLS, clientTLS, err := testTLSConfigs()
	if err != nil {
		fmt.Println(err)
		return
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		fmt.Println(err)
		return
	}
	l, err := zmux.NewListener(nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() { _ = l.Close() }()
	go func() { _ = l.Serve(ctx, tls.NewListener(ln, serverTLS)) }()
	go func() {
		for {
			conn, err := l.Accept(ctx)
			if err != nil {
				return
			}
			go echo(ctx, conn)
		}
	}()

	conn, err := zmux.DialTLS(ctx, "tcp", ln.Addr().String(), clientTLS, nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	reply, err := roundTrip(ctx, conn, "hello over TLS")
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(reply)
	_ = conn.Shutdown(ctx, 0, "")
	_ = l.Shutdown(ctx, 0, "")
	// Output: hello over TLS
}

// ExampleConn_Shutdown ends a session gracefully: the stream the server is
// still reading is delivered in full before the session closes, which
// Close would not wait for.
func ExampleConn_Shutdown() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, server, err := sessions(ctx, nil, nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer closeBoth(client, server)

	got := make(chan int, 1)
	go func() {
		st, err := server.AcceptStream(ctx)
		if err != nil {
			got <- -1
			return
		}
		b, _ := io.ReadAll(st)
		_ = st.Close()
		got <- len(b)
	}()

	st, err := client.OpenStreamSync(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}
	if _, err := st.Write(make([]byte, 1<<20)); err != nil {
		fmt.Println(err)
		return
	}
	_ = st.Close()
	go func() { _, _ = io.Copy(io.Discard, st) }()

	if err := client.Shutdown(ctx, 0, "done"); err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("server read", <-got, "bytes")
	// Output: server read 1048576 bytes
}

// ExampleStream_SetPreamble opens a stream with a preamble that names its
// purpose and a priority, both sent with its first frame.
func ExampleStream_SetPreamble() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, server, err := sessions(ctx, nil, nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer closeBoth(client, server)

	st, err := client.OpenStreamSync(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}
	// Settings that travel with the first frame come before the first
	// Write.
	if err := st.SetPreamble([]byte("route=/chat")); err != nil {
		fmt.Println(err)
		return
	}
	st.SetPriority(zmux.UrgencyHighest, false) // local scheduling
	if err := st.SetPeerPriority(zmux.UrgencyHighest, false); err != nil {
		fmt.Println(err) // the peer's scheduling of its answers
		return
	}
	if _, err := st.Write([]byte("hi")); err != nil {
		fmt.Println(err)
		return
	}

	peer, err := server.AcceptStream(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("%s\n", peer.Preamble())
	// Output: route=/chat
}

// ExampleConn_SendDatagram sends an unreliable datagram; both endpoints
// enable datagrams.
func ExampleConn_SendDatagram() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cfg := &zmux.Config{EnableDatagrams: true}
	client, server, err := sessions(ctx, cfg, cfg)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer closeBoth(client, server)

	// SendDatagram never blocks and drops the oldest queued datagram when
	// the queue is full; SendDatagramWait waits for room instead.
	if err := client.SendDatagramWait(ctx, []byte("ping")); err != nil {
		fmt.Println(err)
		return
	}
	d, err := server.ReceiveDatagram(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("%s\n", d)
	// Output: ping
}

// ExampleConn_AddPipe joins a second pipe to a session and follows its
// state with NextEvent. Multipath needs SteadyAttachments above 1 and a join secret
// on both sides, and a Listener on the server.
func ExampleConn_AddPipe() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cfg := &zmux.Config{SessionPolicy: zmux.SessionPolicy{Limits: zmux.CapacityLimits{SteadyAttachments: 2}}, PipeAuth: zmux.PipeAuth{PSK: []byte("an example pre-shared key, 32 B"), EndToEnd: true}}
	l, err := zmux.NewListener(&zmux.ListenerConfig{Config: cfg})
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() { _ = l.Close() }()

	// Two carriers between the same endpoints (two paths in practice).
	a0, b0 := net.Pipe()
	a1, b1 := net.Pipe()
	go func() { _ = l.HandlePipe(ctx, zmux.NetPipe(b0), zmux.PipeHint{}) }()
	go func() { _ = l.HandlePipe(ctx, zmux.NetPipe(b1), zmux.PipeHint{}) }()

	client, err := zmux.Client(ctx, a0, cfg)
	if err != nil {
		fmt.Println(err)
		return
	}
	server, err := l.Accept(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer closeBoth(client, server)

	p, err := client.AddPipe(ctx, zmux.NetPipe(a1), zmux.PipeOptions{Label: "backup", SendStandby: zmux.Set(true)})
	if err != nil {
		fmt.Println(err)
		return
	}
	for {
		ev, err := client.NextEvent(ctx)
		if err != nil {
			fmt.Println(err)
			return
		}
		if ev.Kind == zmux.EventPipeJoined && ev.PipeID == p.ID() {
			break
		}
	}
	st, ok := p.Stats()
	if !ok {
		fmt.Println(zmux.ErrPipeGone)
		return
	}
	fmt.Println(len(client.Pipes()), "pipes;", st.Label, st.Outbound, "standby:", st.Standby)
	// Output: 2 pipes; backup active standby: true
}

// ExampleSplice relays a stream of one session into a stream of another,
// as a proxy does: data, ends and backpressure cross without a copy.
func ExampleSplice() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// client <-> relay (session A); relay <-> backend (session B).
	client, relayA, err := sessions(ctx, nil, nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer closeBoth(client, relayA)
	relayB, backend, err := sessions(ctx, nil, nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer closeBoth(relayB, backend)
	go echo(ctx, backend)

	res := make(chan zmux.SpliceResult, 1)
	go func() {
		in, err := relayA.AcceptStream(ctx)
		if err != nil {
			return
		}
		out, err := relayB.OpenStreamSync(ctx)
		if err != nil {
			in.CancelRead(zmux.StreamCancelled)
			in.CancelWrite(zmux.StreamCancelled)
			return
		}
		r, _ := zmux.Splice(ctx, in, out, nil)
		res <- r
	}()

	reply, err := roundTrip(ctx, client, "through the relay")
	if err != nil {
		fmt.Println(err)
		return
	}
	r := <-res
	fmt.Println(reply)
	fmt.Println(r.AtoB.Bytes, r.BtoA.Bytes, r.AtoB.End, r.BtoA.End)
	// Output:
	// through the relay
	// 17 17 finished finished
}

// wsConn is the message interface that WebSocket libraries offer, in the
// common shape: whole messages with a message type.
type wsConn interface {
	ReadMessage() (messageType int, p []byte, err error)
	WriteMessage(messageType int, data []byte) error
	Close() error
}

const wsBinary = 2 // the binary message type of RFC 6455

// wsPipe adapts a WebSocket connection to a PipeIO with FramingRecords:
// one zmux record per binary message.
func wsPipe(c wsConn) zmux.PipeIO {
	a := &wsAdapter{c: c}
	return zmux.PipeIO{R: a, W: a, Framing: zmux.FramingRecords}
}

type wsAdapter struct{ c wsConn }

// ReadMessage returns the next binary message of at most limit bytes. A
// library with a read limit (SetReadLimit) should be given it instead, so
// that a longer message is never read whole.
func (a *wsAdapter) ReadMessage(limit int) (zmux.ReceivedMessage, error) {
	for {
		t, p, err := a.c.ReadMessage()
		if err == nil && len(p) > limit {
			return zmux.ReceivedMessage{}, zmux.ErrMessageTooLarge
		}
		if err != nil {
			return zmux.ReceivedMessage{}, err
		}
		if t == wsBinary {
			return zmux.OwnMessage(p), nil
		}
	}
}
func (a *wsAdapter) WriteMessage(p []byte) error { return a.c.WriteMessage(wsBinary, p) }
func (a *wsAdapter) Read([]byte) (int, error)    { return 0, errors.New("message carrier") }
func (a *wsAdapter) Write([]byte) (int, error)   { return 0, errors.New("message carrier") }
func (a *wsAdapter) Close() error                { return a.c.Close() }

// Example_webSocket runs a session over a message-framed carrier such as
// WebSocket, through the adapter wsPipe above; an in-memory connection
// stands in for the WebSocket library here.
func Example_webSocket() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ca, sa := memWebSocketPair()

	done := make(chan *zmux.Conn, 1)
	go func() {
		server, err := zmux.Start(ctx, wsPipe(sa), zmux.RoleServer, nil)
		if err != nil {
			done <- nil
			return
		}
		go echo(ctx, server)
		done <- server
	}()
	client, err := zmux.Start(ctx, wsPipe(ca), zmux.RoleClient, nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	server := <-done
	if server == nil {
		_ = client.Close()
		fmt.Println("server failed")
		return
	}
	defer closeBoth(client, server)

	reply, err := roundTrip(ctx, client, "hello over messages")
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(reply)
	// Output: hello over messages
}

// memWebSocket is an in-memory stand-in for a WebSocket connection.
type memWebSocket struct {
	in, out chan []byte
	done    chan struct{} // closed when either side closes
	once    *sync.Once
}

func memWebSocketPair() (*memWebSocket, *memWebSocket) {
	ab, ba := make(chan []byte, 16), make(chan []byte, 16)
	done, once := make(chan struct{}), &sync.Once{}
	return &memWebSocket{in: ba, out: ab, done: done, once: once},
		&memWebSocket{in: ab, out: ba, done: done, once: once}
}

func (m *memWebSocket) ReadMessage() (int, []byte, error) {
	select {
	case p := <-m.in:
		return wsBinary, p, nil
	case <-m.done:
		return 0, nil, net.ErrClosed
	}
}

func (m *memWebSocket) WriteMessage(_ int, p []byte) error {
	select {
	case m.out <- bytes.Clone(p):
		return nil
	case <-m.done:
		return net.ErrClosed
	}
}

func (m *memWebSocket) Close() error {
	m.once.Do(func() { close(m.done) })
	return nil
}

// testTLSConfigs returns TLS configurations with a fresh self-signed
// certificate for 127.0.0.1.
func testTLSConfigs() (server, client *tls.Config, err error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, nil, err
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, nil, err
	}
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	server = &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}},
		MinVersion: tls.VersionTLS13}
	client = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS13}
	return server, client, nil
}
