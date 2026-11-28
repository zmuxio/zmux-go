// Command dgpeer is a zmux peer over QUIC for the interoperability tests of
// the datagram channel, with the other implementations.
//
//	dgpeer server CERT KEY   listen on 127.0.0.1, print "LISTEN addr", echo
//	                         every session datagram and stream of the first
//	                         session, print "STATS ..." when it ends
//	dgpeer client ADDR N     dial, send N datagrams of 100 bytes (the index
//	                         as a big-endian u32 first), expect each echoed
//	                         within five seconds, print "STATS ..." and close
//
// ALPN qmux-02; the client does not verify the server's certificate (test
// peers). STATS is "STATS sent=<n> sentch=<n> recv=<n> recvch=<n>": session
// datagrams sent and received, and those that used the QUIC datagram channel.
// A 60-second deadline ends the process.
package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/zmuxquic"
)

const (
	loopbackEndpoint = "127.0.0.1:0"
	alpn             = "qmux-02"
	deadline         = 60 * time.Second
	datagramSize     = 100
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "dgpeer:", err)
		os.Exit(1)
	}
}

var runMatrix func(context.Context, io.Reader, io.Writer, string) error

func run(args []string, out io.Writer) error {
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()
	switch {
	case runMatrix != nil && len(args) == 2 && args[0] == "matrix":
		return runMatrix(ctx, os.Stdin, out, args[1])
	case len(args) == 3 && args[0] == "server":
		return serve(ctx, args[1], args[2], out)
	case len(args) == 3 && args[0] == "client":
		n, err := strconv.Atoi(args[2])
		if err != nil || n < 0 {
			return fmt.Errorf("bad datagram count %q", args[2])
		}
		return dial(ctx, args[1], n, out)
	}
	usage := "usage: dgpeer server CERT KEY | dgpeer client ADDR N"
	if runMatrix != nil {
		usage += " | dgpeer matrix ARTIFACT_DIR"
	}
	return errors.New(usage)
}

func config() *zmux.Config { return &zmux.Config{EnableDatagrams: true} }

func stats(c *zmux.Conn) string {
	s := c.ConnectionStats()
	return fmt.Sprintf("STATS sent=%d sentch=%d recv=%d recvch=%d",
		s.DatagramsSent, s.DatagramsSentChannel, s.DatagramsReceived, s.DatagramsReceivedChannel)
}

func serve(ctx context.Context, certFile, keyFile string, out io.Writer) error {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return err
	}
	tc := &tls.Config{Certificates: []tls.Certificate{cert}, NextProtos: []string{alpn}, MinVersion: tls.VersionTLS13}
	ln, err := quic.ListenAddrEarly(loopbackEndpoint, tc, zmuxquic.QUICConfig(nil))
	if err != nil {
		return err
	}
	defer func() { _ = ln.Close() }()
	l, err := zmux.NewListener(&zmux.ListenerConfig{Config: config()})
	if err != nil {
		return err
	}
	defer func() { _ = l.Close() }()
	go func() { _ = zmuxquic.Serve(ctx, l, ln) }()
	if _, err := fmt.Fprintln(out, "LISTEN", ln.Addr()); err != nil {
		return err
	}
	c, err := l.Accept(ctx)
	if err != nil {
		return err
	}
	go echoStreams(ctx, c)
	for {
		p, err := c.ReceiveDatagram(ctx)
		if err != nil {
			break
		}
		_ = c.SendDatagram(p)
	}
	_, err = fmt.Fprintln(out, stats(c))
	return err
}

// echoStreams copies every accepted bidirectional stream back until the
// peer's FIN.
func echoStreams(ctx context.Context, c *zmux.Conn) {
	for {
		st, err := c.AcceptStream(ctx)
		if err != nil {
			return
		}
		go func() {
			_, _ = io.Copy(st, st)
			_ = st.Close()
		}()
	}
}

func stamp(i int) []byte {
	d := make([]byte, datagramSize)
	binary.BigEndian.PutUint32(d, uint32(i))
	return d
}

func dial(ctx context.Context, addr string, n int, out io.Writer) error {
	tc := &tls.Config{
		InsecureSkipVerify: true, ServerName: "localhost", NextProtos: []string{alpn}, MinVersion: tls.VersionTLS13,
	}
	c, err := zmuxquic.Dial(ctx, addr, tc, nil, config())
	if err != nil {
		return err
	}
	defer func() { _ = c.Close() }()
	for i := range n {
		d := stamp(i)
		if err := c.SendDatagram(d); err != nil {
			return err
		}
		e, err := c.ReceiveDatagram(ctx)
		if err != nil {
			return fmt.Errorf("echo %d: %w", i, err)
		}
		if !bytes.Equal(e, d) {
			return fmt.Errorf("echo %d: got %x", i, e)
		}
	}
	_, err = fmt.Fprintln(out, stats(c))
	return err
}
