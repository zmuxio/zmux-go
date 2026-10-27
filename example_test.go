package zmux_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/zmuxio/zmux-go"
)

// sessions returns a client and a server session over an in-memory carrier.
func sessions(ctx context.Context, ccfg, scfg *zmux.Config) (client, server *zmux.Conn, err error) {
	a, b := net.Pipe()
	ch := make(chan error, 1)
	go func() {
		var err error
		server, err = zmux.Server(ctx, b, scfg)
		ch <- err
	}()
	client, err = zmux.Client(ctx, a, ccfg)
	if serr := <-ch; err == nil {
		err = serr
	}
	if err != nil {
		if client != nil {
			_ = client.Close()
		}
		if server != nil {
			_ = server.Close()
		}
		return nil, nil, err
	}
	return client, server, nil
}

// closeBoth closes both sessions and waits for their ends.
func closeBoth(a, b *zmux.Conn) {
	_ = a.Close()
	_ = b.Close()
	_ = a.Wait()
	_ = b.Wait()
}

// ExampleStreamConn hands a stream to code written for net.Conn.
func ExampleStreamConn() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, server, err := sessions(ctx, nil, nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer closeBoth(client, server)
	go func() {
		st, err := server.AcceptStream(ctx)
		if err != nil {
			return
		}
		_, _ = io.Copy(st, st) // echo
		_ = st.Close()
	}()

	st, err := client.OpenStreamSync(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}
	nc := zmux.StreamConn(st, client.LocalAddr(), client.RemoteAddr())
	if _, err := nc.Write([]byte("hello")); err != nil {
		fmt.Println(err)
		return
	}
	buf := make([]byte, 5)
	if _, err := io.ReadFull(nc, buf); err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(string(buf))
	// Close finishes the stream and stops reading, as net.Conn users expect.
	_ = nc.Close()
	// Output: hello
}

// ExampleStreamError shows a stream canceled with a conventional code: the
// peer sees that code.
func ExampleStreamError() {
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
	if _, err := st.Write([]byte("x")); err != nil {
		fmt.Println(err)
		return
	}
	peer, err := server.AcceptStream(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}
	st.CancelWrite(zmux.StreamCancelled)

	_, err = io.ReadAll(peer)
	var se *zmux.StreamError
	if errors.As(err, &se) {
		switch se.ErrorCode {
		case zmux.StreamNoError:
			fmt.Println("no error")
		case zmux.StreamCancelled:
			fmt.Println("cancelled by the peer")
		case zmux.StreamRejected, zmux.StreamCarrierFailed:
			fmt.Println("rejected or carrier failed")
		default:
			fmt.Printf("application code %#x\n", uint64(se.ErrorCode))
		}
	}
	// Output: cancelled by the peer
}

// ExampleTransportError sorts session ends by their transport error code.
func ExampleTransportError() {
	describe := func(err error) string {
		var te *zmux.TransportError
		if !errors.As(err, &te) {
			return "not a transport error"
		}
		switch te.ErrorCode {
		case zmux.TransportNoError:
			return "closed without error"
		case zmux.TransportProtocolViolation, zmux.TransportFrameEncodingError, zmux.TransportFlowControlError,
			zmux.TransportStreamLimitError, zmux.TransportStreamStateError, zmux.TransportFinalSizeError,
			zmux.TransportParameterError, zmux.TransportRoleConflict:
			return "peer misbehaved: " + te.ErrorCode.String()
		case zmux.TransportInternalError, zmux.TransportExcessiveLoad, zmux.TransportConnectionRefused:
			return "peer refused or failed: " + te.ErrorCode.String()
		case zmux.TransportNoViablePath, zmux.TransportResumeTimeout, zmux.TransportResumeFailed, zmux.TransportIdleTimeout:
			return "carriers lost: " + te.ErrorCode.String()
		default:
			return "other: " + te.ErrorCode.String()
		}
	}
	fmt.Println(describe(&zmux.TransportError{Remote: true, ErrorCode: zmux.TransportProtocolViolation}))
	fmt.Println(describe(&zmux.TransportError{Remote: true, ErrorCode: zmux.TransportIdleTimeout}))
	// Output:
	// peer misbehaved: PROTOCOL_VIOLATION
	// carriers lost: ZMUX_IDLE_TIMEOUT
}

// ExampleScheduler sets a session's scheduling policy in its Config and
// changes it while the session runs; it applies once there are several pipes.
func ExampleScheduler() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cfg := &zmux.Config{Scheduler: zmux.SessionAffinity()} // one active pipe per tier
	client, server, err := sessions(ctx, cfg, nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer closeBoth(client, server)

	// Share chunks by weight times measured rate from now on.
	client.SetScheduler(zmux.SpreadRateWeighted())
	fmt.Println(len(client.PipeStats()))
	// Output: 1
}
