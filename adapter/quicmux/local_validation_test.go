package quicmux

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/zmuxio/zmux-go"
)

// outOfRangeQUICErrorCodes have no varint62 encoding. quic-go accepts them in
// CancelRead / CancelWrite / CloseWithError and then panics in its connection
// goroutine while packing the frame, which kills the process.
var outOfRangeQUICErrorCodes = []uint64{1 << 62, math.MaxUint64}

func requireAdapterUnsupported(t *testing.T, what string, err error) {
	t.Helper()
	if !errors.Is(err, zmux.ErrAdapterUnsupported) {
		t.Fatalf("%s err = %v, want ErrAdapterUnsupported", what, err)
	}
}

func readExactly(t *testing.T, stream io.Reader, want string) {
	t.Helper()
	buf := make([]byte, len(want))
	if _, err := io.ReadFull(stream, buf); err != nil {
		t.Fatalf("read %q err = %v", want, err)
	}
	if string(buf) != want {
		t.Fatalf("read %q, want %q", buf, want)
	}
}

func requireStreamResetCode(t *testing.T, stream io.Reader, code uint64) {
	t.Helper()
	buf := make([]byte, 16)
	for {
		_, err := stream.Read(buf)
		if err == nil {
			continue
		}
		appErr, ok := findError[*zmux.ApplicationError](err)
		if !ok || appErr.Code != code {
			t.Fatalf("Read err = %v, want ApplicationError(%d)", err, code)
		}
		return
	}
}

func requireStopSendingCode(t *testing.T, stream zmux.SendStream, code uint64) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		_, err := stream.Write([]byte("x"))
		if err == nil {
			time.Sleep(5 * time.Millisecond)
			continue
		}
		appErr, ok := findError[*zmux.ApplicationError](err)
		if !ok || appErr.Code != code {
			t.Fatalf("Write err = %v, want ApplicationError(%d)", err, code)
		}
		return
	}
	t.Fatalf("Write never observed STOP_SENDING(%d)", code)
}

// requireSessionUsable exchanges data on a fresh stream, which needs quic-go's
// connection goroutine to keep packing frames.
func requireSessionUsable(t *testing.T, ctx context.Context, client, server zmux.Session) {
	t.Helper()
	clientStream, serverStream := openAndAcceptVisibleStream(t, ctx, client, server, []byte("alive"))
	readExactly(t, serverStream, "alive")
	if _, err := serverStream.Write([]byte("ack")); err != nil {
		t.Fatalf("server Write err = %v", err)
	}
	readExactly(t, clientStream, "ack")
}

func TestWrapSessionStreamRejectsOutOfRangeErrorCodesLocally(t *testing.T) {
	for _, code := range outOfRangeQUICErrorCodes {
		t.Run(fmt.Sprintf("bidi/code_%d", code), func(t *testing.T) {
			client, server := newWrappedPair(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			clientStream, serverStream := openAndAcceptVisibleStream(t, ctx, client, server, []byte("a"))
			readExactly(t, serverStream, "a")

			requireAdapterUnsupported(t, "CancelWrite", clientStream.CancelWrite(code))
			requireAdapterUnsupported(t, "CancelRead", clientStream.CancelRead(code))
			requireAdapterUnsupported(t, "CloseWithError", clientStream.CloseWithError(code, "bad"))

			// The rejected calls left both halves open.
			if _, err := clientStream.Write([]byte("b")); err != nil {
				t.Fatalf("Write after rejected codes err = %v", err)
			}
			readExactly(t, serverStream, "b")
			if _, err := serverStream.Write([]byte("c")); err != nil {
				t.Fatalf("server Write err = %v", err)
			}
			readExactly(t, clientStream, "c")

			if err := clientStream.CancelWrite(44); err != nil {
				t.Fatalf("CancelWrite(44) err = %v", err)
			}
			requireStreamResetCode(t, serverStream, 44)
			requireSessionUsable(t, ctx, client, server)
		})

		t.Run(fmt.Sprintf("uni/code_%d", code), func(t *testing.T) {
			client, server := newWrappedPair(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			acceptCh := acceptUniStreamAsync(ctx, server, 1)
			send, err := client.OpenUniStream(ctx)
			if err != nil {
				t.Fatalf("OpenUniStream err = %v", err)
			}
			if _, err := send.Write([]byte("a")); err != nil {
				t.Fatalf("uni Write err = %v", err)
			}
			recv := requireAcceptedUniStream(t, acceptCh)
			readExactly(t, recv, "a")

			requireAdapterUnsupported(t, "send CancelWrite", send.CancelWrite(code))
			requireAdapterUnsupported(t, "send CloseWithError", send.CloseWithError(code, "bad"))
			requireAdapterUnsupported(t, "recv CancelRead", recv.CancelRead(code))
			requireAdapterUnsupported(t, "recv CloseWithError", recv.CloseWithError(code, "bad"))

			if _, err := send.Write([]byte("b")); err != nil {
				t.Fatalf("uni Write after rejected codes err = %v", err)
			}
			readExactly(t, recv, "b")

			if err := recv.CancelRead(46); err != nil {
				t.Fatalf("CancelRead(46) err = %v", err)
			}
			requireStopSendingCode(t, send, 46)
			requireSessionUsable(t, ctx, client, server)
		})
	}
}

// The largest varint62 is still a valid QUIC error code and reaches the peer.
func TestWrapSessionStreamCarriesMaxVarint62ErrorCode(t *testing.T) {
	client, server := newWrappedPair(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	clientStream, serverStream := openAndAcceptVisibleStream(t, ctx, client, server, []byte("a"))
	readExactly(t, serverStream, "a")
	if err := clientStream.CancelWrite(zmux.MaxVarint62); err != nil {
		t.Fatalf("CancelWrite(MaxVarint62) err = %v", err)
	}
	requireStreamResetCode(t, serverStream, zmux.MaxVarint62)
	requireSessionUsable(t, ctx, client, server)
}

func TestWrapSessionCloseWithErrorOutOfRangeCodeClosesWithInternal(t *testing.T) {
	for _, code := range outOfRangeQUICErrorCodes {
		client, server := newWrappedPair(t)

		// No panic in quic-go's connection goroutine: the code is replaced
		// before CONNECTION_CLOSE is packed.
		client.CloseWithError(&zmux.ApplicationError{Code: code, Reason: "bad code"})

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := server.Wait(ctx)
		cancel()
		appErr, ok := findError[*zmux.ApplicationError](err)
		if !ok || appErr.Code != uint64(zmux.CodeInternal) || appErr.Reason != "bad code" {
			t.Fatalf("server Wait err = %v, want ApplicationError(INTERNAL, %q)", err, "bad code")
		}
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		err = client.Wait(ctx)
		cancel()
		if appErr, ok := findError[*zmux.ApplicationError](err); !ok || appErr.Code != uint64(zmux.CodeInternal) {
			t.Fatalf("client Wait err = %v, want ApplicationError(INTERNAL)", err)
		}
	}
}

func TestWrapSessionInvalidOpenMetadataFailsBeforeOpeningQUICStream(t *testing.T) {
	tooLargePriority := uint64(1 << 62)
	cases := []struct {
		name    string
		opts    zmux.OpenOptions
		wantErr error
	}{
		{name: "open_info_over_prelude_cap", opts: zmux.OpenOptions{OpenInfo: make([]byte, 20000)}, wantErr: zmux.ErrOpenMetadataTooLarge},
		{name: "priority_over_varint62", opts: zmux.OpenOptions{InitialPriority: &tooLargePriority}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clientConn, serverConn := newQUICConnPair(t)
			client := WrapSession(clientConn)
			t.Cleanup(func() { _ = client.Close() })
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			requireOpenErr := func(what string, err error) {
				t.Helper()
				if err == nil {
					t.Fatalf("%s err = nil, want local validation error", what)
				}
				if tc.wantErr != nil && !errors.Is(err, tc.wantErr) {
					t.Fatalf("%s err = %v, want %v", what, err, tc.wantErr)
				}
			}
			_, err := client.OpenStreamWithOptions(ctx, tc.opts)
			requireOpenErr("OpenStreamWithOptions", err)
			_, err = client.OpenUniStreamWithOptions(ctx, tc.opts)
			requireOpenErr("OpenUniStreamWithOptions", err)

			// Nothing reached the wire: the raw peer sees no stream at all.
			acceptCtx, acceptCancel := context.WithTimeout(ctx, 200*time.Millisecond)
			defer acceptCancel()
			if stream, err := serverConn.AcceptStream(acceptCtx); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("raw peer AcceptStream = (%v, %v), want DeadlineExceeded", stream, err)
			}
			if stream, err := serverConn.AcceptUniStream(acceptCtx); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("raw peer AcceptUniStream = (%v, %v), want DeadlineExceeded", stream, err)
			}

			// And no QUIC stream ID was allocated for the rejected opens.
			bidi, err := client.OpenStream(ctx)
			if err != nil {
				t.Fatalf("OpenStream err = %v", err)
			}
			defer func() { _ = bidi.Close() }()
			if got := bidi.StreamID(); got != 0 {
				t.Fatalf("next bidi StreamID = %d, want 0", got)
			}
			uni, err := client.OpenUniStream(ctx)
			if err != nil {
				t.Fatalf("OpenUniStream err = %v", err)
			}
			defer func() { _ = uni.Close() }()
			if got := uni.StreamID(); got != 2 {
				t.Fatalf("next uni StreamID = %d, want 2", got)
			}

			// The valid streams still carry data to the raw peer.
			if _, err := bidi.Write([]byte("ok")); err != nil {
				t.Fatalf("bidi Write err = %v", err)
			}
			raw, err := serverConn.AcceptStream(ctx)
			if err != nil {
				t.Fatalf("raw peer AcceptStream err = %v", err)
			}
			if raw.StreamID() != quic.StreamID(0) {
				t.Fatalf("raw peer accepted stream %d, want 0", raw.StreamID())
			}
			// Empty prelude, then the payload.
			readExactly(t, raw, "\x00ok")
		})
	}
}
