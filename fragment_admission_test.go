package zmux

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"
)

// TestWriteFragmentsFitLocalQueueAdmission writes more than one per-stream
// queued-data high watermark at once. A peer may advertise a max_frame_payload
// far above the local admission limits (SPEC §5.3), and a local HWM may be
// configured below one peer frame; each queued DATA frame must still fit the
// local limits, or the write can never be admitted.
func TestWriteFragmentsFitLocalQueueAdmission(t *testing.T) {
	t.Parallel()

	largeFrames := DefaultSettings()
	largeFrames.MaxFramePayload = 1 << 20

	largeFramesAndWindows := largeFrames
	largeFramesAndWindows.InitialMaxData = 64 << 20
	largeFramesAndWindows.InitialMaxStreamDataBidiPeerOpened = 16 << 20

	tests := []struct {
		name      string
		clientCfg *Config
		serverCfg *Config
		openInfo  []byte
		size      int
	}{
		{
			name:      "peer_large_frames_default_windows",
			serverCfg: &Config{Settings: largeFrames},
			size:      1 << 20,
		},
		{
			name:      "peer_large_frames_and_windows",
			serverCfg: &Config{Settings: largeFramesAndWindows},
			size:      300 << 10,
		},
		{
			name:      "peer_large_frames_and_windows_one_mib",
			serverCfg: &Config{Settings: largeFramesAndWindows},
			size:      1 << 20,
		},
		{
			name:      "local_stream_hwm_below_one_frame",
			clientCfg: &Config{PerStreamQueuedDataHWM: 4096},
			size:      60000,
		},
		{
			name:      "local_session_hwm_below_one_frame",
			clientCfg: &Config{SessionQueuedDataHWM: 4096},
			size:      60000,
		},
		{
			// The opener alone exceeds the watermark; an empty queue still
			// admits it.
			name:      "local_stream_hwm_below_opener_prefix",
			clientCfg: &Config{PerStreamQueuedDataHWM: 64},
			openInfo:  bytes.Repeat([]byte{0x01}, 300),
			size:      5000,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client, server := newConnPairWithConfig(t, tc.clientCfg, tc.serverCfg)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			payload := bytes.Repeat([]byte{0x5a}, tc.size)
			received := make(chan []byte, 1)
			go func() {
				stream, err := server.AcceptStream(ctx)
				if err != nil {
					received <- nil
					return
				}
				data, _ := io.ReadAll(stream)
				_ = stream.Close()
				received <- data
			}()

			stream, err := client.OpenStreamWithOptions(ctx, OpenOptions{OpenInfo: tc.openInfo})
			if err != nil {
				t.Fatalf("OpenStream: %v", err)
			}
			done := make(chan error, 1)
			go func() {
				if _, err := stream.Write(payload); err != nil {
					done <- err
					return
				}
				done <- stream.CloseWrite()
			}()
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("Write/CloseWrite err = %v", err)
				}
			case <-ctx.Done():
				t.Fatalf("Write of %d bytes did not complete (client err = %v)", tc.size, client.err())
			}
			select {
			case data := <-received:
				if !bytes.Equal(data, payload) {
					t.Fatalf("server read %d bytes, want %d", len(data), len(payload))
				}
			case <-ctx.Done():
				t.Fatal("server did not read the whole stream")
			}
		})
	}
}

func TestTxFragmentCapFitsQueueAdmission(t *testing.T) {
	t.Parallel()

	settings := DefaultSettings()
	settings.MaxFramePayload = 1 << 20
	c := newSessionMemoryTestConn()
	c.config.peer.Settings = settings
	stream := c.newLocalStreamWithIDLocked(4, streamArityBidi, OpenOptions{}, nil)

	tests := []struct {
		name      string
		streamHWM uint64
		prefixLen uint64
	}{
		{name: "default_hwm", prefixLen: 0},
		{name: "default_hwm_with_opener_prefix", prefixLen: 300},
		{name: "small_hwm", streamHWM: 4096, prefixLen: 0},
		{name: "small_hwm_with_opener_prefix", streamHWM: 4096, prefixLen: 300},
		{name: "hwm_below_prefix", streamHWM: 64, prefixLen: 300},
	}
	for _, tc := range tests {
		c.flow.perStreamDataHWM = tc.streamHWM
		queueCap := stream.writeRequestQueueCapLocked()
		fragmentCap := stream.txFragmentCapLocked(tc.prefixLen)
		if fragmentCap == 0 {
			t.Fatalf("%s: fragment cap = 0, want a positive cap", tc.name)
		}
		frameCost := fragmentCap + tc.prefixLen + 1
		if queueCap > tc.prefixLen+1 && frameCost > queueCap {
			t.Fatalf("%s: frame cost %d exceeds queue cap %d (fragment cap %d)", tc.name, frameCost, queueCap, fragmentCap)
		}
	}
}

// TestQueueReleaseToEmptyWakesAdmissionWithoutMemoryWake drains a session
// queue that already sits below the low watermark, with the rest of session
// memory keeping the memory wake from firing. A request larger than the gap
// between the watermarks can be blocked by those queued bytes, and an empty
// queue admits it, so the drain itself must wake writers blocked on queue
// admission rather than rely on unrelated tracked memory falling.
func TestQueueReleaseToEmptyWakesAdmissionWithoutMemoryWake(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		release func(*Conn, *writeRequest)
	}{
		{name: "writer_batch", release: func(c *Conn, req *writeRequest) {
			c.releaseBatchReservations([]writeRequest{*req})
		}},
		{name: "single_request", release: func(c *Conn, req *writeRequest) {
			c.releaseWriteQueueReservation(req)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newSessionMemoryTestConn()
			stream := testBuildStream(c, 8, testWithLocalSend())
			req := writeRequest{
				frames:         testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: stream.id, Payload: []byte("held")}}),
				done:           make(chan error, 1),
				origin:         writeRequestOriginStream,
				queueReserved:  true,
				queuedBytes:    5,
				reservedStream: stream,
			}

			c.mu.Lock()
			c.flow.sessionMemoryCap = 16
			c.flow.sessionDataHWM = 100
			c.flow.perStreamDataHWM = 100
			c.flow.recvSessionUsed = 20
			c.flow.queuedDataBytes = 5
			stream.queuedDataBytes = 5
			wake := c.currentWriteWakeLocked()
			c.mu.Unlock()

			tc.release(c, &req)

			select {
			case <-wake:
			default:
				t.Fatal("draining the queue did not wake writers blocked on queue admission")
			}
		})
	}
}
