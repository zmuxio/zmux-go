package zmux

import (
	"errors"
	"net"
	"testing"
)

type testReadWriteCloser struct{}

func (testReadWriteCloser) Read([]byte) (int, error)    { return 0, nil }
func (testReadWriteCloser) Write(p []byte) (int, error) { return len(p), nil }
func (testReadWriteCloser) Close() error                { return nil }

func TestCloseSessionClearsProtocolBacklogAndDrainsBufferedDataQueues(t *testing.T) {
	t.Parallel()

	ordinary := make(chan writeRequest, 1)
	advisory := make(chan writeRequest, 1)
	urgent := make(chan writeRequest, 1)

	ordinaryDone := make(chan error, 1)
	advisoryDone := make(chan error, 1)
	urgentDone := make(chan error, 1)

	ordinary <- writeRequest{
		done:   ordinaryDone,
		frames: testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: 4, Payload: []byte("ordinary")}}),
	}
	advisory <- writeRequest{
		done:   advisoryDone,
		frames: testTxFramesFrom([]Frame{{Type: FrameTypeEXT, StreamID: 4, Payload: []byte{0x01}}}),
	}
	urgent <- writeRequest{
		done:   urgentDone,
		frames: testTxFramesFrom([]Frame{{Type: FrameTypePING, Payload: []byte("urgent")}}),
	}

	c := &Conn{
		lifecycle: connLifecycleState{
			closedCh:     make(chan struct{}),
			terminalCh:   make(chan struct{}),
			sessionState: connStateReady,
		},
		io: connIOState{conn: testReadWriteCloser{}},
		writer: connWriterRuntimeState{
			writeCh:         ordinary,
			advisoryWriteCh: advisory,
			urgentWriteCh:   urgent,
		},
		pending: connPendingControlState{
			controlNotify:  make(chan struct{}, 1),
			terminalNotify: make(chan struct{}, 1),
		},
		protocol: connProtocolRuntimeState{
			notifyCh: make(chan struct{}, 1),
			tasks: []protocolTask{
				{kind: protocolTaskQueueFrame, frame: flatTxFrame(Frame{Type: FrameTypePONG, Payload: []byte("held")})},
			},
		},
	}

	c.closeSessionWithOptions(ErrSessionClosed, closeOriginInternal, closeFrameDefault)

	if c.protocol.tasks != nil {
		t.Fatalf("protocol backlog not cleared after close")
	}
	if got := len(ordinary); got != 0 {
		t.Fatalf("ordinary write lane still buffered %d request(s)", got)
	}
	if got := len(advisory); got != 0 {
		t.Fatalf("advisory write lane still buffered %d request(s)", got)
	}
	if got := len(urgent); got != 1 {
		t.Fatalf("urgent write lane depth = %d, want 1 buffered urgent request", got)
	}

	for name, done := range map[string]chan error{
		"ordinary": ordinaryDone,
		"advisory": advisoryDone,
	} {
		select {
		case err := <-done:
			if !errors.Is(err, ErrSessionClosed) {
				t.Fatalf("%s drained request err = %v, want ErrSessionClosed", name, err)
			}
		default:
			t.Fatalf("%s drained request did not complete", name)
		}
	}
	select {
	case err := <-urgentDone:
		t.Fatalf("urgent request completed unexpectedly with %v", err)
	default:
	}
}

func TestWriteBatchScratchScatterGatherBuffersDropsOversizedRetention(t *testing.T) {
	t.Parallel()

	scratch := writeBatchScratch{
		sgBuffers: make(net.Buffers, 0, maxRetainedScatterGatherSegments+512),
	}

	got := scratch.scatterGatherBuffers(1)
	if cap(got) > maxRetainedScatterGatherSegments {
		t.Fatalf("scatter-gather scratch cap = %d, want <= %d", cap(got), maxRetainedScatterGatherSegments)
	}
}
