package zmux

import (
	"errors"
	"fmt"
	"net"
	"testing"
)

// TestErrorCodeRegistry checks that the exported error codes are the
// registry's (SPEC.md 10.2, EXTENSIONS.md 7).
func TestErrorCodeRegistry(t *testing.T) {
	transport := map[TransportErrorCode]string{
		TransportNoError: "NO_ERROR", TransportInternalError: "INTERNAL_ERROR", TransportConnectionRefused: "CONNECTION_REFUSED",
		TransportFlowControlError: "FLOW_CONTROL_ERROR", TransportStreamLimitError: "STREAM_LIMIT_ERROR",
		TransportStreamStateError: "STREAM_STATE_ERROR", TransportFinalSizeError: "FINAL_SIZE_ERROR",
		TransportFrameEncodingError: "FRAME_ENCODING_ERROR", TransportParameterError: "TRANSPORT_PARAMETER_ERROR",
		TransportProtocolViolation: "PROTOCOL_VIOLATION", TransportNoViablePath: "NO_VIABLE_PATH",
		TransportExcessiveLoad: "ZMUX_EXCESSIVE_LOAD", TransportRoleConflict: "ZMUX_ROLE_CONFLICT",
		TransportResumeTimeout: "ZMUX_RESUME_TIMEOUT", TransportResumeFailed: "ZMUX_RESUME_FAILED",
		TransportIdleTimeout: "ZMUX_IDLE_TIMEOUT",
	}
	for c, name := range transport {
		if got := c.String(); got != name {
			t.Errorf("%#x: String() = %s, want %s", uint64(c), got, name)
		}
	}
	app := map[StreamErrorCode]uint64{
		StreamNoError: 0, StreamCancelled: 0x5a4d0001, StreamRejected: 0x5a4d0002, StreamCarrierFailed: 0x5a4d0003,
	}
	for c, v := range app {
		if uint64(c) != v {
			t.Errorf("application code %#x, want %#x", uint64(c), v)
		}
	}
}

// TestSessionErrorSentinels checks that every session end matches
// net.ErrClosed and its own sentinel, and only that one.
func TestSessionErrorSentinels(t *testing.T) {
	cause := errors.New("carrier reset")
	sentinels := []error{ErrIdleTimeout, ErrHandshakeTimeout, ErrEarlyDataRejected, ErrResumeTimeout, ErrPipesLost}
	for _, c := range []struct {
		err  error
		want error
	}{
		{&IdleTimeoutError{}, ErrIdleTimeout},
		{&HandshakeTimeoutError{}, ErrHandshakeTimeout},
		{&EarlyDataRejectedError{}, ErrEarlyDataRejected},
		{&ResumeTimeoutError{LastPipeErr: cause}, ErrResumeTimeout},
		{&PipesLostError{LastPipeErr: cause}, ErrPipesLost},
		{&ApplicationError{}, nil},
		{&TransportError{}, nil},
	} {
		err := fmt.Errorf("op: %w", c.err)
		if !errors.Is(err, net.ErrClosed) {
			t.Errorf("%T does not match net.ErrClosed", c.err)
		}
		for _, s := range sentinels {
			if got := errors.Is(err, s); got != errors.Is(c.want, s) {
				t.Errorf("errors.Is(%T, %v) = %v", c.err, s, got)
			}
		}
	}
	if !errors.Is(&ResumeTimeoutError{LastPipeErr: cause}, cause) || !errors.Is(&PipesLostError{LastPipeErr: cause}, cause) {
		t.Error("a pipe-loss end does not unwrap to its cause")
	}
	if !IsRetryable(fmt.Errorf("x: %w", &EarlyDataRejectedError{})) {
		t.Error("a rejected early-data session is not retryable")
	}
}
