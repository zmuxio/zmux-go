package zmux

import "testing"

// TestErrorCodeRegistry checks that the exported error codes are the
// registry's (SPEC.md 10.2, EXTENSIONS.md 7).
func TestErrorCodeRegistry(t *testing.T) {
	transport := map[TransportErrorCode]string{
		NoError: "NO_ERROR", InternalError: "INTERNAL_ERROR", ConnectionRefused: "CONNECTION_REFUSED",
		FlowControlError: "FLOW_CONTROL_ERROR", StreamLimitError: "STREAM_LIMIT_ERROR",
		StreamStateError: "STREAM_STATE_ERROR", FinalSizeError: "FINAL_SIZE_ERROR",
		FrameEncodingError: "FRAME_ENCODING_ERROR", TransportParameterError: "TRANSPORT_PARAMETER_ERROR",
		ProtocolViolation: "PROTOCOL_VIOLATION", NoViablePath: "NO_VIABLE_PATH",
		ExcessiveLoad: "ZMUX_EXCESSIVE_LOAD", RoleConflict: "ZMUX_ROLE_CONFLICT",
		ResumeTimeoutCode: "ZMUX_RESUME_TIMEOUT", ResumeFailed: "ZMUX_RESUME_FAILED",
		IdleTimeoutCode: "ZMUX_IDLE_TIMEOUT",
	}
	for c, name := range transport {
		if got := c.String(); got != name {
			t.Errorf("%#x: String() = %s, want %s", uint64(c), got, name)
		}
	}
	app := map[StreamErrorCode]uint64{
		CodeNoError: 0, CodeCancelled: 0x5a4d0001, CodeRejected: 0x5a4d0002, CodeCarrierFailed: 0x5a4d0003,
	}
	for c, v := range app {
		if uint64(c) != v {
			t.Errorf("application code %#x, want %#x", uint64(c), v)
		}
	}
}
