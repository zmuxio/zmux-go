package zmux

import (
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/state"
)

// rawFramePeer drives the far end of a net.Pipe with hand-built frames so a
// test can act as a peer that a real session would never emulate: one that
// never grants credit, keeps sending after STOP_SENDING, or violates FIN.
type rawFramePeer struct {
	conn   net.Conn
	role   Role
	limits Limits

	mu       sync.Mutex
	frames   []Frame
	consumed []bool
	readErr  error
	notify   chan struct{}
}

// newRawPeerConn establishes a real session with localCfg (its Role picks the
// side) against a raw peer that announces peerSettings.
func newRawPeerConn(t *testing.T, localCfg *Config, peerSettings Settings) (*Conn, *rawFramePeer) {
	t.Helper()
	local, peer := newStalledRawPeerConn(t, localCfg, peerSettings)
	peer.startReading()
	return local, peer
}

// newStalledRawPeerConn is newRawPeerConn with a peer that reads nothing after
// the preface until startReading. Over net.Pipe every session write blocks
// meanwhile, like a transport whose peer stopped draining it.
func newStalledRawPeerConn(t *testing.T, localCfg *Config, peerSettings Settings) (*Conn, *rawFramePeer) {
	t.Helper()
	cfg := DefaultConfig()
	if localCfg != nil {
		copied := *localCfg
		cfg = &copied
	}
	peerRole := RoleInitiator
	if cfg.Role == RoleInitiator {
		peerRole = RoleResponder
	}
	peerCfg := DefaultConfig()
	peerCfg.Role = peerRole
	peerCfg.Settings = peerSettings
	peerPreface, err := peerCfg.LocalPreface()
	if err != nil {
		t.Fatalf("raw peer preface: %v", err)
	}
	encoded, err := peerPreface.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal raw peer preface: %v", err)
	}

	left, right := net.Pipe()
	type result struct {
		conn *Conn
		err  error
	}
	localCh := make(chan result, 1)
	go func() {
		var c *Conn
		var err error
		if cfg.Role == RoleInitiator {
			c, err = Client(left, cfg)
		} else {
			c, err = Server(left, cfg)
		}
		localCh <- result{c, err}
	}()
	writeErr := make(chan error, 1)
	go func() {
		_, err := right.Write(encoded)
		writeErr <- err
	}()
	if _, err := ReadPreface(right); err != nil {
		t.Fatalf("raw peer read preface: %v", err)
	}
	if err := <-writeErr; err != nil {
		t.Fatalf("raw peer write preface: %v", err)
	}
	local := <-localCh
	if local.err != nil {
		t.Fatalf("establish against raw peer: %v", local.err)
	}

	peer := &rawFramePeer{
		conn:   right,
		role:   peerRole,
		limits: peerSettings.Limits(),
		notify: make(chan struct{}, 1),
	}
	t.Cleanup(func() {
		_ = right.Close()
		_ = local.conn.Close()
	})
	return local.conn, peer
}

func (p *rawFramePeer) startReading() {
	go p.readLoop()
}

func (p *rawFramePeer) readLoop() {
	for {
		frame, err := ReadFrame(p.conn, p.limits)
		p.mu.Lock()
		if err != nil {
			p.readErr = err
		} else {
			p.frames = append(p.frames, frame)
			p.consumed = append(p.consumed, false)
		}
		p.mu.Unlock()
		select {
		case p.notify <- struct{}{}:
		default:
		}
		if err != nil {
			return
		}
	}
}

// bidiID returns the n-th (0-based) bidirectional stream ID the raw peer opens.
func (p *rawFramePeer) bidiID(n uint64) uint64 {
	return state.FirstLocalStreamID(p.role, true) + 4*n
}

// uniID returns the n-th (0-based) unidirectional stream ID the raw peer opens.
func (p *rawFramePeer) uniID(n uint64) uint64 {
	return state.FirstLocalStreamID(p.role, false) + 4*n
}

func (p *rawFramePeer) write(t *testing.T, frame Frame) {
	t.Helper()
	encoded, err := frame.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal raw frame %+v: %v", frame, err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := p.conn.Write(encoded)
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("raw peer write %v on stream %d: %v", frame.Type, frame.StreamID, err)
		}
	case <-time.After(testSignalTimeout):
		t.Fatalf("raw peer write %v on stream %d did not complete", frame.Type, frame.StreamID)
	}
}

func (p *rawFramePeer) writeData(t *testing.T, streamID uint64, flags byte, payload []byte) {
	t.Helper()
	p.write(t, Frame{Type: FrameTypeDATA, Flags: flags, StreamID: streamID, Payload: payload})
}

// await returns the oldest received frame matching match that no earlier
// await consumed.
func (p *rawFramePeer) await(t *testing.T, what string, match func(Frame) bool) Frame {
	t.Helper()
	deadline := time.NewTimer(testSignalTimeout)
	defer deadline.Stop()
	for {
		p.mu.Lock()
		for i, frame := range p.frames {
			if !p.consumed[i] && match(frame) {
				p.consumed[i] = true
				p.mu.Unlock()
				return frame
			}
		}
		readErr := p.readErr
		p.mu.Unlock()
		if readErr != nil {
			t.Fatalf("waiting for %s: raw peer read ended: %v (received %s)", what, readErr, p.describe())
		}
		select {
		case <-p.notify:
		case <-deadline.C:
			t.Fatalf("timed out waiting for %s (received %s)", what, p.describe())
		}
	}
}

// awaitPong round-trips a PING so every frame the raw peer wrote before it has
// been handled by the session.
func (p *rawFramePeer) awaitPong(t *testing.T, token byte) {
	t.Helper()
	payload := []byte{0, 0, 0, 0, 0, 0, 0x7a, token}
	p.write(t, Frame{Type: FrameTypePING, Payload: payload})
	p.await(t, "PONG", func(f Frame) bool {
		return f.Type == FrameTypePONG && len(f.Payload) >= len(payload) && string(f.Payload[:len(payload)]) == string(payload)
	})
}

func (p *rawFramePeer) received() []Frame {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]Frame(nil), p.frames...)
}

func (p *rawFramePeer) describe() string {
	frames := p.received()
	out := ""
	for i, frame := range frames {
		if i > 0 {
			out += ", "
		}
		out += frame.Type.String()
		if frame.StreamID != 0 {
			out += "@" + uintString(frame.StreamID)
		}
	}
	if out == "" {
		return "nothing"
	}
	return out
}

func uintString(v uint64) string {
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[i:])
}

func (p *rawFramePeer) assertNoClose(t *testing.T) {
	t.Helper()
	for _, frame := range p.received() {
		if frame.Type == FrameTypeCLOSE {
			code, reason, _ := parseErrorPayload(frame.Payload)
			t.Fatalf("session sent CLOSE(code %d, %q), want it to stay open", code, reason)
		}
	}
	p.mu.Lock()
	readErr := p.readErr
	p.mu.Unlock()
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		t.Fatalf("raw peer transport failed: %v", readErr)
	}
}

func isFrame(frameType FrameType, streamID uint64) func(Frame) bool {
	return func(f Frame) bool { return f.Type == frameType && f.StreamID == streamID }
}

func isAbortWithCode(streamID uint64, code ErrorCode) func(Frame) bool {
	return func(f Frame) bool {
		if f.Type != FrameTypeABORT || f.StreamID != streamID {
			return false
		}
		got, _, err := parseErrorPayload(f.Payload)
		return err == nil && got == uint64(code)
	}
}

func isCloseWithCode(code ErrorCode) func(Frame) bool {
	return func(f Frame) bool {
		if f.Type != FrameTypeCLOSE {
			return false
		}
		got, _, err := parseErrorPayload(f.Payload)
		return err == nil && got == uint64(code)
	}
}

func maxDataValue(t *testing.T, frame Frame) uint64 {
	t.Helper()
	value, _, err := ParseVarint(frame.Payload)
	if err != nil {
		t.Fatalf("parse MAX_DATA payload %x: %v", frame.Payload, err)
	}
	return value
}

// highestMaxData returns the largest MAX_DATA value the session has sent for
// streamID (0 = session), and whether it sent any.
func (p *rawFramePeer) highestMaxData(t *testing.T, streamID uint64) (uint64, bool) {
	t.Helper()
	var best uint64
	found := false
	for _, frame := range p.received() {
		if frame.Type != FrameTypeMAXDATA || frame.StreamID != streamID {
			continue
		}
		if v := maxDataValue(t, frame); !found || v > best {
			best = v
		}
		found = true
	}
	return best, found
}
