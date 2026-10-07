package zmux

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// lateWriterConn models a preface writer goroutine that is scheduled only
// after establishment has already failed: Write waits until the write deadline
// was changed after arming (the failure path's expedite) and then enforces it
// the way net.Conn does, rejecting a write whose deadline has passed before
// sending any byte.
type lateWriterConn struct {
	readMu  sync.Mutex
	readBuf *bytes.Reader

	mu           sync.Mutex
	deadline     time.Time
	deadlineSets int
	changed      chan struct{}
	written      bytes.Buffer
	closed       bool
}

func newLateWriterConn(inbound []byte) *lateWriterConn {
	return &lateWriterConn{readBuf: bytes.NewReader(inbound), changed: make(chan struct{})}
}

func (c *lateWriterConn) Read(p []byte) (int, error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()
	return c.readBuf.Read(p)
}

func (c *lateWriterConn) Write(p []byte) (int, error) {
	timeout := time.NewTimer(testSignalTimeout)
	defer timeout.Stop()
	for {
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return 0, io.ErrClosedPipe
		}
		if c.deadlineSets >= 2 {
			if !c.deadline.IsZero() && !time.Now().Before(c.deadline) {
				c.mu.Unlock()
				return 0, os.ErrDeadlineExceeded
			}
			n, err := c.written.Write(p)
			c.mu.Unlock()
			return n, err
		}
		changed := c.changed
		c.mu.Unlock()
		select {
		case <-changed:
		case <-timeout.C:
			return 0, errors.New("lateWriterConn: write deadline was never changed after arming")
		}
	}
}

func (c *lateWriterConn) SetWriteDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deadline = t
	c.deadlineSets++
	close(c.changed)
	c.changed = make(chan struct{})
	return nil
}

func (c *lateWriterConn) SetReadDeadline(time.Time) error { return nil }

func (c *lateWriterConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.closed {
		c.closed = true
		close(c.changed)
		c.changed = make(chan struct{})
	}
	return nil
}

func (c *lateWriterConn) bytes() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]byte(nil), c.written.Bytes()...)
}

func assertEstablishmentPrefaceThenClose(t *testing.T, raw []byte, wantRole Role, wantCode ErrorCode) {
	t.Helper()
	if len(raw) == 0 {
		t.Fatal("peer received no bytes: neither the local preface nor an establishment CLOSE")
	}
	got, prefaceLen := testWrittenPrefacePrefix(t, raw)
	if got.Role != wantRole {
		t.Fatalf("written preface role = %s, want %s", got.Role, wantRole)
	}
	frames := establishmentFramesAfterPreface(t, raw, prefaceLen)
	if len(frames) != 1 || frames[0].Type != FrameTypeCLOSE {
		t.Fatalf("frames after preface = %+v, want exactly one CLOSE", frames)
	}
	code, _, err := parseErrorPayload(frames[0].Payload)
	if err != nil {
		t.Fatalf("parse CLOSE payload: %v", err)
	}
	if code != uint64(wantCode) {
		t.Fatalf("CLOSE code = %d, want %d (%s)", code, uint64(wantCode), wantCode)
	}
}

func TestEstablishmentFailureBeforePrefaceWriterRunsStillSendsPrefaceAndClose(t *testing.T) {
	t.Parallel()

	invalidInitiator := append([]byte(nil), testPrefaceBytesForRole(t, RoleInitiator)...)
	invalidInitiator[5] = 7
	for _, tc := range []struct {
		name     string
		server   bool
		peer     []byte
		wantRole Role
		wantCode ErrorCode
	}{
		{"client_role_conflict", false, testPrefaceBytesForRole(t, RoleInitiator), RoleInitiator, CodeRoleConflict},
		{"server_role_conflict", true, testPrefaceBytesForRole(t, RoleResponder), RoleResponder, CodeRoleConflict},
		{"server_invalid_preface", true, invalidInitiator, RoleResponder, CodeProtocol},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			conn := newLateWriterConn(tc.peer)
			var (
				session *Conn
				err     error
			)
			if tc.server {
				session, err = Server(conn, nil)
			} else {
				session, err = Client(conn, nil)
			}
			if session != nil {
				_ = session.Close()
				t.Fatal("establishment succeeded, want failure")
			}
			if !IsErrorCode(err, tc.wantCode) {
				t.Fatalf("establish err = %v, want %s", err, tc.wantCode)
			}
			assertEstablishmentPrefaceThenClose(t, conn.bytes(), tc.wantRole, tc.wantCode)
		})
	}
}

func TestEstablishmentFailureExpediteKeepsAnExpiredDeadline(t *testing.T) {
	t.Parallel()

	conn := newRecordingDeadlineDuplexConn(nil)
	past := time.Now().Add(-time.Second)
	establishmentWriteDeadline{setter: conn, deadline: past, armed: true}.expedite()
	start := time.Now()
	establishmentWriteDeadline{setter: conn, deadline: start.Add(time.Hour), armed: true}.expedite()

	got := conn.snapshotWriteDeadlines()
	if len(got) != 2 {
		t.Fatalf("write deadline calls = %d, want 2", len(got))
	}
	if !got[0].Equal(past) {
		t.Fatalf("expedite after the establishment deadline set %v, want the expired %v kept", got[0], past)
	}
	if got[1].Before(start.Add(establishmentFailureWriteWait)) || got[1].After(time.Now().Add(establishmentFailureWriteWait)) {
		t.Fatalf("expedite set %v, want about now+%v (not an already-expired deadline)", got[1], establishmentFailureWriteWait)
	}
}

// rawTCPPeer runs one side of a loopback TCP connection as a raw peer: it
// optionally waits, writes its preface bytes, and reads until EOF.
type rawTCPResult struct {
	data []byte
	err  error
}

func runRawTCPPeer(conn net.Conn, delay time.Duration, preface []byte, readFor time.Duration) rawTCPResult {
	defer func() { _ = conn.Close() }()
	if delay > 0 {
		time.Sleep(delay)
	}
	if preface != nil {
		if _, err := conn.Write(preface); err != nil {
			return rawTCPResult{err: err}
		}
	}
	_ = conn.SetReadDeadline(time.Now().Add(readFor))
	data, err := io.ReadAll(conn)
	return rawTCPResult{data: data, err: err}
}

// establishOverLoopback connects a Go session (Server or Client per goServer)
// to a raw TCP peer. The Go side starts establishment after goDelay.
func establishOverLoopback(t *testing.T, goServer bool, goDelay time.Duration, cfg *Config, peer func(net.Conn) rawTCPResult) (*Conn, <-chan rawTCPResult, error) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("loopback TCP unavailable: %v", err)
	}
	defer func() { _ = ln.Close() }()

	rawCh := make(chan rawTCPResult, 1)
	var goConn net.Conn
	if goServer {
		go func() {
			conn, err := net.Dial("tcp", ln.Addr().String())
			if err != nil {
				rawCh <- rawTCPResult{err: err}
				return
			}
			rawCh <- peer(conn)
		}()
		goConn, err = ln.Accept()
	} else {
		go func() {
			conn, err := ln.Accept()
			if err != nil {
				rawCh <- rawTCPResult{err: err}
				return
			}
			rawCh <- peer(conn)
		}()
		goConn, err = net.Dial("tcp", ln.Addr().String())
	}
	if err != nil {
		t.Fatalf("loopback connect: %v", err)
	}
	if goDelay > 0 {
		time.Sleep(goDelay)
	}
	var session *Conn
	if goServer {
		session, err = Server(goConn, cfg)
	} else {
		session, err = Client(goConn, cfg)
	}
	if session == nil {
		_ = goConn.Close()
	}
	return session, rawCh, err
}

func awaitRawTCPPeer(t *testing.T, rawCh <-chan rawTCPResult) []byte {
	t.Helper()
	select {
	case res := <-rawCh:
		if res.err != nil {
			t.Fatalf("raw peer: %v (read %d bytes)", res.err, len(res.data))
		}
		return res.data
	case <-time.After(5 * time.Second):
		t.Fatal("raw peer did not finish")
		return nil
	}
}

func TestEstablishmentFailureWithBufferedPeerPrefaceSendsPrefaceAndClose(t *testing.T) {
	t.Parallel()

	invalidInitiator := append([]byte(nil), testPrefaceBytesForRole(t, RoleInitiator)...)
	invalidInitiator[5] = 7
	for _, tc := range []struct {
		name     string
		server   bool
		peer     []byte
		wantRole Role
		wantCode ErrorCode
	}{
		{"server_role_conflict", true, testPrefaceBytesForRole(t, RoleResponder), RoleResponder, CodeRoleConflict},
		{"client_role_conflict", false, testPrefaceBytesForRole(t, RoleInitiator), RoleInitiator, CodeRoleConflict},
		{"server_invalid_preface", true, invalidInitiator, RoleResponder, CodeProtocol},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			// The race needs the peer preface already buffered when establishment
			// starts, so repeat it.
			for i := 0; i < 20; i++ {
				session, rawCh, err := establishOverLoopback(t, tc.server, 5*time.Millisecond, nil, func(conn net.Conn) rawTCPResult {
					return runRawTCPPeer(conn, 0, tc.peer, 3*time.Second)
				})
				if session != nil {
					_ = session.Close()
					t.Fatalf("run %d: establishment succeeded, want failure", i)
				}
				if !IsErrorCode(err, tc.wantCode) {
					t.Fatalf("run %d: establish err = %v, want %s", i, err, tc.wantCode)
				}
				raw := awaitRawTCPPeer(t, rawCh)
				if len(raw) == 0 {
					t.Fatalf("run %d: peer received no bytes: neither the local preface nor an establishment CLOSE", i)
				}
				assertEstablishmentPrefaceThenClose(t, raw, tc.wantRole, tc.wantCode)
			}
		})
	}
}

func TestEstablishmentTimeoutConfigResolution(t *testing.T) {
	t.Parallel()

	if got := DefaultConfig().EstablishmentTimeout; got != 0 {
		t.Fatalf("DefaultConfig().EstablishmentTimeout = %v, want 0 (use the default)", got)
	}
	for _, tc := range []struct {
		configured time.Duration
		want       time.Duration
	}{
		{0, 10 * time.Second},
		{3 * time.Second, 3 * time.Second},
		{time.Millisecond, time.Millisecond},
		{-1, 0},
		{-time.Hour, 0},
	} {
		cfg := cloneConfig(&Config{EstablishmentTimeout: tc.configured})
		if got := cfg.establishmentTimeout(); got != tc.want {
			t.Fatalf("EstablishmentTimeout %v resolves to %v, want %v", tc.configured, got, tc.want)
		}
	}
}

func TestEstablishmentTimeoutArmsConfiguredDeadlines(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		cfg  *Config
		want time.Duration
	}{
		{"default", nil, 10 * time.Second},
		{"configured", &Config{EstablishmentTimeout: 3 * time.Second}, 3 * time.Second},
		{"disabled", &Config{EstablishmentTimeout: -1}, 0},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			conn := newRecordingDeadlineDuplexConn(testPrefaceBytesForRole(t, RoleResponder))
			start := time.Now()
			client, err := Client(conn, tc.cfg)
			end := time.Now()
			if err != nil {
				t.Fatalf("Client err = %v", err)
			}
			t.Cleanup(func() { _ = client.Close() })

			reads := conn.snapshotReadDeadlines()
			writes := conn.snapshotWriteDeadlines()
			if tc.want == 0 {
				if len(reads) != 0 || len(writes) != 0 {
					t.Fatalf("disabled establishment timeout armed deadlines: read %v, write %v", reads, writes)
				}
				return
			}
			for name, got := range map[string][]time.Time{"read": reads, "write": writes} {
				if len(got) < 2 || !got[len(got)-1].IsZero() {
					t.Fatalf("%s deadlines = %v, want arm then clear", name, got)
				}
				if got[0].Before(start.Add(tc.want)) || got[0].After(end.Add(tc.want)) {
					t.Fatalf("%s deadline armed at %v, want start+%v", name, got[0].Sub(start), tc.want)
				}
			}
		})
	}
}

func TestEstablishmentToleratesPeerPrefaceLaterThanOneSecond(t *testing.T) {
	t.Parallel()

	// A compliant peer whose preface needs a TCP retransmission can arrive
	// after the old hard-coded 1s bound; the default must still accept it.
	session, rawCh, err := establishOverLoopback(t, true, 0, nil, func(conn net.Conn) rawTCPResult {
		return runRawTCPPeer(conn, 1300*time.Millisecond, testPrefaceBytesForRole(t, RoleInitiator), 3*time.Second)
	})
	if err != nil {
		t.Fatalf("Server with a peer preface after 1.3s: %v", err)
	}
	if got := session.State(); got != SessionStateReady {
		t.Fatalf("session state = %v, want ready", got)
	}
	_ = session.Close()
	raw := awaitRawTCPPeer(t, rawCh)
	if got, _ := testWrittenPrefacePrefix(t, raw); got.Role != RoleResponder {
		t.Fatalf("server preface role = %s, want %s", got.Role, RoleResponder)
	}
}

func TestEstablishmentTimeoutFailsSilentPeerWithInternalClose(t *testing.T) {
	t.Parallel()

	const timeout = 150 * time.Millisecond
	start := time.Now()
	session, rawCh, err := establishOverLoopback(t, true, 0, &Config{EstablishmentTimeout: timeout}, func(conn net.Conn) rawTCPResult {
		return runRawTCPPeer(conn, 0, nil, 3*time.Second)
	})
	elapsed := time.Since(start)
	if session != nil {
		_ = session.Close()
		t.Fatal("establishment succeeded without a peer preface")
	}
	if !errors.Is(err, errEstablishmentPrefaceReadTimeout) || !IsErrorCode(err, CodeInternal) {
		t.Fatalf("Server err = %v, want INTERNAL %v", err, errEstablishmentPrefaceReadTimeout)
	}
	if elapsed < timeout || elapsed > timeout+testSignalTimeout {
		t.Fatalf("Server failed after %v, want the configured %v bound", elapsed, timeout)
	}
	assertEstablishmentPrefaceThenClose(t, awaitRawTCPPeer(t, rawCh), RoleResponder, CodeInternal)
}

func livenessSeedStates(c *Conn) (jitter, ping uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.liveness.keepaliveJitterState, c.liveness.pingNonceState
}

// smallGammaMultiple reports whether v is k*gamma for a small k, which is
// what the old zero-based per-process seed counter produced.
func smallGammaMultiple(v uint64) bool {
	const gamma uint64 = 0x9e3779b97f4a7c15
	inverse := uint64(1)
	for i := 0; i < 6; i++ {
		inverse *= 2 - gamma*inverse
	}
	return v*inverse < 1<<20
}

func TestSessionLivenessSeedsComeFromNonceSource(t *testing.T) {
	t.Parallel()

	seedBytes := func(base byte) []byte {
		out := make([]byte, 16)
		for i := range out {
			out[i] = base + byte(i)*17
		}
		return out
	}
	clientBytes := seedBytes(0x11)
	serverBytes := seedBytes(0x5a)
	// Explicit roles force both tie_breaker_nonces to zero, and without preface
	// or PING padding the nonce source feeds only the two liveness seeds.
	client, server := newConnPairWithConfig(t,
		&Config{NonceSource: bytes.NewReader(clientBytes)},
		&Config{NonceSource: bytes.NewReader(serverBytes)},
	)
	for _, tc := range []struct {
		name string
		conn *Conn
		src  []byte
	}{
		{"client", client, clientBytes},
		{"server", server, serverBytes},
	} {
		wantJitter, err := randomUint62(bytes.NewReader(tc.src[:8]))
		if err != nil {
			t.Fatal(err)
		}
		wantPing, err := randomUint62(bytes.NewReader(tc.src[8:]))
		if err != nil {
			t.Fatal(err)
		}
		jitter, ping := livenessSeedStates(tc.conn)
		if jitter != wantJitter || ping != wantPing {
			t.Fatalf("%s liveness seeds = (%#x, %#x), want nonce-source draws (%#x, %#x)", tc.name, jitter, ping, wantJitter, wantPing)
		}
	}
}

func TestSessionLivenessSeedsFallBackToCryptoRand(t *testing.T) {
	t.Parallel()

	client, server := newConnPairWithConfig(t,
		&Config{NonceSource: bytes.NewReader(nil)},
		&Config{NonceSource: bytes.NewReader(make([]byte, 64))},
	)
	cj, cp := livenessSeedStates(client)
	sj, sp := livenessSeedStates(server)
	seen := map[uint64]bool{}
	for _, v := range []uint64{cj, cp, sj, sp} {
		if v == 0 || smallGammaMultiple(v) {
			t.Fatalf("liveness seed %#x looks like the deterministic fallback counter", v)
		}
		if seen[v] {
			t.Fatalf("liveness seeds (%#x, %#x, %#x, %#x) repeat, want independent draws", cj, cp, sj, sp)
		}
		seen[v] = true
	}
}

const livenessSeedProbeEnv = "ZMUX_GO_LIVENESS_SEED_PROBE"

func TestSessionLivenessSeedsDifferAcrossProcesses(t *testing.T) {
	if os.Getenv(livenessSeedProbeEnv) == "1" {
		// Child: the first session of a fresh process, explicit role and
		// default config, prints its initial PRNG states.
		session, _ := newRawPeerConn(t, nil, DefaultSettings())
		jitter, ping := livenessSeedStates(session)
		fmt.Printf("liveness-seed %x %x\n", jitter, ping)
		return
	}
	t.Parallel()

	probe := func() (uint64, uint64) {
		cmd := exec.Command(os.Args[0], "-test.run=^TestSessionLivenessSeedsDifferAcrossProcesses$", "-test.count=1", "-test.v")
		cmd.Env = append(os.Environ(), livenessSeedProbeEnv+"=1")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("seed probe process: %v\n%s", err, out)
		}
		scanner := bufio.NewScanner(bytes.NewReader(out))
		for scanner.Scan() {
			fields := strings.Fields(scanner.Text())
			if len(fields) != 3 || fields[0] != "liveness-seed" {
				continue
			}
			jitter, err1 := strconv.ParseUint(fields[1], 16, 64)
			ping, err2 := strconv.ParseUint(fields[2], 16, 64)
			if err1 != nil || err2 != nil {
				t.Fatalf("bad probe line %q", scanner.Text())
			}
			return jitter, ping
		}
		t.Fatalf("seed probe printed no liveness-seed line:\n%s", out)
		return 0, 0
	}
	j1, p1 := probe()
	j2, p2 := probe()
	if p1 == p2 || j1 == j2 {
		t.Fatalf("first sessions of two processes share liveness seeds: jitter %#x/%#x, ping %#x/%#x", j1, j2, p1, p2)
	}
	if smallGammaMultiple(p1) || smallGammaMultiple(p2) {
		t.Fatalf("ping seeds %#x/%#x come from the deterministic fallback counter", p1, p2)
	}
}
