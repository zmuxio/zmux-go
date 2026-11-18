package machine_test

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/shaping/machine"
)

// Shaping against another implementation's peer (ZMUX_SHAPING_PEER names
// its binary; `server | client <addr>`, SHAPE_DEF its definition file): eight
// streams, stream i carrying 200000 + i*7919 bytes of xorshift64 output
// seeded with i+1, echoed back. Both roles, every combination of shaped
// and unshaped sides.

const (
	interopRecords = `{"machines": [
 {"start": 0, "states": [{"record": {"max_len": {"t": [[1, 200, 1400]]}, "pad": {"t": [[1, 0, 64]]}}}]},
 {"start": 0, "states": [{"enter": [{"op": "inject", "len": {"t": [[1, 50, 300]]}, "after": 10000}], "next": [{"on": "injected", "to": 0}]}]}
]}`
	interopDelayed = `{"machines": [
 {"start": 0, "states": [{"record": {"max_len": {"t": [[1, 200, 1400]]}, "pad": {"t": [[1, 0, 64]]}, "delay": {"t": [[1, 0, 2000]]}}}]},
 {"start": 0, "states": [{"enter": [{"op": "inject", "len": {"t": [[1, 50, 300]]}, "after": 10000}], "next": [{"on": "injected", "to": 0}]}]}
]}`
	interopStreams = 8
)

func interopPayload(i int) []byte {
	x := uint64(i) + 1
	b := make([]byte, 200000+i*7919)
	for j := range b {
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		b[j] = byte(x)
	}
	return b
}

type shapingPeer struct {
	cmd   *exec.Cmd
	lines chan string
	all   []string
}

func startShapingPeer(t *testing.T, def string, args ...string) *shapingPeer {
	t.Helper()
	bin := os.Getenv("ZMUX_SHAPING_PEER")
	if bin == "" {
		t.Skip("ZMUX_SHAPING_PEER not set")
	}
	cmd := exec.Command(bin, args...)
	cmd.Stderr = os.Stderr
	cmd.Env = append(os.Environ(), "SHAPE_DEF="+def)
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	p := &shapingPeer{cmd: cmd, lines: make(chan string, 16)}
	go func() {
		sc := bufio.NewScanner(out)
		for sc.Scan() {
			p.lines <- sc.Text()
		}
		close(p.lines)
	}()
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	return p
}

// next returns the next stdout line.
func (p *shapingPeer) next(t *testing.T) string {
	t.Helper()
	select {
	case l, ok := <-p.lines:
		if !ok {
			t.Fatalf("peer output ended after %q", p.all)
		}
		p.all = append(p.all, l)
		return l
	case <-time.After(30 * time.Second):
		t.Fatalf("peer: no output after %q", p.all)
	}
	return ""
}

// finish checks that the peer printed OK and exited with status 0.
func (p *shapingPeer) finish(t *testing.T) {
	t.Helper()
	for l := ""; l != "OK"; {
		l = p.next(t)
	}
	done := make(chan error, 1)
	go func() { done <- p.cmd.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("peer: %v (output %q)", err, p.all)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("peer did not exit")
	}
}

func interopFactory(t *testing.T, def string) zmux.ShaperFactory {
	t.Helper()
	if def == "" {
		return nil
	}
	f, err := machine.NewFactory([]byte(def), machine.Options{})
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func checkShaped(t *testing.T, c *zmux.Conn, shaped bool) {
	t.Helper()
	st := c.ShapingStats()
	t.Logf("STATS %+v", st)
	if shaped && (st.WireSpent == 0 || st.Faults != 0) {
		t.Fatalf("shaped side: %+v", st)
	}
}

func TestInteropShaping(t *testing.T) {
	if os.Getenv("ZMUX_SHAPING_PEER") == "" {
		t.Skip("ZMUX_SHAPING_PEER not set")
	}
	for _, d := range []struct{ name, def string }{{"records", interopRecords}, {"delayed", interopDelayed}} {
		file := filepath.Join(t.TempDir(), d.name+".json")
		if err := os.WriteFile(file, []byte(d.def), 0o600); err != nil {
			t.Fatal(err)
		}
		for _, server := range []bool{true, false} {
			for _, goShaped := range []bool{false, true} {
				for _, peerShaped := range []bool{false, true} {
					name := fmt.Sprintf("%s/goServer=%v/goShaped=%v/peerShaped=%v", d.name, server, goShaped, peerShaped)
					t.Run(name, func(t *testing.T) {
						peerDef, goDef := "", ""
						if peerShaped {
							peerDef = file
						}
						if goShaped {
							goDef = d.def
						}
						cfg := &zmux.Config{Shaper: interopFactory(t, goDef)}
						ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
						defer cancel()
						if server {
							goServe(t, ctx, cfg, peerDef, goShaped)
						} else {
							goDial(t, ctx, cfg, peerDef, goShaped)
						}
					})
				}
			}
		}
	}
}

// goServe is the server role: the peer dials, Go echoes.
func goServe(t *testing.T, ctx context.Context, cfg *zmux.Config, peerDef string, shaped bool) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	p := startShapingPeer(t, peerDef, "client", ln.Addr().String())
	nc, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	c, err := zmux.Server(ctx, nc, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	var wg sync.WaitGroup
	errs := make(chan error, interopStreams)
	for range interopStreams {
		st, err := c.AcceptStream(ctx)
		if err != nil {
			t.Fatal(err)
		}
		wg.Go(func() {
			data, err := io.ReadAll(st)
			if err == nil {
				_, err = st.Write(data)
			}
			if err == nil {
				err = st.Close()
			}
			errs <- err
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-c.Done(): // the peer closes the session
	case <-time.After(15 * time.Second):
	}
	checkShaped(t, c, shaped)
	p.finish(t)
}

// goDial is the client role: Go dials the peer's server and compares echoes.
func goDial(t *testing.T, ctx context.Context, cfg *zmux.Config, peerDef string, shaped bool) {
	t.Helper()
	p := startShapingPeer(t, peerDef, "server")
	addr := strings.TrimPrefix(p.next(t), "LISTEN ")
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	c, err := zmux.Client(ctx, nc, cfg)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, interopStreams)
	for i := range interopStreams {
		wg.Go(func() { errs <- goExchange(ctx, c, i) })
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(300 * time.Millisecond) // the shaper's last injections go out
	checkShaped(t, c, shaped)
	if err := c.Close(); err != nil {
		t.Logf("close: %v", err)
	}
	p.finish(t)
}

func goExchange(ctx context.Context, c *zmux.Conn, i int) error {
	st, err := c.OpenStreamSync(ctx)
	if err != nil {
		return err
	}
	want := interopPayload(i)
	werr := make(chan error, 1)
	go func() {
		_, err := st.Write(want)
		if err == nil {
			err = st.Close()
		}
		werr <- err
	}()
	got, err := io.ReadAll(st)
	if err != nil {
		return err
	}
	if err := <-werr; err != nil {
		return err
	}
	if !bytes.Equal(got, want) {
		return fmt.Errorf("stream %d: %d of %d bytes", i, len(got), len(want))
	}
	return nil
}
