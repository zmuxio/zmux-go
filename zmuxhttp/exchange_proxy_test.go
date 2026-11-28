package zmuxhttp

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	mrand "math/rand/v2"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

// exFault is a fault the proxy injects into one exchange.
type exFault int

const (
	faultDropRequest  exFault = iota // close without forwarding
	faultDropResponse                // forward, then close without answering
	fault5xx                         // answer 502 without forwarding
	faultDuplicate                   // forward twice
	faultReplay                      // forward, answer with an earlier response
	faultReorder                     // forward a later UP request first
	faultCut                         // close the first DOWN request after forwarding it
	faultCount
)

var faultNames = [faultCount]string{"drop-request", "drop-response", "5xx", "duplicate", "replay", "reorder", "cut"}

// faultProxy is a reverse proxy at its worst: it reads every request and
// every response completely before forwarding it, speaks HTTP/1.0 with
// close-delimited connections and Content-Length (no chunked coding) on
// both sides over raw TCP, and injects faults into selected requests.
type faultProxy struct {
	ln          net.Listener
	rng         *mrand.Rand
	conns       map[net.Conn]struct{}
	done        chan struct{}
	reorderGate chan struct{}
	origin      string
	cache       [][]byte // complete 200 responses, newest last
	downResp    []byte   // replayDown: the response to replay
	rates       [faultCount]float64
	counts      [faultCount]int
	wg          sync.WaitGroup
	downSeq     uint64
	reorderSeq  uint64
	reorderSize int64
	mu          sync.Mutex
	// replayDown serves, once, the earliest DOWN response that carried
	// data to a later DOWN request for another piece (the origin still
	// processes the request): a deterministic cache replay.
	replayDown  bool
	replayed    bool
	reorderHeld bool
	cutDown     bool
}

// newFaultProxy starts a proxy to origin, configured by set before it
// serves.
func newFaultProxy(t *testing.T, origin string, seed uint64, set func(*faultProxy)) *faultProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &faultProxy{ln: ln, origin: origin, rng: mrand.New(mrand.NewPCG(seed, seed^0x9e3779b97f4a7c15)),
		conns: map[net.Conn]struct{}{}, done: make(chan struct{}), reorderGate: make(chan struct{})}
	set(p)
	p.wg.Add(1)
	go p.serve()
	return p
}

func (p *faultProxy) url() string { return "http://" + p.ln.Addr().String() }

func (p *faultProxy) close() {
	close(p.done)
	_ = p.ln.Close()
	p.mu.Lock()
	for c := range p.conns {
		_ = c.Close()
	}
	p.mu.Unlock()
	p.wg.Wait()
}

func (p *faultProxy) track(c net.Conn) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	select {
	case <-p.done:
		_ = c.Close()
		return false
	default:
	}
	p.conns[c] = struct{}{}
	return true
}

func (p *faultProxy) untrack(c net.Conn) {
	p.mu.Lock()
	delete(p.conns, c)
	p.mu.Unlock()
	_ = c.Close()
}

func (p *faultProxy) serve() {
	defer p.wg.Done()
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		if !p.track(c) {
			continue
		}
		p.wg.Go(func() {
			defer p.untrack(c)
			p.exchange(c)
		})
	}
}

// roll picks this exchange's faults.
func (p *faultProxy) roll() (f [faultCount]bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := range faultReorder {
		f[i] = p.rng.Float64() < p.rates[i]
		if f[i] {
			p.counts[i]++
		}
	}
	return f
}

func (p *faultProxy) count(f exFault) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.counts[f]
}

// order holds the first full UP piece until a later UP reaches the origin.
// The client pipelines pieces, and the origin acknowledges out-of-order UP
// requests before delivering them to the session.
func (p *faultProxy) order(q exReq) (wait, advance bool) {
	if p.reorderSize == 0 || q.kind != exKindUp {
		return false, false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.counts[faultReorder] != 0 {
		return false, false
	}
	if !p.reorderHeld && int64(len(q.data)) == p.reorderSize {
		p.reorderSeq, p.reorderHeld = q.seq, true
	}
	return p.reorderHeld && q.seq == p.reorderSeq, p.reorderHeld && q.seq > p.reorderSeq
}

func (p *faultProxy) allowHeldUp() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.counts[faultReorder] == 0 {
		close(p.reorderGate)
		p.counts[faultReorder]++
	}
}

func (p *faultProxy) cut(q exReq) bool {
	if !p.cutDown || q.kind != exKindDown {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.counts[faultCut] > 0 {
		return false
	}
	p.counts[faultCut]++
	return true
}

func (p *faultProxy) exchange(c net.Conn) {
	_ = c.SetDeadline(time.Now().Add(time.Minute))
	req, err := http.ReadRequest(bufio.NewReader(c))
	if err != nil {
		return
	}
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return
	}
	q, _ := parseExRequest(body)
	target := req.URL.RequestURI()
	if p.cut(q) {
		_, _ = p.forward(target, body, true)
		return
	}
	wait, advance := p.order(q)
	if wait {
		select {
		case <-p.reorderGate:
		case <-p.done:
			return
		}
	}
	var f [faultCount]bool
	if !advance {
		f = p.roll()
	}
	switch {
	case f[faultDropRequest]:
		return
	case f[fault5xx]:
		_, _ = io.WriteString(c, "HTTP/1.0 502 Bad Gateway\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
		return
	}
	if f[faultDuplicate] {
		p.wg.Go(func() {
			_, _ = p.forward(target, body, false)
		})
	}
	resp, err := p.forward(target, body, false)
	if advance && err == nil {
		p.allowHeldUp()
	}
	if err != nil || f[faultDropResponse] {
		return
	}
	if r := p.replay(f[faultReplay], body, resp); r != nil {
		resp = r
	}
	_, _ = c.Write(resp)
}

// replay returns an earlier response to send instead of resp, and caches
// resp.
func (p *faultProxy) replay(random bool, reqBody, resp []byte) []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []byte
	if random && len(p.cache) > 0 {
		out = p.cache[p.rng.IntN(len(p.cache))]
	}
	ok200 := bytes.HasPrefix(resp, []byte("HTTP/1.0 200 "))
	if p.replayDown {
		if q, ok := parseExRequest(reqBody); ok && q.kind == exKindDown && ok200 {
			switch {
			case p.downResp == nil && len(resp)-bytes.Index(resp, []byte("\r\n\r\n"))-4 > exRespHdr:
				p.downResp, p.downSeq = resp, q.seq
			case p.downResp != nil && !p.replayed && q.seq > p.downSeq:
				p.replayed = true
				p.counts[faultReplay]++
				out = p.downResp
			}
		}
	}
	if ok200 {
		p.cache = append(p.cache, resp)
		if len(p.cache) > 32 {
			p.cache = p.cache[1:]
		}
	}
	return out
}

// forward sends one HTTP/1.0 request to the origin and returns the
// complete response, re-encoded as an HTTP/1.0 response with
// Content-Length. A cut exchange closes both ends before reading the response.
func (p *faultProxy) forward(target string, body []byte, cut bool) ([]byte, error) {
	oc, err := net.Dial("tcp", p.origin)
	if err != nil {
		return nil, err
	}
	if !p.track(oc) {
		return nil, net.ErrClosed
	}
	defer p.untrack(oc)
	_ = oc.SetDeadline(time.Now().Add(time.Minute))
	head := fmt.Sprintf("POST %s HTTP/1.0\r\nHost: %s\r\nContent-Type: application/octet-stream\r\nContent-Length: %d\r\n\r\n",
		target, p.origin, len(body))
	if _, err := oc.Write(append([]byte(head), body...)); err != nil {
		return nil, err
	}
	if cut {
		return nil, nil
	}
	resp, err := http.ReadResponse(bufio.NewReader(oc), nil)
	if err != nil {
		return nil, err
	}
	rb, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		return nil, err
	}
	out := fmt.Appendf(nil, "HTTP/1.0 %d %s\r\nContent-Type: application/octet-stream\r\nContent-Length: %s\r\nConnection: close\r\n\r\n",
		resp.StatusCode, http.StatusText(resp.StatusCode), strconv.Itoa(len(rb)))
	return append(out, rb...), nil
}

// faultConfig is the exchange configuration of the fault runs: small
// pieces and short timers so that a run sees many exchanges and recovers
// quickly.
func faultConfig() *exCfg {
	return &exCfg{PSK: []byte("fault"), UpPieceSize: 32 << 10, DownPieceSize: 32 << 10,
		HoldTime: 300 * time.Millisecond, RequestTimeout: 2 * time.Second,
		BackoffMin: 5 * time.Millisecond, BackoffMax: 100 * time.Millisecond, Deadline: 20 * time.Second}
}

// TestHTTPExchangeFaults runs a session through the fault proxy under each
// fault alone and under a random mix, with fixed seeds. Each run echoes
// 2 MiB and checks its hash.
func TestHTTPExchangeFaults(t *testing.T) {
	type run struct {
		name         string
		rates        [faultCount]float64
		check        exFault
		reorder, cut bool
	}
	runs := make([]run, 0, faultCount+2)
	for f := range faultReorder {
		var r [faultCount]float64
		r[f] = 0.15
		runs = append(runs, run{name: faultNames[f], rates: r, check: f})
	}
	runs = append(runs, run{name: "reorder", reorder: true, check: faultReorder},
		run{name: "cut", cut: true, check: faultCut},
		run{name: "mix", rates: [faultCount]float64{0.04, 0.04, 0.04, 0.04, 0.04},
			reorder: true, cut: true, check: -1})
	for i, r := range runs {
		t.Run(r.name, func(t *testing.T) {
			leakcheck.Check(t)
			var px *faultProxy
			cfg := faultConfig()
			e := newExEnv(t, cfg, cfg, func(origin string) string {
				px = newFaultProxy(t, origin[len("http://"):], uint64(1000+i), func(p *faultProxy) {
					p.rates = r.rates
					p.cutDown = r.cut
					if r.reorder {
						p.reorderSize = cfg.UpPieceSize
					}
				})
				t.Cleanup(px.close)
				return px.url()
			})
			defer e.close(t)
			e.echo(t, 2<<20, 60*time.Second)
			if r.check >= 0 && px.count(r.check) == 0 {
				t.Fatalf("no %s fault was injected", faultNames[r.check])
			}
		})
	}
}

// TestHTTPExchangeReplayNeedsMAC is the negative control of the replay
// run: the proxy serves one earlier DOWN response in place of a later one.
// With the response MAC checked the session completes; with the check
// disabled the replayed piece is taken as data and the session breaks.
func TestHTTPExchangeReplayNeedsMAC(t *testing.T) {
	for _, skip := range []bool{false, true} {
		t.Run(fmt.Sprintf("skip=%v", skip), func(t *testing.T) {
			leakcheck.Check(t)
			var px *faultProxy
			cfg := faultConfig()
			ccfg := *cfg
			ccfg.testSkipResponseMAC = skip
			e := newExEnv(t, &ccfg, cfg, func(origin string) string {
				px = newFaultProxy(t, origin[len("http://"):], 1, func(*faultProxy) {})
				t.Cleanup(px.close)
				return px.url()
			})
			defer e.close(t)
			// Exercise replay during echo. Replaying handshake bytes can
			// abort setup before the negative control starts.
			px.mu.Lock()
			px.replayDown = true
			px.mu.Unlock()
			err := e.echoErr(1<<20, time.Minute)
			if px.count(faultReplay) != 1 {
				t.Fatalf("%d replays, want 1", px.count(faultReplay))
			}
			switch {
			case !skip && err != nil:
				t.Fatalf("with the MAC check: %v", err)
			case errors.Is(err, context.DeadlineExceeded), errors.Is(err, os.ErrDeadlineExceeded), errors.Is(err, errExchangeDeadline):
				t.Fatalf("replay test timed out: %v", err)
			case skip && err == nil:
				t.Fatal("a replayed response was not noticed without the MAC check")
			}
		})
	}
}

// A replayed DOWN response during setup has a MAC for the wrong request.
// The client retries it and still establishes the session.
func TestHTTPExchangeReplayDuringHandshake(t *testing.T) {
	leakcheck.Check(t)
	cfg := faultConfig()
	// Split the server parameters across DOWN responses so there is a
	// replay before the client can finish its handshake.
	cfg.DownPieceSize = 8
	cfg.DownRequests = 1
	var px *faultProxy
	e := newExEnv(t, cfg, cfg, func(origin string) string {
		px = newFaultProxy(t, origin[len("http://"):], 1, func(p *faultProxy) { p.replayDown = true })
		t.Cleanup(px.close)
		return px.url()
	})
	defer e.close(t)
	if got := px.count(faultReplay); got != 1 {
		t.Fatalf("replays during handshake = %d, want 1", got)
	}
}
