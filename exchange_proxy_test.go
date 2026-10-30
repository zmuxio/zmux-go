package zmux

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	mrand "math/rand/v2"
	"net"
	"net/http"
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
	faultDelay                       // hold the request before forwarding (reorders)
	faultReplay                      // forward, answer with an earlier response
	faultCount
)

var faultNames = [faultCount]string{"drop-request", "drop-response", "5xx", "duplicate", "delay", "replay"}

// faultProxy is a reverse proxy at its worst: it reads every request and
// every response completely before forwarding it, speaks HTTP/1.0 with
// close-delimited connections and Content-Length (no chunked coding) on
// both sides over raw TCP, cuts exchanges that take longer than maxDur,
// and injects faults at the given rates from a seeded generator.
type faultProxy struct {
	ln       net.Listener
	origin   string
	rates    [faultCount]float64
	maxDur   time.Duration
	maxDelay time.Duration
	// replayDown serves, once, the earliest DOWN response that carried
	// data to a later DOWN request for another piece (the origin still
	// processes the request): a deterministic cache replay.
	replayDown bool

	mu       sync.Mutex
	replayed bool
	rng      *mrand.Rand
	cache    [][]byte // complete 200 responses, newest last
	downResp []byte   // replayDown: the response to replay
	downSeq  uint64
	counts   [faultCount]int
	conns    map[net.Conn]struct{}
	done     chan struct{}
	wg       sync.WaitGroup
}

// newFaultProxy starts a proxy to origin, configured by set before it
// serves.
func newFaultProxy(t *testing.T, origin string, seed uint64, set func(*faultProxy)) *faultProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &faultProxy{ln: ln, origin: origin, maxDur: 5 * time.Second, maxDelay: 50 * time.Millisecond,
		rng: mrand.New(mrand.NewPCG(seed, seed^0x9e3779b97f4a7c15)), conns: map[net.Conn]struct{}{}, done: make(chan struct{})}
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
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			defer p.untrack(c)
			p.exchange(c)
		}()
	}
}

// roll picks this exchange's faults.
func (p *faultProxy) roll() (f [faultCount]bool, delay time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := range f {
		f[i] = p.rng.Float64() < p.rates[i]
		if f[i] {
			p.counts[i]++
		}
	}
	return f, time.Duration(p.rng.Int64N(int64(p.maxDelay) + 1))
}

func (p *faultProxy) count(f exFault) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.counts[f]
}

func (p *faultProxy) exchange(c net.Conn) {
	_ = c.SetDeadline(time.Now().Add(p.maxDur))
	req, err := http.ReadRequest(bufio.NewReader(c))
	if err != nil {
		return
	}
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return
	}
	f, delay := p.roll()
	switch {
	case f[faultDropRequest]:
		return
	case f[fault5xx]:
		_, _ = io.WriteString(c, "HTTP/1.0 502 Bad Gateway\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
		return
	}
	if f[faultDelay] {
		select {
		case <-time.After(delay):
		case <-p.done:
			return
		}
	}
	if f[faultDuplicate] {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			_, _ = p.forward(req.URL.RequestURI(), body)
		}()
	}
	resp, err := p.forward(req.URL.RequestURI(), body)
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
		if q, ok := parseExReq(reqBody); ok && q.kind == exKindDown && ok200 {
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
// Content-Length.
func (p *faultProxy) forward(target string, body []byte) ([]byte, error) {
	oc, err := net.Dial("tcp", p.origin)
	if err != nil {
		return nil, err
	}
	if !p.track(oc) {
		return nil, net.ErrClosed
	}
	defer p.untrack(oc)
	_ = oc.SetDeadline(time.Now().Add(p.maxDur))
	head := fmt.Sprintf("POST %s HTTP/1.0\r\nHost: %s\r\nContent-Type: application/octet-stream\r\nContent-Length: %d\r\n\r\n",
		target, p.origin, len(body))
	if _, err := oc.Write(append([]byte(head), body...)); err != nil {
		return nil, err
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
func faultConfig() *HTTPExchangeConfig {
	return &HTTPExchangeConfig{PSK: []byte("fault"), UpPieceSize: 32 << 10, DownPieceSize: 32 << 10,
		HoldTime: 300 * time.Millisecond, RequestTimeout: 2 * time.Second,
		BackoffMin: 5 * time.Millisecond, BackoffMax: 100 * time.Millisecond, Deadline: 20 * time.Second}
}

// TestHTTPExchangeFaults runs a session through the fault proxy under each
// fault alone, under cut-off held requests, and under a random mix, with
// fixed seeds; each run echoes 2 MiB and checks its hash.
func TestHTTPExchangeFaults(t *testing.T) {
	type run struct {
		name   string
		rates  [faultCount]float64
		maxDur time.Duration
		check  exFault
	}
	var runs []run
	for f := range faultCount {
		var r [faultCount]float64
		r[f] = 0.15
		runs = append(runs, run{name: faultNames[f], rates: r, check: f})
	}
	// Held DOWN requests outlive the proxy's limit and are cut.
	runs = append(runs, run{name: "cut", maxDur: 150 * time.Millisecond, check: -1})
	runs = append(runs, run{name: "mix", rates: [faultCount]float64{0.04, 0.04, 0.04, 0.04, 0.1, 0.04},
		maxDur: time.Second, check: -1})
	for i, r := range runs {
		t.Run(r.name, func(t *testing.T) {
			leakcheck.Check(t)
			var px *faultProxy
			cfg := faultConfig()
			e := newExEnv(t, cfg, cfg, func(origin string) string {
				px = newFaultProxy(t, origin[len("http://"):], uint64(1000+i), func(p *faultProxy) {
					p.rates = r.rates
					if r.maxDur > 0 {
						p.maxDur = r.maxDur
					}
				})
				return px.url()
			})
			e.echo(t, 2<<20, 60*time.Second)
			e.close(t)
			px.close()
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
				px = newFaultProxy(t, origin[len("http://"):], 1, func(p *faultProxy) { p.replayDown = true })
				return px.url()
			})
			err := e.echoErr(1<<20, 15*time.Second)
			e.close(t)
			px.close()
			if px.count(faultReplay) != 1 {
				t.Fatalf("%d replays, want 1", px.count(faultReplay))
			}
			switch {
			case !skip && err != nil:
				t.Fatalf("with the MAC check: %v", err)
			case skip && err == nil:
				t.Fatal("a replayed response was not noticed without the MAC check")
			}
		})
	}
}
