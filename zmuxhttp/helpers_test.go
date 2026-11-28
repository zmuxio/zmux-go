package zmuxhttp

import (
	"bytes"
	"context"
	"encoding/hex"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/admit"
	"github.com/zmuxio/zmux-go/internal/testscale"
	"github.com/zmuxio/zmux-go/internal/testsession"
)

// Helpers of the root package's tests that these tests share.

func mpConfig() *zmux.Config {
	return &zmux.Config{SessionPolicy: zmux.SessionPolicy{Limits: zmux.CapacityLimits{SteadyAttachments: 4}}, PipeAuth: zmux.PipeAuth{PSK: bytes.Repeat([]byte{7}, 32), EndToEnd: true},
		SchedulerParams: zmux.SchedulerParams{Probation: -1}}
}

// mustNewListener is NewListener for a configuration the test knows valid.
func mustNewListener(cfg *zmux.ListenerConfig) *zmux.Listener {
	l, err := zmux.NewListener(cfg)
	if err != nil {
		panic(err)
	}
	return l
}

func waitDone(t testing.TB, c *zmux.Conn) {
	t.Helper()
	done := make(chan struct{})
	go func() { _ = c.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(testscale.D(10 * time.Second)):
		t.Fatal("session did not end")
	}
}

// siteHandler is a host's own handler: every response it gives.
func siteHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, "site "+r.Method+" "+hex.EncodeToString(b))
	})
}

type siteResp struct {
	ct   string
	body string
	code int
}

func doReq(t *testing.T, c *http.Client, method, url string, body []byte) siteResp {
	t.Helper()
	return doReqHeader(t, c, method, url, body, nil)
}

func doReqHeader(t *testing.T, c *http.Client, method, url string, body []byte, h http.Header) siteResp {
	t.Helper()
	req, err := http.NewRequest(method, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	addHeader(req.Header, h)
	if body == nil {
		req.Body = http.NoBody
	}
	resp, err := c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	return siteResp{code: resp.StatusCode, ct: resp.Header.Get("Content-Type"), body: string(b)}
}

// variantNamed is the per-variant count of name (zero if none).
func variantNamed(vs []zmux.VariantStat, name string) zmux.VariantStat {
	for _, v := range vs {
		if !v.Overflow && v.Name == name {
			return v
		}
	}
	return zmux.VariantStat{}
}

func testHeader() []byte {
	h := bytes.Repeat([]byte{'h'}, headerLen)
	h[0], h[1] = 0x01, headerLen
	return h
}

// classifyingAdmission tells zmux first records ("") from carriers that
// begin with a test header ("x"), which it reads with VerdictMore and
// skips; calls counts its calls.
func classifyingAdmission(calls *sync.Map) *zmux.Admission {
	return &zmux.Admission{Peek: 2, MaxPeek: 64, PeekTimeout: 5 * time.Second,
		Decide: func(_ context.Context, _ zmux.CarrierInfo, prefix []byte) zmux.Decision {
			n, _ := calls.LoadOrStore("n", new(int))
			*n.(*int)++
			c, need := admit.ClassifyFirstBytes(prefix, zmux.FramingByteStream)
			switch {
			case c == admit.FirstBytesRecord:
				return zmux.Decision{Verdict: zmux.VerdictAccept}
			case c == admit.FirstBytesNeedMore:
				return zmux.Decision{Verdict: zmux.VerdictMore, Need: need}
			case len(prefix) < 2 || prefix[0] != 0x01:
				return zmux.Decision{Verdict: zmux.VerdictClose, Variant: "unknown"}
			case len(prefix) < int(prefix[1]):
				return zmux.Decision{Verdict: zmux.VerdictMore, Need: int(prefix[1])}
			}
			return zmux.Decision{Verdict: zmux.VerdictAccept, Skip: int(prefix[1]), Variant: "x"}
		}}
}

// firstRecord returns the bytes a client session sends first on a carrier.
func firstRecord(t *testing.T) []byte {
	t.Helper()
	return testsession.FirstRecord(t, mpConfig())
}

// eqAdm compares admission totals.
func eqAdm(a, b zmux.AdmissionStats) bool { return a == b }

// decideWith is an Admission whose every decision is v, counted in n.
func decideWith(v zmux.AdmissionVerdict, n *atomic.Int32) *zmux.Admission {
	return &zmux.Admission{PeekTimeout: 5 * time.Second, StallMin: time.Millisecond, StallMax: time.Millisecond,
		Decide: func(context.Context, zmux.CarrierInfo, []byte) zmux.Decision {
			n.Add(1)
			return zmux.Decision{Verdict: v}
		}}
}

func atkConfig() *zmux.Config {
	return &zmux.Config{SessionPolicy: zmux.SessionPolicy{Limits: zmux.CapacityLimits{SteadyAttachments: 4}}, PipeAuth: zmux.PipeAuth{PSK: atkPSK, EndToEnd: true}, ResumeTimeout: 60 * time.Second,
		HandshakeTimeout: 30 * time.Second, SchedulerParams: zmux.SchedulerParams{Probation: -1},
		AllowRecordMode: func(zmux.RecordPolicyInput, zmux.RecordMode, any) bool { return true }}
}

// headerLen is the length of the test transform's header: a lead byte
// that ClassifyFirstBytes judges Other, a length byte and the rest.
const headerLen = 24

var atkPSK = bytes.Repeat([]byte{0x5a}, 32)

// exCfg holds the settings of both ends of an exchange carrier in one
// value, as the tests of one carrier use them.
type exCfg struct {
	Header                     http.Header
	DownURL, Instance          string
	NonceParam                 string
	PSK                        []byte
	UpPieceSize, DownPieceSize int64
	DownRequests, UpRequests   int
	RequestTimeout, Deadline   time.Duration
	BackoffMin, BackoffMax     time.Duration
	HoldTime, Coalesce         time.Duration
	IdleTimeout, GoneMemory    time.Duration
	DownWindow                 int
	UpStoreLimit               int64
	MaxClockSkew               time.Duration
	NonceCacheSize             int
	AllowInsecure, Base64      bool
	CacheBust                  zmux.Switch
	testSkipResponseMAC        bool
}

func (x *exCfg) config() ExchangeConfig {
	if x == nil {
		return ExchangeConfig{}
	}
	return ExchangeConfig{PSK: x.PSK, AllowInsecure: x.AllowInsecure, Base64: x.Base64,
		UpPieceSize: x.UpPieceSize, DownPieceSize: x.DownPieceSize}
}

// exchange is the client end at url.
func (x *exCfg) exchange(client *http.Client, url string) Exchange {
	if x == nil {
		return Exchange{Client: client, URL: url}
	}
	return Exchange{Client: client, URL: url, Config: x.config(), CacheBust: x.CacheBust,
		testSkipResponseMAC: x.testSkipResponseMAC, Header: x.Header, DownURL: x.DownURL, Instance: x.Instance,
		NonceParam: x.NonceParam, DownRequests: x.DownRequests, UpRequests: x.UpRequests,
		RequestTimeout: x.RequestTimeout, Deadline: x.Deadline, Coalesce: x.Coalesce,
		Backoff: zmux.Backoff{Min: x.BackoffMin, Max: x.BackoffMax}}
}

// server is the server end's options with o's HTTP options.
func (x *exCfg) server(o *Options) *ExchangeOptions {
	s := &ExchangeOptions{}
	if o != nil {
		s.Options = *o
	}
	if x != nil {
		s.HoldTime, s.Coalesce, s.DownWindow, s.UpStoreLimit = x.HoldTime, x.Coalesce, x.DownWindow, x.UpStoreLimit
		s.IdleTimeout, s.GoneMemory, s.MaxClockSkew, s.NonceCacheSize = x.IdleTimeout, x.GoneMemory, x.MaxClockSkew, x.NonceCacheSize
	}
	return s
}

// handler is NewExchangeHandler with the server end of x.
func (x *exCfg) handler(l *zmux.Listener, o *Options) (*ExchangeHandler, error) {
	return NewExchangeHandler(l, x.server(o), x.config())
}
