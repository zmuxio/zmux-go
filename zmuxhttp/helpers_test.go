package zmuxhttp

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"io"
	"math/big"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/admit"
)

// Helpers of the root package's tests that these tests share.

func mpConfig() *zmux.Config {
	return &zmux.Config{MaxPipes: 4, PipeAuth: zmux.PipeAuth{PSK: bytes.Repeat([]byte{7}, 32), EndToEnd: true},
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
	done := make(chan struct{})
	go func() { _ = c.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("session did not end")
	}
}

// waitUntil polls cond every 10 ms until it holds or d passes.
func waitUntil(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("%s: not within %v", what, d)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// tlsWrapConfig is a session config whose join secret is pipe 0's TLS
// exporter: a session starts only when both ends see the same TLS.
func tlsWrapConfig() *zmux.Config {
	return &zmux.Config{MaxPipes: 4, PipeAuth: zmux.PipeAuth{Exporter: true, EndToEnd: true},
		SchedulerParams: zmux.SchedulerParams{Probation: -1}}
}

// serverTLSWrap is a WrapPipe that runs a TLS server over every carrier.
func serverTLSWrap(cfg *tls.Config) func(context.Context, zmux.PipeIO, zmux.PipeHint) (zmux.PipeIO, error) {
	return func(ctx context.Context, p zmux.PipeIO, _ zmux.PipeHint) (zmux.PipeIO, error) {
		tc := tls.Server(zmux.PipeConn(p), cfg)
		if err := tc.HandshakeContext(ctx); err != nil {
			_ = tc.Close()
			return zmux.PipeIO{}, err
		}
		return zmux.NetPipe(tc), nil
	}
}

// clientTLSWrap wraps a Candidate's carriers in TLS.
func clientTLSWrap(c zmux.Candidate, cfg *tls.Config) zmux.Candidate {
	dial := c.Dial
	c.Dial = func(ctx context.Context, cred zmux.CarrierCredential) (zmux.PipeIO, error) {
		p, err := dial(ctx, cred)
		if err != nil {
			return zmux.PipeIO{}, err
		}
		tc := tls.Client(zmux.PipeConn(p), cfg)
		if err := tc.HandshakeContext(ctx); err != nil {
			_ = tc.Close()
			return zmux.PipeIO{}, err
		}
		return zmux.NetPipe(tc), nil
	}
	return c
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
	code int
	ct   string
	body string
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
	return siteResp{resp.StatusCode, resp.Header.Get("Content-Type"), string(b)}
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
		Decide: func(_ context.Context, info *zmux.CarrierInfo, prefix []byte) zmux.Decision {
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

func selfSignedTLS(t testing.TB) (srv, cli *tls.Config) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "localhost"},
		DNSNames: []string{"localhost"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour)}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, _ := x509.ParseCertificate(der)
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}},
		&tls.Config{RootCAs: pool, ServerName: "localhost"}
}

// firstRecord returns the bytes a client session sends first on a carrier.
func firstRecord(t *testing.T) []byte {
	t.Helper()
	c, s := net.Pipe()
	defer func() { _ = s.Close() }()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		conn, err := zmux.Start(ctx, zmux.NetPipe(c), zmux.RoleClient, mpConfig())
		if err == nil {
			defer func() { _ = conn.Close(); _ = conn.Wait() }()
		}
		<-ctx.Done()
	}()
	b := make([]byte, 4096)
	_ = s.SetReadDeadline(time.Now().Add(5 * time.Second))
	n, err := s.Read(b)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	_ = c.Close()
	return b[:n]
}

// eqAdm compares admission totals.
func eqAdm(a, b zmux.AdmissionStats) bool { return a == b }

// decideWith is an Admission whose every decision is v, counted in n.
func decideWith(v zmux.AdmissionVerdict, n *atomic.Int32) *zmux.Admission {
	return &zmux.Admission{PeekTimeout: 5 * time.Second, StallMin: time.Millisecond, StallMax: time.Millisecond,
		Decide: func(context.Context, *zmux.CarrierInfo, []byte) zmux.Decision {
			n.Add(1)
			return zmux.Decision{Verdict: v}
		}}
}

func atkConfig() *zmux.Config {
	return &zmux.Config{MaxPipes: 4, PipeAuth: zmux.PipeAuth{PSK: atkPSK, EndToEnd: true}, ResumeTimeout: 60 * time.Second,
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
