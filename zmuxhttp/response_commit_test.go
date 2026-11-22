package zmuxhttp

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/zmuxio/zmux-go"
)

type setupResponseGate struct {
	err error
	*httptest.ResponseRecorder
	entered, interrupted, release chan struct{}
	onInterrupt                   func()
	phase                         string
	stop                          sync.Once
}

func newSetupResponseGate(phase string) *setupResponseGate {
	return &setupResponseGate{ResponseRecorder: httptest.NewRecorder(), phase: phase, entered: make(chan struct{}), interrupted: make(chan struct{}), release: make(chan struct{})}
}
func (w *setupResponseGate) WriteHeader(status int) {
	if w.phase == "header" {
		close(w.entered)
		<-w.release
	}
	w.ResponseRecorder.WriteHeader(status)
}
func (w *setupResponseGate) FlushError() error {
	if w.phase == "flush" {
		close(w.entered)
		<-w.release
	}
	return w.err
}
func (w *setupResponseGate) SetWriteDeadline(time.Time) error {
	w.stop.Do(func() {
		if w.onInterrupt != nil {
			w.onInterrupt()
		}
		close(w.interrupted)
	})
	return nil
}

// HS-15, HS-16, HS-16a, HS-19.
func TestTakeoverResponseCommit(t *testing.T) {
	for _, carrier := range []string{"stream-h1", "stream-h2", "stream-h3", "exchange-up", "exchange-down", "exchange-up64", "exchange-down64"} {
		for _, phase := range []string{"header", "flush"} {
			for _, end := range []string{"success", "error", "client", "deadline", "listener"} {
				t.Run(carrier+"/"+phase+"/"+end, func(t *testing.T) {
					synctest.Test(t, func(t *testing.T) {
						ctx, cancel := context.WithCancel(t.Context())
						defer cancel()
						called := make(chan struct{}, 2)
						cfg := mpConfig()
						cfg.HandshakeTimeout = time.Second
						up := carrier == "exchange-up" || carrier == "exchange-up64"
						cfgAd := &zmux.Admission{Decide: func(context.Context, zmux.CarrierInfo, []byte) zmux.Decision {
							return zmux.Decision{Verdict: zmux.VerdictTakeover}
						}}
						l := mustNewListener(&zmux.ListenerConfig{Config: cfg, MaxPending: 1, Admission: cfgAd, Fallback: func(_ context.Context, take *zmux.Takeover) {
							if up {
								var b [4]byte
								if _, err := io.ReadFull(take.Pipe().R, b[:]); err != nil || string(b[:]) != "data" {
									t.Errorf("first UP %q, %v", b, err)
								}
							}
							called <- struct{}{}
						}})
						defer func() { _ = l.Close() }()
						var handler http.Handler
						b64 := carrier == "exchange-up64" || carrier == "exchange-down64"
						body := []byte("stream")
						stream := carrier[:6] == "stream"
						if stream {
							var err error
							handler, err = NewHandler(l, nil)
							if err != nil {
								t.Fatal(err)
							}
						} else {
							psk := bytes.Repeat([]byte{7}, 32)
							h := newExHandler(t, l, nil, &exCfg{PSK: psk, Base64: b64})
							defer func() { _ = h.Close() }()
							handler = h
							cid, nonce := [exCIDLen]byte{1}, [exNonceLen]byte{1}
							kind := byte(exKindDown)
							var data []byte
							if up {
								kind = exKindUp
								data = []byte("data")
							}
							body = exRequest(newExKey(psk, cid[:]), &cid, kind, 0, 0, 0, uint32(time.Now().Unix()), &nonce, data)
							if b64 {
								body = []byte(base64.StdEncoding.EncodeToString(body))
							}
						}
						req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/", bytes.NewReader(body))
						if stream {
							req.ContentLength = -1
							req.ProtoMajor = int(carrier[len(carrier)-1] - '0')
						}
						w := newSetupResponseGate(phase)
						if end == "error" {
							w.err = io.ErrClosedPipe
						}
						done := make(chan any, 1)
						go func() { defer func() { done <- recover() }(); handler.ServeHTTP(w, req) }()
						<-w.entered
						synctest.Wait()
						if len(called) != 0 || l.Stats().Admission.Takeover != 0 {
							t.Fatal("handoff before response commit")
						}
						switch end {
						case "client":
							cancel()
						case "listener":
							_ = l.Close()
						}
						if end != "success" && end != "error" {
							<-w.interrupted
							synctest.Wait()
							select {
							case v := <-done:
								t.Fatalf("handler ended before the host write returned: %v", v)
							default:
							}
							if end != "listener" {
								checkResponseCapacity(t, l, false)
							}
						}
						close(w.release)
						v := <-done
						if end == "success" {
							if v != nil {
								t.Fatal(v)
							}
							<-called
							if !stream {
								response := w.Body.Bytes()
								if b64 {
									var err error
									response, err = base64.StdEncoding.DecodeString(string(response))
									if err != nil {
										t.Fatal(err)
									}
								}
								if len(response) != exRespHdr || response[exMACLen] != 0 {
									t.Fatalf("first response %x", response)
								}
							}
						} else {
							err, _ := v.(error)
							if !errors.Is(err, http.ErrAbortHandler) {
								t.Fatalf("failed write: %v", v)
							}
						}
						synctest.Wait()
						want := zmux.AdmissionStats{HandshakeFailed: 1}
						if end == "success" {
							want = zmux.AdmissionStats{Takeover: 1}
						}
						if got := l.Stats().Admission; got != want {
							t.Fatalf("outcome %+v, want %+v", got, want)
						}
						if len(called) != 0 {
							t.Fatal("unexpected callback")
						}
						if end != "listener" {
							checkResponseCapacity(t, l, true)
						}
					})
				})
			}
		}
	}
}
func checkResponseCapacity(t *testing.T, l *zmux.Listener, available bool) {
	t.Helper()
	a, err := l.Adapter().Arrive(netip.AddrPort{}, netip.AddrPort{})
	if a != nil {
		a.Release()
	}
	if (err == nil) != available {
		t.Fatalf("capacity available=%t, err=%v", available, err)
	}
}

// HS-16a. Cancelling the host write and releasing the parked arrival leave
// the preparation's reference alive until the actual WriteHeader or Flush exits.
func TestTakeoverInterruptKeepsSlotThroughResponseWrite(t *testing.T) {
	for _, phase := range []string{"header", "flush"} {
		for _, source := range []string{"request", "listener"} {
			t.Run(phase+"/"+source, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					ctx, cancel := context.WithCancel(t.Context())
					defer cancel()
					l := mustNewListener(&zmux.ListenerConfig{MaxPending: 1, Admission: &zmux.Admission{Decide: func(context.Context, zmux.CarrierInfo, []byte) zmux.Decision {
						return zmux.Decision{Verdict: zmux.VerdictTakeover}
					}}, Fallback: func(context.Context, *zmux.Takeover) { t.Error("cancelled response handed off") }})
					defer func() { _ = l.Close() }()
					w := newSetupResponseGate(phase)
					releaseWrite := sync.OnceFunc(func() { close(w.release) })
					defer releaseWrite()
					r := httptest.NewRequestWithContext(ctx, http.MethodPost, "/", http.NoBody)
					a, ok := l.Adapter().AdmitRequest(w, r, nil, nil)
					if !ok {
						t.Fatal("request was not admitted")
					}
					defer a.Release()
					interruptEntered := make(chan struct{})
					allowReturn := make(chan struct{})
					releaseInterrupt := sync.OnceFunc(func() { close(allowReturn) })
					defer releaseInterrupt()
					w.onInterrupt = func() {
						close(interruptEntered)
						if source == "listener" {
							<-allowReturn
						}
						// Reenter the same arrival and listener from the host interrupt. Giving
						// back the parked reference leaves only the outstanding write holding quota.
						a.Release()
						l.Undrain()
					}
					done := make(chan error, 1)
					go func() {
						done <- a.PrepareCarrier(ctx, func(bound context.Context) error {
							return commitResponse(bound, w, func() error { w.WriteHeader(http.StatusOK); return nil })
						})
					}()
					<-w.entered
					stopped := make(chan struct{})
					go func() {
						if source == "request" {
							cancel()
						} else {
							_ = l.Close()
						}
						close(stopped)
					}()
					<-interruptEntered
					synctest.Wait()
					select {
					case <-stopped:
					default:
						t.Fatal("stopping setup waited for the host interrupt")
					}
					releaseInterrupt()
					<-w.interrupted
					synctest.Wait() // SetWriteDeadline has returned, the writer is still gated.
					select {
					case err := <-done:
						t.Fatalf("preparation ended during the write: %v", err)
					default:
					}
					if source == "request" {
						checkResponseCapacity(t, l, false)
					}
					releaseWrite()
					if err := <-done; !errors.Is(err, context.Canceled) {
						t.Fatalf("preparation after cancellation: %v", err)
					}
					if source == "request" {
						checkResponseCapacity(t, l, true)
					}
					if got := l.Stats().Admission; got != (zmux.AdmissionStats{HandshakeFailed: 1}) {
						t.Fatalf("admission after write: %+v", got)
					}
				})
			})
		}
	}
}

// An exchange request may disappear after the server has assigned its
// response. The listener still owns the carrier and a fresh retry must
// get the assigned piece from that same carrier.
func TestExchangeFirstResponseInterrupted(t *testing.T) {
	t.Run("retry", func(t *testing.T) { exchangeFirstResponseInterrupted(t, true) })
	t.Run("idle", func(t *testing.T) { exchangeFirstResponseInterrupted(t, false) })
}

func exchangeFirstResponseInterrupted(t *testing.T, retryFirst bool) {
	t.Helper()
	synctest.Test(t, func(t *testing.T) {
		l := mustNewListener(&zmux.ListenerConfig{Config: mpConfig(), Admission: &zmux.Admission{
			Decide: func(context.Context, zmux.CarrierInfo, []byte) zmux.Decision {
				return zmux.Decision{Verdict: zmux.VerdictAccept}
			},
		}})
		defer func() { _ = l.Close() }()
		psk := []byte("exchange test key")
		const idle = time.Second
		h := newExHandler(t, l, nil, &exCfg{PSK: psk, IdleTimeout: idle})
		defer func() { _ = h.Close() }()
		cid := [exCIDLen]byte{1}
		key := newExKey(psk, cid[:])
		request := func(ctx context.Context, nonce byte) *http.Request {
			n := [exNonceLen]byte{nonce}
			body := exRequest(key, &cid, exKindDown, 0, 0, 0, uint32(time.Now().Unix()), &n, nil)
			return httptest.NewRequestWithContext(ctx, http.MethodPost, "/", bytes.NewReader(body))
		}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		rec := newSetupResponseGate("flush")
		done := make(chan any, 1)
		go func() {
			defer func() { done <- recover() }()
			h.ServeHTTP(rec, request(ctx, 1))
		}()
		<-rec.entered
		cancel()
		<-rec.interrupted
		close(rec.release)
		result := <-done
		err, _ := result.(error)
		if result != nil && !errors.Is(err, http.ErrAbortHandler) {
			t.Fatalf("first request returned %v", result)
		}

		if !retryFirst {
			h.mu.Lock()
			c := h.carriers[cid]
			h.mu.Unlock()
			if c == nil {
				t.Fatal("interrupted response did not leave a carrier for retry")
			}
			time.Sleep(idle)
			synctest.Wait()
			h.mu.Lock()
			_, live := h.carriers[cid]
			_, gone := h.goneLocked(cid)
			h.mu.Unlock()
			c.mu.Lock()
			cause := c.err
			c.mu.Unlock()
			if live || !gone || !errors.Is(cause, errExchangeIdle) {
				t.Fatalf("after idle deadline: live=%v gone=%v cause=%v", live, gone, cause)
			}
			return
		}

		retry := httptest.NewRecorder()
		h.ServeHTTP(retry, request(t.Context(), 2))
		body := retry.Body.Bytes()
		if retry.Code != http.StatusOK || len(body) != exRespHdr || body[exMACLen] != 0 {
			t.Fatalf("retry returned status %d and response %x", retry.Code, body)
		}
		if got := l.Stats().Admission; got != (zmux.AdmissionStats{Accepted: 1}) {
			t.Fatalf("admission after retry = %+v", got)
		}
	})
}
