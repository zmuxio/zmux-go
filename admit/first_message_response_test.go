package admit

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/zmuxio/zmux-go"
)

type prefixResponse struct {
	Name, Fixture  string
	Step           int
	InvalidPrefix  vectorHex `json:"invalid_prefix_hex"`
	HostOutput     vectorHex `json:"host_output_hex"`
	Output         vectorHex `json:"expect_output_hex"`
	InnerOutput    vectorHex `json:"expect_inner_output_hex"`
	OuterEarlyData bool      `json:"outer_http_early_data"`
	HostStatus     int       `json:"expect_host_status"`
}

func TestFirstMessagePrefixResponses(t *testing.T) {
	vectors := readPrefixFixture[struct{ Responses []prefixResponse }](t, "first_message_cases")
	for _, v := range vectors.Responses {
		switch v.Name {
		case "protected-plain-prefix":
			t.Run(v.Name, func(t *testing.T) {
				wire := responseTraceBytes(t, v.Fixture, v.Step)
				testPrefixEOF(t, wire, nil, v.InnerOutput)
			})
		case "prefix-fallback":
			t.Run(v.Name, func(t *testing.T) {
				testPrefixEOF(t, v.InvalidPrefix, v.HostOutput, v.Output)
			})
		case "outer-early-data":
			t.Run(v.Name, func(t *testing.T) {
				l := firstMessageListener(t, nil)
				r := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
				if v.OuterEarlyData {
					r.Header.Set("Early-Data", "1")
				}
				w := httptest.NewRecorder()
				a, ok := l.Adapter().AdmitUpgrade(w, r, nil, nil)
				if a != nil || ok || w.Code != v.HostStatus || !bytes.Equal(w.Body.Bytes(), v.InnerOutput) {
					t.Fatalf("early upgrade = %d, %x, admitted %v", w.Code, w.Body.Bytes(), ok)
				}
			})
		}
	}
}

func responseTraceBytes(t *testing.T, id string, step int) []byte {
	t.Helper()
	f, err := os.Open("../testdata/spec/fixtures/traces/" + strings.ReplaceAll(id, ".", "/") + ".ndjson")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	scan := bufio.NewScanner(f)
	for scan.Scan() {
		var v struct {
			Step int       `json:"step"`
			Hex  vectorHex `json:"hex"`
		}
		if err := json.Unmarshal(scan.Bytes(), &v); err != nil {
			t.Fatal(err)
		}
		if v.Step == step {
			return v.Hex
		}
	}
	t.Fatalf("trace %s step %d: %v", id, step, scan.Err())
	return nil
}

func firstMessageListener(t *testing.T, fallback func(context.Context, *zmux.Takeover)) *zmux.Listener {
	t.Helper()
	return firstMessageListenerOptions(t, fallback, false)
}

func firstMessageListenerOptions(t *testing.T, fallback func(context.Context, *zmux.Takeover), encrypted bool) *zmux.Listener {
	t.Helper()
	v := prefixVectors(t)[0]
	store, err := NewMemoryPrefixStore(PrefixWindow{Slot: 30 * time.Second, Skew: 1},
		[]PrefixGeneration{{Key: [32]byte(v.Outer), LastSlot: math.MaxUint64}}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AddTicket(PrefixTicket{PrefixCredential: v.credential(), LastSlot: math.MaxUint64}); err != nil {
		t.Fatal(err)
	}
	carrier := PrefixCarrierOptions{AllowCleartext: true}
	opts := PrefixAuthVerifierOptions{PrefixCarrierOptions: carrier, Store: store, Clock: func() time.Time { return time.Unix(v.UnixSeconds, 0) }}
	if fallback != nil {
		opts.Reject = zmux.VerdictFallback
	}
	verifier, err := NewPrefixAuthVerifier(opts)
	if err != nil {
		t.Fatal(err)
	}
	admission := verifier.Admission()
	admission.PeekTimeout = 2 * time.Second
	admission.Encrypted = encrypted
	l, err := zmux.NewListener(&zmux.ListenerConfig{Admission: admission, Fallback: fallback})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l
}

func testPrefixEOF(t *testing.T, input, hostOutput, output []byte) {
	t.Helper()
	var fallback func(context.Context, *zmux.Takeover)
	var replay []byte
	var hostErr error
	if hostOutput != nil {
		fallback = func(_ context.Context, a *zmux.Takeover) {
			p := a.Pipe()
			replay, hostErr = io.ReadAll(p.R)
			if hostErr == nil {
				_, hostErr = p.W.Write(hostOutput)
			}
		}
	}
	l := firstMessageListener(t, fallback)
	p := &prefixClosedInput{Reader: bytes.NewReader(input)}
	_ = l.HandlePipe(t.Context(), zmux.PipeIO{R: p, W: p}, zmux.PipeHint{})
	if !p.closed || !bytes.Equal(p.out.Bytes(), output) || fallback != nil && (!bytes.Equal(replay, input) || hostErr != nil) {
		t.Fatalf("closed %v, output %x, host input %x and error %v", p.closed, p.out.Bytes(), replay, hostErr)
	}
}

type prefixClosedInput struct {
	*bytes.Reader
	out    bytes.Buffer
	closed bool
}

func (p *prefixClosedInput) Write(b []byte) (int, error) { return p.out.Write(b) }

func (p *prefixClosedInput) Close() error {
	p.closed = true
	return nil
}

func TestFirstMessageTimeoutFallback(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		out := []byte{0xab, 0xcd}
		l := firstMessageListener(t, func(_ context.Context, a *zmux.Takeover) {
			p := a.Pipe()
			buf := make([]byte, 1)
			if _, err := io.ReadFull(p.R, buf); err != nil || buf[0] != 0xff {
				t.Errorf("fallback prefix %x, %v", buf, err)
			}
			if _, err := p.W.Write(out); err != nil {
				t.Error(err)
			}
		})
		client, server := net.Pipe()
		defer func() { _ = client.Close() }()
		done := make(chan struct{})
		go func() {
			defer close(done)
			_ = l.HandlePipe(t.Context(), zmux.NetPipe(server), zmux.PipeHint{})
		}()
		if _, err := client.Write([]byte{0xff}); err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(client)
		<-done
		if err != nil || !bytes.Equal(got, out) {
			t.Fatalf("response %x, %v", got, err)
		}
	})
}

type prefixDeadlineWriter struct {
	*httptest.ResponseRecorder
	conn    net.Conn
	expired bool
}

func (w *prefixDeadlineWriter) SetReadDeadline(deadline time.Time) error {
	if deadline.IsZero() && w.expired {
		return http.ErrNotSupported
	}
	w.expired = !deadline.IsZero()
	return w.conn.SetReadDeadline(deadline)
}

func TestFirstMessageHTTPBodyTimeoutWriter(t *testing.T) {
	for _, supported := range []bool{false, true} {
		name := "unsupported"
		if supported {
			name = "deadline-cannot-be-reset"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				l := firstMessageListener(t, func(context.Context, *zmux.Takeover) {})
				client, server := net.Pipe()
				defer func() { _ = client.Close() }()
				defer func() { _ = server.Close() }()
				r := httptest.NewRequest(http.MethodPost, "/", server)
				w := httptest.NewRecorder()
				var writer http.ResponseWriter = w
				if supported {
					writer = &prefixDeadlineWriter{ResponseRecorder: w, conn: server}
					written := make(chan error, 1)
					go func() { _, err := client.Write([]byte{0xff}); written <- err }()
					defer func() {
						_ = client.Close()
						if err := <-written; err != nil {
							t.Error(err)
						}
					}()
				} else {
					if err := l.Adapter().ValidateRequestWriter(writer); !errors.Is(err, zmux.ErrInvalidConfig) {
						t.Fatalf("writer validation: %v", err)
					}
				}
				called := false
				fallback := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					called = true
					body, err := io.ReadAll(r.Body)
					if !bytes.Equal(body, []byte{0xff}) || !errors.Is(err, os.ErrDeadlineExceeded) {
						t.Errorf("fallback body %x: %v", body, err)
					}
					w.WriteHeader(http.StatusTeapot)
				})
				a, ok := l.Adapter().AdmitRequest(writer, r, nil, fallback)
				want := http.StatusInternalServerError
				if supported {
					want = http.StatusTeapot
				}
				if a != nil || ok || called != supported || w.Code != want {
					t.Fatalf("arrival %v, accepted=%t, fallback=%t, status=%d", a, ok, called, w.Code)
				}
			})
		})
	}
}

func TestFirstMessageTrustedEncryptedEntrance(t *testing.T) {
	v := prefixVectors(t)[0]
	a, err := NewPrefixAuth(PrefixAuthOptions{
		Slot: 30 * time.Second, Clock: func() time.Time { return time.Unix(v.UnixSeconds, 0) }, Random: bytes.NewReader(v.Nonce),
	}, v.credential(), PrefixGeneration{Key: [32]byte(v.Outer), LastSlot: math.MaxUint64})
	if err != nil {
		t.Fatal(err)
	}
	name, header, err := a.Header(&zmux.CarrierInfo{Encrypted: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, encrypted := range []bool{false, true} {
		l := firstMessageListenerOptions(t, func(context.Context, *zmux.Takeover) {}, encrypted)
		r := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
		r.Header.Set(name, header)
		r.Header.Set("Forwarded", "proto=https")
		w := httptest.NewRecorder()
		arrival, ok := l.Adapter().AdmitUpgrade(w, r, nil, nil)
		if arrival != nil {
			arrival.Release()
		}
		if ok != encrypted {
			t.Fatalf("trusted encryption=%t, accepted=%t", encrypted, ok)
		}
	}
}
