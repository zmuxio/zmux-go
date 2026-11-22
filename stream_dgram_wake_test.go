package zmux

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"testing"
	"testing/synctest"
	"time"

	"github.com/djylb/timeshim"
)

// bubblePair returns a client and a server session over net.Pipe inside the
// calling synctest bubble, closed and waited for at the end of the test.
func bubblePair(t *testing.T, cfg func() *Config) (*Conn, *Conn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	a, b := net.Pipe()
	ch := make(chan *Conn, 1)
	go func() {
		s, err := Server(ctx, b, cfg())
		if err != nil {
			s = nil
		}
		ch <- s
	}()
	client, err := Client(ctx, a, cfg())
	if err != nil {
		t.Fatal(err)
	}
	server := <-ch
	if server == nil {
		t.Fatal("no server session")
	}
	t.Cleanup(func() { _ = client.Close(); _ = server.Close(); _ = client.Wait(); _ = server.Wait() })
	return client, server
}

// TestStreamDatagramReaderWakes: a ReceiveDatagram blocked on a stream that
// never had a deadline ends when another goroutine sets a past read
// deadline, stops the receiving part, or the stream is discarded.
func TestStreamDatagramReaderWakes(t *testing.T) {
	cases := []struct {
		cut  func(st *Stream)
		want func(err error) bool
		name string
	}{
		{name: "deadline", cut: func(st *Stream) { _ = st.SetReadDeadline(time.Unix(1, 0)) },
			want: func(err error) bool { return errors.Is(err, os.ErrDeadlineExceeded) }},
		{name: "cancel-read", cut: func(st *Stream) { st.CancelRead(7) },
			want: func(err error) bool { return errors.Is(err, io.EOF) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				client, _ := bubblePair(t, func() *Config {
					return &Config{Clock: timeshim.Real, EnableDatagrams: true, Liveness: Liveness{KeepAlivePeriod: -1}, MaxIdleTimeout: -1}
				})
				st, err := client.OpenStream()
				if err != nil {
					t.Fatal(err)
				}
				got := make(chan error, 1)
				go func() {
					_, _, err := st.ReceiveDatagram(context.Background())
					got <- err
				}()
				synctest.Wait()
				tc.cut(st)
				synctest.Wait()
				select {
				case err := <-got:
					if !tc.want(err) {
						t.Fatalf("ReceiveDatagram ended with %v", err)
					}
				default:
					t.Fatal("ReceiveDatagram not woken")
				}
			})
		})
	}
}

// TestStreamDatagramsSurviveAutoRelease: a stream whose application read
// the end and closed its side is released by itself; the datagrams that
// arrived before the end, and were not taken yet, stay readable through the
// handle. Datagrams of a released stream that arrive later are dropped.
func TestStreamDatagramsSurviveAutoRelease(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client, server := bubblePair(t, func() *Config {
			return &Config{Clock: timeshim.Real, EnableDatagrams: true, Liveness: Liveness{KeepAlivePeriod: -1}, MaxIdleTimeout: -1}
		})
		ctx := context.Background()
		st, err := client.OpenStream()
		if err != nil {
			t.Fatal(err)
		}
		for i := range byte(3) {
			if err := st.SendDatagram(1, []byte{i}); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := st.Write([]byte("data")); err != nil {
			t.Fatal(err)
		}
		if err := st.Close(); err != nil {
			t.Fatal(err)
		}
		rst, err := server.AcceptStream(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if b, err := io.ReadAll(rst); err != nil || string(b) != "data" {
			t.Fatalf("read %q, %v", b, err)
		}
		if err := rst.Close(); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		for server.ConnectionStats().StreamsActive != 0 {
			time.Sleep(time.Millisecond)
			synctest.Wait()
		}
		for i := range byte(3) {
			_, p, err := rst.ReceiveDatagram(ctx)
			if err != nil || len(p) != 1 || p[0] != i {
				t.Fatalf("datagram %d: %v, %v", i, p, err)
			}
		}
		if _, _, err := rst.ReceiveDatagram(ctx); !errors.Is(err, io.EOF) {
			t.Fatalf("after the queue: %v", err)
		}
		if n := server.ConnectionStats().DatagramsDroppedRecv; n != 0 {
			t.Fatalf("%d datagrams dropped", n)
		}
	})
}
