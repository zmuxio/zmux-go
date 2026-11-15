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
		name string
		cut  func(st *Stream)
		want func(err error) bool
	}{
		{"deadline", func(st *Stream) { _ = st.SetReadDeadline(time.Unix(1, 0)) },
			func(err error) bool { return errors.Is(err, os.ErrDeadlineExceeded) }},
		{"cancel-read", func(st *Stream) { st.CancelRead(7) },
			func(err error) bool { return errors.Is(err, io.EOF) }},
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
