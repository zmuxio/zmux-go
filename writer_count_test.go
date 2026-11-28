package zmux

import (
	"io"
	"net"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/djylb/timeshim"
	"github.com/zmuxio/zmux-go/internal/core"
)

type countedWriterConn struct {
	net.Conn
	queries, writes atomic.Uint64
}

func (c *countedWriterConn) Unsent() (uint64, bool) {
	c.queries.Add(1)
	return 0, true
}

func (c *countedWriterConn) WriteBatch(batch *core.Batch) (int, error) {
	c.writes.Add(1)
	bufs := net.Buffers(batch.Bufs())
	n, err := bufs.WriteTo(c.Conn)
	return int(n), err
}

func TestWriterSamplesOnlyForPublishedWork(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		raw, peer := net.Pipe()
		w := &countedWriterConn{Conn: raw}
		clk := timeshim.NewFake(time.Now())
		cfg := &Config{ClockDomain: NewClockDomain(clk), MaxIdleTimeout: -1}
		cfg.Liveness.KeepAlivePeriod = -1
		client, server := pair(t, w, peer, cfg, cfg)
		out, err := client.OpenStreamSync(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := out.Write([]byte{1}); err != nil {
			t.Fatal(err)
		}
		in, err := server.AcceptStream(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		var b [1]byte
		if _, err := io.ReadFull(in, b[:]); err != nil || b[0] != 1 {
			t.Fatalf("read %v: %v", b, err)
		}
		synctest.Wait()
		queries, writes := w.queries.Load(), w.writes.Load()
		if queries != writes || writes == 0 {
			t.Fatalf("%d carrier samples for %d writes", queries, writes)
		}
		for range 32 {
			client.locked(func() {}) // unrelated work, including timer rearming
			client.locked(func() { client.pipes[0].writer.wake.FireLocked() })
			synctest.Wait()
		}
		client.locked(func() {
			at := client.writerIdle.Next()
			if at == core.Never {
				t.Fatal("writer did not register its expiry")
			}
			client.expireWritersLocked(at)
		})
		synctest.Wait()
		if w.queries.Load() != queries || w.writes.Load() != writes {
			t.Fatalf("idle writer made %d queries and %d writes", w.queries.Load()-queries, w.writes.Load()-writes)
		}
		if _, err := out.Write([]byte{2}); err != nil {
			t.Fatal(err)
		}
		if _, err := io.ReadFull(in, b[:]); err != nil || b[0] != 2 {
			t.Fatalf("read %v: %v", b, err)
		}
		synctest.Wait()
		if w.queries.Load() != queries+1 || w.writes.Load() != writes+1 {
			t.Fatal("a single new request did not produce one sample and one write")
		}
	})
}
