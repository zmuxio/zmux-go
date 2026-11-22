package zmux

import (
	"encoding/binary"
	"runtime"
	"sync"
	"testing"

	"github.com/zmuxio/zmux-go/internal/core"
)

// One operation is a wave across 100,000 independent sessions. Keeping
// transport and accept setup outside the measurement isolates registry
// contention and host timer reprogramming on the serving machine.
func BenchmarkLifecycle100k(b *testing.B) {
	const size = 100_000
	parallel := func(f func(int)) {
		var wg sync.WaitGroup
		workers := runtime.GOMAXPROCS(0)
		for w := range workers {
			begin, end := size*w/workers, size*(w+1)/workers
			wg.Go(func() {
				for i := begin; i < end; i++ {
					f(i)
				}
			})
		}
		wg.Wait()
	}
	b.Run("Unregister", func(b *testing.B) {
		for range b.N {
			b.StopTimer()
			l, err := NewListener(nil)
			if err != nil {
				b.Fatal(err)
			}
			cs := make([]Conn, size)
			for i := range cs {
				c := &cs[i]
				c.id = SessionID(i + 1)
				var loc [16]byte
				binary.BigEndian.PutUint64(loc[8:], uint64(i+1))
				l.register(c, loc[:])
			}
			b.StartTimer()
			parallel(func(i int) { l.sessionClosed(&cs[i]) })
			b.StopTimer()
			_ = l.Close()
		}
	})
	b.Run("DeferDeadline", func(b *testing.B) {
		cfg := &Config{}
		clk := newClock(nil)
		// The core has no protocol deadline yet. The measured deadline
		// is the session orphan sweep, moved by the application.
		cs := make([]Conn, size)
		for i := range cs {
			c := &cs[i]
			c.cfg, c.clk, c.timerAt = cfg, clk, core.Never
			c.s = core.New(core.Config{HoldFirstFlight: true}, 0)
			c.handles, c.sweepAt = 1, core.Time(3600*core.Second)
			c.rearmLocked(core.Never)
		}
		b.ResetTimer()
		for range b.N {
			parallel(func(i int) {
				c := &cs[i]
				for range 8 {
					c.lock()
					c.sweepAt = c.sweepAt.Add(core.Second)
					c.rearmLocked(core.Never)
					c.unlockPeek()
				}
			})
		}
		b.StopTimer()
		for i := range cs {
			cs[i].timer.Stop()
		}
	})
}
