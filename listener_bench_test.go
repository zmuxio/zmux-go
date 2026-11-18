package zmux

import (
	"context"
	"encoding/binary"
	"net"
	"net/netip"
	"sync/atomic"
	"testing"
)

// BenchmarkListenerConcurrentAccept starts sessions on one listener from
// parallel clients; run with -mutexprofile to see whether the listener's
// lock is contended.
func BenchmarkListenerConcurrentAccept(b *testing.B) {
	l, err := NewListener(&ListenerConfig{Backlog: 1 << 20}) // Accept is not what is measured
	if err != nil {
		b.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	accepted := make(chan struct{})
	go func() {
		defer close(accepted)
		for {
			if _, err := l.Accept(ctx); err != nil {
				return // each session ends with its client's close
			}
		}
	}()
	var next atomic.Uint32
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			cn, sn := net.Pipe()
			// A source each, as distinct clients: no per-source bound applies.
			var a [4]byte
			binary.BigEndian.PutUint32(a[:], 10<<24|next.Add(1))
			hint := PipeHint{Source: netip.AddrPortFrom(netip.AddrFrom4(a), 1)}
			go func() { _ = l.HandlePipe(ctx, NetPipe(sn), hint) }()
			c, err := Client(ctx, cn, nil)
			if err != nil {
				b.Error(err)
				return
			}
			_ = c.Close()
			_ = c.Wait()
		}
	})
	_ = l.Close()
	cancel()
	<-accepted
}
