package zmux

import (
	"context"
	"net"
	"testing"
	"testing/synctest"
)

// A listener owns no goroutine until it takes a carrier, and its
// memory-pressure watcher ends with Close.
func TestListenerWatcherLifetime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := mustNewListener(nil)
		synctest.Wait()
		if l.watching {
			t.Fatal("watcher started before the listener took a carrier")
		}
		a, b := net.Pipe()
		defer func() { _ = b.Close() }()
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { _ = l.HandlePipe(ctx, NetPipe(a), PipeHint{}); close(done) }()
		synctest.Wait()
		l.mu.Lock()
		w := l.watching
		l.mu.Unlock()
		if !w {
			t.Fatal("no watcher once the listener took a carrier")
		}
		cancel()
		<-done
		_ = l.Close()
		synctest.Wait() // the bubble ends only once the watcher ended
	})
}
