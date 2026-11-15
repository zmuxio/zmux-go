package zmux

import (
	"testing"
)

// TestWaitCoversEverySideGoroutine: a side goroutine started before the
// session's end is waited for; once the end is published no side
// goroutine starts any more, so none can outlive Wait.
func TestWaitCoversEverySideGoroutine(t *testing.T) {
	a, b := tcpPair(t)
	c, _ := pair(t, a, b, nil, nil)
	release := make(chan struct{})
	if !c.goSide(c.lctx, "test", func() { <-release }) {
		t.Fatal("goSide refused on a live session")
	}
	_ = c.Close()
	<-c.done
	waited := make(chan struct{})
	go func() { _ = c.Wait(); close(waited) }()
	select {
	case <-waited:
		t.Fatal("Wait returned while a side goroutine ran")
	default:
	}
	ran := false
	if c.goSide(c.lctx, "late", func() { ran = true }) {
		t.Fatal("goSide started a goroutine after the end was published")
	}
	close(release)
	<-waited
	if ran {
		t.Fatal("a refused side function ran")
	}
}
