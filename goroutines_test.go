package zmux

import "testing"

// finished closes once the owner finished and the last goroutine ended,
// whichever comes last, with no goroutine of its own.
func TestPipeGoroutinesFinished(t *testing.T) {
	closed := func(ch <-chan struct{}) bool {
		select {
		case <-ch:
			return true
		default:
			return false
		}
	}
	var g pipeGoroutines
	g.add()
	ch := g.finished()
	g.finish()
	if closed(ch) {
		t.Fatal("finished with a goroutine running")
	}
	g.done()
	if !closed(ch) || !closed(g.finished()) {
		t.Fatal("not finished after the last goroutine")
	}
	var h pipeGoroutines
	h.finish()
	if !closed(h.finished()) {
		t.Fatal("an idle owner that finished is not finished")
	}
}

func BenchmarkDoneChannel(b *testing.B) {
	var g pipeGoroutines
	b.ReportAllocs()
	for b.Loop() {
		_ = g.finished()
	}
}

// A pipe's Done closes once it was retired and its driver's goroutines
// ended, with no goroutine of its own.
func TestPipeEndDone(t *testing.T) {
	e := &pipeEnd{retired: make(chan struct{})}
	var g pipeGoroutines
	g.add()
	ch := e.done()
	g.finishThen(e.end)
	select {
	case <-ch:
		t.Fatal("Done closed while a goroutine of the pipe runs")
	default:
	}
	g.done()
	select {
	case <-ch:
	default:
		t.Fatal("Done open after the pipe's last goroutine")
	}
	if e.done() != closedChan {
		t.Fatal("a later Done is not closed")
	}
}
