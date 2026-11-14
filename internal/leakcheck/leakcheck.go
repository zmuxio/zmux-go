// Package leakcheck verifies that tests leave no goroutines behind (a
// standard-library replacement for goleak): Check for one test, Main for
// a whole test binary.
//
// Two checks run. The goroutineleak profile reports goroutines blocked
// forever on a channel or lock that no live goroutine can reach, at any
// time and whatever else runs. The goroutine count catches the leaks the
// profile cannot see (a goroutine still reachable, such as one in a loop
// over a ticker), by comparison with the goroutines alive before.
package leakcheck

import (
	"bytes"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"testing"
	"time"
)

// grace is how long the goroutines of a finished test may take to end.
const grace = 5 * time.Second

// Check records the goroutines alive now and, at the test's end, fails
// the test if others are still alive after a grace period, or if the
// goroutineleak profile holds a goroutine not reported before.
func Check(t testing.TB) {
	t.Helper()
	before := snapshot()
	t.Cleanup(func() {
		if extra := settle(before); len(extra) > 0 {
			t.Errorf("%d goroutines leaked:\n%s", len(extra), strings.Join(extra, "\n\n"))
		}
		if leaked := newLeaks(before); leaked != "" {
			t.Errorf("goroutineleak profile:\n%s", leaked)
		}
	})
}

// Main runs the tests of m and then fails the binary, printing the stacks,
// if the goroutineleak profile holds a goroutine or goroutines started by
// the tests are still alive after a grace period. A package's TestMain is
//
//	func TestMain(m *testing.M) { leakcheck.Main(m) }
func Main(m *testing.M) {
	before := snapshot()
	code := m.Run()
	if code == 0 {
		if extra := settle(before); len(extra) > 0 {
			_, _ = fmt.Fprintf(os.Stderr, "leakcheck: %d goroutines alive after the tests:\n%s\n", len(extra), strings.Join(extra, "\n\n"))
			code = 1
		}
		if leaked := newLeaks(nil); leaked != "" {
			_, _ = fmt.Fprintf(os.Stderr, "leakcheck: goroutineleak profile:\n%s\n", leaked)
			code = 1
		}
	}
	os.Exit(code)
}

// settle waits up to grace for the goroutines not in before to end and
// returns the stacks of those still alive.
func settle(before map[string]string) []string {
	deadline := time.Now().Add(grace)
	var extra []string
	for {
		extra = extra[:0]
		for id, stack := range snapshot() {
			if _, ok := before[id]; !ok && !ignored(stack) {
				extra = append(extra, stack)
			}
		}
		if len(extra) == 0 || time.Now().After(deadline) {
			return extra
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// reported holds the goroutines newLeaks reported: a leaked goroutine
// stays leaked, and only the first check to see it fails.
var reported struct {
	ids map[string]bool
	sync.Mutex
}

// newLeaks returns the stacks of the goroutines in the goroutineleak
// profile that are not in before and that no earlier call returned, or "".
func newLeaks(before map[string]string) string {
	p := pprof.Lookup("goroutineleak")
	if p == nil {
		return ""
	}
	var buf bytes.Buffer
	// debug 2 runs the leak-detecting GC and prints every goroutine; the
	// leaked ones are marked "(leaked)" in their status.
	if err := p.WriteTo(&buf, 2); err != nil {
		return "write: " + err.Error()
	}
	reported.Lock()
	defer reported.Unlock()
	if reported.ids == nil {
		reported.ids = map[string]bool{}
	}
	var out []string
	for g := range strings.SplitSeq(buf.String(), "\n\n") {
		if !strings.HasPrefix(g, "goroutine ") {
			continue
		}
		head, _, _ := strings.Cut(g, "\n")
		if !strings.Contains(head, "(leaked)") {
			continue
		}
		id := strings.Fields(g)[1]
		if _, old := before[id]; !old && !reported.ids[id] {
			reported.ids[id] = true
			out = append(out, g)
		}
	}
	return strings.Join(out, "\n\n")
}

func ignored(stack string) bool {
	for _, s := range []string{"testing.(*T).Run", "testing.tRunner", "runtime.goexit", "signal.signal_recv",
		"testing.(*M).", "runtime/trace", "testing.runFuzzing"} {
		if strings.Contains(firstFrame(stack), s) {
			return true
		}
	}
	return strings.Contains(stack, "testing.tRunner(") && strings.Contains(stack, "testing.(*T).Run(")
}

func firstFrame(stack string) string {
	lines := strings.SplitN(stack, "\n", 3)
	if len(lines) >= 2 {
		return lines[1]
	}
	return stack
}

func snapshot() map[string]string {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	out := map[string]string{}
	for g := range bytes.SplitSeq(buf, []byte("\n\n")) {
		s := string(g)
		if !strings.HasPrefix(s, "goroutine ") {
			continue
		}
		id := strings.Fields(s)[1]
		out[id] = s
	}
	return out
}
