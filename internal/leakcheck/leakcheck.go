// Package leakcheck verifies that a test leaves no goroutines behind (a
// standard-library replacement for goleak).
package leakcheck

import (
	"bytes"
	"runtime"
	"strings"
	"testing"
	"time"
)

// Check records the goroutines alive now and, at the test's end, fails
// the test if others are still alive after a grace period.
func Check(t testing.TB) {
	t.Helper()
	before := snapshot()
	t.Cleanup(func() {
		deadline := time.Now().Add(5 * time.Second)
		var extra []string
		for {
			extra = extra[:0]
			for id, stack := range snapshot() {
				if _, ok := before[id]; !ok && !ignored(stack) {
					extra = append(extra, stack)
				}
			}
			if len(extra) == 0 || time.Now().After(deadline) {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		if len(extra) > 0 {
			t.Errorf("%d goroutines leaked:\n%s", len(extra), strings.Join(extra, "\n\n"))
		}
	})
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
	for _, g := range bytes.Split(buf, []byte("\n\n")) {
		s := string(g)
		if !strings.HasPrefix(s, "goroutine ") {
			continue
		}
		id := strings.Fields(s)[1]
		out[id] = s
	}
	return out
}
