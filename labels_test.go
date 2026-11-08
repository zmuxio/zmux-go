package zmux

import (
	"bytes"
	"context"
	"net"
	"regexp"
	"runtime/pprof"
	"testing"
	"testing/synctest"

	"github.com/djylb/timeshim"
	"github.com/zmuxio/zmux-go/internal/labels"
)

// The goroutines of a session carry its labels: the task, the role, the
// session's sequence number and the pipe, and nothing else.
func TestGoroutineLabels(t *testing.T) {
	synctest.Test(t, testGoroutineLabels)
}

func testGoroutineLabels(t *testing.T) {
	a, b := net.Pipe()
	pair(t, a, b, &Config{Clock: timeshim.Real}, &Config{Clock: timeshim.Real})
	var buf bytes.Buffer
	if err := pprof.Lookup("goroutine").WriteTo(&buf, 1); err != nil {
		t.Fatal(err)
	}
	for _, task := range []string{"read", "write"} {
		re := regexp.MustCompile(`# labels: \{"zmux":"` + task + `", "zmux.pipe":"0", "zmux.role":"(initiator|responder)", "zmux.session":"[0-9]+"\}`)
		if n := len(re.FindAll(buf.Bytes(), -1)); n != 2 {
			t.Errorf("%d %s goroutines with the session labels, want 2", n, task)
		}
	}
	for _, m := range regexp.MustCompile(`# labels: \{[^}]*\}`).FindAll(buf.Bytes(), -1) {
		if !regexp.MustCompile(`^# labels: \{("zmux(\.[a-z]+)?":"[a-z0-9-]+"(, )?)+\}$`).Match(m) {
			t.Errorf("unexpected labels %s", m)
		}
	}
}

// A goroutine working for one call keeps the caller's labels under its
// own: the first-record read of a carrier fed to a Listener with a
// labelled context.
func TestGoroutineLabelsKeepCaller(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := mustNewListener(nil)
		defer func() { _ = l.Close() }()
		a, b := net.Pipe()
		defer func() { _ = b.Close() }()
		ctx, cancel := context.WithCancel(pprof.WithLabels(context.Background(), pprof.Labels("app.req", "42")))
		done := make(chan struct{})
		go func() { _ = l.HandlePipe(ctx, NetPipe(a), PipeHint{}); close(done) }()
		synctest.Wait()
		var buf bytes.Buffer
		if err := pprof.Lookup("goroutine").WriteTo(&buf, 1); err != nil {
			t.Fatal(err)
		}
		cancel()
		<-done
		if !bytes.Contains(buf.Bytes(), []byte(`"app.req":"42", "zmux":"first-record"`)) {
			t.Errorf("the first-record goroutine lost the caller's labels:\n%s", buf.Bytes())
		}
		if bytes.Contains(buf.Bytes(), []byte(`"app.req":"42", "zmux":"listener-budget"`)) {
			t.Error("the listener's own goroutine took a caller's labels")
		}
	})
}

// BenchmarkGoTask is the cost of starting a goroutine with labels.Go (its
// label set and SetGoroutineLabels) against a bare go statement.
func BenchmarkGoTask(b *testing.B) {
	base := sessionLabels(0)
	done := make(chan struct{}, 1)
	f := func() { done <- struct{}{} }
	b.Run("go", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			go f()
			<-done
		}
	})
	b.Run("labels.Go", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			labels.Go(base, "read", f)
			<-done
		}
	})
}
