package zmux

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"regexp"
	"runtime/pprof"
	"testing"
	"testing/synctest"
	"time"

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
	gate := &writerGate{Conn: a}
	client, server := pair(t, gate, b, &Config{ClockDomain: NewClockDomain(timeshim.Real)}, &Config{ClockDomain: NewClockDomain(timeshim.Real)})
	synctest.Wait()
	profile := func() bytes.Buffer {
		var buf bytes.Buffer
		if err := pprof.Lookup("goroutine").WriteTo(&buf, 1); err != nil {
			t.Fatal(err)
		}
		return buf
	}
	count := func(buf bytes.Buffer, task string, want int) {
		t.Helper()
		re := regexp.MustCompile(fmt.Sprintf(`# labels: \{"zmux":%q, "zmux.pipe":"0", "zmux.role":"(initiator|responder)", "zmux.session":"(%d|%d)"\}`,
			task, client.id, server.id))
		if n := len(re.FindAll(buf.Bytes(), -1)); n != want {
			t.Fatalf("%d %s goroutines with the session labels, want %d", n, task, want)
		}
	}
	time.Sleep(2 * writerIdle) // synctest virtual time
	synctest.Wait()
	buf := profile()
	count(buf, "read", 2)
	count(buf, "write", 0)
	gate.arm()
	out, err := client.OpenStreamSync(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = out.Write([]byte{0}); err != nil {
		t.Fatal(err)
	}
	<-gate.entered
	buf = profile()
	close(gate.release)
	count(buf, "write", 1)
	in, err := server.AcceptStream(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for i := range 8 {
		if i > 0 {
			if _, err = out.Write([]byte{byte(i)}); err != nil {
				t.Fatal(err)
			}
		}
		var v [1]byte
		if _, err = io.ReadFull(in, v[:]); err != nil || v[0] != byte(i) {
			t.Fatalf("round %d read %v: %v", i, v, err)
		}
		synctest.Wait()
		time.Sleep(2 * writerIdle)
		synctest.Wait()
		count(profile(), "write", 0)
	}
	if gate.overlap.Load() {
		t.Fatal("concurrent writes on lane zero")
	}
	testSession := regexp.MustCompile(fmt.Sprintf(`# labels: \{[^}]*"zmux.session":"(%d|%d)"[^}]*}`, client.id, server.id))
	for _, m := range testSession.FindAll(buf.Bytes(), -1) {
		if !regexp.MustCompile(`^# labels: \{("zmux(\.[a-z]+)?":"[a-z0-9-]+"(, )?)+}$`).Match(m) {
			t.Errorf("unexpected labels %s", m)
		}
	}
}

// A carrier fed to a Listener is read on the caller's goroutine, with no
// goroutine of its own, and the listener's own goroutine takes none of the
// caller's labels.
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
		if bytes.Contains(buf.Bytes(), []byte(`"zmux":"first-record"`)) {
			t.Errorf("a goroutine of its own reads the first record:\n%s", buf.Bytes())
		}
		if bytes.Contains(buf.Bytes(), []byte(`"app.req":"42", "zmux":"listener-budget"`)) {
			t.Error("the listener's own goroutine took a caller's labels")
		}
	})
}

// BenchmarkGoTask is the cost of starting a goroutine with labels.Go (its
// label set and SetGoroutineLabels) against a bare go statement.
func BenchmarkGoTask(b *testing.B) {
	base := sessionLabels(nextSessionID(), 0)
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
