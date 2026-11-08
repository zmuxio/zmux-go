package labels

import (
	"bytes"
	"context"
	"runtime/pprof"
	"testing"
)

// Go keeps the labels of base under the task; a nil base has none.
func TestGo(t *testing.T) {
	for _, tc := range []struct {
		base context.Context
		task string
		want string
	}{
		{nil, "test-nil", `# labels: {"zmux":"test-nil"}`},
		{pprof.WithLabels(context.Background(), pprof.Labels("app", "1")), "test-base", `# labels: {"app":"1", "zmux":"test-base"}`},
	} {
		started, release := make(chan struct{}), make(chan struct{})
		Go(tc.base, tc.task, func() { close(started); <-release })
		<-started
		var buf bytes.Buffer
		err := pprof.Lookup("goroutine").WriteTo(&buf, 1)
		close(release)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Contains(buf.Bytes(), []byte(tc.want)) {
			t.Errorf("no goroutine with %s:\n%s", tc.want, buf.Bytes())
		}
	}
}
