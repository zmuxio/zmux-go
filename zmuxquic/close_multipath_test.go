package zmuxquic

import (
	"bytes"
	"context"
	"io"
	"runtime"
	"testing"

	"time"

	"github.com/zmuxio/zmux-go"
)

// In a multipath session the server writes 256 KiB on a stream and closes
// with a code at once; the client must read all of it before the close.
// Without the wait for acknowledgements about half the runs lost the tail.
func TestMultipathCloseDeliversTail(t *testing.T) {
	const runs = 20
	fails := 0
	for i := range runs {
		if n := multipathCloseOnce(t); n != 256<<10 {
			fails++
			t.Logf("run %d: read %d of %d", i, n, 256<<10)
		}
	}
	if fails > 0 {
		t.Fatalf("%d/%d truncated", fails, runs)
	}
}

func multipathCloseOnce(t *testing.T) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	e := newEnv(t, &zmux.Config{SessionPolicy: zmux.SessionPolicy{Limits: zmux.CapacityLimits{SteadyAttachments: 8}}, PipeAuth: zmux.PipeAuth{PSK: bytes.Repeat([]byte{7}, 32)}})
	client, err := Dial(ctx, e.ln.Addr().String(), e.cli, nil, &zmux.Config{SessionPolicy: zmux.SessionPolicy{Limits: zmux.CapacityLimits{SteadyAttachments: 8}}, PipeAuth: zmux.PipeAuth{PSK: bytes.Repeat([]byte{7}, 32)}})
	if err != nil {
		t.Fatal(err)
	}
	server, err := e.lst.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	got := make(chan int, 1)
	go func() {
		in, err := client.AcceptStream(ctx)
		if err != nil {
			got <- -1
			return
		}
		b, _ := io.ReadAll(in)
		got <- len(b)
	}()
	st, err := server.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Write(bytes.Repeat([]byte("abcd"), 64<<10)); err != nil {
		t.Fatal(err)
	}
	_ = server.CloseWithError(0x10, "")
	n := <-got
	<-client.Done()
	runtime.KeepAlive(st) // a collected stream is reset
	return n
}
