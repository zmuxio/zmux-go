package zmuxws

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go"
)

// TestCloseDelivery (N16) over WebSocket: the peer reads the whole 1 MiB
// written before a coded close and ends with the code, the wait confirms
// it before the linger timeout, and the stream left open fails with the
// session's error; the plain close gives code 0 and an empty reason.
func TestCloseDelivery(t *testing.T) {
	for _, c := range []struct {
		code   zmux.ApplicationErrorCode
		reason string
	}{{0x2a, "bye"}, {0, ""}} {
		t.Run(fmt.Sprintf("%#x", uint64(c.code)), func(t *testing.T) {
			ctx := testContext(t)
			l := mustNewListener(nil)
			u := server(t, l, nil)
			client, err := Dial(ctx, u+"/zmux", nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			srv, err := l.Accept(ctx)
			if err != nil {
				t.Fatal(err)
			}
			msg := make([]byte, 1<<20)
			for i := range msg {
				msg[i] = byte(i * 7)
			}
			read := make(chan error, 1)
			go func() {
				in, err := srv.AcceptStream(ctx)
				if err == nil {
					var got []byte
					got, err = io.ReadAll(in)
					if err == nil && !bytes.Equal(got, msg) {
						err = fmt.Errorf("read %d bytes, not the 1 MiB written", len(got))
					}
				}
				read <- err
			}()
			st, err := client.OpenStreamSync(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := st.Write(msg); err != nil {
				t.Fatal(err)
			}
			_ = st.Close()
			open, err := client.OpenStreamSync(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := open.Write([]byte("left open")); err != nil {
				t.Fatal(err)
			}
			t0 := time.Now()
			_ = client.CloseWithError(c.code, c.reason)
			if d := time.Since(t0); d > time.Second {
				t.Fatalf("the close took %v", d)
			}
			t1 := time.Now()
			select {
			case <-client.Done():
			case <-time.After(5 * time.Second):
				t.Fatal("the close was neither delivered nor given up within 5 s")
			}
			if d := time.Since(t1); d >= 250*time.Millisecond {
				t.Fatalf("the wait took %v: the linger timeout, not the peer's end", d)
			}
			if err := <-read; err != nil {
				t.Fatalf("the peer's read: %v", err)
			}
			var ae *zmux.ApplicationError
			if err := srv.Wait(); !errors.As(err, &ae) || ae.ErrorCode != c.code || ae.ErrorMessage != c.reason || !ae.Remote {
				t.Fatalf("the peer's session ended with %v", err)
			}
			if _, err := open.Write([]byte("x")); !errors.As(err, &ae) || ae.ErrorCode != c.code || ae.Remote {
				t.Fatalf("the open stream after the close: %v", err)
			}
		})
	}
}
