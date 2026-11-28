package zmuxws

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/zmuxio/zmux-go"
)

// TestCloseDelivery (N16) checks delivery of the tail before a coded close,
// and the error reported by a stream that was still open.
func TestCloseDelivery(t *testing.T) {
	for _, c := range []struct {
		reason string
		code   zmux.ApplicationErrorCode
	}{{code: 0x2a, reason: "bye"}, {code: 0, reason: ""}} {
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
			const prefix = 4 << 10
			msg := make([]byte, 2*prefix)
			for i := range msg {
				msg[i] = byte(i * 7)
			}
			read, accepted := make(chan error, 1), make(chan error, 1)
			go func() {
				in, err := srv.AcceptStream(ctx)
				got := make([]byte, prefix)
				if err == nil {
					_, err = io.ReadFull(in, got)
				}
				accepted <- err
				if err != nil {
					return
				}
				tail, err := io.ReadAll(in)
				if err == nil && (!bytes.Equal(got, msg[:prefix]) || !bytes.Equal(tail, msg[prefix:])) {
					err = fmt.Errorf("read %d of %d expected bytes", len(got)+len(tail), len(msg))
				}
				read <- err
			}()
			st, err := client.OpenStreamSync(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := st.Write(msg[:prefix]); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-accepted:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if _, err := st.Write(msg[prefix:]); err != nil {
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
			_ = client.CloseWithError(c.code, c.reason)
			select {
			case err := <-read:
				if err != nil {
					t.Fatalf("the peer's read: %v", err)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			for _, conn := range []*zmux.Conn{srv, client} {
				select {
				case <-conn.Done():
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
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
