package zmuxquic

import (
	"context"
	"errors"
	"io"
	"runtime"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/zmuxio/zmux-go"
)

func TestQUICDefaultBudgetAdmits64FullWindows(t *testing.T) {
	const sessions = 64
	const window = 1 << 20
	cfg := mpConfig()
	cfg.InitialStreamReceiveWindow = 2 * window
	// Keep inbound records ordered for the FIN marker below.
	cfg.MaxLanes = 1
	e := newEnv(t, cfg)
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	payload := make([]byte, window)
	var clients, servers []*zmux.Conn
	var streams []*zmux.Stream
	t.Cleanup(func() {
		for i, c := range clients {
			c.Abort(0, "")
			if i < len(servers) {
				servers[i].Abort(0, "")
			}
		}
		for _, c := range clients {
			_ = c.Wait()
		}
		for _, s := range servers {
			_ = s.Wait()
		}
	})
	for i := range sessions {
		c, err := Dial(ctx, e.ln.Addr().String(), e.cli, nil, mpConfig())
		if err != nil {
			t.Fatalf("dial session %d: %v", i, err)
		}
		clients = append(clients, c)
		s, err := e.lst.Accept(ctx)
		if err != nil {
			t.Fatalf("accept session %d: %v", i, err)
		}
		servers = append(servers, s)
		st, err := c.OpenStreamSync(ctx)
		if err != nil {
			t.Fatal(err)
		}
		streams = append(streams, st)
		deadline, _ := ctx.Deadline()
		_ = st.SetWriteDeadline(deadline)
		if n, err := st.Write(payload); n != window || err != nil {
			t.Fatalf("session %d wrote %d bytes: %v", i, n, err)
		}
		// Flush hands the stream data to the transport without reading it.
		if err := st.Flush(); err != nil {
			t.Fatalf("session %d flush: %v", i, err)
		}
		// An empty unidirectional stream follows the flushed data on the
		// only carrier. Its FIN lets us observe that data without reading it.
		marker, err := c.OpenUniStreamSync(ctx)
		if err != nil {
			t.Fatalf("session %d open marker: %v", i, err)
		}
		if err := marker.Close(); err != nil {
			t.Fatalf("session %d finish marker: %v", i, err)
		}
		end, err := s.AcceptUniStream(ctx)
		if err != nil {
			t.Fatalf("session %d accept marker: %v", i, err)
		}
		_ = end.SetReadDeadline(deadline)
		if n, err := end.Read(make([]byte, 1)); n != 0 || !errors.Is(err, io.EOF) {
			t.Fatalf("session %d received marker: %d, %v", i, n, err)
		}
		if got := s.ConnectionStats().RecvBuffered; got != window {
			t.Fatalf("session %d retained %d bytes, want %d", i, got, window)
		}
	}
	runtime.KeepAlive(streams)
	used := e.lst.Adapter().MemoryUsed()
	if used <= sessions*window || used > 1<<30 {
		t.Fatalf("listener charged %d bytes", used)
	}
	t.Logf("%d sessions each retained %d bytes, listener charged %d bytes", sessions, window, used)
	path, err := quic.DialAddr(ctx, e.ln.Addr().String(), e.cli, testQUIC(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = path.CloseWithError(0, "") }()
	pipe, err := ConnPipe(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := clients[0].AddPipe(ctx, pipe, zmux.PipeOptions{}); err != nil {
		t.Fatalf("Join with all windows full: %v", err)
	}
}
