package zmuxquic

import (
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/varint"
)

// unknownJoin is a byte-stream record holding a PIPE_JOIN whose locator
// starts with id.
func unknownJoin(id []byte) []byte {
	loc := make([]byte, 16)
	_, _ = rand.Read(loc[copy(loc, id):])
	f := varint.AppendVarint(nil, 0x1a24de36) // PIPE_JOIN
	f = append(f, byte(len(loc)))
	f = append(f, loc...)
	f = append(f, 1, 2, 0, 0) // pipe 1, receive-only, no fate group, priority 0
	f = append(f, make([]byte, 16)...)
	return append(varint.AppendVarint(nil, uint64(len(f))), f...)
}

// TestLaneUnknownJoin: a lane (a unidirectional stream, no sending half)
// carrying an unknown join is closed without an answer and counted as
// rejected, with AnswerUnknownJoins on.
func TestLaneUnknownJoin(t *testing.T) {
	id := []byte("inst")
	srv, cli := tlsConfigs(t)
	ln, err := quic.ListenAddrEarly("127.0.0.1:0", srv, testQUIC(&quic.Config{}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	lst := zmux.NewListener(&zmux.ListenerConfig{Config: mpConfig(), InstanceID: id, AnswerUnknownJoins: true})
	defer func() { _ = lst.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go func() { _ = Serve(ctx, ln, lst, nil) }()
	qc, err := quic.DialAddr(ctx, ln.Addr().String(), cli, QUICConfig(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = qc.CloseWithError(0, "") }()
	s, err := qc.OpenUniStream()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(unknownJoin(id)); err != nil {
		t.Fatal(err)
	}
	for lst.Stats().Rejected != 1 {
		if ctx.Err() != nil {
			t.Fatalf("stats %+v", lst.Stats())
		}
		time.Sleep(5 * time.Millisecond)
	}
}
