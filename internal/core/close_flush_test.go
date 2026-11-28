package core

import (
	"testing"

	"github.com/zmuxio/zmux-go/internal/wire"
)

// A graceful close sends what the streams wrote before CONNECTION_CLOSE: a
// RESET_STREAM still queued counts, or the close discards it and the peer
// never learns of the reset.
func TestFlushableQueuedReset(t *testing.T) {
	s := capacitySession()
	if err := onStreamTest(s, &wire.Frame{Kind: wire.KindStream, StreamID: 1, Data: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	st, ok := s.AcceptStream(true)
	if !ok {
		t.Fatal("no stream to accept")
	}
	if s.Flushable() {
		t.Fatal("flushable before anything was written")
	}
	s.Reset(st, 9)
	if !s.Flushable() {
		t.Fatal("a queued RESET_STREAM is not flushed before the close")
	}
	st.snd.resetSent = true
	if s.Flushable() {
		t.Fatal("flushable after the reset was sent")
	}
}
