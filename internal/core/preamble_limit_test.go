package core

import (
	"bytes"
	"errors"
	"testing"

	"github.com/zmuxio/zmux-go/internal/wire"
)

// preambleSession is an established session with the preamble negotiated
// and a peer limit of lim.
func preambleSession(t *testing.T, lim uint64) (*Session, *Pipe) {
	t.Helper()
	s, p := channelSession(t, nil, false)
	s.feat.Preamble = true
	s.peer.Has |= wire.HasPreamble
	s.peer.Preamble = lim
	s.peer.MaxStreamDataBidiRem = 1 << 20
	s.loc[0].limit = 10
	s.conn.sendMax = 1 << 20
	s.greaseFirst = false
	return s, p
}

func wantPreambleErr(t *testing.T, err error, lim uint64) {
	t.Helper()
	var se *StreamError
	if !errors.As(err, &se) || se.Kind != StreamPreambleTooLarge || se.Code != lim {
		t.Fatalf("err = %v, want preamble too large (max %d)", err, lim)
	}
}

// Setters refuse a preamble above the peer's limit and keep the old one.
func TestPreambleSetterLimit(t *testing.T) {
	s, _ := preambleSession(t, 100)
	st, err := s.OpenStream(true)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetOpenInfo(st, []byte("small")); err != nil {
		t.Fatal(err)
	}
	wantPreambleErr(t, s.SetOpenInfo(st, make([]byte, 100)), 100)
	wantPreambleErr(t, s.AddPreambleTLV(st, 0x1000, make([]byte, 95)), 100)
	if !bytes.Equal(st.pre.OpenInfo, []byte("small")) || len(st.pre.App) != 0 {
		t.Fatalf("preamble changed: %+v", st.pre)
	}
	// Exactly at the limit fits.
	for v := 100; s.AddPreambleTLV(st, 0x1000, make([]byte, v)) != nil; v-- {
	}
	if st.preSize() != 100 {
		t.Fatalf("preamble of %d bytes, want 100", st.preSize())
	}
	wantPreambleErr(t, s.SetPeerPriority(st, 1, true), 100)
}

// Before the peer's parameters are known the floor applies.
func TestPreambleSetterFloor(t *testing.T) {
	s := New(DefaultConfig(), 0)
	st, err := s.OpenStream(true)
	if err != nil {
		t.Fatal(err)
	}
	wantPreambleErr(t, s.SetOpenInfo(st, make([]byte, floorPreamble)), floorPreamble)
	if err := s.SetOpenInfo(st, make([]byte, floorPreamble-5)); err != nil {
		t.Fatal(err)
	}
}

// A limit that shrinks before the stream is announced fails only that
// stream, locally: nothing of it is sent and the session goes on.
func TestPreambleLimitShrinks(t *testing.T) {
	s, p := preambleSession(t, 4096)
	big, err := s.OpenStream(true)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetOpenInfo(big, make([]byte, 2000)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(big, []byte("lost"), false); err != nil {
		t.Fatal(err)
	}
	s.peer.Preamble = 1024
	b := GetBatch()
	defer PutBatch(b)
	if s.PollTransmit(p.id, 0, b, DefaultBatch, Time(Second)) {
		if n := sum(recordData(t, b)); n > 0 {
			t.Fatalf("%d stream bytes sent", n)
		}
		s.WriteDone(b, nil, Time(Second))
	}
	if big.idSet {
		t.Fatal("identifier allocated")
	}
	_, err = s.Write(big, []byte("x"), false)
	wantPreambleErr(t, err, 1024)
	if _, err := s.PinID(big); err == nil {
		t.Fatal("PinID succeeded")
	}
	ok, err := s.OpenStream(true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(ok, []byte("data"), false); err != nil {
		t.Fatal(err)
	}
	if !s.PollTransmit(p.id, 0, b, DefaultBatch, Time(Second)) || sum(recordData(t, b)) == 0 {
		t.Fatal("the next stream sent nothing")
	}
	if s.err != nil {
		t.Fatalf("session failed: %v", s.err)
	}
}

// The grease TLV is left to a later stream when it would not fit.
func TestPreambleGreaseFits(t *testing.T) {
	s, _ := preambleSession(t, 64)
	s.greaseFirst = true
	st, err := s.OpenStream(true)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetOpenInfo(st, make([]byte, 64-3)); err != nil {
		t.Fatal(err)
	}
	if !s.allocID(st) {
		t.Fatal("allocID failed")
	}
	if st.snd.preLen != 64 || !s.greaseFirst {
		t.Fatalf("preamble %d bytes, grease pending %v", st.snd.preLen, s.greaseFirst)
	}
}
