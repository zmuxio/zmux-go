package core

import (
	"bytes"
	"errors"
	"slices"
	"testing"

	"github.com/zmuxio/zmux-go/internal/wire"
	"github.com/zmuxio/zmux-go/varint"
)

// pollKinds runs a few output rounds and returns every frame kind sent.
func pollKinds(t *testing.T, s *Session, p *Pipe, rounds int) []wire.Kind {
	t.Helper()
	var all []wire.Kind
	now := Time(Second)
	for range rounds {
		b := GetBatch()
		s.PollTransmit(p.id, 0, b, DefaultBatch, now)
		for _, r := range batchFrames(t, b) {
			all = append(all, r...)
		}
		s.WriteDone(b, b.Len(), nil, now)
		PutBatch(b)
	}
	return all
}

// A stream that got its identifier but no opening STREAM frame (no
// session credit) sends its reset at once: the frame carries the right ID
// and opens the stream at the peer.
func TestResetAfterAllocWithoutOpen(t *testing.T) {
	s, p := channelSession(t, nil, false)
	s.feat.Preamble = false
	s.peer.MaxStreamDataBidiRem = 1 << 20
	s.loc[0].limit = 10
	s.conn.sendMax = 0
	st, _ := s.OpenStream(true)
	_, _ = s.Write(st, []byte("data"), false)
	pollKinds(t, s, p, 2)
	if !st.idSet || st.opened {
		t.Fatalf("idSet=%v opened=%v", st.idSet, st.opened)
	}
	s.Reset(st, 9)
	if k := pollKinds(t, s, p, 2); !slices.Contains(k, wire.KindResetStream) {
		t.Fatalf("no RESET_STREAM: %v", k)
	}
	if st.ctl != 0 {
		t.Fatalf("control bits left: %b", st.ctl)
	}
}

// A preamble that does not fit the stream credit is reported by
// STREAM_DATA_BLOCKED at once: the opening frame needs that
// credit, so the report cannot wait for it.
func TestPreambleBlockedReportsAtOnce(t *testing.T) {
	s, p := channelSession(t, nil, false)
	s.feat.Preamble = true
	s.peer.MaxStreamDataBidiRem = 1
	s.loc[0].limit = 10
	s.conn.sendMax = 1 << 20
	st, _ := s.OpenStream(true)
	_, _ = s.Write(st, []byte("data"), false)
	if k := pollKinds(t, s, p, 3); !slices.Contains(k, wire.KindStreamDataBlocked) {
		t.Fatalf("no STREAM_DATA_BLOCKED: %v (idSet=%v ctl=%b)", k, st.idSet, st.ctl)
	}
}

// A deferred PSK supplied before establishment is dropped when the
// negotiation does not select it: SetJoinSecret then reports
// ErrNotNegotiated rather than ErrJoinSecretSet.
func TestJoinSecretEarlyNotSelected(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DeferredPSK = true
	s := New(cfg, 0)
	psk := []byte("0123456789abcdef")
	if err := s.SetJoinSecret(psk); err != nil {
		t.Fatal(err)
	}
	held := s.cfg.PSK
	peer := DefaultConfig().Local
	if err := s.processPeerParams(&peer); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(held, make([]byte, len(held))) {
		t.Fatal("dropped PSK not cleared")
	}
	if s.secretSet || s.cfg.PSK != nil || s.JoinPendingNow() {
		t.Fatalf("secret kept: set=%v pending=%v", s.secretSet, s.JoinPendingNow())
	}
	if err := s.SetJoinSecret(psk); !errors.Is(err, ErrNotNegotiated) {
		t.Fatalf("SetJoinSecret after establishment: %v", err)
	}
}

// streamFrames returns the STREAM frames of a batch.
func streamFrames(t *testing.T, b *Batch) []wire.Frame {
	t.Helper()
	var all []byte
	for _, x := range b.Bufs() {
		all = append(all, x...)
	}
	var out []wire.Frame
	for len(all) > 0 {
		size, n := varint.ConsumeVarint(all)
		rec := all[n : n+int(size)]
		all = all[n+int(size):]
		for len(rec) > 0 {
			var f wire.Frame
			l, err := wire.DecodeFrame(rec, &f)
			if err != nil {
				t.Fatal(err)
			}
			if f.Kind == wire.KindStream {
				out = append(out, f)
			}
			rec = rec[l:]
		}
	}
	return out
}

func pollStream(t *testing.T, s *Session, p *Pipe) []wire.Frame {
	t.Helper()
	b := GetBatch()
	defer PutBatch(b)
	now := Time(Second)
	s.PollTransmit(p.id, 0, b, DefaultBatch, now)
	fs := streamFrames(t, b)
	s.WriteDone(b, b.Len(), nil, now)
	return fs
}

// Data written before Flush goes in the opening frame; data written
// after it in a later one.
func TestFlushFrames(t *testing.T) {
	for _, before := range []bool{true, false} {
		s, p := channelSession(t, nil, false)
		s.peer.MaxStreamDataBidiRem = 1 << 20
		s.loc[0].limit = 10
		s.conn.sendMax = 1 << 20
		st, _ := s.OpenStream(true)
		if before {
			_, _ = s.Write(st, []byte("data"), false)
		}
		target, err := s.Flush(st)
		if err != nil {
			t.Fatal(err)
		}
		fs := pollStream(t, s, p)
		if done, _ := s.FlushDone(st, target); !done {
			t.Fatal("Flush not done after the opening frame")
		}
		if !before {
			_, _ = s.Write(st, []byte("data"), false)
			fs = append(fs, pollStream(t, s, p)...)
		}
		want := 1
		if !before {
			want = 2
		}
		if len(fs) != want || fs[0].Offset != 0 || string(fs[len(fs)-1].Data) != "data" {
			t.Fatalf("before=%v: frames %+v", before, fs)
		}
		// A STOP_SENDING after Flush carries the stream's ID.
		s.Stop(st, 3)
		b := GetBatch()
		s.PollTransmit(p.id, 0, b, DefaultBatch, Time(Second))
		var all []byte
		for _, x := range b.Bufs() {
			all = append(all, x...)
		}
		size, n := varint.ConsumeVarint(all)
		rec := all[n : n+int(size)]
		var f wire.Frame
		if _, err := wire.DecodeFrame(rec, &f); err != nil || f.Kind != wire.KindStopSending || f.StreamID != st.id {
			t.Fatalf("frame %+v, stream %d, %v", f, st.id, err)
		}
		s.WriteDone(b, b.Len(), nil, Time(Second))
		PutBatch(b)
	}
}

// After an early-data restart, a preamble larger than the stream credit
// of the peer's new parameters is reported by STREAM_DATA_BLOCKED at
// once rather than held for the opening STREAM frame it blocks
// (streams open in identifier order); the peer's credit then lets the stream finish.
func TestRestartPreambleBlocked(t *testing.T) {
	s, p := channelSession(t, nil, false)
	s.feat.Preamble = true
	s.peer.MaxStreamDataBidiRem = 1 << 20
	s.loc[0].limit = 10
	s.conn.sendMax = 1 << 20
	s.earlyHold = &earlyHold{} // carrier early data: the bytes stay for a re-issue
	st, _ := s.OpenStream(true)
	if err := s.SetOpenInfo(st, make([]byte, 16)); err != nil {
		t.Fatal(err)
	}
	_, _ = s.Write(st, []byte("data"), true)
	pollKinds(t, s, p, 2)
	if !st.opened {
		t.Fatal("not opened before the restart")
	}
	s.peerSeen = false
	if !s.RestartEarly(CarrierInfo{}, nil, false, Time(Second)) {
		t.Fatal("RestartEarly refused")
	}
	if k := pollKinds(t, s, p, 2); slices.Contains(k, wire.KindStream) {
		t.Fatalf("stream re-issued before the peer's parameters: %v", k)
	}
	peer := s.cfg.Local
	peer.Preamble = 16
	peer.MaxStreamDataBidiRem = 16
	record := func(f []byte) {
		receiveTest(s, p.id, 0, append(varint.AppendVarint(nil, uint64(len(f))), f...), Time(Second))
	}
	enc, _ := wire.AppendParams(nil, &peer)
	record(wire.AppendTransportParams(nil, enc))
	if !s.peerSeen || s.state != StateActive {
		t.Fatalf("peer parameters not taken: state=%v %v", s.state, s.err)
	}
	s.loc[0].limit = 10
	k := pollKinds(t, s, p, 3)
	if !slices.Contains(k, wire.KindStreamDataBlocked) || slices.Contains(k, wire.KindStream) {
		t.Fatalf("after the restart: %v (opened=%v ctl=%b)", k, st.opened, st.ctl)
	}
	record(wire.AppendMaxStreamData(nil, st.id, 1<<20))
	fs := pollStream(t, s, p)
	if len(fs) == 0 || !fs[len(fs)-1].Fin || !st.opened {
		t.Fatalf("after the credit: %+v (opened=%v)", fs, st.opened)
	}
}

// A STOP_SENDING that arrives after the opening frame and the data were sent
// does not fail the Flush that waited for them; the next Write reports it.
// A stop before anything was sent still fails the Flush.
func TestFlushDoneStopAfterSent(t *testing.T) {
	for _, sentFirst := range []bool{true, false} {
		s, p := channelSession(t, nil, false)
		s.peer.MaxStreamDataBidiRem = 1 << 20
		s.loc[0].limit = 10
		s.conn.sendMax = 1 << 20
		st, _ := s.OpenStream(true)
		_, _ = s.Write(st, []byte("data"), false)
		target, err := s.Flush(st)
		if err != nil {
			t.Fatal(err)
		}
		if sentFirst {
			pollStream(t, s, p)
		}
		s.onStopSending(st, 7)
		done, err := s.FlushDone(st, target)
		if !done {
			t.Fatalf("sentFirst=%v: not done", sentFirst)
		}
		if sentFirst && err != nil {
			t.Fatalf("Flush after the data was sent: %v", err)
		}
		if !sentFirst && err == nil {
			t.Fatal("stop before the send did not fail the Flush")
		}
		if _, err := s.Write(st, []byte("more"), false); err == nil {
			t.Fatalf("sentFirst=%v: Write after the stop succeeded", sentFirst)
		}
	}
}

// A STOP_SENDING that arrives before Flush is called, after everything
// written was sent: Flush succeeds, the next Write reports the stop.
func TestFlushStopBeforeFlushAfterSent(t *testing.T) {
	s, p := channelSession(t, nil, false)
	s.peer.MaxStreamDataBidiRem = 1 << 20
	s.loc[0].limit = 10
	s.conn.sendMax = 1 << 20
	st, _ := s.OpenStream(true)
	_, _ = s.Write(st, []byte("data"), false)
	pollStream(t, s, p)
	s.onStopSending(st, 7)
	target, err := s.Flush(st)
	if err != nil {
		t.Fatalf("Flush after the data was sent: %v", err)
	}
	if done, err := s.FlushDone(st, target); !done || err != nil {
		t.Fatalf("FlushDone: %v %v", done, err)
	}
	if _, err := s.Write(st, []byte("more"), false); err == nil {
		t.Fatal("Write after the stop succeeded")
	}
}

// A PRIORITY_UPDATE written on a rejected early carrier is queued again
// by the restart.
func TestRestartEarlyRequeuesPriority(t *testing.T) {
	s, p := channelSession(t, nil, false)
	s.feat.Priority = true
	s.peer.MaxStreamDataBidiRem = 1 << 20
	s.loc[0].limit = 10
	s.conn.sendMax = 1 << 20
	s.earlyHold = &earlyHold{}
	st, _ := s.OpenStream(true)
	_, _ = s.Write(st, []byte("data"), false)
	pollKinds(t, s, p, 2)
	if err := s.SetPeerPriority(st, 2, false); err != nil {
		t.Fatal(err)
	}
	if k := pollKinds(t, s, p, 2); !slices.Contains(k, wire.KindPriorityUpdate) || st.ctl&ctlPrio != 0 {
		t.Fatalf("PRIORITY_UPDATE not written: %v", k)
	}
	s.peerSeen = false
	if !s.RestartEarly(CarrierInfo{}, nil, false, Time(Second)) {
		t.Fatal("RestartEarly refused")
	}
	if st.ctl&ctlPrio == 0 {
		t.Fatal("PRIORITY_UPDATE of the early carrier not queued again")
	}
}

// A local CancelWrite after everything written was sent fails the Flush
// that follows it.
func TestFlushAfterLocalCancelWrite(t *testing.T) {
	s, p := channelSession(t, nil, false)
	s.peer.MaxStreamDataBidiRem = 1 << 20
	s.loc[0].limit = 10
	s.conn.sendMax = 1 << 20
	st, _ := s.OpenStream(true)
	_, _ = s.Write(st, []byte("data"), false)
	pollStream(t, s, p)
	s.Reset(st, 3)
	_, err := s.Flush(st)
	var se *StreamError
	if !errors.As(err, &se) || se.Kind != StreamClosedLocally {
		t.Fatalf("Flush after CancelWrite: %v", err)
	}
}

// TestRestartEarlyChecksAssumed: early sends re-issued after a rejected
// carrier keep the identifiers and preambles of the set they assumed, so
// peer parameters below it end the session locally, as without a restart.
func TestRestartEarlyChecksAssumed(t *testing.T) {
	s, _ := channelSession(t, nil, false)
	s.earlyHold = &earlyHold{check: true, assumed: wire.Params{MaxStreamsBidi: 100}}
	s.peer = wire.Params{MaxStreamsBidi: 10}
	if err := s.earlyOutcome(); err == nil {
		t.Fatal("peer parameters below the assumed set taken after a restart")
	}
	s.earlyHold = &earlyHold{check: true, assumed: wire.Params{MaxStreamsBidi: 10}}
	if err := s.earlyOutcome(); err != nil || s.earlyHold != nil {
		t.Fatalf("parameters that admit the assumed set: %v", err)
	}
}
