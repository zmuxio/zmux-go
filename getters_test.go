package zmux

import (
	"context"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
	"github.com/zmuxio/zmux-go/shaping"
)

// isScheduler reports whether s is the built-in policy want; Schedulers of
// custom policies are not comparable, so only built-in ones are compared.
func isScheduler(s, want Scheduler) bool {
	b, ok := s.(builtin)
	w, wok := want.(builtin)
	return ok && wok && b == w
}

// Every runtime setting of a session reads back as set, and keeps its last
// value once the session has ended.
func TestSessionSettingGetters(t *testing.T) {
	leakcheck.Check(t)
	e := newMPEnv(t, mpConfig(), mpConfig())
	client := e.client
	if !isScheduler(client.Scheduler(), SchedulerAffinity()) {
		t.Fatalf("default scheduler %T", client.Scheduler())
	}
	client.SetScheduler(SchedulerSpreadETA())
	band := QualityBand{RTT: 40 * time.Millisecond, RateRatio: 2, MinInterval: time.Second}
	floor := CapacityFloor{Rate: 1 << 20, RecoveryRatio: 3}
	limits := shaping.Limits{MaxDelay: 3 * time.Second, NoPause: true}
	live := Liveness{KeepAlivePeriod: 20 * time.Second}
	if err := client.SetQualityBand(band); err != nil {
		t.Fatal(err)
	}
	if err := client.SetCapacityFloor(floor); err != nil {
		t.Fatal(err)
	}
	if err := client.SetShapingLimits(limits); err != nil {
		t.Fatal(err)
	}
	if err := client.SetLiveness(live); err != nil {
		t.Fatal(err)
	}
	check := func(when string) {
		t.Helper()
		if !isScheduler(client.Scheduler(), SchedulerSpreadETA()) || client.QualityBand() != band ||
			client.CapacityFloor() != floor || client.ShapingLimits() != limits ||
			client.Liveness().KeepAlivePeriod != live.KeepAlivePeriod {
			t.Fatalf("%s: scheduler %T band %+v floor %+v limits %+v liveness %+v",
				when, client.Scheduler(), client.QualityBand(), client.CapacityFloor(), client.ShapingLimits(), client.Liveness())
		}
	}
	check("open")
	// Unset readings: the zero value of a band or floor that was cleared.
	if err := client.SetQualityBand(QualityBand{}); err != nil {
		t.Fatal(err)
	}
	if client.QualityBand() != (QualityBand{}) {
		t.Fatalf("cleared band reads %+v", client.QualityBand())
	}
	if err := client.SetQualityBand(band); err != nil {
		t.Fatal(err)
	}
	_ = client.Close()
	<-client.Done()
	check("closed")
}

type noPick struct{}

func (noPick) Pick([]PickRequest, []PickPipe, []PickAnswer) {}

// A custom policy reads back as a Scheduler of its own that is not a
// built-in one.
func TestSchedulerGetterCustom(t *testing.T) {
	leakcheck.Check(t)
	e := newMPEnv(t, mpConfig(), mpConfig())
	client := e.client
	client.SetScheduler(SchedulerCustom(noPick{}))
	s := client.Scheduler()
	if _, ok := s.(customPolicy); !ok {
		t.Fatalf("custom policy reads as %T", s)
	}
	client.SetScheduler(nil) // the session has no unset: the default policy
	if !isScheduler(client.Scheduler(), SchedulerAffinity()) {
		t.Fatalf("nil scheduler reads as %T", client.Scheduler())
	}
}

// The settings of a stream read back; a stream's scheduler goes back to
// following the session with the zero Scheduler, and a following stream reads
// as the zero Scheduler, never as the session's policy.
func TestStreamSettingGettersAll(t *testing.T) {
	leakcheck.Check(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cfg := mpConfig()
	cfg.SendBuffer = 256 << 10
	e := newMPEnv(t, cfg, mpConfig())
	client, server := e.client, e.server
	st, err := client.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if u, inc := st.Priority(); u != UrgencyDefault || !inc {
		t.Fatalf("default priority %d %v", u, inc)
	}
	if st.Scheduler() != nil {
		t.Fatalf("a stream that follows the session reads %T", st.Scheduler())
	}
	if on, set := st.Redundant(); on || set {
		t.Fatalf("default redundancy %v %v", on, set)
	}
	if n := st.SendBuffer(); n != 256<<10 {
		t.Fatalf("default send buffer %d, the session's configuration is %d", n, 256<<10)
	}
	if _, _, ok := st.PeerPriority(); ok {
		t.Fatal("a peer priority nobody requested")
	}
	initial, limit := st.ReceiveWindow()
	if initial == 0 {
		t.Fatalf("default receive window %d %d", initial, limit)
	}

	st.SetPriority(1, false)
	st.SetScheduler(SchedulerSpreadRoundRobin())
	st.SetRedundant(true)
	st.SetSendBuffer(100 << 10)
	if err := st.SetReceiveWindow(64<<10, 128<<10); err != nil {
		t.Fatal(err)
	}
	if err := st.SetPeerPriority(2, true); err != nil { // before the first frame: the preamble
		t.Fatal(err)
	}
	check := func(when string, st *Stream) {
		t.Helper()
		u, inc := st.Priority()
		on, set := st.Redundant()
		pu, pinc, pok := st.PeerPriority()
		in, lim := st.ReceiveWindow()
		if u != 1 || inc || !isScheduler(st.Scheduler(), SchedulerSpreadRoundRobin()) || !on || !set ||
			st.SendBuffer() != 100<<10 || pu != 2 || !pinc || !pok || in < 64<<10 || lim != 128<<10 {
			t.Fatalf("%s: priority %d %v scheduler %T redundant %v %v send buffer %d peer priority %d %v %v receive window %d %d",
				when, u, inc, st.Scheduler(), on, set, st.SendBuffer(), pu, pinc, pok, in, lim)
		}
	}
	check("before the first frame", st)
	if _, err := st.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := st.Flush(); err != nil {
		t.Fatal(err)
	}
	check("after the opening frame", st)

	// The same settings read through each part.
	rs, err := server.AcceptStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if u, inc, ok := rs.PeerPriority(); ok {
		t.Fatalf("the accepting end requested a priority: %d %v", u, inc)
	}
	if u, inc, ok := rs.PreamblePriority(); !ok || u != 2 || !inc {
		t.Fatalf("the peer's preamble priority %d %v %v", u, inc, ok)
	}
	// A later request replaces the preamble's.
	if err := rs.SetPeerPriority(5, false); err != nil {
		t.Fatal(err)
	}
	if u, inc, ok := rs.PeerPriority(); !ok || u != 5 || inc {
		t.Fatalf("after the update %d %v %v", u, inc, ok)
	}

	// The zero Scheduler follows the session again, the redundancy too.
	st.SetScheduler(nil)
	st.ClearRedundant()
	if st.Scheduler() != nil {
		t.Fatalf("after SetScheduler(nil) %T", st.Scheduler())
	}
	if on, set := st.Redundant(); on || set {
		t.Fatalf("after ClearRedundant %v %v", on, set)
	}
	// A stream that ended keeps its last values.
	st.SetScheduler(SchedulerSpreadETA())
	st.CancelWrite(1)
	st.CancelRead(1)
	if !isScheduler(st.Scheduler(), SchedulerSpreadETA()) || st.SendBuffer() != 100<<10 {
		t.Fatalf("after the end: %T %d", st.Scheduler(), st.SendBuffer())
	}
	_ = client.Close()
	<-client.Done()
	if !isScheduler(st.Scheduler(), SchedulerSpreadETA()) || st.SendBuffer() != 100<<10 {
		t.Fatalf("after the session ended: %T %d", st.Scheduler(), st.SendBuffer())
	}
}

// The sending and receiving parts of a unidirectional stream carry the same
// getters.
func TestPartSettingGetters(t *testing.T) {
	leakcheck.Check(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	e := newMPEnv(t, mpConfig(), mpConfig())
	client, server := e.client, e.server
	us, err := client.OpenUniStream()
	if err != nil {
		t.Fatal(err)
	}
	us.SetPriority(6, true)
	us.SetScheduler(SchedulerSessionAffinity())
	us.SetRedundant(false)
	us.SetSendBuffer(1) // raised to the 16 KiB floor
	if u, inc := us.Priority(); u != 6 || !inc || !isScheduler(us.Scheduler(), SchedulerSessionAffinity()) || us.SendBuffer() != 16<<10 {
		t.Fatalf("sending part: %d %v %T %d", u, inc, us.Scheduler(), us.SendBuffer())
	}
	if on, set := us.Redundant(); on || !set {
		t.Fatalf("redundancy %v %v", on, set)
	}
	us.SetScheduler(nil)
	if us.Scheduler() != nil {
		t.Fatalf("after SetScheduler(nil) %T", us.Scheduler())
	}
	if _, err := us.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := us.Flush(); err != nil {
		t.Fatal(err)
	}
	rs, err := server.AcceptUniStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := rs.SetReceiveWindow(4096, 4096); err != nil {
		t.Fatal(err)
	}
	if _, limit := rs.ReceiveWindow(); limit != 4096 {
		t.Fatalf("receive window cap %d", limit)
	}
	if err := rs.SetPeerPriority(4, true); err != nil {
		t.Fatal(err)
	}
	if u, inc, ok := rs.PeerPriority(); !ok || u != 4 || !inc {
		t.Fatalf("peer priority %d %v %v", u, inc, ok)
	}
}
