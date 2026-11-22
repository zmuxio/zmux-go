package zmux

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
)

// An epoch exchange over a Sealed pipe: both ends move to epoch 1, and joins
// afterwards use the new key phase and the server's new Locator, which the
// listener now finds.
func TestEpochAdvanceThenJoin(t *testing.T) {
	leakcheck.Check(t)
	cc, sc := mpConfig(), mpConfig()
	sc.EpochPeriod = -1
	e := newMPEnv(t, cc, sc)
	first := e.addPipe(t, PipeOptions{}) // RecordSealed: the exchange's pipe
	if err := e.client.AdvanceEpoch(); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 30*time.Second, "epoch 1 at both ends", func() bool {
		return e.client.Epoch() == 1 && e.server.Epoch() == 1
	})
	before := e.server.ConnectionStats().JoinAuthFailures
	second := e.addPipe(t, PipeOptions{})
	for _, tc := range []struct {
		id    PipeID
		epoch uint64
	}{{0, 0}, {first.ID(), 0}, {second.ID(), 1}} {
		for _, c := range []*Conn{e.client, e.server} {
			p, ok := c.Pipe(tc.id)
			if !ok {
				t.Fatalf("missing pipe %d", tc.id)
			}
			st, ok := p.Stats()
			if !ok || !st.EpochKnown || st.Epoch != tc.epoch || st.ReplaceBy.IsZero() != (c == e.server) {
				t.Fatalf("pipe %d: epoch %d (known %t), replace by %v", tc.id, st.Epoch, st.EpochKnown, st.ReplaceBy)
			}
		}
	}
	if got := e.server.ConnectionStats().JoinAuthFailures; got != before {
		t.Fatalf("%d join authentication failures after the exchange", got-before)
	}
	// The next exchange waits for the retention of epoch 0 (three maximum
	// join deadlines, the conformance traces cover it); joins go on meanwhile.
	if err := e.client.AdvanceEpoch(); err != nil {
		t.Fatal(err)
	}
	e.addPipe(t, PipeOptions{KeyGroup: 1})
	if e.client.Epoch() != 1 || e.server.Epoch() != 1 {
		t.Fatalf("epochs %d %d during the retention", e.client.Epoch(), e.server.Epoch())
	}
}

// The first Sealed or Bound pipe of a session whose secret comes from a
// pre-shared key starts one exchange on its own; EpochAfterFirstPipe off at both
// ends leaves the epoch alone.
func TestEpochAfterFirstPipe(t *testing.T) {
	leakcheck.Check(t)
	for _, off := range []bool{false, true} {
		ccfg, scfg := mpConfig(), mpConfig()
		if off {
			ccfg.EpochAfterFirstPipe, scfg.EpochAfterFirstPipe = SwitchOff, SwitchOff
		}
		e := newMPEnv(t, ccfg, scfg)
		e.addPipe(t, PipeOptions{})
		if !off {
			waitUntil(t, 30*time.Second, "epoch 1 after the first pipe", func() bool {
				return e.client.Epoch() == 1 && e.server.Epoch() == 1
			})
			continue
		}
		time.Sleep(300 * time.Millisecond) // time for an exchange that must not start
		if e.client.Epoch() != 0 || e.server.Epoch() != 0 {
			t.Fatalf("off: epochs %d %d", e.client.Epoch(), e.server.Epoch())
		}
	}
}

// The end of a suspension starts one exchange on its own once Stable;
// EpochAfterResume off at both ends leaves the epoch alone.
func TestEpochAfterResume(t *testing.T) {
	leakcheck.Check(t)
	for _, off := range []bool{false, true} {
		ccfg, scfg := mpConfig(), mpConfig()
		ccfg.EpochAfterFirstPipe, scfg.EpochAfterFirstPipe = SwitchOff, SwitchOff
		if off {
			ccfg.EpochAfterResume, scfg.EpochAfterResume = SwitchOff, SwitchOff
		}
		ccfg.ResumeTimeout, scfg.ResumeTimeout = 10*time.Second, 10*time.Second
		e := newMPEnv(t, ccfg, scfg)
		p := e.addPipe(t, PipeOptions{})
		p0, _ := e.client.Pipe(0)
		_ = p0.Close()
		_ = p.Close()
		waitUntil(t, 10*time.Second, "suspended", func() bool { return e.client.ConnectionState().Suspended })
		e.addPipe(t, PipeOptions{})
		waitUntil(t, 10*time.Second, "resumed", func() bool { return !e.client.ConnectionState().Suspended })
		if !off {
			waitUntil(t, 30*time.Second, "epoch 1 after the resumption", func() bool {
				return e.client.Epoch() == 1 && e.server.Epoch() == 1
			})
			continue
		}
		time.Sleep(300 * time.Millisecond)
		if e.client.Epoch() != 0 || e.server.Epoch() != 0 {
			t.Fatalf("off: epochs %d %d", e.client.Epoch(), e.server.Epoch())
		}
	}
}

// No timer advances between the explicit offer and the responder's answer.
func TestIdleWriterEpochExchange(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cc, sc := mpConfig(), mpConfig()
		cc.EpochAfterFirstPipe, sc.EpochAfterFirstPipe = SwitchOff, SwitchOff
		cc.Liveness = Liveness{ProbeInterval: -1, StandbyProbe: -1}
		sc.Liveness = cc.Liveness
		e := newMemMPEnv(t, cc, sc)
		p := e.addPipe(t, PipeOptions{})
		synctest.Wait()
		time.Sleep(2 * writerIdle) // synctest virtual time
		synctest.Wait()
		e.client.peek(func() {
			if e.client.pipes[uint64(p.ID())].writer.running {
				t.Error("sealed pipe still has a writer")
			}
		})
		start := time.Now()
		if err := e.client.AdvanceEpoch(); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if !time.Now().Equal(start) || e.client.Epoch() != 1 {
			t.Fatalf("epoch %d after %v", e.client.Epoch(), time.Since(start))
		}
	})
}
