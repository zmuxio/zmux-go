package zmux

import (
	"context"
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
	synctest.Test(t, func(t *testing.T) {
		cc, sc := mpConfig(), mpConfig()
		sc.EpochPeriod = -1
		cc.EpochAfterFirstPipe, sc.EpochAfterFirstPipe = SwitchOff, SwitchOff
		e := newMemMPEnv(t, cc, sc)
		first := e.addPipe(t, PipeOptions{}) // RecordSealed: the exchange's pipe
		if err := e.client.AdvanceEpoch(); err != nil {
			t.Fatal(err)
		}
		waitEpochCommit(t, e.client, e.server)
		if got := e.client.Epoch(); got != 1 {
			t.Fatalf("initiator joins use epoch %d after commit, want 1", got)
		}
		before := e.server.ConnectionStats().JoinAuthFailures
		// A Join in the new epoch is evidence that the responder can use it.
		second := e.addPipe(t, PipeOptions{})
		if got := e.server.Epoch(); got != 1 {
			t.Fatalf("responder joins use epoch %d after receiving a new-epoch Join, want 1", got)
		}
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
	})
}

// The first Sealed or Bound pipe of a session whose secret comes from a
// pre-shared key starts one exchange on its own; EpochAfterFirstPipe off at both
// ends leaves the epoch alone.
func TestEpochAfterFirstPipe(t *testing.T) {
	leakcheck.Check(t)
	synctest.Test(t, func(t *testing.T) {
		for _, off := range []bool{false, true} {
			ccfg, scfg := mpConfig(), mpConfig()
			if off {
				ccfg.EpochAfterFirstPipe, scfg.EpochAfterFirstPipe = SwitchOff, SwitchOff
			}
			e := newMemMPEnv(t, ccfg, scfg)
			e.addPipe(t, PipeOptions{})
			var want uint64
			if off {
				synctest.Sleep(time.Second)
				synctest.Wait()
			} else {
				waitEpochCommit(t, e.client, e.server)
				e.addPipe(t, PipeOptions{})
				want = 1
			}
			if e.client.Epoch() != want || e.server.Epoch() != want {
				t.Fatalf("off=%v: epochs %d %d, want %d", off, e.client.Epoch(), e.server.Epoch(), want)
			}
		}
	})
}

func waitEpochCommit(t *testing.T, endpoints ...*Conn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	for _, c := range endpoints {
		for {
			event, err := c.NextEvent(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if event.Kind == EventEpochCommitted && event.Epoch == 1 {
				break
			}
		}
	}
}

// The end of a suspension starts one exchange on its own once Stable;
// EpochAfterResume off at both ends leaves the epoch alone.
func TestEpochAfterResume(t *testing.T) {
	for _, tc := range []struct {
		name string
		off  bool
		want uint64
	}{{"enabled", false, 1}, {"off", true, 0}} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ccfg, scfg := mpConfig(), mpConfig()
				ccfg.EpochAfterFirstPipe, scfg.EpochAfterFirstPipe = SwitchOff, SwitchOff
				ccfg.ResumeTimeout, scfg.ResumeTimeout = 10*time.Second, 10*time.Second
				if tc.off {
					ccfg.EpochAfterResume, scfg.EpochAfterResume = SwitchOff, SwitchOff
				}
				e := newMemMPEnv(t, ccfg, scfg)
				p := e.addPipe(t, PipeOptions{})
				p0, _ := e.client.Pipe(0)
				if err := p0.Abort(); err != nil {
					t.Fatal(err)
				}
				if err := p.Abort(); err != nil {
					t.Fatal(err)
				}
				synctest.Wait()
				if !e.client.ConnectionState().Suspended {
					t.Fatal("loss of both attachments did not suspend the session")
				}
				e.addPipe(t, PipeOptions{})
				synctest.Wait()
				if e.client.ConnectionState().Suspended {
					t.Fatal("confirmed Join did not resume the session")
				}
				if !tc.off {
					waitEpochCommit(t, e.client, e.server)
					// A Join in the new epoch supplies the peer with
					// evidence to switch even before its delayed ACK.
					e.addPipe(t, PipeOptions{})
				}
				if c, s := e.client.Epoch(), e.server.Epoch(); c != tc.want || s != tc.want {
					t.Fatalf("epochs %d and %d after resumption, want %d", c, s, tc.want)
				}
			})
		})
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
