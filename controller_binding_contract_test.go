package zmux

import (
	"errors"
	"net"
	"testing"
	"testing/synctest"

	"github.com/djylb/timeshim"
)

// Both successors prepare while the session lock prevents either commit.
// The losing comparison must release its grant without disturbing the winner.
func TestTakeOverCompetingPreparedHandles(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clk, client, server := fakeConfigs(func(c *Config) { c.MaxIdleTimeout = -1 })
		env := newMemMPEnv(t, client, server)
		cleanupFakeMPEnv(t, env, clk)
		old := installTestController(t, "initial", env, nil)
		candidate := old.ctl.opts.Candidates[0]
		newCtl := func(name string) *SessionController {
			ctl, err := NewSessionController(ControllerOptions{ClockDomain: client.ClockDomain, Domain: name, Candidates: []Candidate{candidate}})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = ctl.Close(); <-ctl.Done() })
			return ctl
		}
		type result struct {
			handle *SessionControl
			err    error
		}
		results := make(chan result, 2)
		first, second := newCtl("first"), newCtl("second")
		env.client.lock()
		for _, ctl := range []*SessionController{first, second} {
			go func() { h, e := ctl.TakeOver(old, SessionTarget{Revision: 1}); results <- result{h, e} }()
			for {
				s := ctl.Snapshot()
				if s.ProvisionalEntries == 1 {
					break
				}
				<-s.Changed
			}
		}
		env.client.unlock()
		var winner *SessionControl
		var rejected int
		for range 2 {
			r := <-results
			switch {
			case r.err == nil && r.handle != nil:
				if winner != nil {
					t.Fatal("both successors committed")
				}
				winner = r.handle
			case errors.Is(r.err, ErrStaleBinding) && r.handle == nil:
				rejected++
			default:
				t.Fatalf("unexpected takeover: %v, %v", r.handle, r.err)
			}
		}
		if winner == nil || rejected != 1 {
			t.Fatalf("winner %v, stale comparisons %d", winner, rejected)
		}
		s, e := winner.Snapshot()
		if e != nil || s.BindingGeneration != old.view.BindingGeneration+1 || s.PreviousGeneration != old.view.BindingGeneration || first.Snapshot().ProvisionalEntries+second.Snapshot().ProvisionalEntries != 0 {
			t.Fatalf("handoff: %+v, %v", s, e)
		}
		if err := old.Release(); !errors.Is(err, ErrStaleBinding) {
			t.Fatalf("release of predecessor: %v", err)
		}
	})
}

func TestTakeOverFailureLeavesBindingUntouched(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clk, client, server := fakeConfigs(func(c *Config) { c.MaxIdleTimeout = -1 })
		env := newMemMPEnv(t, client, server)
		cleanupFakeMPEnv(t, env, clk)
		old := installTestController(t, "current", env, nil)
		synctest.Wait()
		before, err := old.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		for _, bad := range []string{"key", "kind", "layout", "clock", "unregistered", "closed"} {
			candidate := old.ctl.opts.Candidates[0]
			domain := client.ClockDomain
			var options []ManageOptions
			want := ErrInvalidConfig
			switch bad {
			case "key":
				candidate.CacheKey = "different"
			case "kind":
				candidate.Kind = CarrierQUIC
			case "layout":
				candidate.Attachments = []AttachmentSpec{{Directions: Send}, {Directions: Receive}}
			case "clock":
				domain = NewClockDomain(timeshim.NewFake(clk.Now()))
				want = ErrClockDomainMismatch
			case "unregistered":
				options = []ManageOptions{{BootstrapCandidate: "missing"}}
				want = ErrNoEligibleCandidate
			case "closed":
				want = net.ErrClosed
			}
			ctl, err := NewSessionController(ControllerOptions{Domain: bad, ClockDomain: domain, Candidates: []Candidate{candidate}})
			if err != nil {
				t.Fatal(err)
			}
			if bad == "closed" {
				_ = ctl.Close()
			}
			h, err := ctl.TakeOver(old, SessionTarget{Revision: 1}, options...)
			_ = ctl.Close()
			<-ctl.Done()
			if h != nil || !errors.Is(err, want) {
				t.Fatalf("%s: handle %v, error %v, want %v", bad, h, err, want)
			}
			after, err := old.Snapshot()
			if err != nil || after.BindingGeneration != before.BindingGeneration || after.TargetRevision != before.TargetRevision || after.Capacity.Observed != before.Capacity.Observed {
				t.Fatalf("%s changed the old binding: %+v, %v", bad, after, err)
			}
			if err := old.SetTarget(SessionTarget{}); err != nil {
				t.Fatalf("%s changed ownership: %v", bad, err)
			}
		}
	})
}
