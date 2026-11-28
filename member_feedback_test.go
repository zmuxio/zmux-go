package zmux

import (
	"errors"
	"testing"
	"testing/synctest"

	"github.com/zmuxio/zmux-go/internal/core"
)

func (sc *SessionControl) memberRead(t *testing.T, member MemberID) MemberSnapshot {
	t.Helper()
	s, err := sc.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range s.Capacity.Members {
		if m.ID == member {
			return m
		}
	}
	t.Fatal("member no longer present")
	return MemberSnapshot{}
}

func newTestController(t *testing.T, name string, env *mpEnv, cache *CarrierCache) *SessionController {
	t.Helper()
	opts := ControllerOptions{Domain: name, ClockDomain: env.client.clk.domain, Candidates: []Candidate{
		{ID: "repair", CacheKey: "repair", Attachments: testMemberSpec("", Both).Attachments, Kind: CarrierStream, Open: testMemberOpener(env)},
	}, Cache: cache}
	ctl, err := NewSessionController(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ctl.Close(); <-ctl.Done() })
	return ctl
}

func installTestController(t *testing.T, name string, env *mpEnv, cache *CarrierCache) *SessionControl {
	t.Helper()
	ctl := newTestController(t, name, env, cache)
	// The Join environment supplies the scripted carrier kind at handoff.
	env.client.peek(func() { env.client.pipes[0].hc.io.Carrier = CarrierStream })
	sc, err := ctl.Manage(env.client, SessionTarget{}, ManageOptions{BootstrapCandidate: "repair"})
	if err != nil {
		t.Fatal(err)
	}
	return sc
}

func TestQualityWithdrawalIsFieldSpecific(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clk, client, server := fakeConfigs(func(cfg *Config) { cfg.MaxIdleTimeout = -1 })
		env := newMemMPEnv(t, client, server)
		cleanupFakeMPEnv(t, env, clk)
		sc := installTestController(t, "withdraw", env, nil)
		id, err := env.client.MemberOf(0)
		if err != nil {
			t.Fatal(err)
		}
		c := env.client
		now := c.clk.now()
		c.locked(func() {
			c.s.RecordMinimumRTT(0, core.Millisecond, now, 0)
			c.s.RecordProtocolRTT(0, 2*core.Millisecond, now, 0)
		})
		first := sc.memberRead(t, id).Paths[0].Send
		if first.RTT == nil || first.MinimumRTT == nil {
			t.Fatal("missing initial estimates")
		}
		c.locked(func() {
			c.s.WithdrawMeasurement(0, core.CurrentRTTEvidence)
			c.s.RecordProtocolRTT(0, 3*core.Millisecond, now, 0)
		})
		for _, tc := range []struct {
			ref  QualityReference
			want error
		}{{first.RTT.Reference, ErrStaleDecision}, {first.MinimumRTT.Reference, nil}} {
			target := SessionTarget{Replace: []ReplaceIntent{{Members: []MemberID{id}, Observations: []QualityReference{tc.ref}}}}
			c.peek(func() { err = sc.checkQualityLocked(target, true) })
			if !errors.Is(err, tc.want) {
				t.Errorf("field %v: %v, want %v", tc.ref.field, err, tc.want)
			}
		}
	})
}
