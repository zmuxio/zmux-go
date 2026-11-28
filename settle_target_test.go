package zmux

import (
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

// Renewing a replacement's evidence while its batch waits for Settle keeps
// the batch under the new revision. The wait must withdraw the demand that
// SetTarget queued and park, not spin on arbitration signals.
func TestRenewedReplacementWaitsForSettle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clk, client, server := fakeConfigs(func(cfg *Config) {
			cfg.MaxIdleTimeout = -1
			cfg.ResumeTimeout = time.Hour
		})
		env := newMemMPEnv(t, client, server)
		cleanupFakeMPEnv(t, env, clk)
		sc := installTestController(t, "settle", env, nil)
		base, err := env.client.MemberOf(0)
		if err != nil {
			t.Fatal(err)
		}
		target := func(revision uint64) SessionTarget {
			return SessionTarget{Revision: revision, Groups: []GroupTarget{{ID: "main",
				Quotas: []MemberQuota{{ID: "member", Count: 1, Directions: Both, Candidates: []TargetCandidate{{ID: "repair"}}}},
			}}, Replace: []ReplaceIntent{{ID: "replace", Members: []MemberID{base}}}}
		}
		if err := sc.SetTarget(target(1)); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		for revision := uint64(2); revision < 6; revision++ {
			if err := sc.SetTarget(target(revision)); err != nil {
				t.Fatal(err)
			}
			synctest.Wait()
			v, err := sc.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			if !errors.Is(v.Block, ErrChangePending) || sc.ctl.Snapshot().PendingOwners != 0 {
				t.Fatalf("revision %d: block %v, pending owners %d", revision, v.Block, sc.ctl.Snapshot().PendingOwners)
			}
		}
	})
}
