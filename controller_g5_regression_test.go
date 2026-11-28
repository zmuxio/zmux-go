package zmux

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/zmuxio/zmux-go/internal/carrierstats"
)

func TestControllerManualBatchCannotSettleAcrossBindingCut(t *testing.T) {
	for _, takeover := range []bool{false, true} {
		t.Run(map[bool]string{false: "target-revision", true: "takeover"}[takeover], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				clk, client, server := fakeConfigs(func(c *Config) { c.MaxIdleTimeout = -1 })
				env := newMemMPEnv(t, client, server)
				cleanupFakeMPEnv(t, env, clk)
				old := installTestController(t, "manual-predecessor", env, nil)
				candidate := old.ctl.opts.Candidates[0]
				capacity, err := env.client.Capacity()
				if err != nil {
					t.Fatal(err)
				}
				tx, err := env.client.BeginChange(t.Context(), ChangeRequest{
					PolicyRevision: capacity.PolicyRevision,
					Add:            []MemberSpec{{Candidate: candidate.ID, Attachments: []AttachmentSpec{{Directions: Both}}}},
				})
				if err != nil {
					t.Fatal(err)
				}
				entered, release := make(chan struct{}), make(chan struct{})
				a, err := tx.StartMember(0, func(context.Context, OpenRequest) ([]PipeIO, error) {
					close(entered)
					<-release
					return nil, errors.New("old opener failed")
				})
				defer close(release)
				if err != nil {
					t.Fatal(err)
				}
				<-entered
				if takeover {
					next, err := NewSessionController(ControllerOptions{
						Domain: "manual-successor", ClockDomain: client.ClockDomain, Cache: old.ctl.cache,
						Candidates: []Candidate{candidate},
					})
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = next.Close(); <-next.Done() })
					if _, err := next.TakeOver(old, SessionTarget{Revision: 1}); err != nil {
						t.Fatal(err)
					}
				} else if err := old.SetTarget(SessionTarget{Revision: 2}); err != nil {
					t.Fatal(err)
				}
				result := a.Snapshot().Result
				if result.Reason != ReasonCancelled || result.Attribution != AttributionNone || result.BindingGeneration != tx.generation || result.Batch != tx.id {
					t.Fatalf("settlement at cut = %+v", result)
				}
				if _, err := tx.Commit(); !errors.Is(err, ErrStaleBinding) {
					t.Fatalf("stale batch commit: %v", err)
				}
				want := []EventKind{EventBatchReserved, EventRedialCalled, EventRedialFinished, EventBatchRolledBack}
				for _, expected := range want {
					for {
						ev, err := env.client.NextEvent(t.Context())
						if err != nil {
							t.Fatal(err)
						}
						if ev.Batch == tx.id {
							if ev.Kind != expected || ev.BindingGeneration != tx.generation {
								t.Fatalf("event = %+v, want %v", ev, expected)
							}
							break
						}
					}
				}
			})
		})
	}
}

func TestControllerSuppressionFollowsActualCandidateStart(t *testing.T) {
	cache, err := NewCarrierCache(CarrierCacheOptions{})
	if err != nil {
		t.Fatal(err)
	}
	key := CarrierCacheKey{Key: "first", Kind: CarrierRaw}
	other := CarrierCacheKey{Key: "second", Kind: CarrierRaw}
	starts := dueStarts{}
	block := func() { starts.suppress(cache, key, time.Time{}, ErrControllerDialAfter) }
	block()
	starts.started(other)
	block()
	if m := cache.RetryMetrics(key.Key, key.Kind, key.Variant); m.Suppressions[RetryDialAfter] != 1 {
		t.Fatalf("another candidate consumed the opportunity: %+v", m)
	}
	starts.started(key)
	block()
	if m := cache.RetryMetrics(key.Key, key.Kind, key.Variant); m.Suppressions[RetryDialAfter] != 2 {
		t.Fatalf("new opportunity not counted: %+v", m)
	}
}

func TestControllerLossReferenceDoesNotReviveAfterWithdrawal(t *testing.T) {
	ref := QualityReference{counterGeneration: 1, version: 3}
	measurement := carrierstats.Observation{Generation: 1, LossVersion: 3, Interval: time.Second, Known: true}
	if !ref.matchesLoss(measurement) {
		t.Fatal("new sample in the same field version superseded the old sample")
	}
	measurement.Interval = 0
	if ref.matchesLoss(measurement) {
		t.Fatal("withdrawn interval is still valid")
	}
	measurement.Interval = time.Second
	measurement.LossVersion++
	if ref.matchesLoss(measurement) {
		t.Fatal("new interval revived a withdrawn reference")
	}
}

func TestControllerOpenSkipsIncompatibleBootstrapLayout(t *testing.T) {
	opener := func(context.Context, OpenRequest) ([]PipeIO, error) { panic("test orders candidates without dialing") }
	ctl, err := NewSessionController(ControllerOptions{Domain: t.Name(), Candidates: []Candidate{
		{ID: "split", CacheKey: "split", Kind: CarrierRaw, CanStart: true, Open: opener,
			Attachments: []AttachmentSpec{{Directions: Send}, {Directions: Receive}}},
		{ID: "duplex", CacheKey: "duplex", Kind: CarrierRaw, CanStart: true, Open: opener},
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := ctl.Close(); err != nil {
			t.Error(err)
		}
	})
	if got := ctl.order(nil, true); len(got) != 1 || got[0].ID != "duplex" {
		t.Fatalf("bootstrap candidates: %v", got)
	}
}
