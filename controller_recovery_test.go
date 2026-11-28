package zmux

import (
	"context"
	"errors"
	"fmt"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/djylb/timeshim"
	"github.com/zmuxio/zmux-go/internal/carrierstats"
)

func recoveryTestCandidate(id CandidateID, key string, dir Directions) Candidate {
	return Candidate{
		ID: id, CacheKey: key, Kind: CarrierRaw,
		Attachments: []AttachmentSpec{{Directions: dir}},
		Open:        func(context.Context, OpenRequest) ([]PipeIO, error) { return nil, ErrInvalidConfig },
	}
}

func newRecoveryTestController(t *testing.T, opts ControllerOptions) *SessionController {
	t.Helper()
	ctl, err := NewSessionController(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ctl.Close(); <-ctl.Done() })
	return ctl
}

func TestSharedDirectionDemandAcrossTakeover(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clk, cc, server := fakeConfigs(func(cfg *Config) {
			cfg.MaxIdleTimeout = -1
			cfg.ResumeTimeout = time.Minute
		})
		env := newMemMPEnv(t, cc, server)
		cleanupFakeMPEnv(t, env, clk)
		dropBootstrapSend(t, env)
		synctest.Wait()
		draws := make(chan float64, 3)
		draws <- 0.5
		draws <- 0.25
		draws <- 1
		cache := testCarrierCache(t, CarrierCacheOptions{ClockDomain: cc.ClockDomain, RecoverySamples: draws})
		out, in := recoveryTestCandidate("out", "shared", Send), recoveryTestCandidate("in", "shared", Receive)
		makeController := func(name string, list []Candidate) *SessionController {
			later := clk.Now().Add(time.Hour)
			return newRecoveryTestController(t, ControllerOptions{Domain: name, ClockDomain: cc.ClockDomain, Cache: cache, Candidates: list, DialAfter: map[CandidateID]time.Time{out.ID: later, in.ID: later}})
		}
		key := candKey(out.CacheKey, out)
		old, err := makeController("old", []Candidate{out, in}).Manage(env.client, SessionTarget{})
		if err != nil {
			t.Fatal(err)
		}
		failure := AttemptSnapshot{Candidate: out.ID, Cause: AttemptRecovery, Result: AttemptResult{Reason: ReasonDialFailed, Attribution: AttributionCarrier}}
		env.client.locked(func() { old.observeAttemptLocked(failure, 0) })
		score := func() RecoveryCadence { return cache.Score(key.Key, key.Kind, key.Variant).Recovery }
		initial := score()
		if initial.Advances != 1 || initial.Sampled != time.Second/2 {
			t.Fatalf("send demand after receive registration: %+v", initial)
		}
		next, err := makeController("new", []Candidate{in, out}).TakeOver(old, SessionTarget{})
		if err != nil {
			t.Fatal(err)
		}
		if got := score(); got != initial {
			t.Fatalf("handoff changed the shared sequence: %+v -> %+v", initial, got)
		}
		// The observation retains its issuing owner across the handoff.
		env.client.locked(func() { old.observeAttemptLocked(failure, 0) })
		if got := score(); got.Advances != 2 || got.Nominal != 2*time.Second || got.Sampled != time.Second/2 {
			t.Fatalf("late failure: %+v", got)
		}
		if err := next.Release(); err != nil {
			t.Fatal(err)
		}
		env.client.locked(func() { old.observeAttemptLocked(failure, 0) })
		if got := score(); got != (RecoveryCadence{}) {
			t.Fatalf("advanced without demand: %+v", got)
		}
		// The original session no longer owns the demand, but another
		// sharer of the key still consumes late carrier evidence.
		cache.setRecoveryDemand(env.server, key, true)
		defer cache.setRecoveryDemand(env.server, key, false)
		env.client.locked(func() { old.observeAttemptLocked(failure, 0) })
		if got := score(); got.Advances != 1 || got.Sampled != time.Second {
			t.Fatalf("late failure with a different live session: %+v", got)
		}
	})
}

func TestFeedbackTransferSplitsRiskAtKeyChange(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clk, cc, server := fakeConfigs(func(cfg *Config) {
			cfg.MaxIdleTimeout = -1
			cfg.ResumeTimeout = time.Minute
		})
		env := newMemMPEnv(t, cc, server)
		cleanupFakeMPEnv(t, env, clk)
		cache := testCarrierCache(t, CarrierCacheOptions{ClockDomain: cc.ClockDomain})
		old := installTestController(t, "source", env, cache)
		id, err := env.client.MemberOf(0)
		if err != nil {
			t.Fatal(err)
		}
		dst := newRecoveryTestController(t, ControllerOptions{Domain: "destination", ClockDomain: cc.ClockDomain, Cache: cache, Candidates: []Candidate{recoveryTestCandidate("repair", "destination", Both)}})
		next := &SessionControl{ctl: dst, c: env.client}
		env.client.locked(func() {
			f := old.feedback[id]
			f.window = time.Time{}
			f.risk = clk.Now().Add(-2 * time.Second)
			f.stable = true
			cache.observeHealthyMember(f, clk.Now(), true)
			old.transferFeedbackLocked(next)
			if next.feedback[id] != f || f.key.Key != "destination" || !f.risk.Equal(clk.Now()) {
				t.Fatalf("transferred feedback: %+v", f)
			}
		})
		before := cache.Score("repair", CarrierRaw, "").ExposureSeconds
		if before <= 1.99 || cache.Score("destination", CarrierRaw, "").ExposureSeconds != 0 {
			t.Fatal("pre-cut risk charged to wrong key")
		}
		advanceClock(clk, time.Second)
		env.client.locked(next.stopFeedbackLocked)
		if got := cache.Score("destination", CarrierRaw, "").ExposureSeconds; got < 0.99 || got > 1 {
			t.Fatalf("post-cut exposure %v", got)
		}
	})
}

func TestRecoveryOrderScoresAndRegistrationTies(t *testing.T) {
	for _, choice := range []ChoicePolicy{ChoiceStatic, ChoiceD160} {
		t.Run(fmt.Sprint(choice), func(t *testing.T) {
			domain := NewClockDomain(timeshim.NewFake(time.Unix(0, 0)))
			cache := testCarrierCache(t, CarrierCacheOptions{ClockDomain: domain})
			a, b, c := recoveryTestCandidate("a", "a", Both), recoveryTestCandidate("b", "b", Both), recoveryTestCandidate("c", "c", Both)
			ctl := newRecoveryTestController(t, ControllerOptions{Domain: t.Name(), ClockDomain: domain, Choice: choice, Cache: cache, Candidates: []Candidate{a, b, c}})
			if err := cache.update(candKey("c", c), CacheUsable, CarrierObservation{}); err != nil {
				t.Fatal(err)
			}
			got := ctl.recoveryOrder([]Candidate{b, c, a})
			if got[0].ID != "c" || got[1].ID != "a" || got[2].ID != "b" {
				t.Fatalf("recovery order: %v %v %v", got[0].ID, got[1].ID, got[2].ID)
			}
		})
	}
}

func TestUnattributedRoundWaitsForChange(t *testing.T) {
	for _, wake := range []string{"capacity", "domain", "cache", "demand"} {
		t.Run(wake, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				clk, cc, server := fakeConfigs(func(cfg *Config) {
					cfg.MaxIdleTimeout = -1
					cfg.ResumeTimeout = time.Minute
				})
				env := newMemMPEnv(t, cc, server)
				cleanupFakeMPEnv(t, env, clk)
				dropBootstrapSend(t, env)
				synctest.Wait()
				cache := testCarrierCache(t, CarrierCacheOptions{ClockDomain: cc.ClockDomain})
				candidates := []Candidate{recoveryTestCandidate("one", "k", Send), recoveryTestCandidate("two", "k", Send)}
				var starts atomic.Int32
				x, y := net.Pipe()
				defer func() { _ = y.Close() }()
				late := &blockedCloseConn{Conn: x, entered: make(chan struct{}), gate: make(chan struct{})}
				release := sync.OnceFunc(func() { close(late.gate) })
				defer release()
				opened := make(chan CandidateID, len(candidates))
				for i := range candidates {
					id := candidates[i].ID
					candidates[i].Open = func(ctx context.Context, _ OpenRequest) ([]PipeIO, error) {
						n := starts.Add(1)
						if n > 2 {
							<-ctx.Done()
							return nil, ctx.Err()
						}
						opened <- id
						if n == 2 {
							return []PipeIO{{W: late}}, ErrInvalidConfig
						}
						return nil, ErrInvalidConfig
					}
				}
				ctl := newRecoveryTestController(t, ControllerOptions{Domain: t.Name(), ClockDomain: cc.ClockDomain, Cache: cache, Candidates: candidates})
				sc, err := ctl.Manage(env.client, SessionTarget{})
				if err != nil {
					t.Fatal(err)
				}
				synctest.Wait()
				checkIdle := func() {
					t.Helper()
					v, err := sc.Snapshot()
					if err != nil {
						t.Fatal(err)
					}
					if starts.Load() != 2 || !errors.Is(v.Block, ErrInvalidConfig) || !v.NextRetry.IsZero() || cache.Score("k", CarrierRaw, "").Recovery.Advances != 0 {
						t.Fatalf("exhausted round: starts %d, blocker %v, next retry %v", starts.Load(), v.Block, v.NextRetry)
					}
				}
				checkIdle()
				if first, second := <-opened, <-opened; first != "one" || second != "two" {
					t.Fatalf("round tried %q, %q", first, second)
				}
				<-late.entered
				release()
				synctest.Wait()
				checkIdle()
				advanceClock(clk, 3*controllerStagger)
				checkIdle()
				switch wake {
				case "capacity":
					env.client.locked(env.client.s.NotifyCapacity)
				case "domain":
					ctl.mu.Lock()
					ctl.signalLocked()
					ctl.mu.Unlock()
				case "cache":
					cache.mu.Lock()
					cache.signalLocked()
					cache.mu.Unlock()
				case "demand":
					if err := sc.SetTarget(SessionTarget{Revision: 1}); err != nil {
						t.Fatal(err)
					}
				}
				synctest.Wait()
				if got := starts.Load(); got != 3 {
					t.Fatalf("starts after %s change = %d", wake, got)
				}
			})
		})
	}
}

func TestStaleRetrySuppression(t *testing.T) {
	for _, err := range []error{ErrStaleBinding, ErrStaleDecision} {
		if got := retrySuppression(fmt.Errorf("handoff: %w", err)); got != RetryStale {
			t.Errorf("%v classified as %q", err, got)
		}
	}
}

func TestLossReferenceExpiresOnSessionClock(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clk, cc, server := fakeConfigs(func(cfg *Config) {
			cfg.MaxIdleTimeout = -1
			cfg.Liveness = Liveness{ProbeInterval: -1, KeepAlivePeriod: -1}
		})
		env := newMemMPEnv(t, cc, server)
		cleanupFakeMPEnv(t, env, clk)
		cache := testCarrierCache(t, CarrierCacheOptions{ClockDomain: cc.ClockDomain, PerformanceWindow: time.Second})
		sc := installTestController(t, "loss", env, cache)
		id, err := env.client.MemberOf(0)
		if err != nil {
			t.Fatal(err)
		}
		counts := &carrierstats.Readout{Sent: 10, Known: true}
		sampler := carrierstats.For(counts, CarrierSourceQUICGo, func(r *carrierstats.Readout) carrierstats.Readout { return *r })
		carrierstats.Prime(sampler)
		time.Sleep(time.Second)
		counts.Sent++
		carrierstats.Sample(sampler)
		synctest.Wait()
		env.client.locked(func() { env.client.pipes[0].sampler = sampler })
		sample := sc.memberRead(t, id).Paths[0].Send
		if sample.PhysicalLoss == nil {
			t.Fatal("missing native interval")
		}
		target := SessionTarget{Replace: []ReplaceIntent{{Members: []MemberID{id}, Observations: []QualityReference{sample.LossReference}}}}
		advanceClock(clk, sample.PhysicalLoss.ValidUntil.Sub(clk.Now())-time.Nanosecond)
		env.client.peek(func() { err = sc.checkQualityLocked(target, true) })
		if err != nil {
			t.Fatalf("before expiry: %v", err)
		}
		advanceClock(clk, time.Nanosecond)
		env.client.peek(func() { err = sc.checkQualityLocked(target, true) })
		if !errors.Is(err, ErrStaleDecision) || sc.memberRead(t, id).Paths[0].Send.PhysicalLoss != nil {
			t.Fatalf("at expiry: %v", err)
		}
		runtime.KeepAlive(counts)
	})
}

func TestRecoveryWaitsAtStrictGraceDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const grace = time.Second
		clock, client, server := fakeConfigs(func(c *Config) {
			c.MaxIdleTimeout = -1
			c.ResumeTimeout = grace
		})
		env := newMemMPEnv(t, client, server)
		cleanupFakeMPEnv(t, env, clock)
		dropBootstrapSend(t, env)
		if !env.client.ConnectionState().Suspended {
			t.Fatal("session did not suspend")
		}
		candidate := recoveryTestCandidate("repair", "repair", Send)
		fence := clock.Now().Add(grace - time.Nanosecond)
		ctl := newRecoveryTestController(t, ControllerOptions{
			Domain: t.Name(), ClockDomain: client.ClockDomain, Candidates: []Candidate{candidate},
			DialAfter: map[CandidateID]time.Time{candidate.ID: fence},
		})
		if _, err := ctl.Manage(env.client, SessionTarget{}); err != nil {
			t.Fatal(err)
		}
		advanceClock(clock, grace)
		select {
		case <-env.client.Done():
		default:
			t.Fatal("session remained open after recovery grace expired")
		}
	})
}

func TestRecoveryWaitsForAttachmentCapacity(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clock, client, server := fakeConfigs(func(c *Config) {
			c.MaxIdleTimeout = -1
			c.ResumeTimeout = time.Minute
		})
		env := newMemMPEnv(t, client, server)
		cleanupFakeMPEnv(t, env, clock)
		dropBootstrapSend(t, env)
		synctest.Wait()

		policy := client.SessionPolicy
		policy.Revision++
		policy.Limits.SteadyAttachments = 2
		policy.Limits.JoinHeadroom = new(int)
		if err := env.client.SetSessionPolicy(policy); err != nil {
			t.Fatal(err)
		}
		candidate := recoveryTestCandidate("repair", "repair", Send)
		candidate.Attachments = []AttachmentSpec{{Directions: Send}, {Directions: Receive}, {Directions: Send}}
		opened := make(chan struct{}, 1)
		candidate.Open = func(ctx context.Context, _ OpenRequest) ([]PipeIO, error) {
			opened <- struct{}{}
			<-ctx.Done()
			return nil, ctx.Err()
		}
		ctl := newRecoveryTestController(t, ControllerOptions{
			Domain: t.Name(), ClockDomain: client.ClockDomain, Candidates: []Candidate{candidate},
		})
		sc, err := ctl.Manage(env.client, SessionTarget{})
		if err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		view, err := sc.Snapshot()
		if err != nil || !errors.Is(view.Block, ErrCapacityBlocked) {
			t.Fatalf("expected recovery to wait for attachment capacity, got %v (%v)", view.Block, err)
		}
		select {
		case <-opened:
			t.Fatal("opened a carrier while attachment capacity was exhausted")
		default:
		}

		policy.Revision++
		policy.Limits = client.SessionPolicy.Limits
		if err := env.client.SetSessionPolicy(policy); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		select {
		case <-opened:
		default:
			t.Fatal("recovery did not wake after attachment capacity became available")
		}
	})
}
