package zmux

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"testing/synctest"
	"time"

	"github.com/djylb/timeshim"
	"github.com/zmuxio/zmux-go/internal/core"
)

// Two independently closed stream halves can report the same physical fault.
type faultingCarrier struct {
	net.Conn
	fault *CarrierFaultError
	fail  chan struct{}
}

func (c *faultingCarrier) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if err != nil {
		select {
		case <-c.fail:
			err = fmt.Errorf("read: %w", c.fault)
		default:
		}
	}
	return n, err
}

func TestPhysicalFaultOneCooldownForTwoMembers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clk, cc, sc := fakeConfigs(func(c *Config) { c.MaxIdleTimeout = -1 })
		e := newMemMPEnv(t, cc, sc)
		cleanupFakeMPEnv(t, e, clk)
		carrier := NewCarrierID()
		const id = CandidateID("shared")
		var halves []*faultingCarrier
		for range 2 {
			batch := testChange(t, e.client, []MemberSpec{testMemberSpec(id, Receive)})
			startTestMember(t, batch, func(ctx context.Context, _ OpenRequest) ([]PipeIO, error) {
				raw, err := e.dial(ctx)
				if err != nil {
					return nil, err
				}
				half := &faultingCarrier{Conn: raw, fail: make(chan struct{}),
					fault: &CarrierFaultError{CarrierID: carrier, Generation: 1, Cause: errors.New("connection failed")}}
				halves = append(halves, half)
				return []PipeIO{NetPipe(half)}, nil
			})
			if _, err := batch.Commit(); err != nil {
				t.Fatal(err)
			}
		}
		choice := Candidate{ID: id, CacheKey: string(id), Kind: CarrierRaw, Open: testMemberOpener(e)}
		cache := testCarrierCache(t, CarrierCacheOptions{ClockDomain: cc.ClockDomain, Backoff: Backoff{Min: 30 * time.Second, Max: time.Hour, Jitter: Set(0.0)}})
		subscription := cache.CooldownEvents()
		defer subscription.Close()
		events := subscription.Events()
		ctl, err := NewSessionController(ControllerOptions{Domain: t.Name(), ClockDomain: cc.ClockDomain, Cache: cache, Candidates: []Candidate{choice}})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = ctl.Close(); <-ctl.Done() })
		if _, err := ctl.Manage(e.client, SessionTarget{}); err != nil {
			t.Fatal(err)
		}
		for _, half := range halves {
			close(half.fail)
			_ = half.Close()
		}
		synctest.Wait()
		key := candKey(choice.CacheKey, choice)
		cache.mu.Lock()
		entry := cache.entry(key)
		drops, nominal, deadline, level := entry.history.ShortDrop, entry.cooling.nominal, entry.cooling.until, entry.cooling.Level
		cache.mu.Unlock()
		if drops != 2 || nominal != 30*time.Second || !deadline.Equal(clk.Now().Add(30*time.Second)) || level != 1 {
			t.Fatalf("drops=%g nominal=%d deadline=%v level=%d", drops, nominal, deadline, level)
		}
		select {
		case event := <-events:
			want := CooldownStarted{Key: key, Episode: 1, Level: 1, Reason: CooldownShortDrop,
				StartedAt: clk.Now(), RetryAt: deadline, NominalWait: 30 * time.Second, SampledWait: 30 * time.Second}
			if event != want {
				t.Fatalf("commit %+v, want %+v", event, want)
			}
		default:
			t.Fatal("missing commit event")
		}
		if len(events) != 0 || cache.RetryMetrics(key.Key, key.Kind, key.Variant).Faults != 1 {
			t.Fatal("the same physical fault committed more than one episode")
		}
	})
}

func TestCooldownLevelSurvivesRestartAndClearsOnHealthReset(t *testing.T) {
	clk := timeshim.NewFake(time.Unix(1800000000, 0))
	domain := NewClockDomain(clk)
	backoff := Backoff{Min: time.Second, Max: 2 * time.Second, Jitter: Set(0.0)}
	cache := testCarrierCache(t, CarrierCacheOptions{ClockDomain: domain, Backoff: backoff})
	report := func(outcome CacheOutcome, observations ...CarrierObservation) {
		t.Helper()
		var sample CarrierObservation
		if len(observations) != 0 {
			sample = observations[0]
		}
		if err := cache.update(CarrierCacheKey{Key: "candidate", Kind: CarrierRaw}, outcome, sample); err != nil {
			t.Fatal(err)
		}
	}
	score := func() CarrierScore { return cache.Score("candidate", CarrierRaw, "") }
	for range 3 {
		report(CacheFailed)
		clk.Advance(backoff.Max)
	}
	snapshot, err := cache.Export()
	if err != nil {
		t.Fatal(err)
	}
	cache = testCarrierCache(t, CarrierCacheOptions{ClockDomain: domain, Backoff: backoff})
	if err := cache.Import(snapshot); err != nil {
		t.Fatal(err)
	}
	if got := score(); got.CooldownLevel != 3 || !got.CoolingUntil.IsZero() {
		t.Fatalf("after restart: %+v", got)
	}
	report(CacheUsable)
	report(CacheFailed)
	if err := cache.Import(snapshot); err != nil {
		t.Fatal(err)
	}
	if got := score(); got.CooldownLevel != 4 || !got.CoolingUntil.Equal(clk.Now().Add(backoff.Max)) {
		t.Fatalf("after a new failure and replay: %+v", got)
	}
	clk.Advance(10 * time.Minute)
	report(CacheRecovered, CarrierObservation{Duration: 10 * time.Minute})
	if got := score(); got.CooldownLevel != 0 {
		t.Fatalf("after health reset: %+v", got)
	}
	report(CacheFailed)
	if got := score(); got.CooldownLevel != 1 || !got.CoolingUntil.Equal(clk.Now().Add(backoff.Min)) {
		t.Fatalf("after the first failure since reset: %+v", got)
	}
}

func TestPhysicalFaultGenerationsCanArriveOutOfOrder(t *testing.T) {
	clk, cc, _ := fakeConfigs(func(*Config) {})
	cache := testCarrierCache(t, CarrierCacheOptions{ClockDomain: cc.ClockDomain, Backoff: Backoff{Min: time.Second, Max: time.Minute, Jitter: Set(0.0)}})
	a, b := NewCarrierID(), NewCarrierID()
	key := CarrierCacheKey{Key: "shared", Kind: CarrierRaw}
	for _, fault := range []core.PhysicalFault{{CarrierID: a, Generation: 3}, {CarrierID: a, Generation: 2},
		{CarrierID: a, Generation: 3}, {CarrierID: b, Generation: 3}} {
		if err := cache.applyFeedback(key, CacheLateDrop, CarrierObservation{}, cacheFeedback{fault: &fault}); err != nil {
			t.Fatal(err)
		}
	}
	got := cache.Score(key.Key, key.Kind, "").CoolingUntil
	if want := clk.Now().Add(4 * time.Second); !got.Equal(want) {
		t.Fatalf("cooldown until %v, want %v", got, want)
	}
}

func TestControllerHonorsAuthenticatedPeerRetry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clk, cc, sc := fakeConfigs(func(c *Config) { c.MaxIdleTimeout = -1 })
		e := newMemMPEnv(t, cc, sc)
		cleanupFakeMPEnv(t, e, clk)
		start := clk.Now()
		starts := make(chan time.Time, 4)
		first := true // the reservation serializes openers on this key
		candidate := Candidate{ID: "candidate", CacheKey: "key", Kind: CarrierRaw, Attachments: []AttachmentSpec{{Directions: Both}},
			Open: func(ctx context.Context, req OpenRequest) ([]PipeIO, error) {
				starts <- clk.Now()
				if first {
					first = false
					return nil, fmt.Errorf("authenticated refusal: %w", &CarrierRetryError{RetryAfter: 3 * time.Second})
				}
				return testMemberOpener(e)(ctx, req)
			}}
		cache := testCarrierCache(t, CarrierCacheOptions{ClockDomain: cc.ClockDomain, Backoff: Backoff{Min: time.Second, Max: time.Second, Jitter: Set(0.0)}})
		ctl, err := NewSessionController(ControllerOptions{Domain: t.Name(), ClockDomain: cc.ClockDomain, Cache: cache, Candidates: []Candidate{candidate}})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = ctl.Close(); <-ctl.Done() })
		_, err = ctl.Manage(e.client, SessionTarget{Revision: 1, Groups: []GroupTarget{{
			ID: "group", Quotas: []MemberQuota{{
				ID: "member", Directions: Both, Count: 1, Candidates: []TargetCandidate{{ID: candidate.ID}},
			}},
		}}})
		if err != nil {
			t.Fatal(err)
		}
		<-starts
		advanceClock(clk, time.Second)
		score := cache.Score(candidate.CacheKey, candidate.Kind, candidate.Variant)
		if !score.CoolingUntil.IsZero() || !score.PeerRetryUntil.Equal(start.Add(3*time.Second)) {
			t.Fatalf("deadlines at local cooldown expiry: %+v", score)
		}
		var demand controllerDemand
		defer ctl.dropDemand(&demand)
		work, next, err := ctl.acquire(candidate, AttemptRecovery, &demand, len(candidate.Attachments), time.Time{})
		if work != nil {
			work.releaseWork()
			work.releaseKey()
			work.releaseCleanup()
		}
		if !errors.Is(err, ErrPeerRetry) || !next.Equal(score.PeerRetryUntil) {
			t.Fatalf("recovery admitted: %v, %v", err, next)
		}
		if len(starts) != 0 {
			t.Fatal("opener started during the peer hold")
		}
		advanceClock(clk, 2*time.Second)
		select {
		case at := <-starts:
			if !at.Equal(start.Add(3 * time.Second)) {
				t.Fatalf("opener resumed at %v", at)
			}
		default:
			t.Fatal("controller did not start an opener at the peer deadline")
		}
	})
}

func TestCooldownMetricsAndResetFromAlreadyHealthyMember(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clock := timeshim.NewFake(time.Unix(0, 0))
		const reset = time.Minute
		cache := testCarrierCache(t, CarrierCacheOptions{ClockDomain: NewClockDomain(clock),
			Backoff: Backoff{Min: 10 * time.Second, Max: 20 * time.Second, Jitter: Set(0.0)}, HealthReset: reset})
		key := CarrierCacheKey{Key: "candidate", Kind: CarrierRaw, Variant: "v"}
		subscription := cache.CooldownEvents()
		defer subscription.Close()
		events := subscription.Events()
		metrics := func() RetryMetrics { return cache.RetryMetrics(key.Key, key.Kind, key.Variant) }
		healthy := &memberFeedback{key: key, expected: Both}
		cache.observeHealthyMember(healthy, clock.Now(), true)
		defer func() { cache.observeHealthyMember(healthy, clock.Now(), false) }()
		advanceClock(clock, time.Second)
		faultAt := clock.Now()
		if err := cache.update(key, CacheLateDrop, CarrierObservation{}); err != nil {
			t.Fatal(err)
		}
		if _, _, err := cache.admitChecked(key, AttemptGrowth, nil, time.Time{}, nil); !errors.Is(err, ErrCarrierCooling) {
			t.Fatalf("ordinary admission during cooldown: %v", err)
		}
		select {
		case event := <-events:
			want := CooldownStarted{Key: key, Episode: 1, Level: 1, Reason: CooldownLateDrop,
				StartedAt: faultAt, RetryAt: faultAt.Add(10 * time.Second), NominalWait: 10 * time.Second,
				SampledWait: 10 * time.Second}
			if event != want {
				t.Fatalf("start commit %+v, want %+v", event, want)
			}
		default:
			t.Fatal("retry admission did not observe a committed cooldown event")
		}
		const elapsed = 5 * time.Second
		advanceClock(clock, elapsed)
		r, _, err := cache.admitChecked(key, AttemptRecovery, nil, time.Time{}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := r.start(); err != nil {
			t.Fatal(err)
		}
		r.release()
		m := metrics()
		if m.Faults != 1 || m.AttemptedStarts != 1 || m.Elapsed == nil || *m.Elapsed != elapsed || m.Remaining != 5*time.Second {
			t.Fatalf("cooldown metrics: %+v", m)
		}
		advanceClock(clock, reset-elapsed-time.Nanosecond)
		if len(events) != 0 || metrics().Elapsed == nil || metrics().Remaining != 0 {
			t.Fatal("level reset before the complete post-fault health interval")
		}
		advanceClock(clock, time.Nanosecond)
		select {
		case event := <-events:
			if want := (CooldownReset{Key: key, Episode: 1, ResetAt: clock.Now()}); event != want {
				t.Fatalf("reset commit %+v, want %+v", event, want)
			}
		default:
			t.Fatal("full-direction member did not reset the cooldown")
		}
		if m := metrics(); m.Elapsed != nil || m.Remaining != 0 || m.Faults != 1 {
			t.Fatalf("after reset: %+v", m)
		}
	})
}

func TestCooldownDropsAreLocalToSubscription(t *testing.T) {
	cache := testCarrierCache(t, CarrierCacheOptions{ClockDomain: NewClockDomain(timeshim.NewFake(time.Unix(0, 0)))})
	slow, fast := cache.CooldownEvents(), cache.CooldownEvents()
	defer slow.Close()
	defer fast.Close()
	key := CarrierCacheKey{Key: "candidate", Kind: CarrierRaw}
	for range cap(slow.Events()) + 2 {
		if err := cache.update(key, CacheFailed, CarrierObservation{}); err != nil {
			t.Fatal(err)
		}
		select {
		case <-fast.Events():
		default:
			t.Fatal("active subscriber missed a commit")
		}
	}
	if slow.Dropped() != 2 || fast.Dropped() != 0 {
		t.Fatalf("slow lost %d events, fast lost %d", slow.Dropped(), fast.Dropped())
	}
	slow.Close()
	if slow.Dropped() != 2 {
		t.Fatal("closing the subscription reset its accounting")
	}
}
