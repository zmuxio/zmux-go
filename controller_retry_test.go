package zmux

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/djylb/timeshim"
)

func TestRecoverySuppressionCountsDueStartOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fake, client, server := fakeConfigs(func(cfg *Config) {
			cfg.MaxIdleTimeout = -1
			cfg.ResumeTimeout = time.Minute
		})
		env := newMemMPEnv(t, client, server)
		cleanupFakeMPEnv(t, env, fake)
		const id = "repair"
		opened := make(chan struct{}, 1)
		candidate := Candidate{ID: id, CacheKey: id, Kind: CarrierRaw,
			Attachments: testMemberSpec(id, Send).Attachments, Open: func(ctx context.Context, _ OpenRequest) ([]PipeIO, error) {
				opened <- struct{}{}
				<-ctx.Done()
				return nil, ctx.Err()
			}}
		cache := testCarrierCache(t, CarrierCacheOptions{ClockDomain: client.ClockDomain,
			Backoff: Backoff{Min: time.Second, Max: time.Second, Jitter: Set(0.0)}})
		key := candKey(id, candidate)
		if err := cache.update(key, CacheFailed, CarrierObservation{}); err != nil {
			t.Fatal(err)
		}
		ctl, err := NewSessionController(ControllerOptions{Domain: t.Name(), ClockDomain: client.ClockDomain,
			Cache: cache, DialAfter: map[CandidateID]time.Time{id: fake.Now().Add(time.Second)}, Candidates: []Candidate{candidate}})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = ctl.Close(); <-ctl.Done() }()
		if _, err := ctl.Manage(env.client, SessionTarget{}); err != nil {
			t.Fatal(err)
		}
		dropBootstrapSend(t, env)
		synctest.Wait()
		for range 3 {
			env.client.locked(env.client.s.NotifyCapacity)
			ctl.mu.Lock()
			ctl.signalLocked()
			ctl.mu.Unlock()
			synctest.Wait()
		}
		if m := cache.RetryMetrics(id, CarrierRaw, ""); m.Suppressions[RetryDialAfter] != 1 || m.AttemptedStarts != 0 {
			t.Fatalf("suppression charged on rescan: %+v", m)
		}
		advanceClock(fake, time.Second)
		select {
		case <-opened:
		default:
			t.Fatal("scheduled retry did not start")
		}
		if m := cache.RetryMetrics(id, CarrierRaw, ""); m.Suppressions[RetryDialAfter] != 1 || m.AttemptedStarts != 1 {
			t.Fatalf("after cooldown expiry: %+v", m)
		}
	})
}

func TestBootstrapSuppressionCountsScheduleOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fake := timeshim.NewFake(time.Unix(0, 0))
		domain := NewClockDomain(fake)
		const id = "bootstrap"
		cache := testCarrierCache(t, CarrierCacheOptions{ClockDomain: domain})
		opened := make(chan struct{}, 1)
		candidate := Candidate{ID: id, CacheKey: id, Kind: CarrierRaw, CanStart: true,
			Open: func(ctx context.Context, _ OpenRequest) ([]PipeIO, error) {
				opened <- struct{}{}
				<-ctx.Done()
				return nil, ctx.Err()
			}}
		ctl, err := NewSessionController(ControllerOptions{Domain: t.Name(), ClockDomain: domain, Cache: cache,
			Candidates: []Candidate{candidate}, DialAfter: map[CandidateID]time.Time{id: fake.Now().Add(time.Second)}})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = ctl.Close(); <-ctl.Done() }()
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan error, 1)
		go func() { _, _, err := ctl.Open(ctx, &Config{ClockDomain: domain}, SessionTarget{}); done <- err }()
		synctest.Wait()
		for range 3 {
			ctl.mu.Lock()
			ctl.signalLocked()
			ctl.mu.Unlock()
			synctest.Wait()
		}
		if m := cache.RetryMetrics(id, CarrierRaw, ""); m.Suppressions[RetryDialAfter] != 1 || m.AttemptedStarts != 0 {
			t.Fatalf("suppression charged on wake: %+v", m)
		}
		advanceClock(fake, time.Second)
		select {
		case <-opened:
		default:
			t.Fatal("bootstrap did not start after DialAfter")
		}
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("bootstrap: %v", err)
		}
	})
}
