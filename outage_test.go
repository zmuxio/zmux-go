package zmux

import (
	"errors"
	"github.com/zmuxio/zmux-go/internal/core"
	"testing"
	"testing/synctest"
	"time"
)

type outageRig struct {
	t       *testing.T
	env     *mpEnv
	cache   *CarrierCache
	sc      *SessionControl
	a, b    Candidate
	advance func(time.Duration)
}

func newOutageRig(t *testing.T, draws ...float64) *outageRig {
	t.Helper()
	clk, cc, server := fakeConfigs(func(cfg *Config) {
		cfg.MaxIdleTimeout = -1
		cfg.ResumeTimeout = 30 * time.Second
	})
	env := newMemMPEnv(t, cc, server)
	cleanupFakeMPEnv(t, env, clk)
	samples := make(chan float64, len(draws))
	for _, u := range draws {
		samples <- u
	}
	cache := testCarrierCache(t, CarrierCacheOptions{ClockDomain: cc.ClockDomain, RecoverySamples: samples})
	a, b := recoveryTestCandidate("a", "a", Both), recoveryTestCandidate("b", "b", Both)
	later := clk.Now().Add(24 * time.Hour)
	ctl := newRecoveryTestController(t, ControllerOptions{Domain: "outage", ClockDomain: cc.ClockDomain, Cache: cache,
		Candidates: []Candidate{a, b}, DialAfter: map[CandidateID]time.Time{a.ID: later, b.ID: later}})
	sc, err := ctl.Manage(env.client, SessionTarget{})
	if err != nil {
		t.Fatal(err)
	}
	return &outageRig{t: t, env: env, cache: cache, sc: sc, a: a, b: b, advance: func(d time.Duration) { advanceClock(clk, d) }}
}

// disconnect aborts every pipe; the resume timeout keeps the session open.
func (r *outageRig) disconnect() {
	r.t.Helper()
	var ids []PipeID
	r.env.client.peek(func() {
		for id := range r.env.client.pipes {
			ids = append(ids, PipeID(id))
		}
	})
	for _, id := range ids {
		if p, ok := r.env.client.Pipe(id); ok {
			_ = p.Abort()
		}
	}
	synctest.Wait()
	r.env.client.locked(func() {
		if !r.sc.outageLocked() {
			r.t.Fatal("session still covers a direction")
		}
	})
}

func (r *outageRig) observe(c Candidate, reason AttemptReason, attribution Attribution) {
	a := AttemptSnapshot{Candidate: c.ID, Cause: AttemptRecovery, Started: time.Unix(1, 0), Result: AttemptResult{Reason: reason, Attribution: attribution}}
	r.env.client.locked(func() { r.sc.observeAttemptLocked(a, 0) })
}

func (r *outageRig) fail(c Candidate) { r.observe(c, ReasonDialFailed, AttributionCarrier) }

func (r *outageRig) score(c Candidate) CarrierScore {
	return r.cache.Score(c.CacheKey, c.Kind, c.Variant)
}

func (r *outageRig) admit(c Candidate, cause AttemptCause) error {
	res, _, err := r.cache.admitChecked(candKey(c.CacheKey, c), cause, r.env.client, time.Time{}, nil)
	if err == nil {
		res.release()
	}
	return err
}

func TestOutageSuccessRetractsProvisionalFailures(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newOutageRig(t, 1, 1)
		events := r.cache.CooldownEvents()
		defer events.Close()
		r.disconnect()
		r.fail(r.a)
		r.fail(r.b)
		// History and cadence take the failure; ordinary cooldown does not rise.
		if s := r.score(r.b); s.CooldownLevel != 0 || !s.CoolingUntil.IsZero() || s.Attempts != 1 || s.Recovery.Advances != 1 {
			t.Fatalf("provisional failure: %+v", s)
		}
		r.observe(r.a, ReasonConfirmed, AttributionNone)
		for _, c := range []Candidate{r.a, r.b} {
			s := r.score(c)
			if s.CooldownLevel != 0 || !s.CoolingUntil.IsZero() || s.Attempts != 0 || s.Weight != 0 || s.Recovery != (RecoveryCadence{}) {
				t.Fatalf("%s kept outage failures: %+v", c.ID, s)
			}
		}
		if err := r.admit(r.b, AttemptGrowth); err != nil {
			t.Fatalf("second pipe blocked after reconnect: %v", err)
		}
		retracted := 0
		for len(events.Events()) > 0 {
			if _, ok := (<-events.Events()).(CooldownRetracted); ok {
				retracted++
			}
		}
		if retracted != 0 {
			t.Fatalf("retracted events %d", retracted)
		}
	})
}

func TestOutageWithoutSuccessKeepsFailures(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newOutageRig(t)
		r.disconnect()
		r.fail(r.b)
		if err := r.sc.Release(); err != nil {
			t.Fatal(err)
		}
		if s := r.score(r.b); s.CooldownLevel != 1 || s.CoolingUntil.IsZero() {
			t.Fatalf("final failure lost: %+v", s)
		}
	})
}

func TestFailureWithPipeCoolsNormally(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newOutageRig(t)
		r.fail(r.b)
		r.observe(r.a, ReasonConfirmed, AttributionNone)
		if s := r.score(r.b); s.CooldownLevel != 1 || s.CoolingUntil.IsZero() || s.Attempts != 1 {
			t.Fatalf("attributed failure retracted: %+v", s)
		}
		if err := r.admit(r.b, AttemptGrowth); !errors.Is(err, ErrCarrierCooling) {
			t.Fatalf("growth admission %v", err)
		}
	})
}

func TestOutageRetriesFollowRecoveryCadence(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newOutageRig(t, 1, 1, 1, 1)
		r.disconnect()
		for _, nominal := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second} {
			r.fail(r.a)
			s := r.score(r.a)
			if s.Recovery.Nominal != nominal || !s.CoolingUntil.IsZero() {
				t.Fatalf("cadence %+v", s)
			}
			if err := r.admit(r.a, AttemptRecovery); !errors.Is(err, ErrRecoveryWait) {
				t.Fatalf("before retry: %v", err)
			}
			r.advance(nominal)
			if err := r.admit(r.a, AttemptRecovery); err != nil {
				t.Fatalf("ordinary cooldown blocked recovery after %v: %v", nominal, err)
			}
		}
	})
}

func TestOutageOnsetDropIsRetracted(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newOutageRig(t)
		const member MemberID = 7
		c := r.env.client
		c.locked(func() {
			r.sc.trackMemberLocked(member, r.a.ID, Both, Both, Both)
			// The member drops while the bootstrap pipe still covers both
			// directions, so this drop is not yet known to start an outage.
			r.sc.observeMembersLocked([]core.MemberHealthChange{{ID: member, At: c.clk.now(), Failed: true}})
		})
		if s := r.score(r.a); s.CooldownLevel != 1 || s.ShortDropWeight == 0 {
			t.Fatalf("drop not observed: %+v", s)
		}
		r.disconnect()
		// The outage began: the onset drop's round is suspended.
		if s := r.score(r.a); s.CooldownLevel != 0 || !s.CoolingUntil.IsZero() || s.ShortDropWeight == 0 {
			t.Fatalf("onset round during outage: %+v", s)
		}
		r.observe(r.b, ReasonConfirmed, AttributionNone)
		if s := r.score(r.a); s.CooldownLevel != 0 || s.ShortDropWeight != 0 || !s.CoolingUntil.IsZero() {
			t.Fatalf("onset drop kept: %+v", s)
		}
	})
}

// The shared cadence wait never pushes a suspended session's last recovery
// start past its grace deadline minus one attempt timeout.
func TestRecoveryCadenceCappedByGrace(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		draws := make([]float64, 8)
		for i := range draws {
			draws[i] = 1
		}
		r := newOutageRig(t, draws...)
		r.disconnect()
		key := candKey(r.a.CacheKey, r.a)
		r.cache.setRecoveryDemand(r.env.client, key, true)
		for range 7 {
			r.cache.advanceRecovery(key, nil)
		}
		retry := r.score(r.a).Recovery.RetryAt
		var cutoff time.Time
		r.env.client.peek(func() { cutoff = r.sc.recoveryCutoffLocked() })
		if cutoff.IsZero() || !cutoff.Before(retry) {
			t.Fatalf("cutoff %v, retry %v", cutoff, retry)
		}
		if got := r.sc.recoveryWake(r.a); !got.Equal(cutoff) {
			t.Fatalf("wake %v, want cutoff %v", got, cutoff)
		}
		r.advance(cutoff.Sub(r.env.client.clk.source().Now()))
		res, _, err := r.cache.admitChecked(key, AttemptRecovery, r.env.client, cutoff, nil)
		if err != nil {
			t.Fatalf("admission at cutoff: %v", err)
		}
		res.release()
		if _, _, err := r.cache.admitChecked(key, AttemptRecovery, r.env.client, time.Time{}, nil); !errors.Is(err, ErrRecoveryWait) {
			t.Fatalf("uncapped admission: %v", err)
		}
	})
}

func TestOutageCloseCommitsDeferredRounds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newOutageRig(t)
		const member MemberID = 7
		c := r.env.client
		c.locked(func() {
			r.sc.trackMemberLocked(member, r.a.ID, Both, Both, Both)
			r.sc.observeMembersLocked([]core.MemberHealthChange{{ID: member, At: c.clk.now(), Failed: true}})
		})
		r.disconnect()
		r.fail(r.b)
		for _, cand := range []Candidate{r.a, r.b} {
			if s := r.score(cand); s.CooldownLevel != 0 {
				t.Fatalf("%s cooled during outage: %+v", cand.ID, s)
			}
		}
		if err := r.sc.Release(); err != nil {
			t.Fatal(err)
		}
		for _, cand := range []Candidate{r.a, r.b} {
			if s := r.score(cand); s.CooldownLevel != 1 || s.CoolingUntil.IsZero() {
				t.Fatalf("%s final round missing: %+v", cand.ID, s)
			}
		}
	})
}

func TestResetCooldownClearsLevelAndCadence(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newOutageRig(t, 1)
		key := candKey(r.b.CacheKey, r.b)
		r.cache.setRecoveryDemand(r.env.client, key, true)
		defer r.cache.setRecoveryDemand(r.env.client, key, false)
		r.fail(r.b)
		if s := r.score(r.b); s.CooldownLevel != 1 || s.Recovery.Advances != 1 {
			t.Fatalf("setup %+v", s)
		}
		if !r.cache.ResetCooldown(r.b.CacheKey, r.b.Kind, r.b.Variant) {
			t.Fatal("nothing cleared")
		}
		s := r.score(r.b)
		if s.CooldownLevel != 0 || !s.CoolingUntil.IsZero() || s.Recovery != (RecoveryCadence{}) || s.Attempts != 1 {
			t.Fatalf("after reset %+v", s)
		}
		if err := r.admit(r.b, AttemptGrowth); err != nil {
			t.Fatal(err)
		}
		if err := r.admit(r.b, AttemptRecovery); err != nil {
			t.Fatal(err)
		}
		if r.cache.ResetCooldown(r.b.CacheKey, r.b.Kind, r.b.Variant) || r.cache.ResetCooldowns() != 0 {
			t.Fatal("second reset cleared state")
		}
	})
}

// A retraction restores only this session's own state: a later fault and a
// later cadence advance from another session stay.
func TestOutageRetractionKeepsAnotherSessionsLaterState(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newOutageRig(t, 1, 0.5)
		key := candKey(r.b.CacheKey, r.b)
		r.disconnect()
		r.fail(r.b)
		// Another session sharing the cache commits a fault and its own
		// recovery advance on the same key.
		r.cache.setRecoveryDemand(r.env.server, key, true)
		defer r.cache.setRecoveryDemand(r.env.server, key, false)
		if err := r.cache.update(key, CacheFailed, CarrierObservation{}); err != nil {
			t.Fatal(err)
		}
		r.cache.advanceRecovery(key, nil)
		other := r.score(r.b)
		r.observe(r.a, ReasonConfirmed, AttributionNone)
		got := r.score(r.b)
		if got.CooldownLevel != 1 || !got.CoolingUntil.Equal(other.CoolingUntil) || got.Recovery != other.Recovery || got.Attempts != 1 {
			t.Fatalf("retraction overwrote the later state: before %+v, after %+v", other, got)
		}
	})
}

// A host reset during the outage survives the retraction.
func TestResetCooldownDuringOutage(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newOutageRig(t, 1)
		r.disconnect()
		r.fail(r.b)
		if r.score(r.b).Recovery.Advances != 1 || !r.cache.ResetCooldown(r.b.CacheKey, r.b.Kind, r.b.Variant) {
			t.Fatal("reset found nothing to clear")
		}
		r.observe(r.a, ReasonConfirmed, AttributionNone)
		if s := r.score(r.b); s.CooldownLevel != 0 || s.Recovery != (RecoveryCadence{}) || s.Attempts != 0 {
			t.Fatalf("after reset and retraction: %+v", s)
		}
	})
}

// The grace cap applies only to a wait drawn before the cutoff.
func TestRecoveryGateCutoffAlreadyPassed(t *testing.T) {
	commit := time.Unix(100, 0)
	r := RecoveryCadence{RetryAt: commit.Add(time.Minute), Sampled: time.Minute}
	for _, tc := range []struct {
		cutoff, want time.Time
	}{
		{time.Time{}, r.RetryAt},
		{commit.Add(10 * time.Second), commit.Add(10 * time.Second)},
		{commit, r.RetryAt},                      // the failure came at the cutoff
		{commit.Add(-time.Second), r.RetryAt},    // the cutoff had passed
		{commit.Add(2 * time.Minute), r.RetryAt}, // later than the drawn wait
	} {
		if got := r.gate(tc.cutoff); !got.Equal(tc.want) {
			t.Errorf("cutoff %v: gate %v, want %v", tc.cutoff, got, tc.want)
		}
	}
}

func TestRecoveryRoundTreatsMissingRetryAsDue(t *testing.T) {
	now := time.Unix(100, 0)
	var r recoveryRound
	r.noteFailure(now, time.Time{})
	if !r.retryAt.Equal(now) {
		t.Fatalf("retry %v, want %v", r.retryAt, now)
	}
	r.noteFailure(now, now.Add(-time.Second))
	if !r.retryAt.Equal(now.Add(-time.Second)) {
		t.Fatalf("passed retry dropped: %v", r.retryAt)
	}
}

// One deferred round per key, and the most severe one: a rejection.
func TestOutageCommitsOneRoundPerKeyKeepingRejection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newOutageRig(t, 1, 1, 1)
		key := candKey(r.b.CacheKey, r.b)
		r.disconnect()
		r.fail(r.b)
		r.advance(time.Second)
		r.env.client.locked(func() {
			journal := r.sc.provisionalLocked()
			_ = r.cache.applyFeedback(key, CacheRejected, CarrierObservation{}, cacheFeedback{journal: journal})
		})
		r.advance(2 * time.Second)
		r.fail(r.b)
		if s := r.score(r.b); s.CooldownLevel != 0 {
			t.Fatalf("cooled during outage: %+v", s)
		}
		if err := r.sc.Release(); err != nil {
			t.Fatal(err)
		}
		s := r.score(r.b)
		if s.CooldownLevel != 1 || s.CooldownReason != CooldownRejected {
			t.Fatalf("final round: %+v", s)
		}
	})
}

// A superseded binding's late failure during an outage joins the journal
// the takeover carried, and its late confirmation ends the outage.
func TestTakeoverCarriesOutageJournalToLateObservations(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newOutageRig(t, 1, 1)
		r.disconnect()
		next := newRecoveryTestController(t, ControllerOptions{Domain: "next", ClockDomain: r.sc.ctl.opts.ClockDomain, Cache: r.cache,
			Candidates: []Candidate{r.a, r.b}, DialAfter: map[CandidateID]time.Time{r.a.ID: r.env.client.clk.source().Now().Add(time.Hour), r.b.ID: r.env.client.clk.source().Now().Add(time.Hour)}})
		cur, err := next.TakeOver(r.sc, SessionTarget{})
		if err != nil {
			t.Fatal(err)
		}
		r.fail(r.b) // observed through the superseded binding
		if s := r.score(r.b); s.CooldownLevel != 0 || s.Attempts != 1 {
			t.Fatalf("late failure was charged: %+v", s)
		}
		r.observe(r.a, ReasonConfirmed, AttributionNone)
		if s := r.score(r.b); s.Attempts != 0 || s.Recovery != (RecoveryCadence{}) {
			t.Fatalf("late confirmation did not retract: %+v", s)
		}
		_ = cur
	})
}

// A member becoming stable outside an outage is a pipe gain: the drop before
// it is final and a later outage cannot retract it.
func TestPipeGainMakesEarlierDropsFinal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newOutageRig(t)
		c := r.env.client
		c.locked(func() {
			r.sc.trackMemberLocked(7, r.a.ID, Both, Both, Both)
			r.sc.trackMemberLocked(8, r.b.ID, Both, 0, 0)
			r.sc.observeMembersLocked([]core.MemberHealthChange{{ID: 7, At: c.clk.now(), Failed: true}})
			r.sc.observeMembersLocked([]core.MemberHealthChange{{ID: 8, At: c.clk.now(), Covered: Both, Stable: Both}})
		})
		r.disconnect()
		r.observe(r.b, ReasonConfirmed, AttributionNone)
		if s := r.score(r.a); s.CooldownLevel != 1 || s.ShortDropWeight == 0 {
			t.Fatalf("drop before a pipe gain was retracted: %+v", s)
		}
	})
}
