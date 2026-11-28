package zmux

import (
	"context"
	"errors"
	"net"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/djylb/netx"
	"github.com/djylb/timeshim"
)

func testChange(t *testing.T, c *Conn, add []MemberSpec, drain ...MemberID) *CapacityChange {
	t.Helper()
	v, err := c.Capacity()
	if err != nil {
		t.Fatal(err)
	}
	tx, err := c.BeginChange(t.Context(), ChangeRequest{PolicyRevision: v.PolicyRevision, Observed: v.Observed, Add: add, Drain: drain})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Cancel() })
	return tx
}

func testMember(t *testing.T, e *mpEnv, id CandidateID, dirs ...Directions) AttemptResult {
	t.Helper()
	tx := testChange(t, e.client, []MemberSpec{testMemberSpec(id, dirs...)})
	r := startTestMember(t, tx, testMemberOpener(e))
	if _, err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return r
}

func testMemberSpec(id CandidateID, dirs ...Directions) MemberSpec {
	spec := MemberSpec{Candidate: id}
	for _, d := range dirs {
		spec.Attachments = append(spec.Attachments, AttachmentSpec{Directions: d})
	}
	return spec
}

func testMemberOpener(e *mpEnv) CarrierOpener {
	return func(ctx context.Context, req OpenRequest) ([]PipeIO, error) {
		pipes := make([]PipeIO, 0, len(req.Layout))
		for range req.Layout {
			p, err := e.pipeDialer()(ctx)
			if err != nil {
				return pipes, err
			}
			pipes = append(pipes, p)
		}
		return pipes, nil
	}
}

func startTestMember(t *testing.T, tx *CapacityChange, open CarrierOpener) AttemptResult {
	t.Helper()
	a, err := tx.StartMember(0, open)
	if err != nil {
		t.Fatal(err)
	}
	r, err := a.Wait(t.Context())
	if err != nil || r.Reason != ReasonConfirmed {
		t.Fatalf("attempt = %+v, %v", r, err)
	}
	return r
}

type testCarrierLane struct {
	net.Conn
	physical *int
}

func (l *testCarrierLane) PhysicalCarrier() any { return l.physical }

func TestCapacitySharesPhysicalConnectionAcrossLanes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := mpConfig()
		cfg.SessionPolicy.Limits.LocalCarriers = 2
		e := newMemMPEnv(t, cfg, mpConfig())
		tx := testChange(t, e.client, []MemberSpec{{Attachments: []AttachmentSpec{{Directions: Send}, {Directions: Receive}}}})
		shared := new(int)
		a, err := tx.StartMember(0, func(ctx context.Context, req OpenRequest) ([]PipeIO, error) {
			ps := make([]PipeIO, 0, len(req.Layout))
			for range req.Layout {
				p, err := e.pipeDialer()(ctx)
				if err != nil {
					return ps, err
				}
				lane := &testCarrierLane{Conn: p.R.(net.Conn), physical: shared}
				p.R, p.W = lane, lane
				ps = append(ps, p)
			}
			return ps, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		r, err := a.Wait(t.Context())
		if err != nil || r.Reason != ReasonConfirmed || len(r.Pipes) != 2 {
			t.Fatalf("attempt %+v, %v", r, err)
		}
		if _, err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		if v, err := e.client.Capacity(); err != nil || v.LocalCarriers != 2 || v.OwnAttachments != 3 {
			t.Fatalf("two lanes on one physical connection: %+v, %v", v, err)
		}
		for i, id := range r.Pipes {
			p, ok := e.client.Pipe(id)
			if !ok {
				t.Fatalf("missing lane %d", id)
			}
			if err := p.Abort(); err != nil {
				t.Fatal(err)
			}
			synctest.Wait()
			want := 2 - i
			if v, err := e.client.Capacity(); err != nil || v.LocalCarriers != want {
				t.Fatalf("after retiring lane %d: %+v, %v, want %d physical connections", id, v, err, want)
			}
		}
	})
}

func TestControllerBootstrapStagger(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clk := timeshim.NewFake(time.Now())
		domain := NewClockDomain(clk)
		cfg := &Config{ClockDomain: domain}
		lst := mustNewListener(&ListenerConfig{Config: cfg})
		defer func() { _ = lst.Close() }()
		ln := netx.NewChanListener(nil, 0)
		defer func() { _ = ln.Close() }()
		go func() {
			for {
				c, err := ln.Accept()
				if err != nil {
					return
				}
				go func() { _ = lst.HandlePipe(t.Context(), NetPipe(c), PipeHint{}) }()
			}
		}()
		firstStarted, secondStarted, late := make(chan struct{}), make(chan struct{}), newCountingCarrier()
		ctl, err := NewSessionController(ControllerOptions{Domain: t.Name(), ClockDomain: domain, Candidates: []Candidate{
			{ID: "first", CacheKey: "first", Kind: CarrierRaw, CanStart: true, Open: func(ctx context.Context, _ OpenRequest) ([]PipeIO, error) {
				close(firstStarted)
				<-ctx.Done()
				return []PipeIO{{R: late, W: late}}, nil
			}},
			{ID: "second", CacheKey: "second", Kind: CarrierRaw, CanStart: true, Open: func(ctx context.Context, req OpenRequest) ([]PipeIO, error) {
				close(secondStarted)
				if req.Stage != OpenBootstrap || len(req.Layout) != 1 || req.Layout[0].Directions != Both {
					return nil, errors.New("wrong bootstrap layout")
				}
				c, err := ln.Dial(ctx, nil)
				if err != nil {
					return nil, err
				}
				return []PipeIO{NetPipe(c)}, nil
			}},
		}})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = ctl.Close(); <-ctl.Done() }()
		type outcome struct {
			c   *Conn
			err error
		}
		result := make(chan outcome, 1)
		go func() { c, _, err := ctl.Open(t.Context(), cfg, SessionTarget{}); result <- outcome{c, err} }()
		<-firstStarted
		advanceClock(clk, controllerStagger-time.Nanosecond)
		select {
		case <-secondStarted:
			t.Fatal("backup started before the stagger")
		default:
		}
		advanceClock(clk, time.Nanosecond)
		<-secondStarted
		r := <-result
		if r.err != nil {
			t.Fatal(r.err)
		}
		server, err := lst.Accept(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		late.checkOnce(t)
		r.c.Abort(0, "")
		server.Abort(0, "")
		advanceClock(clk, time.Minute)
		waitDone(t, r.c)
		waitDone(t, server)
	})
}

func TestPeerMemberRevocationDrains(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newMemMPEnv(t, mpConfig(), mpConfig())
		r := testMember(t, e, "peer", Both)
		id, err := e.server.MemberOf(r.Pipes[0])
		if err != nil {
			t.Fatal(err)
		}
		policy := e.server.cfg.SessionPolicy
		policy.Revision++
		policy.AuthGeneration++
		if err := e.server.SetSessionPolicy(policy); err != nil {
			t.Fatal(err)
		}
		v, err := e.server.Capacity()
		if err != nil || v.Covered != Both {
			t.Fatalf("revocation lost bootstrap coverage: %+v, %v", v, err)
		}
		for _, m := range v.Members {
			if m.ID == id && (!m.Draining || m.Covered != 0) {
				t.Fatalf("member still eligible after authorization changed: %+v", m)
			}
		}
	})
}

func TestControllerRecoveryRacesAlternativesInOneBatch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clk, client, server := fakeConfigs(func(c *Config) {
			c.MaxIdleTimeout = -1
			c.ResumeTimeout = time.Minute
		})
		env := newMemMPEnv(t, client, server)
		cleanupFakeMPEnv(t, env, clk)
		first, second := make(chan OpenRequest, 1), make(chan OpenRequest, 1)
		permitWinner := make(chan struct{})
		candidates := []Candidate{
			{ID: "first", CacheKey: "first", Kind: CarrierRaw, Attachments: []AttachmentSpec{{Directions: Send}},
				Open: func(ctx context.Context, r OpenRequest) ([]PipeIO, error) {
					first <- r
					<-ctx.Done()
					return nil, ctx.Err()
				}},
			{ID: "standby", CacheKey: "standby", Kind: CarrierRaw, Standby: true, Attachments: []AttachmentSpec{{Directions: Send}},
				Open: func(ctx context.Context, r OpenRequest) ([]PipeIO, error) {
					second <- r
					select {
					case <-ctx.Done():
						return nil, ctx.Err()
					case <-permitWinner:
						return testMemberOpener(env)(ctx, r)
					}
				}},
		}
		ctl, err := NewSessionController(ControllerOptions{Domain: t.Name(), ClockDomain: client.ClockDomain, Candidates: candidates})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = ctl.Close(); <-ctl.Done() }()
		control, err := ctl.Manage(env.client, SessionTarget{})
		if err != nil {
			t.Fatal(err)
		}
		_, err = env.client.beginChange(control.ctx, ChangeRequest{Add: []MemberSpec{testMemberSpec("first", Send), testMemberSpec("standby", Send)}}, AttemptGrowth, control, 0)
		if !errors.Is(err, errControllerBatchLayout) {
			t.Fatalf("multiple controller transactions: %v", err)
		}

		dropBootstrapSend(t, env)
		r1 := <-first
		synctest.Wait()
		var batch *CapacityChange
		var loser *Attempt
		env.client.peek(func() { batch = control.batch; loser = batch.attempts[0] })
		stagger := ctl.Snapshot().RaceStagger
		if stagger != 250*time.Millisecond {
			t.Fatalf("advertised stagger: %v", stagger)
		}
		advanceClock(clk, stagger-time.Nanosecond)
		if len(second) != 0 {
			t.Fatal("standby started before the race stagger")
		}
		advanceClock(clk, time.Nanosecond)
		var r2 OpenRequest
		select {
		case r2 = <-second:
		default:
			t.Fatal("standby did not start at the stagger while the first opener was blocked")
		}
		v, err := control.Snapshot()
		var sameBatch bool
		env.client.peek(func() { sameBatch = control.batch == batch && len(env.client.changes) == 1 })
		if !sameBatch || err != nil || !slices.Equal(v.Attempts, []AttemptID{r1.Attempt, r2.Attempt}) ||
			v.Capacity.Reserved != 2 || r2.Need != Send {
			t.Fatalf("alternative starts did not share one logical batch: %+v, %v", v, err)
		}

		close(permitWinner)
		synctest.Wait()
		result, err := loser.Wait(t.Context())
		v, viewErr := control.Snapshot()
		if err != nil || result.Reason != ReasonNoLongerNeeded || result.Err != nil || viewErr != nil ||
			len(v.Attempts) != 0 || v.Capacity.Reserved != 0 || v.Capacity.Covered != Both || v.Block != nil {
			t.Fatalf("after confirming an alternative: loser %+v, view %+v, errors %v, %v", result, v, err, viewErr)
		}
	})
}

func TestControllerRecoveryWithdrawnWhenExternalCarrierFillsNeed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clk, client, server := fakeConfigs(func(c *Config) {
			c.MaxIdleTimeout = -1
			c.ResumeTimeout = time.Minute
		})
		env := newMemMPEnv(t, client, server)
		cleanupFakeMPEnv(t, env, clk)
		started, release := make(chan struct{}), make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		late := newCountingCarrier()
		candidate := Candidate{ID: "repair", CacheKey: "repair", Kind: CarrierRaw,
			Attachments: []AttachmentSpec{{Directions: Send}},
			Open: func(context.Context, OpenRequest) ([]PipeIO, error) {
				close(started)
				<-release
				return []PipeIO{late.pipe()}, nil
			}}
		cache := testCarrierCache(t, CarrierCacheOptions{ClockDomain: client.ClockDomain})
		ctl, err := NewSessionController(ControllerOptions{Domain: t.Name(), ClockDomain: client.ClockDomain, Cache: cache, Candidates: []Candidate{candidate}})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { unblock(); _ = ctl.Close(); <-ctl.Done() }()
		control, err := ctl.Manage(env.client, SessionTarget{})
		if err != nil {
			t.Fatal(err)
		}
		dropBootstrapSend(t, env)
		<-started
		var a *Attempt
		env.client.peek(func() { a = control.batch.attempts[0] })
		_ = testMember(t, env, "", Send)
		synctest.Wait()

		result, err := a.Wait(t.Context())
		v, viewErr := control.Snapshot()
		domain := ctl.Snapshot()
		var liveBatch bool
		env.client.peek(func() { liveBatch = control.batch != nil || len(env.client.changes) != 0 })
		for _, m := range v.Capacity.Members {
			if m.ID == a.member.ID {
				t.Fatal("withdrawn provisional member remains in the session")
			}
		}
		if err != nil || result.Reason != ReasonNoLongerNeeded || result.Err != nil || viewErr != nil || liveBatch ||
			v.Capacity.Reserved != 0 || v.Capacity.Covered != Both || v.Block != nil || v.LastRefusal != nil ||
			domain.PendingOwners != 0 || domain.ExecutingOpeners != 1 || domain.CleanupAttachments != 1 {
			t.Fatalf("external recovery: result %+v, view %+v, domain %+v, live batch %v, errors %v, %v",
				result, v, domain, liveBatch, err, viewErr)
		}
		unblock()
		synctest.Wait()
		late.checkOnce(t)
		if m := cache.RetryMetrics(candidate.CacheKey, candidate.Kind, candidate.Variant); m.AttemptedStarts != 1 || m.Faults != 0 {
			t.Fatalf("withdrawal was counted as a carrier fault: %+v", m)
		}
		if domain = ctl.Snapshot(); domain.ExecutingOpeners != 0 || domain.CleanupAttachments != 0 {
			t.Fatalf("late carrier leaked permits: %+v", domain)
		}
	})
}

func dropBootstrapSend(t *testing.T, env *mpEnv) {
	t.Helper()
	testMember(t, env, "", Receive)
	base, ok := env.client.Pipe(0)
	if !ok {
		t.Fatal("missing bootstrap")
	}
	if err := base.Abort(); err != nil {
		t.Fatal(err)
	}
}

func TestControllerRequiresInitializedCacheClockDomain(t *testing.T) {
	domain := NewClockDomain(timeshim.NewFake(time.Unix(0, 0)))
	cache := testCarrierCache(t, CarrierCacheOptions{})
	tests := []struct {
		cache *CarrierCache
		err   error
	}{
		{new(CarrierCache), ErrInvalidConfig},
		{cache, ErrClockDomainMismatch},
	}
	for _, tt := range tests {
		ctl, err := NewSessionController(ControllerOptions{Domain: t.Name(), ClockDomain: domain, Cache: tt.cache})
		if ctl != nil {
			_ = ctl.Close()
		}
		if !errors.Is(err, tt.err) || !strings.Contains(err.Error(), "NewCarrierCache") {
			t.Fatalf("cache configuration error = %v, want %v and constructor guidance", err, tt.err)
		}
	}
}
