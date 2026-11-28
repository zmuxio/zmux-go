package zmux

import (
	"context"
	"errors"
	"net"
	"testing"
	"testing/synctest"
	"time"

	"github.com/djylb/timeshim"
	"github.com/zmuxio/zmux-go/internal/core"
)

func TestAttemptDeadlineTightensOnSuspensionAndNeverExtends(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const grace = 2 * time.Second
		clk, client, server := fakeConfigs(func(c *Config) {
			c.MaxIdleTimeout = -1
			c.ResumeTimeout = grace
		})
		env := newMemMPEnv(t, client, server)
		cleanupFakeMPEnv(t, env, clk)
		opener := make(chan context.Context, 1)
		childError := make(chan error, 1)
		tx := testChange(t, env.client, []MemberSpec{testMemberSpec("", Both)})
		a, err := tx.StartMember(0, func(ctx context.Context, _ OpenRequest) ([]PipeIO, error) {
			child, cancel := context.WithCancel(ctx)
			defer cancel()
			opener <- ctx
			<-child.Done()
			childError <- child.Err()
			return nil, child.Err()
		})
		if err != nil {
			t.Fatal(err)
		}
		ctx := <-opener
		initial, ok := ctx.Deadline()
		if !ok || !initial.After(clk.Now().Add(grace)) {
			t.Fatalf("original operation deadline: %v", initial)
		}
		boot, _ := env.client.Pipe(0)
		if err := boot.Abort(); err != nil {
			t.Fatal(err)
		}
		if !env.client.ConnectionState().Suspended {
			t.Fatal("bootstrap failure did not suspend")
		}
		want := clk.Now().Add(grace - time.Nanosecond)
		if now, _ := ctx.Deadline(); !now.Equal(want) {
			t.Fatalf("opener deadline %v, want %v", now, want)
		}
		_ = testMember(t, env, "", Both)
		if env.client.ConnectionState().Suspended {
			t.Fatal("independent carrier did not resume the session")
		}
		if now, _ := ctx.Deadline(); !now.Equal(want) {
			t.Fatalf("deadline widened after resume: %v", now)
		}
		advanceClock(clk, want.Sub(clk.Now())-time.Nanosecond)
		if ctx.Err() != nil {
			t.Fatalf("deadline fired before its boundary: %v", ctx.Err())
		}
		advanceClock(clk, time.Nanosecond)
		result, err := a.Wait(t.Context())
		if err != nil || result.Reason != ReasonDeadline || !errors.Is(result.Err, context.DeadlineExceeded) ||
			!errors.Is(ctx.Err(), context.DeadlineExceeded) || !errors.Is(context.Cause(ctx), context.DeadlineExceeded) ||
			!errors.Is(<-childError, context.DeadlineExceeded) {
			t.Fatalf("deadline result %+v, opener %v, cause %v, wait %v", result, ctx.Err(), context.Cause(ctx), err)
		}
	})
}

func TestSuspendedAttemptWithNoRepresentableTimeDoesNotStart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const grace = time.Second
		clk, client, server := fakeConfigs(func(c *Config) {
			c.MaxIdleTimeout = -1
			c.ResumeTimeout = grace
		})
		env := newMemMPEnv(t, client, server)
		cleanupFakeMPEnv(t, env, clk)
		boot, _ := env.client.Pipe(0)
		if err := boot.Abort(); err != nil {
			t.Fatal(err)
		}
		advanceClock(clk, grace-time.Nanosecond)
		tx := testChange(t, env.client, []MemberSpec{testMemberSpec("", Both)})
		a, err := tx.StartMember(0, func(context.Context, OpenRequest) ([]PipeIO, error) {
			t.Error("opener invoked on the last representable grace tick")
			return nil, context.DeadlineExceeded
		})
		var issued AttemptID
		env.client.peek(func() { issued = env.client.nextAttempt })
		if a != nil || !errors.Is(err, context.DeadlineExceeded) || issued != 0 {
			t.Fatalf("no remaining grace: attempt %v, issued IDs %d, error %v", a, issued, err)
		}
	})
}

func TestBootstrapOpenerAndHandshakeShareAttemptDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clock := timeshim.NewFake(time.Unix(0, 0))
		domain := NewClockDomain(clock)
		const limit = 5 * time.Second
		origin := clock.Now()
		entered := make(chan time.Time, 1)
		release := make(chan struct{})
		left, right := net.Pipe() // the peer deliberately never handshakes
		defer func() { _ = left.Close(); _ = right.Close() }()
		candidate := Candidate{ID: "bootstrap", CacheKey: "bootstrap", Kind: CarrierStream, CanStart: true,
			Open: func(ctx context.Context, _ OpenRequest) ([]PipeIO, error) {
				deadline, _ := ctx.Deadline()
				entered <- deadline
				select {
				case <-release:
					return []PipeIO{NetPipe(left)}, nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}}
		ctl, err := NewSessionController(ControllerOptions{Domain: t.Name(), ClockDomain: domain, Candidates: []Candidate{candidate},
			Limits: ControllerLimits{AttemptTimeout: limit}})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = ctl.Close(); <-ctl.Done() }()
		done := make(chan error, 1)
		go func() {
			c, _, err := ctl.Open(t.Context(), &Config{ClockDomain: domain}, SessionTarget{})
			if c != nil {
				c.Abort(0, "unexpected establishment")
			}
			done <- err
		}()
		if deadline := <-entered; !deadline.Equal(origin.Add(limit)) {
			t.Fatalf("bootstrap opener deadline: %v", deadline)
		}
		advanceClock(clock, limit-time.Nanosecond)
		close(release)
		synctest.Wait()
		select {
		case err := <-done:
			t.Fatalf("bootstrap ended before the attempt deadline: %v", err)
		default:
		}
		advanceClock(clock, time.Nanosecond)
		select {
		case err := <-done:
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("bootstrap result: %v", err)
			}
		default:
			t.Fatal("bootstrap handshake did not use the opener's deadline")
		}
	})
}

func TestPendingJoinUsesTightenedAttemptDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const grace = time.Second
		clock, client, server := fakeConfigs(func(c *Config) {
			c.MaxIdleTimeout = -1
			c.ResumeTimeout = grace
		})
		env := newMemMPEnv(t, client, server)
		cleanupFakeMPEnv(t, env, clock)
		left, right := net.Pipe()
		defer func() { _ = left.Close(); _ = right.Close() }()
		tx := testChange(t, env.client, []MemberSpec{testMemberSpec("", Send)})
		attempt, err := tx.StartMember(0, func(context.Context, OpenRequest) ([]PipeIO, error) {
			return []PipeIO{NetPipe(left)}, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if view := attempt.Snapshot(); view.Attachments[0].Pipe == UnassignedPipeID || !view.Finished.IsZero() {
			t.Fatalf("expected an allocated Join awaiting its reply: %+v", view)
		}
		boot, _ := env.client.Pipe(0)
		if err := boot.Abort(); err != nil {
			t.Fatal(err)
		}
		if !env.client.ConnectionState().Suspended {
			t.Fatal("the pending send-only Join kept the receive direction alive")
		}
		advanceClock(clock, grace-2*time.Nanosecond)
		if view := attempt.Snapshot(); !view.Finished.IsZero() {
			t.Fatalf("pending Join finished early: %+v", view)
		}
		advanceClock(clock, time.Nanosecond)
		result := attempt.Snapshot().Result
		if result.Reason != ReasonDeadline || !errors.Is(result.Err, context.DeadlineExceeded) ||
			!env.client.ConnectionState().Suspended {
			t.Fatalf("Join outcome at the strict grace fence: %+v", result)
		}
	})
}

func TestAttemptDeadlineClampsToLastCoreTick(t *testing.T) {
	const latest = time.Duration(core.Never - 1)
	origin := time.Unix(1, 0)
	now := origin.Add(latest - time.Nanosecond)
	fake := timeshim.NewFake(now)
	c := &Conn{clk: clock{base: origin, ext: fake}, s: core.New(core.DefaultConfig(), core.Time(latest-time.Nanosecond))}
	tx := &CapacityChange{ctx: context.Background(), trialTimeout: time.Duration(core.Never)}
	if due, err := c.attemptDeadlineLocked(tx, now); err != nil || !due.Equal(origin.Add(latest)) {
		t.Fatalf("saturated deadline = %v, %v", due, err)
	}
	fake.Advance(time.Nanosecond)
	if _, err := c.attemptDeadlineLocked(tx, now); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline already at last tick = %v", err)
	}
}
