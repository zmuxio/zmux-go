package zmux

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

func TestCarrierLivenessDefaultsAreAppliedToJoin(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newMemMPEnv(t, mpConfig(), mpConfig())
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		pio, err := e.pipeDialer()(ctx)
		if err != nil {
			t.Fatal(err)
		}
		pio.Liveness = PipeLiveness{ProbeInterval: 2 * time.Second, ProbeJitter: -1}
		p, err := e.client.AddPipe(ctx, pio, PipeOptions{Liveness: Set(PipeLiveness{ProbeInterval: 3 * time.Second})})
		if err != nil {
			t.Fatal(err)
		}
		got, _ := p.Options().Liveness.get()
		if want := (PipeLiveness{ProbeInterval: 3 * time.Second, ProbeJitter: -1}); got != want {
			t.Fatalf("pipe liveness %+v, want %+v", got, want)
		}
	})
}

func TestSplitReplacementRequiresCurrentReceiveRecord(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const hold = 5 * time.Second
		clk, cc, sc := fakeConfigs(func(c *Config) {
			c.StabilityPolicy.HoldFor = Set(hold)
			c.JoinTimeout = time.Minute
			c.SessionPolicy.Limits.SteadyAttachments = 6
			c.Liveness.ProbeInterval = -1
			c.Liveness.KeepAlivePeriod = -1
		})
		e := newMemMPEnv(t, cc, sc)
		cleanupFakeMPEnv(t, e, clk)
		old := testMember(t, e, "old", Send, Receive)
		tx := testChange(t, e.client, []MemberSpec{testMemberSpec("replacement", Send, Receive)}, old.Member)
		replacement := startTestMember(t, tx, testMemberOpener(e))
		rx, _ := e.client.Pipe(replacement.Pipes[1])
		txPeer, _ := e.server.Pipe(replacement.Pipes[1])
		first, ok := rx.Health()
		if !ok || first.Receive.Proof != PipeHealthProofRecord || first.AttachedAt.IsZero() ||
			!first.Receive.StableSince.Equal(first.AttachedAt) || first.Receive.Settled {
			t.Fatalf("initial verified record: %+v", first)
		}
		if err := rx.Revalidate(); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Commit(); !errors.Is(err, ErrChangePending) {
			t.Fatalf("before settle: %v", err)
		}
		advanceClock(clk, hold)
		waiting, _ := rx.Health()
		if waiting.Receive.Proof != PipeHealthProofNone || waiting.Receive.Settled {
			t.Fatalf("prior receive proof survived revalidation: %+v", waiting)
		}
		if _, err := tx.Commit(); !errors.Is(err, ErrChangePending) {
			t.Fatalf("without current receive proof: %v", err)
		}
		if err := txPeer.Revalidate(); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		proved, _ := rx.Health()
		// The record restores receive service after revalidation.
		if proved.Receive.Proof != PipeHealthProofRecord || !proved.Receive.StableSince.Equal(clk.Now()) || proved.Receive.Generation <= first.Receive.Generation || proved.Receive.Settled {
			t.Fatalf("fresh receive record: %+v", proved)
		}
		advanceClock(clk, hold-time.Microsecond)
		if _, err := tx.Commit(); !errors.Is(err, ErrChangePending) {
			t.Fatalf("before the recovered receive direction settles: %v", err)
		}
		advanceClock(clk, time.Microsecond)
		result, err := tx.Commit()
		if err != nil || len(result.Draining) != 1 || result.Draining[0] != old.Member || result.Capacity.Covered != Both {
			send, _ := e.client.Pipe(replacement.Pipes[0])
			sendHealth, _ := send.Health()
			stats, _ := send.Stats()
			t.Fatalf("replacement commit: %+v, %v, send health %+v, probes %d/%d", result, err, sendHealth,
				stats.PingsAnswered, stats.PingsSent)
		}
	})
}

func TestAttemptIDExhaustionDoesNotCallOpener(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clk, cc, sc := fakeConfigs(func(c *Config) { c.MaxIdleTimeout = -1 })
		e := newMemMPEnv(t, cc, sc)
		cleanupFakeMPEnv(t, e, clk)
		tx := testChange(t, e.client, []MemberSpec{testMemberSpec("candidate", Both)})
		e.client.locked(func() { e.client.nextAttempt = ^AttemptID(0) })
		attempt, err := tx.StartMember(0, func(context.Context, OpenRequest) ([]PipeIO, error) {
			t.Error("exhausted ID space invoked the opener")
			return nil, ErrAttemptIDExhausted
		})
		if attempt != nil || !errors.Is(err, ErrAttemptIDExhausted) {
			t.Fatalf("attempt %v, error %v", attempt, err)
		}
	})
}
