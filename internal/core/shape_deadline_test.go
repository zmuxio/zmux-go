package core

import (
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/wire"
	"github.com/zmuxio/zmux-go/shaping"
)

type scheduledShaper struct {
	initial Time
	fired   int
}

func (*scheduledShaper) Capabilities() shaping.Caps                    { return 0 }
func (*scheduledShaper) Plan(*shaping.RecordView, *shaping.RecordPlan) {}

func (s *scheduledShaper) OnEvent(e *shaping.Event, r *shaping.Effects) {
	if e.Kind == shaping.EventStart {
		r.Set, r.HasWake, r.Wake = shaping.EffectWake, true, time.Duration(s.initial)
		return
	}
	if e.Kind != shaping.EventTimer {
		return
	}
	s.fired++
	r.Set = shaping.EffectWake | shaping.EffectInject
	r.Injection = shaping.Injection{ID: 1, At: e.Now, PadBytes: 5}
}

func TestShaperDeadlineReplacementAndSameInstantInjection(t *testing.T) {
	s, first, second := quietWriterPipes()
	if !s.SetShapeBudget(shaping.Budget{Wire: &shaping.Account{Initial: 64, Ceiling: 64}}) {
		t.Fatal("cover account rejected")
	}
	const due = Time(Second)
	old := &scheduledShaper{initial: due - Time(Millisecond)}
	kept := &scheduledShaper{initial: due}
	late := &scheduledShaper{initial: due + Time(Millisecond)}
	s.SetPipeShaper(first.id, old)
	s.SetPipeShaper(second.id, kept)
	s.SetPipeShaper(first.id, late)
	if got := s.ShapingDeadline(); got != due {
		t.Fatalf("earliest shaping deadline = %v, want %v", got, due)
	}
	s.HandleTimeout(due - 1)
	if old.fired+kept.fired+late.fired != 0 {
		t.Fatal("a replaced or future deadline fired")
	}
	s.HandleTimeout(due)
	if old.fired != 0 || kept.fired != 1 || late.fired != 0 || !second.sh.injDue {
		t.Fatalf("wake counts %d/%d/%d, injection due = %t", old.fired, kept.fired, late.fired, second.sh.injDue)
	}
	frames := drainRequested(t, s, second)
	if len(frames) == 0 || frames[0] != wire.KindPadding {
		t.Fatalf("same-instant injection produced %v", frames)
	}
	s.shapePipeDown(first)
	s.HandleTimeout(late.initial)
	if kept.fired != 1 || late.fired != 0 {
		t.Fatalf("timer fired after clearing or stopping: %d/%d", kept.fired, late.fired)
	}
}

func TestShaperPreservesHardAckDeadlineAfterPromptWake(t *testing.T) {
	s, p := channelSession(t, nil, false)
	const start = Time(Second)
	s.SetNow(start)
	s.ack = newAckState(s)
	p.out.sig = &WriteSignal{}
	p.in.lanes[0].pos = 1
	s.SetPipeShaper(p.id, &delayShaper{delay: time.Second})
	s.ack.onRecord(p, 1, recordFacts{elicit: true}, start)
	if got := drainRequested(t, s, p); len(got) != 0 {
		t.Fatalf("the plan did not delay the prompt ACK: %v", got)
	}
	due := start.Add(s.ack.delay)
	if got := s.NextTimeout(); got != due || s.ShapingDeadline() != due {
		t.Fatalf("next timeout %v and shaping deadline %v, want %v", got, s.ShapingDeadline(), due)
	}
	s.HandleTimeout(due - 1)
	if p.out.sig.Ready.Load() {
		t.Fatal("ACK woke before the hard deadline")
	}
	s.HandleTimeout(due)
	if frames := drainRequested(t, s, p); len(frames) != 1 || frames[0] != wire.KindPipeAck {
		t.Fatalf("hard deadline frames = %v", frames)
	}
	if s.hasShapingDelay() {
		t.Fatal("the written plan left its delay registered")
	}
}
