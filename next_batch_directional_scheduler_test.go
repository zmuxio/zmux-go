package zmux

import (
	"io"
	"testing"
	"testing/synctest"
)

func TestDirectionalSchedulerUsesOnlyLocalSendPolicy(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		placed := make(chan Event, 8)
		cfg := mpConfig()
		cfg.ReceiveScheduler = SchedulerSessionAffinity()
		cfg.Tracer = &Tracer{StreamEvents: true, Event: func(_ SessionID, ev Event) {
			if ev.Kind == EventStreamPlaced {
				select {
				case placed <- ev:
				default:
				}
			}
		}}
		e := newMemMPEnv(t, cfg, mpConfig())
		standby := e.addPipe(t, PipeOptions{SendPriority: Set(uint64(1)), SendStandby: Set(true)})
		if s, err := e.client.DirectionalScheduler(DirectionReceiveOnly); err != nil || s != SchedulerSessionAffinity() {
			t.Fatalf("initial receive scheduler: %v, %v", s, err)
		}
		if err := e.client.SetDirectionalScheduler(DirectionSendOnly, SchedulerSpreadRoundRobin()); err != nil {
			t.Fatal(err)
		}
		if err := e.client.SetDirectionalScheduler(DirectionReceiveOnly, SchedulerSpreadETA()); err != nil {
			t.Fatal(err)
		}
		if s, err := e.client.DirectionalScheduler(DirectionReceiveOnly); err != nil || s != SchedulerSpreadETA() {
			t.Fatalf("receive scheduler: %v, %v", s, err)
		}
		if e.client.Scheduler() != SchedulerSpreadRoundRobin() || e.server.Scheduler() != SchedulerAffinity() {
			t.Fatal("receive preference changed a send scheduler")
		}
		stream, err := e.client.OpenUniStream()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := stream.Write([]byte("a")); err != nil {
			t.Fatal(err)
		}
		if err := stream.Close(); err != nil {
			t.Fatal(err)
		}
		received, err := e.server.AcceptUniStream(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		payload, err := io.ReadAll(received)
		if err != nil || string(payload) != "a" {
			t.Fatalf("received %q: %v", payload, err)
		}
		synctest.Wait()
		select {
		case ev := <-placed:
			if ev.PipeID != 0 || ev.StreamID != stream.StreamID() {
				t.Fatalf("placement %+v with standby pipe %v", ev, standby.ID())
			}
		default:
			t.Fatal("missing stream placement")
		}
	})
}
