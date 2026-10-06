package runtime

import "testing"

func TestPlanQueueReleaseWake(t *testing.T) {
	plan := PlanQueueReleaseWake(90, 80, 80, 10, 4, 4, 8, 3, 4, false)
	if !plan.Broadcast {
		t.Fatal("Broadcast = false, want true when session queue crosses low watermark")
	}
	if plan.StreamWake {
		t.Fatal("StreamWake = true, want false when broadcast already covers wake")
	}
	if plan.Control {
		t.Fatal("Control = true, want false without memory wake or urgent release")
	}
}

func TestPlanPreparedReleaseWakePrefersStreamWakeWhenOnlyStreamCreditReturns(t *testing.T) {
	plan := PlanPreparedReleaseWake(10, 10, 80, 4, 4, 4, 6, 3, 4, 1, 1, 0, 5, false)
	if plan.Broadcast {
		t.Fatal("Broadcast = true, want false without session or memory wake")
	}
	if !plan.StreamWake {
		t.Fatal("StreamWake = false, want true when only stream credit returns")
	}
	if plan.Control {
		t.Fatal("Control = true, want false without memory wake or urgent release")
	}
}

func TestPlanPreparedReleaseWakeDoesNotLetMemoryWakeSuppressStreamWake(t *testing.T) {
	plan := PlanPreparedReleaseWake(70, 60, 80, 4, 4, 4, 6, 3, 4, 1, 1, 0, 5, false)
	if !plan.Broadcast {
		t.Fatal("Broadcast = false, want true for memory wake")
	}
	if !plan.StreamWake {
		t.Fatal("StreamWake = false, want true when stream credit returns")
	}
	if !plan.Control {
		t.Fatal("Control = false, want true for memory wake")
	}
}

func TestPlanLaneReleaseWakeSignalsControlForUrgentRelease(t *testing.T) {
	plan := PlanLaneReleaseWake(10, 10, 80, true)
	if plan.Broadcast {
		t.Fatal("Broadcast = true, want false without memory wake")
	}
	if !plan.Control {
		t.Fatal("Control = false, want true for urgent lane release")
	}
}

func TestPlanLaneReleaseWakeBroadcastsOnMemoryRelief(t *testing.T) {
	plan := PlanLaneReleaseWake(100, 60, 80, false)
	if !plan.Broadcast {
		t.Fatal("Broadcast = false, want true when memory falls below threshold")
	}
	if !plan.Control {
		t.Fatal("Control = false, want true when memory wake occurs")
	}
}

func TestQueueReleaseWakes(t *testing.T) {
	tests := []struct {
		name            string
		prev, next, lwm uint64
		want            bool
	}{
		{name: "crosses_low_watermark", prev: 6, next: 4, lwm: 4, want: true},
		{name: "stays_above_low_watermark", prev: 8, next: 6, lwm: 4, want: false},
		{name: "drains_below_low_watermark", prev: 3, next: 0, lwm: 4, want: true},
		{name: "shrinks_below_low_watermark", prev: 3, next: 1, lwm: 4, want: false},
		{name: "already_empty", prev: 0, next: 0, lwm: 4, want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := QueueReleaseWakes(tc.prev, tc.next, tc.lwm); got != tc.want {
				t.Fatalf("QueueReleaseWakes(%d, %d, %d) = %v, want %v", tc.prev, tc.next, tc.lwm, got, tc.want)
			}
		})
	}
}

// A request larger than the gap between the watermarks can be blocked by a
// queue that is already below the low watermark; draining that queue must
// wake it even when session memory did not fall (no memory wake).
func TestQueueReleaseWakePlansWakeOnDrainBelowLowWatermark(t *testing.T) {
	plan := PlanQueueReleaseWake(90, 90, 80, 3, 0, 4, 2, 2, 4, false)
	if !plan.Broadcast {
		t.Fatal("PlanQueueReleaseWake Broadcast = false, want true when the session queue drains")
	}
	plan = PlanQueueReleaseWake(90, 90, 80, 9, 6, 4, 3, 0, 4, false)
	if plan.Broadcast || !plan.StreamWake {
		t.Fatalf("PlanQueueReleaseWake = %+v, want a stream wake when only the stream queue drains", plan)
	}
	plan = PlanPreparedReleaseWake(90, 90, 80, 3, 0, 4, 2, 2, 4, 1, 1, 1, 1, false)
	if !plan.Broadcast {
		t.Fatal("PlanPreparedReleaseWake Broadcast = false, want true when the session queue drains")
	}
}
