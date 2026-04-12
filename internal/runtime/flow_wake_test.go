package runtime

import "testing"

func TestPlanQueueReleaseWake(t *testing.T) {
	plan := PlanQueueReleaseWake(70, 60, 80, 10, 4, 4, 8, 3, 4, false)
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
