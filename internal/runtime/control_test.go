package runtime

import "testing"

func TestProjectTrackedMemoryDelta(t *testing.T) {
	if got := ProjectTrackedMemoryDelta(100, 30, 20); got != 90 {
		t.Fatalf("ProjectTrackedMemoryDelta(100, 30, 20) = %d, want 90", got)
	}
	if got := ProjectTrackedMemoryDelta(10, 20, 5); got != 5 {
		t.Fatalf("ProjectTrackedMemoryDelta clamps removal to zero, got %d want 5", got)
	}
}

func TestPlanPendingPriorityUpdate(t *testing.T) {
	plan := PlanPendingPriorityUpdate(200, 40, 10, 12, 64, 220)
	if !plan.Accept {
		t.Fatal("Accept = false, want true within budget and hard cap")
	}
	if plan.NextPendingBytes != 42 {
		t.Fatalf("NextPendingBytes = %d, want 42", plan.NextPendingBytes)
	}
	if plan.ProjectedTracked != 202 {
		t.Fatalf("ProjectedTracked = %d, want 202", plan.ProjectedTracked)
	}

	rejectBudget := PlanPendingPriorityUpdate(200, 40, 0, 80, 64, 300)
	if rejectBudget.Accept {
		t.Fatal("Accept = true, want false when pending bytes exceed budget")
	}

	rejectCap := PlanPendingPriorityUpdate(200, 40, 0, 20, 64, 179)
	if rejectCap.Accept {
		t.Fatal("Accept = true, want false when tracked memory exceeds hard cap")
	}
}

func TestPlanPriorityAdvisoryHandoff(t *testing.T) {
	plan := PlanPriorityAdvisoryHandoff(200, 30, 18, 220)
	if !plan.Accept {
		t.Fatal("Accept = false, want true within hard cap")
	}
	if plan.ProjectedTracked != 188 {
		t.Fatalf("ProjectedTracked = %d, want 188", plan.ProjectedTracked)
	}

	reject := PlanPriorityAdvisoryHandoff(200, 10, 40, 220)
	if reject.Accept {
		t.Fatal("Accept = true, want false when handoff would exceed hard cap")
	}
}
