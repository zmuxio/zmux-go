package runtime

import "testing"

func TestBatchSchedulerTrackAndUntrackExplicitGroup(t *testing.T) {
	t.Parallel()

	s := BatchScheduler{
		State: BatchState{
			GroupVirtualTime: map[GroupKey]uint64{{Kind: 1, Value: 7}: 11},
			GroupFinishTag:   map[GroupKey]uint64{{Kind: 1, Value: 7}: 13},
			GroupLastService: map[GroupKey]uint64{{Kind: 1, Value: 7}: 3},
		},
		ActiveGroupRefs: map[uint64]uint32{7: 2},
	}

	s.TrackExplicitGroup(9)
	if got := s.ActiveGroupRefs[9]; got != 1 {
		t.Fatalf("new group ref count = %d, want 1", got)
	}

	s.UntrackExplicitGroup(7)
	if got := s.ActiveGroupRefs[7]; got != 1 {
		t.Fatalf("remaining old group ref count = %d, want 1", got)
	}
	if _, ok := s.State.GroupFinishTag[GroupKey{Kind: 1, Value: 7}]; !ok {
		t.Fatal("shared explicit group tag unexpectedly dropped")
	}

	s.UntrackExplicitGroup(7)
	if _, ok := s.ActiveGroupRefs[7]; ok {
		t.Fatal("old explicit group ref still retained")
	}
	if _, ok := s.State.GroupVirtualTime[GroupKey{Kind: 1, Value: 7}]; ok {
		t.Fatal("old explicit group virtual time still retained")
	}
	if _, ok := s.State.GroupFinishTag[GroupKey{Kind: 1, Value: 7}]; ok {
		t.Fatal("old explicit group finish tag still retained")
	}
	if _, ok := s.State.GroupLastService[GroupKey{Kind: 1, Value: 7}]; ok {
		t.Fatal("old explicit group last-service still retained")
	}
}

func TestBatchSchedulerDropStreamClearsIdleState(t *testing.T) {
	t.Parallel()

	s := BatchScheduler{
		State: BatchState{
			RootVirtualTime:   19,
			ServiceSeq:        9,
			GroupVirtualTime:  map[GroupKey]uint64{{Kind: 0, Value: 4}: 5},
			GroupFinishTag:    map[GroupKey]uint64{{Kind: 0, Value: 4}: 7},
			GroupLastService:  map[GroupKey]uint64{{Kind: 0, Value: 4}: 8},
			GroupLag:          map[GroupKey]int64{{Kind: 0, Value: 4}: 1},
			StreamFinishTag:   map[uint64]uint64{4: 3},
			StreamLastService: map[uint64]uint64{4: 8},
			StreamLag:         map[uint64]int64{4: 1},
			StreamClass:       map[uint64]trafficClass{4: trafficClassInteractive},
			StreamLastSeenBatch: map[uint64]uint64{
				4: 1,
			},
			SmallBurstDisarmed: map[uint64]struct{}{4: {}},
			PreferredStreamHead: map[GroupKey]uint64{
				{Kind: 0, Value: 4}: 4,
			},
			scratch: batchScratch{
				groupOrder:  make([]GroupKey, 0, 1024),
				queuedBytes: map[uint64]uint64{4: 1},
				ordered:     make([]int, 0, 1024),
			},
		},
	}

	s.DropStream(4, false, 0)

	if len(s.State.StreamFinishTag) != 0 ||
		len(s.State.StreamLastService) != 0 ||
		len(s.State.StreamLag) != 0 ||
		len(s.State.StreamClass) != 0 ||
		len(s.State.StreamLastSeenBatch) != 0 ||
		len(s.State.SmallBurstDisarmed) != 0 ||
		len(s.State.GroupVirtualTime) != 0 ||
		len(s.State.GroupFinishTag) != 0 ||
		len(s.State.GroupLastService) != 0 ||
		len(s.State.GroupLag) != 0 ||
		len(s.State.PreferredStreamHead) != 0 {
		t.Fatalf("retained per-stream state not cleared: %#v", s.State)
	}
	if s.State.RootVirtualTime != 0 || s.State.ServiceSeq != 0 {
		t.Fatalf("scheduler clocks = (%d,%d), want (0,0)", s.State.RootVirtualTime, s.State.ServiceSeq)
	}
	if s.State.StreamFinishTag != nil ||
		s.State.StreamLastService != nil ||
		s.State.StreamLag != nil ||
		s.State.StreamClass != nil ||
		s.State.StreamLastSeenBatch != nil ||
		s.State.SmallBurstDisarmed != nil ||
		s.State.GroupVirtualTime != nil ||
		s.State.GroupFinishTag != nil ||
		s.State.GroupLastService != nil ||
		s.State.GroupLag != nil ||
		s.State.PreferredStreamHead != nil {
		t.Fatalf("idle scheduler map storage retained: %#v", s.State)
	}
	if cap(s.State.scratch.groupOrder) != 0 || s.State.scratch.queuedBytes != nil || cap(s.State.scratch.ordered) != 0 {
		t.Fatalf("idle scheduler scratch retained: %#v", s.State.scratch)
	}
}

func TestBatchSchedulerClearResetsSchedulerOwnership(t *testing.T) {
	t.Parallel()

	s := BatchScheduler{
		State: BatchState{
			RootVirtualTime: 17,
			ServiceSeq:      4,
			StreamFinishTag: map[uint64]uint64{4: 1},
		},
		ActiveGroupRefs: map[uint64]uint32{7: 1},
	}

	s.Clear()

	if len(s.ActiveGroupRefs) != 0 {
		t.Fatalf("activeGroupRefs count = %d, want 0", len(s.ActiveGroupRefs))
	}
	if len(s.State.StreamFinishTag) != 0 ||
		len(s.State.StreamLastService) != 0 ||
		len(s.State.GroupVirtualTime) != 0 ||
		len(s.State.GroupFinishTag) != 0 ||
		len(s.State.GroupLastService) != 0 ||
		s.State.RootVirtualTime != 0 ||
		s.State.ServiceSeq != 0 {
		t.Fatalf("scheduler state not cleared: %#v", s.State)
	}
}

func TestBatchSchedulerTrackedExplicitGroupCountIgnoresFallbackAndZero(t *testing.T) {
	t.Parallel()

	s := BatchScheduler{
		ActiveGroupRefs: map[uint64]uint32{
			0:                   1,
			7:                   2,
			9:                   1,
			FallbackGroupBucket: 3,
		},
	}

	if got := s.TrackedExplicitGroupCount(); got != 2 {
		t.Fatalf("TrackedExplicitGroupCount() = %d, want 2", got)
	}
}
