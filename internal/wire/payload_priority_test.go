package wire

import (
	"errors"
	"testing"
)

func TestBuildPriorityUpdatePayloadValidation(t *testing.T) {
	t.Parallel()

	if _, err := BuildPriorityUpdatePayload(CapabilityPriorityUpdate|CapabilityPriorityHints, nil, nil, 1024); !errors.Is(err, ErrEmptyMetadataUpdate) {
		t.Fatalf("empty metadata err = %v, want %v", err, ErrEmptyMetadataUpdate)
	}

	priority := uint64(7)
	if _, err := BuildPriorityUpdatePayload(CapabilityPriorityHints, &priority, nil, 1024); !errors.Is(err, ErrPriorityUpdateUnavailable) {
		t.Fatalf("missing capability err = %v, want %v", err, ErrPriorityUpdateUnavailable)
	}

	if _, err := BuildPriorityUpdatePayload(CapabilityPriorityUpdate|CapabilityPriorityHints, &priority, nil, 1); !errors.Is(err, ErrPriorityUpdateTooLarge) {
		t.Fatalf("oversize err = %v, want %v", err, ErrPriorityUpdateTooLarge)
	}
}

func TestBuildPriorityUpdatePayloadRoundTrip(t *testing.T) {
	t.Parallel()

	priority := uint64(5)
	group := uint64(9)
	caps := CapabilityPriorityUpdate | CapabilityPriorityHints | CapabilityStreamGroups

	payload, err := BuildPriorityUpdatePayload(caps, &priority, &group, 1024)
	if err != nil {
		t.Fatalf("BuildPriorityUpdatePayload err = %v", err)
	}

	subtype, n, err := ParseVarint(payload)
	if err != nil {
		t.Fatalf("ParseVarint subtype err = %v", err)
	}
	if EXTSubtype(subtype) != EXTPriorityUpdate {
		t.Fatalf("subtype = %d, want %d", subtype, EXTPriorityUpdate)
	}

	meta, has, err := ParsePriorityUpdatePayload(payload[n:])
	if err != nil {
		t.Fatalf("ParsePriorityUpdatePayload err = %v", err)
	}
	if !has {
		t.Fatal("ParsePriorityUpdatePayload has = false, want true")
	}
	if !meta.HasPriority || meta.Priority != priority {
		t.Fatalf("priority = (%v, %d), want (true, %d)", meta.HasPriority, meta.Priority, priority)
	}
	if !meta.HasGroup || meta.Group != group {
		t.Fatalf("group = (%v, %d), want (true, %d)", meta.HasGroup, meta.Group, group)
	}
}

func TestAppendPriorityUpdatePayloadTightensOversizedReuseBuffer(t *testing.T) {
	t.Parallel()

	priority := uint64(7)
	oversized := make([]byte, 0, 64)
	payload, err := AppendPriorityUpdatePayload(oversized, CapabilityPriorityUpdate|CapabilityPriorityHints, &priority, nil, 1024)
	if err != nil {
		t.Fatalf("AppendPriorityUpdatePayload err = %v", err)
	}

	if got := cap(payload); got != len(payload) {
		t.Fatalf("cap(payload) = %d, want tight cap %d", got, len(payload))
	}
}
