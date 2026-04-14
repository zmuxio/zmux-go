package runtime

import "testing"

func TestVisibleAcceptBacklogBytesHardCap(t *testing.T) {
	t.Parallel()

	if got := VisibleAcceptBacklogBytesHardCap(0); got != 4<<20 {
		t.Fatalf("VisibleAcceptBacklogBytesHardCap(0) = %d, want %d", got, 4<<20)
	}
	if got := VisibleAcceptBacklogBytesHardCap(16 * 1024); got != 4<<20 {
		t.Fatalf("VisibleAcceptBacklogBytesHardCap(16KiB) = %d, want %d", got, 4<<20)
	}
	if got := VisibleAcceptBacklogBytesHardCap(128 * 1024); got != 8<<20 {
		t.Fatalf("VisibleAcceptBacklogBytesHardCap(128KiB) = %d, want %d", got, 8<<20)
	}
}

func TestLateDataPerStreamCap(t *testing.T) {
	t.Parallel()

	if got := LateDataPerStreamCap(64*1024, 16*1024); got != 8*1024 {
		t.Fatalf("LateDataPerStreamCap(64KiB,16KiB) = %d, want %d", got, 8*1024)
	}
	if got := LateDataPerStreamCap(1<<20, 16*1024); got != 32*1024 {
		t.Fatalf("LateDataPerStreamCap(1MiB,16KiB) = %d, want %d", got, 32*1024)
	}
	if got := LateDataPerStreamCap(0, 16*1024); got != 1024 {
		t.Fatalf("LateDataPerStreamCap(0,16KiB) = %d, want 1024", got)
	}
}

func TestAggregateLateDataCap(t *testing.T) {
	t.Parallel()

	if got := AggregateLateDataCap(0); got != 64*1024 {
		t.Fatalf("AggregateLateDataCap(0) = %d, want %d", got, 64*1024)
	}
	if got := AggregateLateDataCap(16 * 1024); got != 64*1024 {
		t.Fatalf("AggregateLateDataCap(16KiB) = %d, want %d", got, 64*1024)
	}
	if got := AggregateLateDataCap(32 * 1024); got != 128*1024 {
		t.Fatalf("AggregateLateDataCap(32KiB) = %d, want %d", got, 128*1024)
	}
}
