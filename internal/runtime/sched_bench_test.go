package runtime

import (
	"testing"

	"github.com/zmuxio/zmux-go/internal/wire"
)

func BenchmarkNewBatchScheduler(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = NewBatchScheduler()
	}
}

func BenchmarkOrderBatchIndicesClassAwareMixedBatch(b *testing.B) {
	b.ReportAllocs()

	items := []BatchItem{
		{Request: RequestMeta{GroupKey: GroupKey{Kind: 0, Value: 4}, StreamID: 4, StreamScoped: true, Cost: 64}, Stream: StreamMeta{}},
		{Request: RequestMeta{GroupKey: GroupKey{Kind: 0, Value: 4}, StreamID: 4, StreamScoped: true, Cost: 64}, Stream: StreamMeta{}},
		{Request: RequestMeta{GroupKey: GroupKey{Kind: 0, Value: 8}, StreamID: 8, StreamScoped: true, Cost: 900}, Stream: StreamMeta{}},
		{Request: RequestMeta{GroupKey: GroupKey{Kind: 0, Value: 8}, StreamID: 8, StreamScoped: true, Cost: 900}, Stream: StreamMeta{}},
		{Request: RequestMeta{GroupKey: GroupKey{Kind: 1, Value: 11}, StreamID: 12, StreamScoped: true, Cost: 96}, Stream: StreamMeta{Priority: 4}},
		{Request: RequestMeta{GroupKey: GroupKey{Kind: 1, Value: 11}, StreamID: 16, StreamScoped: true, Cost: 1200}, Stream: StreamMeta{}},
		{Request: RequestMeta{GroupKey: GroupKey{Kind: 1, Value: 13}, StreamID: 20, StreamScoped: true, Cost: 128}, Stream: StreamMeta{Priority: 8}},
		{Request: RequestMeta{GroupKey: GroupKey{Kind: 1, Value: 13}, StreamID: 24, StreamScoped: true, Cost: 1400}, Stream: StreamMeta{}},
		{Request: RequestMeta{GroupKey: GroupKey{Kind: 0, Value: 12}, StreamID: 12, StreamScoped: true, IsPriorityUpdate: true, Cost: 1}, Stream: StreamMeta{Priority: 4}},
		{Request: RequestMeta{GroupKey: GroupKey{Kind: 2, Value: 1}, Cost: 1}},
	}
	cfg := BatchConfig{
		GroupFair:       true,
		SchedulerHint:   wire.SchedulerBalancedFair,
		MaxFramePayload: 1024,
	}
	state := &BatchState{}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = OrderBatchIndices(cfg, state, items)
	}
}
