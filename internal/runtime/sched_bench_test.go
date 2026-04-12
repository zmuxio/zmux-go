package runtime

import "testing"

func BenchmarkNewBatchScheduler(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = NewBatchScheduler()
	}
}
