package zmux

import (
	"bytes"
	"errors"
	"io"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	rt "github.com/zmuxio/zmux-go/internal/runtime"
	"github.com/zmuxio/zmux-go/internal/state"
)

func TestCollectWriteBatchDrainsReadyFrames(t *testing.T) {
	t.Parallel()
	c := &Conn{
		writer: connWriterRuntimeState{writeCh: make(chan writeRequest, 8)},
	}

	first := writeRequest{
		frames: testTxFramesFrom([]Frame{{Type: FrameTypePING, Payload: []byte{0, 0, 0, 0, 0, 0, 0, 1}}}),
		done:   make(chan error, 1),
	}
	second := writeRequest{
		frames: testTxFramesFrom([]Frame{{Type: FrameTypePONG, Payload: []byte{0, 0, 0, 0, 0, 0, 0, 2}}}),
		done:   make(chan error, 1),
	}
	third := writeRequest{
		frames: testTxFramesFrom([]Frame{{Type: FrameTypePONG, Payload: []byte{0, 0, 0, 0, 0, 0, 0, 3}}}),
		done:   make(chan error, 1),
	}

	c.writer.writeCh <- second
	c.writer.writeCh <- third

	batch := c.collectWriteBatch(first, writeLaneOrdinary)
	if len(batch) != 3 {
		t.Fatalf("batch len = %d, want 3", len(batch))
	}
	if batch[0].frames[0].Type != FrameTypePING || batch[1].frames[0].Type != FrameTypePONG || batch[2].frames[0].Payload[7] != 3 {
		t.Fatalf("unexpected batch ordering: %+v", batch)
	}
}

func BenchmarkCollectWriteBatchLaneBuffering(b *testing.B) {
	payload := bytes.Repeat([]byte("x"), 32)
	proto := writeRequest{
		frames: testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: 4, Payload: payload}}),
	}

	for _, tc := range []struct {
		name        string
		ordinaryCap int
		advisoryCap int
	}{
		{name: "unbuffered", ordinaryCap: 0, advisoryCap: 0},
		{name: "buffered", ordinaryCap: 64, advisoryCap: 16},
	} {
		b.Run(tc.name, func(b *testing.B) {
			c := &Conn{
				writer: connWriterRuntimeState{
					writeCh:         make(chan writeRequest, tc.ordinaryCap),
					advisoryWriteCh: make(chan writeRequest, tc.advisoryCap),
				},
				registry: connRegistryState{streams: map[uint64]*nativeStream{
					4: {id: 4, idSet: true},
				}},
			}

			var (
				totalRequests uint64
				totalBatches  uint64
			)
			consumerDone := make(chan struct{})
			go func() {
				defer close(consumerDone)
				for first := range c.writer.writeCh {
					batch := c.collectWriteBatch(first, writeLaneOrdinary)
					atomic.AddUint64(&totalRequests, uint64(len(batch)))
					atomic.AddUint64(&totalBatches, 1)
				}
			}()

			var blocked time.Duration
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				start := time.Now()
				c.writer.writeCh <- proto
				blocked += time.Since(start)
			}
			b.StopTimer()

			close(c.writer.writeCh)
			<-consumerDone

			requests := atomic.LoadUint64(&totalRequests)
			batches := atomic.LoadUint64(&totalBatches)
			if batches > 0 {
				b.ReportMetric(float64(requests)/float64(batches), "reqs_per_batch")
			}
			b.ReportMetric(float64(batches), "flushes")
			if b.N > 0 {
				b.ReportMetric(float64(blocked.Nanoseconds())/float64(b.N), "send_block_ns/op")
			}
		})
	}
}

func TestCollectWriteBatchInterleavesStreamsByDefault(t *testing.T) {
	t.Parallel()
	c := &Conn{
		writer: connWriterRuntimeState{writeCh: make(chan writeRequest, 8)}, registry: connRegistryState{streams: map[uint64]*nativeStream{
			4: {id: 4, idSet: true},
			8: {id: 8, idSet: true},
		}},
	}

	first := writeRequest{
		frames: testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: 4, Payload: []byte("a")}}),
		done:   make(chan error, 1),
	}
	second := writeRequest{
		frames: testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: 4, Payload: []byte("b")}}),
		done:   make(chan error, 1),
	}
	third := writeRequest{
		frames: testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: 8, Payload: []byte("c")}}),
		done:   make(chan error, 1),
	}

	c.writer.writeCh <- second
	c.writer.writeCh <- third

	batch := c.collectWriteBatch(first, writeLaneOrdinary)
	if got := []uint64{
		batch[0].frames[0].StreamID,
		batch[1].frames[0].StreamID,
		batch[2].frames[0].StreamID,
	}; !equalUint64s(got, []uint64{4, 8, 4}) {
		t.Fatalf("default batch order = %v, want [4 8 4]", got)
	}
}

func TestCollectWriteBatchKeepsSameStreamBurstOrder(t *testing.T) {
	t.Parallel()

	c := &Conn{
		writer: connWriterRuntimeState{writeCh: make(chan writeRequest, 8)}, registry: connRegistryState{streams: map[uint64]*nativeStream{
			4: {id: 4, idSet: true},
		}},
	}

	first := writeRequest{
		frames: testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: 4, Payload: []byte("a")}}),
		done:   make(chan error, 1),
	}
	second := writeRequest{
		frames: testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: 4, Payload: []byte("b")}}),
		done:   make(chan error, 1),
	}
	third := writeRequest{
		frames: testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: 4, Payload: []byte("c")}}),
		done:   make(chan error, 1),
	}

	c.writer.writeCh <- second
	c.writer.writeCh <- third

	batch := c.collectWriteBatch(first, writeLaneOrdinary)
	if len(batch) != 3 {
		t.Fatalf("batch len = %d, want 3", len(batch))
	}
	if got := []string{
		string(batch[0].frames[0].Payload),
		string(batch[1].frames[0].Payload),
		string(batch[2].frames[0].Payload),
	}; got[0] != "a" || got[1] != "b" || got[2] != "c" {
		t.Fatalf("same-stream batch order = %v, want [a b c]", got)
	}
}

func TestCollectWriteBatchRotatesFlatBatchHeadAcrossBatches(t *testing.T) {
	t.Parallel()

	c := &Conn{
		writer: connWriterRuntimeState{writeCh: make(chan writeRequest, 8)}, registry: connRegistryState{streams: map[uint64]*nativeStream{
			4: {id: 4, idSet: true},
			8: {id: 8, idSet: true},
		}},
	}

	newReq := func(streamID uint64, payload byte) writeRequest {
		return writeRequest{
			frames: testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: streamID, Payload: []byte{payload}}}),
			done:   make(chan error, 1),
		}
	}

	c.writer.writeCh <- newReq(8, 'b')
	firstBatch := c.collectWriteBatch(newReq(4, 'a'), writeLaneOrdinary)
	if got := []uint64{
		firstBatch[0].frames[0].StreamID,
		firstBatch[1].frames[0].StreamID,
	}; !equalUint64s(got, []uint64{4, 8}) {
		t.Fatalf("first batch order = %v, want [4 8]", got)
	}

	c.writer.writeCh <- newReq(8, 'd')
	secondBatch := c.collectWriteBatch(newReq(4, 'c'), writeLaneOrdinary)
	if got := []uint64{
		secondBatch[0].frames[0].StreamID,
		secondBatch[1].frames[0].StreamID,
	}; !equalUint64s(got, []uint64{8, 4}) {
		t.Fatalf("second batch order = %v, want [8 4]", got)
	}
}

func TestCollectWriteBatchPrefersLowerCostPeerWithinSingleExplicitGroup(t *testing.T) {
	t.Parallel()
	c := &Conn{
		writer: connWriterRuntimeState{writeCh: make(chan writeRequest, 8)},

		config: connConfigState{peer: Preface{
			Settings: Settings{
				MaxFramePayload: 16384,
				SchedulerHints:  SchedulerGroupFair,
			},
		}}, registry: connRegistryState{streams: map[uint64]*nativeStream{
			4: {id: 4, idSet: true, group: 7, groupExplicit: true},
			8: {id: 8, idSet: true, group: 7, groupExplicit: true},
		}},
	}

	first := writeRequest{
		frames: testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: 4, Payload: make([]byte, 20000)}}),
		done:   make(chan error, 1),
	}
	second := writeRequest{
		frames: testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: 8, Payload: []byte("small")}}),
		done:   make(chan error, 1),
	}

	c.writer.writeCh <- second
	batch := c.collectWriteBatch(first, writeLaneOrdinary)
	if got := []uint64{
		batch[0].frames[0].StreamID,
		batch[1].frames[0].StreamID,
	}; !equalUint64s(got, []uint64{8, 4}) {
		t.Fatalf("single-group deficit batch order = %v, want [8 4]", got)
	}
}

func TestCollectWriteBatchPrefersLowerCostGroupAcrossExplicitGroups(t *testing.T) {
	t.Parallel()
	c := &Conn{
		writer: connWriterRuntimeState{writeCh: make(chan writeRequest, 8)},

		config: connConfigState{peer: Preface{
			Settings: Settings{
				MaxFramePayload: 16384,
				SchedulerHints:  SchedulerGroupFair,
			},
		}}, registry: connRegistryState{streams: map[uint64]*nativeStream{
			4: {id: 4, idSet: true, group: 7, groupExplicit: true},
			8: {id: 8, idSet: true, group: 9, groupExplicit: true},
		}},
	}

	first := writeRequest{
		frames: testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: 4, Payload: make([]byte, 20000)}}),
		done:   make(chan error, 1),
	}
	second := writeRequest{
		frames: testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: 8, Payload: []byte("small")}}),
		done:   make(chan error, 1),
	}

	c.writer.writeCh <- second
	batch := c.collectWriteBatch(first, writeLaneOrdinary)
	if got := []uint64{
		batch[0].frames[0].StreamID,
		batch[1].frames[0].StreamID,
	}; !equalUint64s(got, []uint64{8, 4}) {
		t.Fatalf("group-level deficit batch order = %v, want [8 4]", got)
	}
}

func TestCollectWriteBatchPrefersHigherPriorityShortFlow(t *testing.T) {
	t.Parallel()
	c := &Conn{
		writer: connWriterRuntimeState{writeCh: make(chan writeRequest, 8)},

		config: connConfigState{peer: Preface{
			Settings: Settings{
				MaxFramePayload: 16384,
			},
		}}, registry: connRegistryState{streams: map[uint64]*nativeStream{
			4: {id: 4, idSet: true},
			8: {id: 8, idSet: true, priority: 20},
		}},
	}

	first := writeRequest{
		frames: testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: 4, Payload: make([]byte, 40000)}}),
		done:   make(chan error, 1),
	}
	second := writeRequest{
		frames: testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: 8, Payload: make([]byte, 40000)}}),
		done:   make(chan error, 1),
	}

	c.writer.writeCh <- second
	batch := c.collectWriteBatch(first, writeLaneOrdinary)
	if got := []uint64{
		batch[0].frames[0].StreamID,
		batch[1].frames[0].StreamID,
	}; !equalUint64s(got, []uint64{8, 4}) {
		t.Fatalf("class-aware batch order = %v, want [8 4]", got)
	}
}

func TestCollectWriteBatchBulkThroughputRetainsPriorityUnderWFQ(t *testing.T) {
	t.Parallel()
	c := &Conn{
		writer: connWriterRuntimeState{writeCh: make(chan writeRequest, 8)},

		config: connConfigState{peer: Preface{
			Settings: Settings{
				MaxFramePayload: 16384,
				SchedulerHints:  SchedulerBulkThroughput,
			},
		}}, registry: connRegistryState{streams: map[uint64]*nativeStream{
			4: {id: 4, idSet: true},
			8: {id: 8, idSet: true, priority: 20},
		}},
	}

	first := writeRequest{
		frames: testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: 8, Payload: make([]byte, 40000)}}),
		done:   make(chan error, 1),
	}
	second := writeRequest{
		frames: testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: 4, Payload: make([]byte, 40000)}}),
		done:   make(chan error, 1),
	}

	c.writer.writeCh <- second
	batch := c.collectWriteBatch(first, writeLaneOrdinary)
	if got := []uint64{
		batch[0].frames[0].StreamID,
		batch[1].frames[0].StreamID,
	}; !equalUint64s(got, []uint64{8, 4}) {
		t.Fatalf("bulk-throughput WFQ order = %v, want [8 4]", got)
	}
}

func TestCollectWriteBatchRetainsHigherPriorityHeadAcrossBatches(t *testing.T) {
	t.Parallel()
	c := &Conn{
		writer: connWriterRuntimeState{writeCh: make(chan writeRequest, 8)},

		config: connConfigState{peer: Preface{
			Settings: Settings{
				MaxFramePayload: 16384,
			},
		}}, registry: connRegistryState{streams: map[uint64]*nativeStream{
			4: {id: 4, idSet: true},
			8: {id: 8, idSet: true, priority: 20},
		}},
	}

	newReq := func(streamID uint64, n int) writeRequest {
		return writeRequest{
			frames: testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: streamID, Payload: make([]byte, n)}}),
			done:   make(chan error, 1),
		}
	}

	c.writer.writeCh <- newReq(4, 40000)
	firstBatch := c.collectWriteBatch(newReq(8, 40000), writeLaneOrdinary)
	if got := []uint64{
		firstBatch[0].frames[0].StreamID,
		firstBatch[1].frames[0].StreamID,
	}; !equalUint64s(got, []uint64{8, 4}) {
		t.Fatalf("first competitive batch order = %v, want [8 4]", got)
	}

	c.writer.writeCh <- newReq(4, 40000)
	secondBatch := c.collectWriteBatch(newReq(8, 40000), writeLaneOrdinary)
	if got := []uint64{
		secondBatch[0].frames[0].StreamID,
		secondBatch[1].frames[0].StreamID,
	}; !equalUint64s(got, []uint64{8, 4}) {
		t.Fatalf("second competitive batch order = %v, want [8 4]", got)
	}
}

func TestCollectWriteBatchSessionScopedOrdinaryDoesNotConsumeRetainedWFQHead(t *testing.T) {
	t.Parallel()

	c := &Conn{
		writer: connWriterRuntimeState{writeCh: make(chan writeRequest, 8)},

		config: connConfigState{peer: Preface{
			Settings: Settings{
				MaxFramePayload: 16384,
			},
		}}, registry: connRegistryState{streams: map[uint64]*nativeStream{
			4: {id: 4, idSet: true},
			8: {id: 8, idSet: true, priority: 20},
		}},
	}

	sessionReq := func(tag byte) writeRequest {
		return writeRequest{
			frames: testTxFramesFrom([]Frame{{Type: FrameTypeEXT, StreamID: 0, Payload: mustEncodeVarint(uint64(tag))}}),
			done:   make(chan error, 1),
		}
	}
	dataReq := func(streamID uint64, n int) writeRequest {
		return writeRequest{
			frames: testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: streamID, Payload: make([]byte, n)}}),
			done:   make(chan error, 1),
		}
	}

	c.writer.writeCh <- dataReq(4, 40000)
	c.writer.writeCh <- dataReq(8, 40000)
	firstBatch := c.collectWriteBatch(sessionReq(1), writeLaneOrdinary)
	if got := []uint64{
		firstBatch[1].frames[0].StreamID,
		firstBatch[2].frames[0].StreamID,
	}; !equalUint64s(got, []uint64{8, 4}) {
		t.Fatalf("first batch first real order = %v, want [8 4]", got)
	}

	c.writer.writeCh <- dataReq(4, 40000)
	c.writer.writeCh <- dataReq(8, 40000)
	secondBatch := c.collectWriteBatch(sessionReq(2), writeLaneOrdinary)
	if got := []uint64{
		secondBatch[1].frames[0].StreamID,
		secondBatch[2].frames[0].StreamID,
	}; !equalUint64s(got, []uint64{8, 4}) {
		t.Fatalf("second batch first real order = %v, want [8 4]", got)
	}
}

func TestCollectWriteBatchOrdersUrgentFramesByPriority(t *testing.T) {
	t.Parallel()
	c := &Conn{
		writer: connWriterRuntimeState{urgentWriteCh: make(chan writeRequest, 8)},
	}

	first := writeRequest{
		frames: testTxFramesFrom([]Frame{{Type: FrameTypePING, Payload: []byte{0, 0, 0, 0, 0, 0, 0, 1}}}),
		done:   make(chan error, 1),
	}
	second := writeRequest{
		frames: testTxFramesFrom([]Frame{{Type: FrameTypeRESET, StreamID: 4, Payload: []byte{byte(CodeCancelled)}}}),
		done:   make(chan error, 1),
	}
	third := writeRequest{
		frames: testTxFramesFrom([]Frame{{Type: FrameTypeGOAWAY, Payload: mustGoAwayPayload(t, 8, 8, uint64(CodeNoError), "")}}),
		done:   make(chan error, 1),
	}

	c.writer.urgentWriteCh <- second
	c.writer.urgentWriteCh <- third

	batch := c.collectWriteBatch(first, writeLaneUrgent)
	if len(batch) != 3 {
		t.Fatalf("batch len = %d, want 3", len(batch))
	}
	if batch[0].frames[0].Type != FrameTypeGOAWAY {
		t.Fatalf("first urgent frame = %s, want GOAWAY", batch[0].frames[0].Type)
	}
	if batch[1].frames[0].Type != FrameTypeRESET {
		t.Fatalf("second urgent frame = %s, want RESET", batch[1].frames[0].Type)
	}
	if batch[2].frames[0].Type != FrameTypePING {
		t.Fatalf("third urgent frame = %s, want PING", batch[2].frames[0].Type)
	}
}

func TestCollectWriteBatchOrdersSameRankUrgentStreamFramesByStreamID(t *testing.T) {
	t.Parallel()

	c := &Conn{
		writer: connWriterRuntimeState{urgentWriteCh: make(chan writeRequest, 8)},
	}

	first := writeRequest{
		frames: testTxFramesFrom([]Frame{{Type: FrameTypeRESET, StreamID: 8, Payload: []byte{byte(CodeCancelled)}}}),
		done:   make(chan error, 1),
	}
	second := writeRequest{
		frames: testTxFramesFrom([]Frame{{Type: FrameTypeRESET, StreamID: 4, Payload: []byte{byte(CodeCancelled)}}}),
		done:   make(chan error, 1),
	}
	third := writeRequest{
		frames: testTxFramesFrom([]Frame{{Type: FrameTypePING, Payload: []byte{0, 0, 0, 0, 0, 0, 0, 1}}}),
		done:   make(chan error, 1),
	}

	c.writer.urgentWriteCh <- second
	c.writer.urgentWriteCh <- third

	batch := c.collectWriteBatch(first, writeLaneUrgent)
	if len(batch) != 3 {
		t.Fatalf("batch len = %d, want 3", len(batch))
	}
	if got := batch[0].frames[0].StreamID; got != 4 {
		t.Fatalf("first same-rank urgent stream = %d, want 4", got)
	}
	if got := batch[1].frames[0].StreamID; got != 8 {
		t.Fatalf("second same-rank urgent stream = %d, want 8", got)
	}
	if got := batch[2].frames[0].Type; got != FrameTypePING {
		t.Fatalf("third urgent frame type = %s, want PING", got)
	}
}

func TestOrderWriteBatchKeepsIdentityOrderWithoutCopy(t *testing.T) {
	t.Parallel()

	c := &Conn{
		writer: connWriterRuntimeState{urgentWriteCh: make(chan writeRequest, 8)},
	}

	batch := []writeRequest{
		{
			frames: testTxFramesFrom([]Frame{{Type: FrameTypeGOAWAY, Payload: mustGoAwayPayload(t, 8, 8, uint64(CodeNoError), "")}}),
			done:   make(chan error, 1),
		},
		{
			frames: testTxFramesFrom([]Frame{{Type: FrameTypeRESET, StreamID: 4, Payload: []byte{byte(CodeCancelled)}}}),
			done:   make(chan error, 1),
		},
		{
			frames: testTxFramesFrom([]Frame{{Type: FrameTypePING, Payload: []byte{0, 0, 0, 0, 0, 0, 0, 1}}}),
			done:   make(chan error, 1),
		},
	}

	ordered := c.orderWriteBatch(batch, writeLaneUrgent)
	if len(ordered) != len(batch) {
		t.Fatalf("ordered len = %d, want %d", len(ordered), len(batch))
	}
	if &ordered[0] != &batch[0] {
		t.Fatal("identity batch order copied requests instead of reusing original batch")
	}
}

func TestCollectWriteBatchSessionScopedControlDoesNotPolluteRetainedSchedulerState(t *testing.T) {
	t.Parallel()

	c := &Conn{
		writer: connWriterRuntimeState{writeCh: make(chan writeRequest, 8)}, registry: connRegistryState{streams: map[uint64]*nativeStream{
			4: {id: 4, idSet: true},
		}},
	}

	first := writeRequest{
		frames: testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: 4, Payload: []byte("a")}}),
		done:   make(chan error, 1),
	}
	second := writeRequest{
		frames: testTxFramesFrom([]Frame{{Type: FrameTypeMAXDATA, StreamID: 0, Payload: mustEncodeVarint(1024)}}),
		done:   make(chan error, 1),
	}
	third := writeRequest{
		frames: testTxFramesFrom([]Frame{{Type: FrameTypeBLOCKED, StreamID: 0, Payload: mustEncodeVarint(1024)}}),
		done:   make(chan error, 1),
	}

	c.writer.writeCh <- second
	c.writer.writeCh <- third

	batch := c.collectWriteBatch(first, writeLaneOrdinary)
	if len(batch) != 3 {
		t.Fatalf("batch len = %d, want 3", len(batch))
	}
	if len(c.writer.scheduler.State.StreamFinishTag) != 1 {
		t.Fatalf("retained stream finish tags = %#v, want one real-stream entry", c.writer.scheduler.State.StreamFinishTag)
	}
	if _, ok := c.writer.scheduler.State.StreamFinishTag[4]; !ok {
		t.Fatalf("real stream finish tag for 4 missing: %#v", c.writer.scheduler.State.StreamFinishTag)
	}
	if len(c.writer.scheduler.State.StreamLastService) != 1 {
		t.Fatalf("retained stream last-service state = %#v, want one real-stream entry", c.writer.scheduler.State.StreamLastService)
	}
	if _, ok := c.writer.scheduler.State.StreamLastService[4]; !ok {
		t.Fatalf("real stream last-service for 4 missing: %#v", c.writer.scheduler.State.StreamLastService)
	}
	if len(c.writer.scheduler.State.GroupFinishTag) != 1 {
		t.Fatalf("retained group finish tags = %#v, want one real-stream entry", c.writer.scheduler.State.GroupFinishTag)
	}
	if _, ok := c.writer.scheduler.State.GroupFinishTag[rt.GroupKey{Kind: 0, Value: 4}]; !ok {
		t.Fatalf("real stream default group finish tag for 4 missing: %#v", c.writer.scheduler.State.GroupFinishTag)
	}
	for key := range c.writer.scheduler.State.GroupVirtualTime {
		if key.Kind == 2 {
			t.Fatalf("retained transient group state for kind=2: %#v", c.writer.scheduler.State.GroupVirtualTime)
		}
	}
	if c.writer.scheduler.State.ServiceSeq == 0 {
		t.Fatal("service sequence did not advance for real stream traffic")
	}
}

func TestCollectWriteBatchSessionScopedOrdinaryScrubsStaleBatchBiasWithoutRetainedRealState(t *testing.T) {
	t.Parallel()

	c := &Conn{

		writer: connWriterRuntimeState{
			writeCh: make(chan writeRequest, 8),
			scheduler: rt.BatchScheduler{
				State: rt.BatchState{
					RootVirtualTime: 11,
					ServiceSeq:      7,
				},
			},
		}, config: connConfigState{peer: Preface{
			Settings: Settings{
				MaxFramePayload: 16384,
			},
		}},
	}

	first := writeRequest{
		frames: testTxFramesFrom([]Frame{{Type: FrameTypeEXT, StreamID: 0, Payload: mustEncodeVarint(99)}}),
		done:   make(chan error, 1),
	}
	second := writeRequest{
		frames: testTxFramesFrom([]Frame{{Type: FrameTypeEXT, StreamID: 0, Payload: mustEncodeVarint(100)}}),
		done:   make(chan error, 1),
	}

	c.writer.writeCh <- second

	batch := c.collectWriteBatch(first, writeLaneOrdinary)
	if len(batch) != 2 {
		t.Fatalf("batch len = %d, want 2", len(batch))
	}
	if got := c.writer.scheduler.State.RootVirtualTime; got != 0 {
		t.Fatalf("root virtual time = %d, want 0", got)
	}
	if got := c.writer.scheduler.State.ServiceSeq; got != 0 {
		t.Fatalf("service seq = %d, want 0", got)
	}
}

func TestCollectWriteBatchSessionScopedOrdinaryPreservesRetainedBiasWhenRealStateExists(t *testing.T) {
	t.Parallel()

	c := &Conn{

		writer: connWriterRuntimeState{
			writeCh: make(chan writeRequest, 8),
			scheduler: rt.BatchScheduler{
				State: rt.BatchState{
					RootVirtualTime: 33,
					ServiceSeq:      5,
					GroupVirtualTime: map[rt.GroupKey]uint64{
						{Kind: 0, Value: 4}: 17,
					},
					GroupFinishTag: map[rt.GroupKey]uint64{
						{Kind: 0, Value: 4}: 21,
					},
					GroupLastService: map[rt.GroupKey]uint64{
						{Kind: 0, Value: 4}: 4,
					},
					StreamFinishTag:   map[uint64]uint64{4: 17},
					StreamLastService: map[uint64]uint64{4: 4},
				},
			},
		}, config: connConfigState{peer: Preface{
			Settings: Settings{
				MaxFramePayload: 16384,
			},
		}},
	}

	first := writeRequest{
		frames: testTxFramesFrom([]Frame{{Type: FrameTypeEXT, StreamID: 0, Payload: mustEncodeVarint(99)}}),
		done:   make(chan error, 1),
	}
	second := writeRequest{
		frames: testTxFramesFrom([]Frame{{Type: FrameTypeEXT, StreamID: 0, Payload: mustEncodeVarint(100)}}),
		done:   make(chan error, 1),
	}

	c.writer.writeCh <- second

	batch := c.collectWriteBatch(first, writeLaneOrdinary)
	if len(batch) != 2 {
		t.Fatalf("batch len = %d, want 2", len(batch))
	}
	if got := c.writer.scheduler.State.RootVirtualTime; got != 33 {
		t.Fatalf("root virtual time = %d, want 33", got)
	}
	if got := c.writer.scheduler.State.ServiceSeq; got != 5 {
		t.Fatalf("service seq = %d, want 5", got)
	}
}

func TestCollectWriteBatchMixedSessionScopedOrdinaryAndRealStreamOnlyAdvancesRealClassCounters(t *testing.T) {
	t.Parallel()

	c := &Conn{
		writer: connWriterRuntimeState{writeCh: make(chan writeRequest, 8)},

		config: connConfigState{peer: Preface{
			Settings: Settings{
				MaxFramePayload: 16384,
			},
		}}, registry: connRegistryState{streams: map[uint64]*nativeStream{
			4: {id: 4, idSet: true, priority: 20},
		}},
	}

	first := writeRequest{
		frames: testTxFramesFrom([]Frame{{Type: FrameTypeEXT, StreamID: 0, Payload: mustEncodeVarint(99)}}),
		done:   make(chan error, 1),
	}
	second := writeRequest{
		frames: testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: 4, Payload: []byte("a")}}),
		done:   make(chan error, 1),
	}

	c.writer.writeCh <- second

	batch := c.collectWriteBatch(first, writeLaneOrdinary)
	if len(batch) != 2 {
		t.Fatalf("batch len = %d, want 2", len(batch))
	}
	if got := c.writer.scheduler.State.ServiceSeq; got != 1 {
		t.Fatalf("service seq = %d, want 1", got)
	}
}

func TestCollectWriteBatchInterleavesExplicitGroupsWhenGroupFair(t *testing.T) {
	t.Parallel()
	c := &Conn{
		writer: connWriterRuntimeState{writeCh: make(chan writeRequest, 8)},

		config: connConfigState{peer: Preface{Settings: Settings{SchedulerHints: SchedulerGroupFair}}}, registry: connRegistryState{streams: map[uint64]*nativeStream{
			4:  {id: 4, idSet: true, group: 7, groupExplicit: true},
			8:  {id: 8, idSet: true, group: 7, groupExplicit: true},
			12: {id: 12, idSet: true, group: 9, groupExplicit: true},
			16: {id: 16, idSet: true, group: 9, groupExplicit: true},
		}},
	}

	first := writeRequest{
		frames: testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: 4, Payload: []byte("a")}}),
		done:   make(chan error, 1),
	}
	second := writeRequest{
		frames: testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: 8, Payload: []byte("b")}}),
		done:   make(chan error, 1),
	}
	third := writeRequest{
		frames: testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: 12, Payload: []byte("c")}}),
		done:   make(chan error, 1),
	}
	fourth := writeRequest{
		frames: testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: 16, Payload: []byte("d")}}),
		done:   make(chan error, 1),
	}

	c.writer.writeCh <- second
	c.writer.writeCh <- third
	c.writer.writeCh <- fourth

	batch := c.collectWriteBatch(first, writeLaneOrdinary)
	if got := []uint64{
		batch[0].frames[0].StreamID,
		batch[1].frames[0].StreamID,
		batch[2].frames[0].StreamID,
		batch[3].frames[0].StreamID,
	}; !equalUint64s(got, []uint64{4, 12, 8, 16}) {
		t.Fatalf("group-fair batch order = %v, want [4 12 8 16]", got)
	}
}

func TestCollectWriteBatchKeepsPerGroupHeadAsBoundedNextBatchBias(t *testing.T) {
	t.Parallel()
	c := &Conn{
		writer: connWriterRuntimeState{writeCh: make(chan writeRequest, 8)},

		config: connConfigState{peer: Preface{Settings: Settings{SchedulerHints: SchedulerGroupFair}}}, registry: connRegistryState{streams: map[uint64]*nativeStream{
			4:  {id: 4, idSet: true, group: 7, groupExplicit: true},
			8:  {id: 8, idSet: true, group: 7, groupExplicit: true},
			12: {id: 12, idSet: true, group: 9, groupExplicit: true},
			16: {id: 16, idSet: true, group: 9, groupExplicit: true},
		}},
	}

	newReq := func(streamID uint64, tag byte) writeRequest {
		return writeRequest{
			frames: testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: streamID, Payload: []byte{tag}}}),
			done:   make(chan error, 1),
		}
	}

	c.writer.writeCh <- newReq(8, 'b')
	c.writer.writeCh <- newReq(12, 'c')
	c.writer.writeCh <- newReq(16, 'd')
	firstBatch := c.collectWriteBatch(newReq(4, 'a'), writeLaneOrdinary)
	if got := []uint64{
		firstBatch[0].frames[0].StreamID,
		firstBatch[1].frames[0].StreamID,
		firstBatch[2].frames[0].StreamID,
		firstBatch[3].frames[0].StreamID,
	}; !equalUint64s(got, []uint64{4, 12, 8, 16}) {
		t.Fatalf("first group-fair batch order = %v, want [4 12 8 16]", got)
	}

	c.writer.writeCh <- newReq(8, 'f')
	c.writer.writeCh <- newReq(12, 'g')
	c.writer.writeCh <- newReq(16, 'h')
	secondBatch := c.collectWriteBatch(newReq(4, 'e'), writeLaneOrdinary)
	if got := []uint64{
		secondBatch[0].frames[0].StreamID,
		secondBatch[1].frames[0].StreamID,
		secondBatch[2].frames[0].StreamID,
		secondBatch[3].frames[0].StreamID,
	}; !equalUint64s(got, []uint64{16, 8, 12, 4}) {
		t.Fatalf("second group-fair batch order = %v, want [16 8 12 4]", got)
	}
}

func TestCollectWriteBatchInterleavesStreamsWithinSingleExplicitGroup(t *testing.T) {
	t.Parallel()
	c := &Conn{
		writer: connWriterRuntimeState{writeCh: make(chan writeRequest, 8)},

		config: connConfigState{peer: Preface{Settings: Settings{SchedulerHints: SchedulerGroupFair}}}, registry: connRegistryState{streams: map[uint64]*nativeStream{
			4: {id: 4, idSet: true, group: 7, groupExplicit: true},
			8: {id: 8, idSet: true, group: 7, groupExplicit: true},
		}},
	}

	first := writeRequest{
		frames: testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: 4, Payload: []byte("a")}}),
		done:   make(chan error, 1),
	}
	second := writeRequest{
		frames: testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: 4, Payload: []byte("b")}}),
		done:   make(chan error, 1),
	}
	third := writeRequest{
		frames: testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: 8, Payload: []byte("c")}}),
		done:   make(chan error, 1),
	}

	c.writer.writeCh <- second
	c.writer.writeCh <- third

	batch := c.collectWriteBatch(first, writeLaneOrdinary)
	if got := []uint64{
		batch[0].frames[0].StreamID,
		batch[1].frames[0].StreamID,
		batch[2].frames[0].StreamID,
	}; !equalUint64s(got, []uint64{4, 8, 4}) {
		t.Fatalf("single-group batch order = %v, want [4 8 4]", got)
	}
}

func TestCollectWriteBatchCapsExplicitGroupCardinality(t *testing.T) {
	t.Parallel()
	c := &Conn{
		writer: connWriterRuntimeState{writeCh: make(chan writeRequest, 32)},

		config: connConfigState{peer: Preface{Settings: Settings{SchedulerHints: SchedulerGroupFair}}}, registry: connRegistryState{streams: make(map[uint64]*nativeStream)},
	}

	newReq := func(streamID uint64, tag byte) writeRequest {
		return writeRequest{
			frames: testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: streamID, Payload: []byte{tag}}}),
			done:   make(chan error, 1),
		}
	}

	streamID := uint64(4)
	first := newReq(streamID, 'a')
	testBuildStream(c, streamID, testWithExplicitGroup(1))

	for g := uint64(2); g <= 16; g++ {
		streamID += 4
		testBuildStream(c, streamID, testWithExplicitGroup(g))
		c.writer.writeCh <- newReq(streamID, byte(g))
	}

	overflowA1ID := streamID + 4
	overflowB1ID := overflowA1ID + 4
	groupOneSecondID := overflowB1ID + 4
	overflowA2ID := groupOneSecondID + 4
	overflowB2ID := overflowA2ID + 4

	testBuildStream(c, overflowA1ID, testWithExplicitGroup(17))
	testBuildStream(c, overflowB1ID, testWithExplicitGroup(18))
	testBuildStream(c, groupOneSecondID, testWithExplicitGroup(1))
	testBuildStream(c, overflowA2ID, testWithExplicitGroup(17))
	testBuildStream(c, overflowB2ID, testWithExplicitGroup(18))

	c.writer.writeCh <- newReq(overflowA1ID, 'x')
	c.writer.writeCh <- newReq(overflowB1ID, 'y')
	c.writer.writeCh <- newReq(groupOneSecondID, 'z')
	c.writer.writeCh <- newReq(overflowA2ID, 'u')
	c.writer.writeCh <- newReq(overflowB2ID, 'v')

	batch := c.collectWriteBatch(first, writeLaneOrdinary)
	got := make([]uint64, 0, len(batch))
	for _, req := range batch {
		got = append(got, req.frames[0].StreamID)
	}

	wantPrefix := []uint64{
		4, 8, 12, 16, 20, 24, 28, 32, 36, 40, 44, 48, 52, 56, 60, 64, overflowA1ID, groupOneSecondID,
	}
	if len(got) < len(wantPrefix) {
		t.Fatalf("batch len = %d, want at least %d", len(got), len(wantPrefix))
	}
	if !equalUint64s(got[:len(wantPrefix)], wantPrefix) {
		t.Fatalf("capped-group batch prefix = %v, want %v", got[:len(wantPrefix)], wantPrefix)
	}
}

func TestCollectWriteBatchOrdersPriorityUpdateAheadOfSameStreamData(t *testing.T) {
	caps := CapabilityPriorityUpdate | CapabilityPriorityHints
	payload, err := buildPriorityUpdatePayload(
		caps,
		MetadataUpdate{Priority: invalidUint64Ptr(7)},
		4096,
	)
	if err != nil {
		t.Fatalf("build priority update payload: %v", err)
	}

	c := &Conn{
		writer: connWriterRuntimeState{writeCh: make(chan writeRequest, 8)},

		config: connConfigState{peer: Preface{
			Settings: Settings{
				SchedulerHints: SchedulerBalancedFair,
			},
		}}, registry: connRegistryState{streams: map[uint64]*nativeStream{
			4: {id: 4, idSet: true},
			8: {id: 8, idSet: true},
		}},
	}

	first := writeRequest{
		frames: testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: 4, Payload: []byte("a")}}),
		done:   make(chan error, 1),
	}
	second := writeRequest{
		frames: testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: 8, Payload: []byte("b")}}),
		done:   make(chan error, 1),
	}
	third := writeRequest{
		frames: testTxFramesFrom([]Frame{{Type: FrameTypeEXT, StreamID: 4, Payload: payload}}),
		done:   make(chan error, 1),
	}

	c.writer.writeCh <- second
	c.writer.writeCh <- third

	batch := c.collectWriteBatch(first, writeLaneOrdinary)
	if len(batch) != 3 {
		t.Fatalf("batch len = %d, want 3", len(batch))
	}
	if batch[0].frames[0].Type != FrameTypeEXT || batch[0].frames[0].StreamID != 4 {
		t.Fatalf("first batch frame = %+v, want EXT on stream 4", batch[0].frames[0])
	}
	if batch[1].frames[0].Type != FrameTypeDATA || batch[1].frames[0].StreamID != 8 {
		t.Fatalf("second batch frame = %+v, want DATA on stream 8", batch[1].frames[0])
	}
	if batch[2].frames[0].Type != FrameTypeDATA || batch[2].frames[0].StreamID != 4 {
		t.Fatalf("third batch frame = %+v, want DATA on stream 4", batch[2].frames[0])
	}
}

func TestCollectWriteBatchMergesAdvisoryAndDataLanes(t *testing.T) {
	caps := CapabilityPriorityUpdate | CapabilityPriorityHints
	payload, err := buildPriorityUpdatePayload(
		caps,
		MetadataUpdate{Priority: invalidUint64Ptr(7)},
		4096,
	)
	if err != nil {
		t.Fatalf("build priority update payload: %v", err)
	}

	c := &Conn{
		writer: connWriterRuntimeState{writeCh: make(chan writeRequest, 8), advisoryWriteCh: make(chan writeRequest, 8)},

		config: connConfigState{peer: Preface{
			Settings: Settings{
				SchedulerHints: SchedulerBalancedFair,
			},
		}}, registry: connRegistryState{streams: map[uint64]*nativeStream{
			4: {id: 4, idSet: true},
			8: {id: 8, idSet: true},
		}},
	}

	first := writeRequest{
		frames: testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: 8, Payload: []byte("b")}}),
		done:   make(chan error, 1),
	}
	second := writeRequest{
		frames: testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: 4, Payload: []byte("a")}}),
		done:   make(chan error, 1),
	}
	third := writeRequest{
		frames: testTxFramesFrom([]Frame{{Type: FrameTypeEXT, StreamID: 4, Payload: payload}}),
		done:   make(chan error, 1),
	}

	c.writer.writeCh <- second
	c.writer.advisoryWriteCh <- third

	batch := c.collectWriteBatch(first, writeLaneOrdinary)
	if len(batch) != 3 {
		t.Fatalf("batch len = %d, want 3", len(batch))
	}
	posExt := -1
	posData4 := -1
	for i, req := range batch {
		frame := req.frames[0]
		if frame.Type == FrameTypeEXT && frame.StreamID == 4 {
			posExt = i
		}
		if frame.Type == FrameTypeDATA && frame.StreamID == 4 {
			posData4 = i
		}
	}
	if posExt < 0 || posData4 < 0 {
		t.Fatalf("merged batch missing advisory/data for stream 4: %+v", batch)
	}
	if posExt >= posData4 {
		t.Fatalf("advisory EXT position = %d, same-stream DATA position = %d, want advisory before data", posExt, posData4)
	}
}

func TestCollectWriteBatchDoesNotStarveOrdinaryLaneWhenAdvisoryFlooded(t *testing.T) {
	caps := CapabilityPriorityUpdate | CapabilityPriorityHints
	payload, err := buildPriorityUpdatePayload(
		caps,
		MetadataUpdate{Priority: invalidUint64Ptr(7)},
		4096,
	)
	if err != nil {
		t.Fatalf("build priority update payload: %v", err)
	}

	c := &Conn{
		writer: connWriterRuntimeState{
			writeCh:         make(chan writeRequest, 1),
			advisoryWriteCh: make(chan writeRequest, maxWriteBatchFrames+8),
		},

		config: connConfigState{peer: Preface{
			Settings: Settings{
				MaxFramePayload:          16384,
				MaxExtensionPayloadBytes: 4096,
				SchedulerHints:           SchedulerBalancedFair,
			},
		}}, registry: connRegistryState{streams: map[uint64]*nativeStream{
			4: {id: 4, idSet: true},
			8: {id: 8, idSet: true},
		}},
	}

	first := writeRequest{
		frames: testTxFramesFrom([]Frame{{Type: FrameTypeEXT, StreamID: 4, Payload: payload}}),
		done:   make(chan error, 1),
	}
	c.writer.writeCh <- writeRequest{
		frames: testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: 8, Payload: []byte("ordinary")}}),
		done:   make(chan error, 1),
	}
	for i := 0; i < maxWriteBatchFrames+8; i++ {
		c.writer.advisoryWriteCh <- writeRequest{
			frames: testTxFramesFrom([]Frame{{Type: FrameTypeEXT, StreamID: 4, Payload: payload}}),
			done:   make(chan error, 1),
		}
	}

	batch := c.collectWriteBatch(first, writeLaneAdvisory)
	if len(batch) != maxWriteBatchFrames {
		t.Fatalf("batch len = %d, want %d", len(batch), maxWriteBatchFrames)
	}

	ordinaryCount := 0
	advisoryCount := 0
	for _, req := range batch {
		frame := req.frames[0]
		switch {
		case frame.Type == FrameTypeDATA && frame.StreamID == 8:
			ordinaryCount++
		case frame.Type == FrameTypeEXT && frame.StreamID == 4:
			advisoryCount++
		default:
			t.Fatalf("unexpected frame in merged batch: %+v", frame)
		}
	}

	if ordinaryCount != 1 {
		t.Fatalf("ordinary request count = %d, want 1", ordinaryCount)
	}
	if advisoryCount != maxWriteBatchFrames-1 {
		t.Fatalf("advisory request count = %d, want %d", advisoryCount, maxWriteBatchFrames-1)
	}
}

func TestCollectWriteBatchGivesOneCrossStreamAdvisoryHeadOpportunity(t *testing.T) {
	caps := CapabilityPriorityUpdate | CapabilityPriorityHints
	payload, err := buildPriorityUpdatePayload(
		caps,
		MetadataUpdate{Priority: invalidUint64Ptr(7)},
		4096,
	)
	if err != nil {
		t.Fatalf("build priority update payload: %v", err)
	}

	c := &Conn{
		writer: connWriterRuntimeState{writeCh: make(chan writeRequest, 8), advisoryWriteCh: make(chan writeRequest, 8)},

		config: connConfigState{peer: Preface{
			Settings: Settings{
				MaxFramePayload:          16384,
				MaxExtensionPayloadBytes: 4096,
				SchedulerHints:           SchedulerBalancedFair,
			},
		}}, registry: connRegistryState{streams: map[uint64]*nativeStream{
			4: {id: 4, idSet: true},
			8: {id: 8, idSet: true},
		}},
	}

	first := writeRequest{
		frames: testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: 8, Payload: []byte("b")}}),
		done:   make(chan error, 1),
	}
	second := writeRequest{
		frames: testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: 4, Payload: []byte("a")}}),
		done:   make(chan error, 1),
	}
	third := writeRequest{
		frames: testTxFramesFrom([]Frame{{Type: FrameTypeEXT, StreamID: 4, Payload: payload}}),
		done:   make(chan error, 1),
	}

	c.writer.writeCh <- second
	c.writer.advisoryWriteCh <- third

	batch := c.collectWriteBatch(first, writeLaneOrdinary)
	if len(batch) != 3 {
		t.Fatalf("batch len = %d, want 3", len(batch))
	}
	if got := batch[0].frames[0]; got.Type != FrameTypeEXT || got.StreamID != 4 {
		t.Fatalf("first frame = %+v, want stream 4 advisory EXT", got)
	}
	if got := batch[1].frames[0]; got.Type != FrameTypeDATA || got.StreamID != 8 {
		t.Fatalf("second frame = %+v, want stream 8 DATA", got)
	}
	if got := batch[2].frames[0]; got.Type != FrameTypeDATA || got.StreamID != 4 {
		t.Fatalf("third frame = %+v, want stream 4 DATA", got)
	}
}

func TestSetStreamGroupLockedReleasesOldExplicitGroupStateWhenLastTracked(t *testing.T) {
	t.Parallel()
	c := &Conn{

		writer: connWriterRuntimeState{
			scheduler: rt.BatchScheduler{
				ActiveGroupRefs: map[uint64]uint32{7: 1},
				State: rt.BatchState{
					GroupVirtualTime: map[rt.GroupKey]uint64{{Kind: 1, Value: 7}: 11},
					GroupFinishTag:   map[rt.GroupKey]uint64{{Kind: 1, Value: 7}: 13},
					GroupLastService: map[rt.GroupKey]uint64{{Kind: 1, Value: 7}: 3},
				},
			},
		}, config: connConfigState{peer: Preface{Settings: Settings{SchedulerHints: SchedulerGroupFair}}},
	}
	stream := testBuildDetachedStream(c, 4, testWithLocalSend(), testWithExplicitGroup(7), testWithTrackedGroup(7))

	c.setStreamGroupLocked(stream, 9, true)

	if stream.group != 9 || !stream.groupExplicit || !stream.groupTracked {
		t.Fatalf("stream group state = (%d,%v,%v), want (9,true,true)", stream.group, stream.groupExplicit, stream.groupTracked)
	}
	if _, ok := c.writer.scheduler.ActiveGroupRefs[7]; ok {
		t.Fatalf("old explicit group ref for 7 still retained")
	}
	if got := c.writer.scheduler.ActiveGroupRefs[9]; got != 1 {
		t.Fatalf("new explicit group ref count = %d, want 1", got)
	}
	if _, ok := c.writer.scheduler.State.GroupFinishTag[rt.GroupKey{Kind: 1, Value: 7}]; ok {
		t.Fatalf("old explicit group state still retained")
	}
}

func TestSetStreamGroupLockedPreservesSharedOldExplicitGroupState(t *testing.T) {
	t.Parallel()
	c := &Conn{

		writer: connWriterRuntimeState{
			scheduler: rt.BatchScheduler{
				ActiveGroupRefs: map[uint64]uint32{7: 2},
				State: rt.BatchState{
					GroupVirtualTime: map[rt.GroupKey]uint64{{Kind: 1, Value: 7}: 11},
					GroupFinishTag:   map[rt.GroupKey]uint64{{Kind: 1, Value: 7}: 13},
					GroupLastService: map[rt.GroupKey]uint64{{Kind: 1, Value: 7}: 3},
				},
			},
		}, config: connConfigState{peer: Preface{Settings: Settings{SchedulerHints: SchedulerGroupFair}}},
	}
	stream := testBuildDetachedStream(c, 4, testWithLocalSend(), testWithExplicitGroup(7), testWithTrackedGroup(7))

	c.setStreamGroupLocked(stream, 9, true)

	if got := c.writer.scheduler.ActiveGroupRefs[7]; got != 1 {
		t.Fatalf("remaining old explicit group ref count = %d, want 1", got)
	}
	if _, ok := c.writer.scheduler.State.GroupFinishTag[rt.GroupKey{Kind: 1, Value: 7}]; !ok {
		t.Fatalf("shared old explicit group state unexpectedly dropped")
	}
	if got := c.writer.scheduler.ActiveGroupRefs[9]; got != 1 {
		t.Fatalf("new explicit group ref count = %d, want 1", got)
	}
}

func TestSetStreamGroupLockedSkipsExplicitTrackingOutsideGroupFair(t *testing.T) {
	t.Parallel()
	c := &Conn{config: connConfigState{peer: Preface{Settings: Settings{SchedulerHints: SchedulerBalancedFair}}}}
	stream := testBuildDetachedStream(c, 4, testWithLocalSend())

	c.setStreamGroupLocked(stream, 7, true)

	if stream.group != 7 || !stream.groupExplicit {
		t.Fatalf("stream group state = (%d,%v), want (7,true)", stream.group, stream.groupExplicit)
	}
	if stream.groupTracked {
		t.Fatal("stream group tracking armed outside group_fair")
	}
	if stream.trackedGroup != 0 {
		t.Fatalf("trackedGroup = %d, want 0 outside group_fair", stream.trackedGroup)
	}
	if len(c.writer.scheduler.ActiveGroupRefs) != 0 {
		t.Fatalf("active group refs = %#v, want none outside group_fair", c.writer.scheduler.ActiveGroupRefs)
	}
}

func TestSetStreamGroupLockedTracksOverflowGroupInFallbackBucket(t *testing.T) {
	t.Parallel()
	c := &Conn{

		writer: connWriterRuntimeState{
			scheduler: rt.BatchScheduler{
				ActiveGroupRefs: make(map[uint64]uint32),
			},
		}, config: connConfigState{peer: Preface{Settings: Settings{SchedulerHints: SchedulerGroupFair}}},
	}
	for group := uint64(1); group <= rt.MaxExplicitGroups; group++ {
		c.writer.scheduler.ActiveGroupRefs[group] = 1
	}
	stream := testBuildDetachedStream(c, 4, testWithLocalSend())

	c.setStreamGroupLocked(stream, 99, true)

	if !stream.groupTracked {
		t.Fatal("stream group tracking = false, want true")
	}
	if stream.trackedGroup != rt.FallbackGroupBucket {
		t.Fatalf("trackedGroup = %d, want fallback bucket %d", stream.trackedGroup, rt.FallbackGroupBucket)
	}
	if _, ok := c.writer.scheduler.ActiveGroupRefs[99]; ok {
		t.Fatal("raw overflow group ref unexpectedly retained")
	}
	if got := c.writer.scheduler.ActiveGroupRefs[rt.FallbackGroupBucket]; got != 1 {
		t.Fatalf("fallback bucket ref count = %d, want 1", got)
	}
}

func TestSetSendFinDropsExplicitGroupTracking(t *testing.T) {
	t.Parallel()
	c := &Conn{
		writer: connWriterRuntimeState{
			scheduler: rt.BatchScheduler{
				ActiveGroupRefs: map[uint64]uint32{7: 1},
				State: rt.BatchState{
					GroupVirtualTime: map[rt.GroupKey]uint64{{Kind: 1, Value: 7}: 11},
					GroupFinishTag:   map[rt.GroupKey]uint64{{Kind: 1, Value: 7}: 13},
					GroupLastService: map[rt.GroupKey]uint64{{Kind: 1, Value: 7}: 3},
				},
			},
		},
	}
	stream := testBuildDetachedStream(c, 4,
		testWithLocalSend(),
		testWithExplicitGroup(7),
		testWithTrackedGroup(7),
		testWithWriteNotify(),
	)

	stream.setSendFin()

	if stream.groupTracked {
		t.Fatal("stream group tracking still armed after send_fin")
	}
	if _, ok := c.writer.scheduler.ActiveGroupRefs[7]; ok {
		t.Fatal("explicit group ref still retained after send_fin")
	}
	if _, ok := c.writer.scheduler.State.GroupFinishTag[rt.GroupKey{Kind: 1, Value: 7}]; ok {
		t.Fatal("explicit group state still retained after send_fin")
	}
}

func equalUint64s(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

type captureWriteCloser struct {
	mu    sync.Mutex
	data  []byte
	wrote chan struct{}
}

func (c *captureWriteCloser) Read(_ []byte) (int, error) {
	return 0, io.EOF
}

func (c *captureWriteCloser) Write(p []byte) (int, error) {
	c.mu.Lock()
	c.data = append(c.data, p...)
	wrote := c.wrote
	c.mu.Unlock()
	if wrote != nil {
		select {
		case wrote <- struct{}{}:
		default:
		}
	}
	return len(p), nil
}

func (c *captureWriteCloser) Close() error { return nil }

func (c *captureWriteCloser) bytes() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]byte(nil), c.data...)
}

func (c *captureWriteCloser) wroteChan() <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.wrote == nil {
		c.wrote = make(chan struct{}, 1)
	}
	return c.wrote
}

type countingWriteCloser struct {
	mu     sync.Mutex
	writes int
}

func (c *countingWriteCloser) Read(_ []byte) (int, error) {
	return 0, io.EOF
}

func (c *countingWriteCloser) Write(p []byte) (int, error) {
	c.mu.Lock()
	c.writes++
	c.mu.Unlock()
	return len(p), nil
}

func (c *countingWriteCloser) Close() error { return nil }

func (c *countingWriteCloser) writeCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.writes
}

type partialWriteCloser struct {
	mu      sync.Mutex
	chunks  []int
	writes  int
	written bytes.Buffer
}

func (c *partialWriteCloser) Read(_ []byte) (int, error) {
	return 0, io.EOF
}

func (c *partialWriteCloser) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.writes++
	n := len(p)
	if len(c.chunks) > 0 {
		n = c.chunks[0]
		c.chunks = c.chunks[1:]
		if n > len(p) {
			n = len(p)
		}
		if n < 0 {
			n = 0
		}
	}
	if n > 0 {
		_, _ = c.written.Write(p[:n])
	}
	return n, nil
}

func (c *partialWriteCloser) Close() error { return nil }

func (c *partialWriteCloser) bytes() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]byte(nil), c.written.Bytes()...)
}

func (c *partialWriteCloser) writeCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.writes
}

type zeroProgressWriteCloser struct{}

func (c *zeroProgressWriteCloser) Read(_ []byte) (int, error) {
	return 0, io.EOF
}

func (c *zeroProgressWriteCloser) Write(_ []byte) (int, error) {
	return 0, nil
}

func (c *zeroProgressWriteCloser) Close() error { return nil }

func decodeFramesForTest(t *testing.T, data []byte) []Frame {
	t.Helper()

	limits := normalizeLimits(Limits{})
	frames := make([]Frame, 0, 4)
	for len(data) > 0 {
		frame, n, err := ParseFrame(data, limits)
		if err != nil {
			t.Fatalf("parse frame: %v", err)
		}
		frames = append(frames, frame)
		data = data[n:]
	}
	return frames
}

func newWriteLoopControlTestConn(conn *captureWriteCloser) *Conn {
	settings := DefaultSettings()
	return &Conn{
		io: connIOState{conn: conn},

		pending:   connPendingControlState{controlNotify: make(chan struct{}, 1)},
		writer:    connWriterRuntimeState{writeCh: make(chan writeRequest), advisoryWriteCh: make(chan writeRequest), urgentWriteCh: make(chan writeRequest)},
		signals:   connRuntimeSignalState{livenessCh: make(chan struct{}, 1)},
		lifecycle: connLifecycleState{closedCh: make(chan struct{}), sessionState: connStateReady},

		sessionControl: connSessionControlState{
			peerGoAwayBidi:  MaxVarint62,
			peerGoAwayUni:   MaxVarint62,
			localGoAwayBidi: MaxVarint62,
			localGoAwayUni:  MaxVarint62,
		},

		ingress: connIngressAccountingState{
			aggregateLateDataCap: aggregateLateDataCapFor(settings.MaxFramePayload),
		}, config: connConfigState{local: Preface{Settings: settings},
			peer:       Preface{Settings: settings},
			negotiated: Negotiated{Proto: ProtoVersion, LocalRole: RoleInitiator, PeerRole: RoleResponder}}, flow: connFlowState{sendSessionMax: settings.InitialMaxData}, registry: connRegistryState{streams: make(map[uint64]*nativeStream),

			nextLocalBidi: state.FirstLocalStreamID(RoleInitiator, true),
			nextLocalUni:  state.FirstLocalStreamID(RoleInitiator, false),
			nextPeerBidi:  state.FirstPeerStreamID(RoleInitiator, true),
			nextPeerUni:   state.FirstPeerStreamID(RoleInitiator, false)},
	}
}

func waitForCapturedFrames(t *testing.T, conn *captureWriteCloser, want int) []Frame {
	t.Helper()

	deadline := time.Now().Add(testSignalTimeout)
	wrote := conn.wroteChan()
	for {
		frames := decodeFramesForTest(t, conn.bytes())
		if len(frames) >= want {
			return frames
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			t.Fatalf("captured frames = %d, want at least %d", len(frames), want)
		}
		wait := time.Millisecond
		if remaining < wait {
			wait = remaining
		}
		select {
		case <-wrote:
		case <-time.After(wait):
			runtime.Gosched()
		}
	}
}

func TestWriteBatchUsesSingleUnderlyingWrite(t *testing.T) {
	t.Parallel()

	conn := &countingWriteCloser{}
	c := &Conn{
		io: connIOState{conn: conn}, config: connConfigState{peer: Preface{
			Settings: Settings{
				MaxFramePayload: 16384,
			},
		}},
	}

	first := writeRequest{
		frames: testTxFramesFrom([]Frame{
			{Type: FrameTypePING, Payload: make([]byte, 8)},
		}),
		done: make(chan error, 1),
	}
	second := writeRequest{
		frames: testTxFramesFrom([]Frame{
			{Type: FrameTypePING, Payload: make([]byte, 8)},
		}),
		done: make(chan error, 1),
	}

	if err := c.writeBatch([]writeRequest{first, second}); err != nil {
		t.Fatalf("writeBatch error = %v", err)
	}
	if got := conn.writeCount(); got != 1 {
		t.Fatalf("underlying write count = %d, want 1", got)
	}
}

func TestWriteBatchPreservesEncodedBytes(t *testing.T) {
	t.Parallel()

	conn := &captureWriteCloser{}
	c := &Conn{
		io: connIOState{conn: conn}, config: connConfigState{peer: Preface{
			Settings: Settings{
				MaxFramePayload: 16384,
			},
		}},
	}

	batch := []writeRequest{
		{
			frames: testTxFramesFrom([]Frame{
				{Type: FrameTypePING, Payload: []byte{0, 1, 2, 3, 4, 5, 6, 7}},
			}),
			done: make(chan error, 1),
		},
		{
			frames: testTxFramesFrom([]Frame{
				{Type: FrameTypeDATA, StreamID: 4, Payload: []byte("hello")},
				{Type: FrameTypePONG, Payload: []byte{7, 6, 5, 4, 3, 2, 1, 0}},
			}),
			done: make(chan error, 1),
		},
	}

	var want []byte
	for _, req := range batch {
		for _, frame := range req.frames {
			var err error
			want, err = appendFrameBinaryTrusted(want, frame)
			if err != nil {
				t.Fatalf("marshal frame: %v", err)
			}
		}
	}

	if err := c.writeBatch(batch); err != nil {
		t.Fatalf("writeBatch error = %v", err)
	}
	if !bytes.Equal(conn.bytes(), want) {
		t.Fatalf("encoded bytes = %x, want %x", conn.bytes(), want)
	}
}

func TestWriteBatchRetriesPartialUnderlyingWrites(t *testing.T) {
	t.Parallel()

	conn := &partialWriteCloser{chunks: []int{3, 2, 1, 4, 8}}
	c := &Conn{
		io: connIOState{conn: conn}, config: connConfigState{peer: Preface{
			Settings: Settings{
				MaxFramePayload: 16384,
			},
		}},
	}

	batch := []writeRequest{
		{
			frames: testTxFramesFrom([]Frame{
				{Type: FrameTypePING, Payload: []byte{0, 1, 2, 3, 4, 5, 6, 7}},
			}),
			done: make(chan error, 1),
		},
		{
			frames: testTxFramesFrom([]Frame{
				{Type: FrameTypeDATA, StreamID: 4, Payload: []byte("hello")},
				{Type: FrameTypePONG, Payload: []byte{7, 6, 5, 4, 3, 2, 1, 0}},
			}),
			done: make(chan error, 1),
		},
	}

	var want []byte
	for _, req := range batch {
		for _, frame := range req.frames {
			var err error
			want, err = appendFrameBinaryTrusted(want, frame)
			if err != nil {
				t.Fatalf("marshal frame: %v", err)
			}
		}
	}

	if err := c.writeBatch(batch); err != nil {
		t.Fatalf("writeBatch error = %v", err)
	}
	if conn.writeCount() < 2 {
		t.Fatalf("underlying write count = %d, want multiple partial writes", conn.writeCount())
	}
	if !bytes.Equal(conn.bytes(), want) {
		t.Fatalf("partial-write bytes = %x, want %x", conn.bytes(), want)
	}
}

func TestWriteBatchReturnsNoProgressOnZeroWrite(t *testing.T) {
	t.Parallel()

	c := &Conn{
		io: connIOState{conn: &zeroProgressWriteCloser{}}, config: connConfigState{peer: Preface{
			Settings: Settings{
				MaxFramePayload: 16384,
			},
		}},
	}

	err := c.writeBatch([]writeRequest{{
		frames: testTxFramesFrom([]Frame{
			{Type: FrameTypePING, Payload: make([]byte, 8)},
		}),
		done: make(chan error, 1),
	}})
	if !errors.Is(err, io.ErrNoProgress) {
		t.Fatalf("writeBatch zero-progress err = %v, want %v", err, io.ErrNoProgress)
	}
}

func TestWriteBatchReturnsSmallEncodingBufferToSharedPool(t *testing.T) {
	t.Parallel()

	c := &Conn{
		io: connIOState{conn: &countingWriteCloser{}}, config: connConfigState{peer: Preface{
			Settings: Settings{
				MaxFramePayload: 16384,
			},
		}},
	}

	batch := []writeRequest{{
		frames: testTxFramesFrom([]Frame{
			{Type: FrameTypePING, Payload: []byte{0, 1, 2, 3, 4, 5, 6, 7}},
		}),
		done: make(chan error, 1),
	}}

	if err := c.writeBatch(batch); err != nil {
		t.Fatalf("first writeBatch error = %v", err)
	}

	if got := cap(c.writer.scratch.encoded); got != 0 {
		t.Fatalf("writeBatchScratch.encoded cap = %d, want 0 after release to shared pool", got)
	}

	if err := c.writeBatch(batch); err != nil {
		t.Fatalf("second writeBatch error = %v", err)
	}
	if got := cap(c.writer.scratch.encoded); got != 0 {
		t.Fatalf("writeBatchScratch.encoded cap after second release = %d, want 0", got)
	}
}

func TestWriteBatchDropsOversizedEncodingBuffer(t *testing.T) {
	t.Parallel()

	c := &Conn{
		io: connIOState{conn: &countingWriteCloser{}}, config: connConfigState{peer: Preface{
			Settings: Settings{
				MaxFramePayload: 16384,
			},
		}},
	}

	batch := []writeRequest{{
		frames: testTxFramesFrom([]Frame{
			{Type: FrameTypeDATA, StreamID: 4, Payload: make([]byte, maxRetainedWriteBatchBytes+1)},
		}),
		done: make(chan error, 1),
	}}

	if err := c.writeBatch(batch); err != nil {
		t.Fatalf("writeBatch error = %v", err)
	}
	if got := cap(c.writer.scratch.encoded); got != 0 {
		t.Fatalf("writeBatchScratch.encoded cap = %d, want 0 after oversized batch", got)
	}
}

func TestDequeueWriteRequestPrefersUrgentLane(t *testing.T) {
	t.Parallel()

	c := &Conn{
		lifecycle: connLifecycleState{closedCh: make(chan struct{})},
		writer:    connWriterRuntimeState{urgentWriteCh: make(chan writeRequest, 4), advisoryWriteCh: make(chan writeRequest, 4), writeCh: make(chan writeRequest, 4)},
	}

	urgentReq := writeRequest{done: make(chan error, 1), frames: testTxFramesFrom([]Frame{{Type: FrameTypePING, Payload: []byte("urgent")}})}
	advisoryReq := writeRequest{done: make(chan error, 1), frames: testTxFramesFrom([]Frame{{Type: FrameTypePING, Payload: []byte("advisory")}})}
	normalReq := writeRequest{done: make(chan error, 1), frames: testTxFramesFrom([]Frame{{Type: FrameTypePING, Payload: []byte("normal")}})}

	c.writer.urgentWriteCh <- urgentReq
	c.writer.advisoryWriteCh <- advisoryReq
	c.writer.writeCh <- normalReq

	work := c.dequeueWriteWork()
	if work.kind != dequeuedWriteWorkRequest {
		t.Fatalf("dequeueWriteWork kind = %v, want request", work.kind)
	}
	if work.lane != writeLaneUrgent {
		t.Fatalf("lane = %v, want %v", work.lane, writeLaneUrgent)
	}
	if work.req.frames[0].Payload[0] != urgentReq.frames[0].Payload[0] {
		t.Fatalf("got = %q, want urgent", work.req.frames[0].Payload)
	}
}

func TestDequeueWriteRequestPrefersAdvisoryOverNormal(t *testing.T) {
	t.Parallel()

	c := &Conn{
		lifecycle: connLifecycleState{closedCh: make(chan struct{})},
		writer:    connWriterRuntimeState{urgentWriteCh: make(chan writeRequest, 1), advisoryWriteCh: make(chan writeRequest, 4), writeCh: make(chan writeRequest, 4)},
	}

	advisoryReq := writeRequest{done: make(chan error, 1), frames: testTxFramesFrom([]Frame{{Type: FrameTypePING, Payload: []byte("advisory")}})}
	normalReq := writeRequest{done: make(chan error, 1), frames: testTxFramesFrom([]Frame{{Type: FrameTypePING, Payload: []byte("normal")}})}

	c.writer.advisoryWriteCh <- advisoryReq
	c.writer.writeCh <- normalReq

	work := c.dequeueWriteWork()
	if work.kind != dequeuedWriteWorkRequest {
		t.Fatalf("dequeueWriteWork kind = %v, want request", work.kind)
	}
	if work.lane != writeLaneAdvisory {
		t.Fatalf("lane = %v, want %v", work.lane, writeLaneAdvisory)
	}
	if work.req.frames[0].Payload[0] != advisoryReq.frames[0].Payload[0] {
		t.Fatalf("got = %q, want advisory", work.req.frames[0].Payload)
	}
}

func TestDequeueWriteWorkPrefersUrgentBeforeControlNotify(t *testing.T) {
	t.Parallel()

	c := &Conn{
		lifecycle: connLifecycleState{closedCh: make(chan struct{})},
		pending:   connPendingControlState{controlNotify: make(chan struct{}, 1)},
		writer:    connWriterRuntimeState{urgentWriteCh: make(chan writeRequest, 1), advisoryWriteCh: make(chan writeRequest, 1), writeCh: make(chan writeRequest, 1)},
	}

	want := writeRequest{
		done:   make(chan error, 1),
		frames: testTxFramesFrom([]Frame{{Type: FrameTypePING, Payload: []byte("urgent")}}),
	}
	c.pending.controlNotify <- struct{}{}
	c.writer.urgentWriteCh <- want

	work := c.dequeueWriteWork()
	if work.kind == dequeuedWriteWorkClosed {
		t.Fatal("dequeueWriteWork returned closed, want urgent request")
	}
	if work.kind == dequeuedWriteWorkControl {
		t.Fatal("dequeueWriteWork returned control wake, want urgent request")
	}
	if work.lane != writeLaneUrgent {
		t.Fatalf("lane = %v, want %v", work.lane, writeLaneUrgent)
	}
	if string(work.req.frames[0].Payload) != "urgent" {
		t.Fatalf("payload = %q, want %q", work.req.frames[0].Payload, "urgent")
	}
}

func TestDequeueWriteWorkPrefersControlBeforeAdvisoryWhenReady(t *testing.T) {
	t.Parallel()

	c := &Conn{
		lifecycle: connLifecycleState{closedCh: make(chan struct{})},
		pending:   connPendingControlState{controlNotify: make(chan struct{}, 1)},
		writer:    connWriterRuntimeState{urgentWriteCh: make(chan writeRequest, 1), advisoryWriteCh: make(chan writeRequest, 1), writeCh: make(chan writeRequest, 1)},
	}

	c.pending.controlNotify <- struct{}{}
	c.writer.advisoryWriteCh <- writeRequest{
		done:   make(chan error, 1),
		frames: testTxFramesFrom([]Frame{{Type: FrameTypePING, Payload: []byte("advisory")}}),
	}

	work := c.dequeueWriteWork()
	if work.kind != dequeuedWriteWorkControl {
		t.Fatalf("dequeueWriteWork kind = %v, want control wake", work.kind)
	}
}

func TestHandleWriteBatchDoesNotWriteTransportAfterClose(t *testing.T) {
	t.Parallel()

	writer := &captureWriteCloser{}
	c := newWriteLoopControlTestConn(writer)

	req := writeRequest{
		done: make(chan error, 1),
		frames: testTxFramesFrom([]Frame{{
			Type:    FrameTypePING,
			Payload: []byte("12345678"),
		}}),
	}

	close(c.lifecycle.closedCh)

	if c.handleWriteBatch([]writeRequest{req}) {
		t.Fatal("handleWriteBatch() = true, want false after closedCh close")
	}

	select {
	case err := <-req.done:
		if !errors.Is(err, ErrSessionClosed) {
			t.Fatalf("done err = %v, want %v", err, ErrSessionClosed)
		}
	case <-time.After(testSignalTimeout):
		t.Fatal("handleWriteBatch did not signal done after closedCh close")
	}

	if got := writer.bytes(); len(got) != 0 {
		t.Fatalf("transport bytes = %x, want none after closedCh close", got)
	}
}

func TestHandleWriteBatchTransportErrorDoesNotEnqueueCloseFrameFromWriterLoop(t *testing.T) {
	t.Parallel()

	c := newWriteLoopControlTestConn(&captureWriteCloser{})
	c.io.conn = &zeroProgressWriteCloser{}
	c.writer.urgentWriteCh = make(chan writeRequest, 1)

	req := writeRequest{
		done: make(chan error, 1),
		frames: testTxFramesFrom([]Frame{{
			Type:    FrameTypePING,
			Payload: []byte("12345678"),
		}}),
	}

	done := make(chan bool, 1)
	go func() {
		done <- c.handleWriteBatch([]writeRequest{req})
	}()

	select {
	case ok := <-done:
		if ok {
			t.Fatal("handleWriteBatch() = true, want false after transport error")
		}
	case <-time.After(testCloseWakeTimeout):
		t.Fatal("handleWriteBatch blocked on writer-loop close path")
	}

	if got := len(c.writer.urgentWriteCh); got != 0 {
		t.Fatalf("urgent queue len = %d, want 0 after writer transport error", got)
	}

	select {
	case err := <-req.done:
		if !errors.Is(err, io.ErrNoProgress) {
			t.Fatalf("done err = %v, want %v", err, io.ErrNoProgress)
		}
	case <-time.After(testSignalTimeout):
		t.Fatal("handleWriteBatch did not signal done after transport error")
	}

	select {
	case <-c.lifecycle.closedCh:
	default:
		t.Fatal("closedCh not closed after writer transport error")
	}
}

func TestWriteLoopFlushesPendingControlOnControlNotify(t *testing.T) {
	t.Parallel()

	writer := &captureWriteCloser{}
	c := newWriteLoopControlTestConn(writer)

	c.mu.Lock()
	stream := newPendingControlTestStream(c)
	if !c.queuePendingSessionControlAsync(sessionControlMaxData, clampVarint62(101)) {
		c.mu.Unlock()
		t.Fatal("queuePendingSessionControlAsync rejected session MAX_DATA")
	}
	c.queueStreamMaxDataAsync(stream.id, 202)
	c.queuePendingSessionControlAsync(sessionControlBlocked, 303)
	c.queueStreamBlockedAsync(stream, 404)
	c.mu.Unlock()

	done := make(chan struct{})
	go func() {
		c.writeLoop()
		close(done)
	}()
	defer func() {
		close(c.lifecycle.closedCh)
		select {
		case <-done:
		case <-time.After(testSignalTimeout):
			t.Fatal("writeLoop did not exit after close")
		}
	}()

	frames := waitForCapturedFrames(t, writer, 4)
	if len(frames) < 4 {
		t.Fatalf("captured frames = %d, want at least 4", len(frames))
	}
	if frames[0].Type != FrameTypeMAXDATA || frames[0].StreamID != 0 {
		t.Fatalf("frame 0 = %+v, want session MAX_DATA", frames[0])
	}
	if frames[1].Type != FrameTypeMAXDATA || frames[1].StreamID != stream.id {
		t.Fatalf("frame 1 = %+v, want stream MAX_DATA for %d", frames[1], stream.id)
	}
	if frames[2].Type != FrameTypeBLOCKED || frames[2].StreamID != 0 {
		t.Fatalf("frame 2 = %+v, want session BLOCKED", frames[2])
	}
	if frames[3].Type != FrameTypeBLOCKED || frames[3].StreamID != stream.id {
		t.Fatalf("frame 3 = %+v, want stream BLOCKED for %d", frames[3], stream.id)
	}
}

func TestWriteLoopFlushesPendingPriorityUpdateOnControlNotify(t *testing.T) {
	t.Parallel()

	caps := CapabilityPriorityUpdate | CapabilityPriorityHints
	payload, err := buildPriorityUpdatePayload(
		caps,
		MetadataUpdate{Priority: invalidUint64Ptr(7)},
		4096,
	)
	if err != nil {
		t.Fatalf("build priority update payload: %v", err)
	}

	writer := &captureWriteCloser{}
	c := newWriteLoopControlTestConn(writer)
	c.config.negotiated.Capabilities = caps

	c.mu.Lock()
	stream := newPendingControlTestStream(c)
	c.queuePriorityUpdateAsync(stream.id, payload, retainedBytesBorrowed)
	c.mu.Unlock()

	done := make(chan struct{})
	go func() {
		c.writeLoop()
		close(done)
	}()
	defer func() {
		close(c.lifecycle.closedCh)
		select {
		case <-done:
		case <-time.After(testSignalTimeout):
			t.Fatal("writeLoop did not exit after close")
		}
	}()

	frames := waitForCapturedFrames(t, writer, 1)
	if len(frames) != 1 {
		t.Fatalf("captured frames = %d, want 1", len(frames))
	}
	if frames[0].Type != FrameTypeEXT || frames[0].StreamID != stream.id {
		t.Fatalf("captured frame = %+v, want EXT for stream %d", frames[0], stream.id)
	}
	if string(frames[0].Payload) != string(payload) {
		t.Fatalf("payload = %x, want %x", frames[0].Payload, payload)
	}
}

func TestWriteLoopDoesNotFlushPendingPriorityUpdateWithoutControlNotify(t *testing.T) {
	t.Parallel()

	caps := CapabilityPriorityUpdate | CapabilityPriorityHints
	payload, err := buildPriorityUpdatePayload(
		caps,
		MetadataUpdate{Priority: invalidUint64Ptr(7)},
		4096,
	)
	if err != nil {
		t.Fatalf("build priority update payload: %v", err)
	}

	writer := &captureWriteCloser{}
	c := newWriteLoopControlTestConn(writer)
	c.config.negotiated.Capabilities = caps

	c.mu.Lock()
	stream := newPendingControlTestStream(c)
	testSetPendingPriorityUpdate(c, stream.id, append([]byte(nil), payload...))
	c.mu.Unlock()

	done := make(chan struct{})
	go func() {
		c.writeLoop()
		close(done)
	}()
	defer func() {
		close(c.lifecycle.closedCh)
		select {
		case <-done:
		case <-time.After(testSignalTimeout):
			t.Fatal("writeLoop did not exit after close")
		}
	}()

	select {
	case <-writer.wroteChan():
		t.Fatal("writeLoop flushed pending priority update before controlNotify")
	case <-time.After(25 * time.Millisecond):
	}

	c.mu.Lock()
	if got := testPendingPriorityUpdateCount(c); got != 1 {
		c.mu.Unlock()
		t.Fatalf("pendingPriorityUpdate count before controlNotify = %d, want 1", got)
	}
	c.mu.Unlock()

	notify(c.pending.controlNotify)

	frames := waitForCapturedFrames(t, writer, 1)
	if len(frames) != 1 {
		t.Fatalf("captured frames = %d, want 1", len(frames))
	}
	if frames[0].Type != FrameTypeEXT || frames[0].StreamID != stream.id {
		t.Fatalf("captured frame = %+v, want EXT for stream %d", frames[0], stream.id)
	}
	if string(frames[0].Payload) != string(payload) {
		t.Fatalf("payload = %x, want %x", frames[0].Payload, payload)
	}
}

func TestWriteLoopDrainsPendingPriorityUpdateBacklogAfterSingleControlNotify(t *testing.T) {
	t.Parallel()

	caps := CapabilityPriorityUpdate | CapabilityPriorityHints
	payload, err := buildPriorityUpdatePayload(
		caps,
		MetadataUpdate{Priority: invalidUint64Ptr(7)},
		4096,
	)
	if err != nil {
		t.Fatalf("build priority update payload: %v", err)
	}

	writer := &captureWriteCloser{}
	c := newWriteLoopControlTestConn(writer)
	c.config.negotiated.Capabilities = caps

	c.mu.Lock()
	wantIDs := make([]uint64, 0, maxWriteBatchFrames+1)
	for i := 0; i < maxWriteBatchFrames+1; i++ {
		streamID := state.FirstLocalStreamID(c.config.negotiated.LocalRole, true) + uint64(i*4)
		stream := c.newLocalStreamWithIDLocked(streamID, streamArityBidi, OpenOptions{}, nil)
		stream.idSet = true
		testMarkLocalOpenVisible(stream)
		c.registry.streams[streamID] = stream
		wantIDs = append(wantIDs, streamID)
		c.queuePriorityUpdateAsync(streamID, payload, retainedBytesBorrowed)
	}
	c.mu.Unlock()

	done := make(chan struct{})
	go func() {
		c.writeLoop()
		close(done)
	}()
	defer func() {
		close(c.lifecycle.closedCh)
		select {
		case <-done:
		case <-time.After(testSignalTimeout):
			t.Fatal("writeLoop did not exit after close")
		}
	}()

	frames := waitForCapturedFrames(t, writer, maxWriteBatchFrames+1)
	if len(frames) != maxWriteBatchFrames+1 {
		t.Fatalf("captured frames = %d, want %d", len(frames), maxWriteBatchFrames+1)
	}
	for i, frame := range frames {
		if frame.Type != FrameTypeEXT {
			t.Fatalf("frame %d type = %v, want %v", i, frame.Type, FrameTypeEXT)
		}
		if frame.StreamID != wantIDs[i] {
			t.Fatalf("frame %d stream = %d, want %d", i, frame.StreamID, wantIDs[i])
		}
		if string(frame.Payload) != string(payload) {
			t.Fatalf("frame %d payload = %x, want %x", i, frame.Payload, payload)
		}
	}

	c.mu.Lock()
	if got := testPendingPriorityUpdateCount(c); got != 0 {
		c.mu.Unlock()
		t.Fatalf("pendingPriorityUpdate count after drain = %d, want 0", got)
	}
	c.mu.Unlock()
}

func TestWriteLoopDrainsPendingUrgentControlBacklogAfterSingleControlNotify(t *testing.T) {
	t.Parallel()

	writer := &captureWriteCloser{}
	c := newWriteLoopControlTestConn(writer)

	c.mu.Lock()
	wantIDs := make([]uint64, 0, maxWriteBatchFrames+1)
	wantValues := make([]uint64, 0, maxWriteBatchFrames+1)
	for i := 0; i < maxWriteBatchFrames+1; i++ {
		streamID := state.FirstLocalStreamID(c.config.negotiated.LocalRole, true) + uint64(i*4)
		stream := c.newLocalStreamWithIDLocked(streamID, streamArityBidi, OpenOptions{}, nil)
		stream.idSet = true
		testMarkLocalOpenVisible(stream)
		c.registry.streams[streamID] = stream
		wantIDs = append(wantIDs, streamID)
		wantValues = append(wantValues, uint64(i+1))
		if !c.setPendingStreamControlLocked(streamControlMaxData, streamID, uint64(i+1)) {
			c.mu.Unlock()
			t.Fatalf("setPendingStreamMaxDataLocked(%d) rejected", streamID)
		}
	}
	c.mu.Unlock()
	notify(c.pending.controlNotify)

	done := make(chan struct{})
	go func() {
		c.writeLoop()
		close(done)
	}()
	defer func() {
		close(c.lifecycle.closedCh)
		select {
		case <-done:
		case <-time.After(testSignalTimeout):
			t.Fatal("writeLoop did not exit after close")
		}
	}()

	frames := waitForCapturedFrames(t, writer, maxWriteBatchFrames+1)
	if len(frames) != maxWriteBatchFrames+1 {
		t.Fatalf("captured frames = %d, want %d", len(frames), maxWriteBatchFrames+1)
	}
	for i, frame := range frames {
		if frame.Type != FrameTypeMAXDATA {
			t.Fatalf("frame %d type = %v, want %v", i, frame.Type, FrameTypeMAXDATA)
		}
		if frame.StreamID != wantIDs[i] {
			t.Fatalf("frame %d stream = %d, want %d", i, frame.StreamID, wantIDs[i])
		}
		if string(frame.Payload) != string(encodeClampedVarint62(wantValues[i])) {
			t.Fatalf("frame %d payload = %x, want %x", i, frame.Payload, encodeClampedVarint62(wantValues[i]))
		}
	}

	c.mu.Lock()
	if got := testPendingStreamMaxDataCount(c); got != 0 {
		c.mu.Unlock()
		t.Fatalf("pendingStreamMaxData count after drain = %d, want 0", got)
	}
	c.mu.Unlock()
}

func TestDispatchPreparedQueueRequestsDispatchesAllAdvisoryRequestsBeforeWaitingForCompletion(t *testing.T) {
	t.Parallel()

	caps := CapabilityPriorityUpdate | CapabilityPriorityHints
	payload, err := buildPriorityUpdatePayload(
		caps,
		MetadataUpdate{Priority: invalidUint64Ptr(7)},
		4096,
	)
	if err != nil {
		t.Fatalf("build priority update payload: %v", err)
	}

	c := newWriteLoopControlTestConn(&captureWriteCloser{})
	c.writer.advisoryWriteCh = make(chan writeRequest, 2)
	c.config.negotiated.Capabilities = caps

	c.mu.Lock()
	for i := 0; i < maxWriteBatchFrames+1; i++ {
		streamID := state.FirstLocalStreamID(c.config.negotiated.LocalRole, true) + uint64(i*4)
		stream := c.newLocalStreamWithIDLocked(streamID, streamArityBidi, OpenOptions{}, nil)
		stream.idSet = true
		testMarkLocalOpenVisible(stream)
		c.registry.streams[streamID] = stream
		c.queuePriorityUpdateAsync(streamID, payload, retainedBytesBorrowed)
	}
	reqs, err := c.takePendingPriorityUpdateRequestsLocked()
	urgent := testDrainPendingUrgentControlFrames(c)
	c.mu.Unlock()
	if err != nil {
		t.Fatalf("takePendingPriorityUpdateRequestsLocked() err = %v", err)
	}
	if got := len(urgent); got != 0 {
		t.Fatalf("urgent frame count = %d, want 0", got)
	}
	if got := len(reqs); got != 2 {
		t.Fatalf("advisory request count = %d, want 2", got)
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- c.dispatchPreparedQueueRequests(reqs, preparedQueueDispatchOptions{
			lane:      writeLaneAdvisory,
			ownership: frameOwned,
		})
	}()

	var first, second writeRequest
	select {
	case first = <-c.writer.advisoryWriteCh:
	case <-time.After(testSignalTimeout):
		t.Fatal("first advisory request was not dispatched")
	}
	select {
	case second = <-c.writer.advisoryWriteCh:
	case <-time.After(testSignalTimeout):
		t.Fatal("second advisory request was not dispatched before first completion")
	}

	if got := len(first.frames); got != maxWriteBatchFrames {
		t.Fatalf("first advisory request frames = %d, want %d", got, maxWriteBatchFrames)
	}
	if got := len(second.frames); got != 1 {
		t.Fatalf("second advisory request frames = %d, want 1", got)
	}

	select {
	case err := <-errCh:
		t.Fatalf("dispatchPreparedQueueRequests returned early with %v", err)
	default:
	}

	first.done <- nil

	select {
	case err := <-errCh:
		t.Fatalf("dispatchPreparedQueueRequests returned before second completion with %v", err)
	default:
	}

	second.done <- nil

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("dispatchPreparedQueueRequests err = %v, want nil", err)
		}
	case <-time.After(testSignalTimeout):
		t.Fatal("dispatchPreparedQueueRequests did not finish after both advisory requests completed")
	}
}

func TestDispatchPreparedQueueRequestsWaitsForDispatchedAdvisoryRequestWhenLaterDispatchFails(t *testing.T) {
	t.Parallel()

	c := newWriteLoopControlTestConn(&captureWriteCloser{})
	c.writer.advisoryWriteCh = make(chan writeRequest, 1)
	c.flow.sessionMemoryCap = 8

	reqs := []writeRequest{
		{
			frames:      testTxFramesFrom([]Frame{{Type: FrameTypeEXT, StreamID: 4, Payload: []byte{0x01}}}),
			done:        make(chan error, 1),
			queuedBytes: 8,
		},
		{
			frames:      testTxFramesFrom([]Frame{{Type: FrameTypeEXT, StreamID: 8, Payload: []byte{0x01}}}),
			done:        make(chan error, 1),
			queuedBytes: 8,
		},
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- c.dispatchPreparedQueueRequests(reqs, preparedQueueDispatchOptions{
			lane:      writeLaneAdvisory,
			ownership: frameOwned,
		})
	}()

	var first writeRequest
	select {
	case first = <-c.writer.advisoryWriteCh:
	case <-time.After(testSignalTimeout):
		t.Fatal("first advisory request was not dispatched")
	}

	select {
	case err := <-errCh:
		t.Fatalf("dispatchPreparedQueueRequests returned before dispatched advisory request completed: %v", err)
	default:
	}

	first.done <- nil

	select {
	case err := <-errCh:
		if !IsErrorCode(err, CodeInternal) {
			t.Fatalf("dispatchPreparedQueueRequests err = %v, want %s", err, CodeInternal)
		}
	case <-time.After(testSignalTimeout):
		t.Fatal("dispatchPreparedQueueRequests did not return after dispatched advisory request completed")
	}
}

func TestFlushPendingControlBatchesMarksTakeErrorFatal(t *testing.T) {
	t.Parallel()

	caps := CapabilityPriorityUpdate | CapabilityPriorityHints
	payload, err := buildPriorityUpdatePayload(
		caps,
		MetadataUpdate{Priority: invalidUint64Ptr(7)},
		4096,
	)
	if err != nil {
		t.Fatalf("build priority update payload: %v", err)
	}

	c := newWriteLoopControlTestConn(&captureWriteCloser{})
	c.config.negotiated.Capabilities = caps

	c.mu.Lock()
	stream := newPendingControlTestStream(c)
	c.queuePriorityUpdateAsync(stream.id, payload, retainedBytesBorrowed)
	c.flow.sessionMemoryCap = 1
	c.mu.Unlock()

	fatal, err := c.flushPendingControlBatches()
	if fatal {
		t.Fatal("flushPendingControlBatches fatal = true, want silent advisory drop")
	}
	if err != nil {
		t.Fatalf("flushPendingControlBatches err = %v, want nil on advisory drop", err)
	}

	select {
	case req := <-c.writer.advisoryWriteCh:
		t.Fatalf("unexpected advisory request dispatched after advisory drop: %+v", req)
	default:
	}

	c.mu.Lock()
	if got := testPendingPriorityUpdateCount(c); got != 0 {
		c.mu.Unlock()
		t.Fatalf("pending priority updates after advisory drop = %d, want 0", got)
	}
	if got := c.pending.priorityBytes; got != 0 {
		c.mu.Unlock()
		t.Fatalf("pendingPriorityBytes after advisory drop = %d, want 0", got)
	}
	c.mu.Unlock()
}

func TestTakePendingUrgentControlRequestLockedLeavesOverflowPending(t *testing.T) {
	t.Parallel()

	c := newWriteLoopControlTestConn(&captureWriteCloser{})

	c.mu.Lock()
	for i := 0; i < maxWriteBatchFrames+1; i++ {
		streamID := state.FirstLocalStreamID(c.config.negotiated.LocalRole, true) + uint64(i*4)
		stream := c.newLocalStreamWithIDLocked(streamID, streamArityBidi, OpenOptions{}, nil)
		stream.idSet = true
		testMarkLocalOpenVisible(stream)
		c.registry.streams[streamID] = stream
		if !c.setPendingStreamControlLocked(streamControlMaxData, streamID, uint64(i+1)) {
			c.mu.Unlock()
			t.Fatalf("setPendingStreamMaxDataLocked(%d) rejected", streamID)
		}
	}

	first := c.takePendingUrgentControlRequestLocked()
	if first.err != nil {
		c.mu.Unlock()
		t.Fatalf("takePendingUrgentControlRequestLocked() err = %v", first.err)
	}
	if !first.hasRequest() {
		c.mu.Unlock()
		t.Fatal("takePendingUrgentControlRequestLocked() returned !ok")
	}
	if got := len(first.request.frames); got != maxWriteBatchFrames {
		c.mu.Unlock()
		t.Fatalf("first urgent chunk frames = %d, want %d", got, maxWriteBatchFrames)
	}
	if got := testPendingStreamMaxDataCount(c); got != 1 {
		c.mu.Unlock()
		t.Fatalf("pendingStreamMaxData after first chunk = %d, want 1", got)
	}
	if c.pending.controlBytes == 0 {
		c.mu.Unlock()
		t.Fatal("pendingControlBytes after first urgent chunk = 0, want > 0")
	}

	second := c.takePendingUrgentControlRequestLocked()
	if c.pending.controlBytes != 0 {
		c.mu.Unlock()
		t.Fatalf("pendingControlBytes after second urgent chunk = %d, want 0", c.pending.controlBytes)
	}
	c.mu.Unlock()
	if second.err != nil {
		t.Fatalf("second takePendingUrgentControlRequestLocked() err = %v", second.err)
	}
	if !second.hasRequest() {
		t.Fatal("second takePendingUrgentControlRequestLocked() returned !ok")
	}
	if got := len(second.request.frames); got != 1 {
		t.Fatalf("second urgent chunk frames = %d, want 1", got)
	}
}

func TestTakePendingPriorityUpdateRequestLockedLeavesOverflowPending(t *testing.T) {
	t.Parallel()

	caps := CapabilityPriorityUpdate | CapabilityPriorityHints
	payload, err := buildPriorityUpdatePayload(
		caps,
		MetadataUpdate{Priority: invalidUint64Ptr(7)},
		4096,
	)
	if err != nil {
		t.Fatalf("build priority update payload: %v", err)
	}

	c := newWriteLoopControlTestConn(&captureWriteCloser{})
	c.config.negotiated.Capabilities = caps

	c.mu.Lock()
	for i := 0; i < maxWriteBatchFrames+1; i++ {
		streamID := state.FirstLocalStreamID(c.config.negotiated.LocalRole, true) + uint64(i*4)
		stream := c.newLocalStreamWithIDLocked(streamID, streamArityBidi, OpenOptions{}, nil)
		stream.idSet = true
		testMarkLocalOpenVisible(stream)
		c.registry.streams[streamID] = stream
		c.queuePriorityUpdateAsync(streamID, payload, retainedBytesBorrowed)
	}

	first := c.takePendingPriorityUpdateRequestLocked()
	if first.err != nil {
		c.mu.Unlock()
		t.Fatalf("takePendingPriorityUpdateRequestLocked() err = %v", first.err)
	}
	if !first.hasRequest() {
		c.mu.Unlock()
		t.Fatal("takePendingPriorityUpdateRequestLocked() returned !ok")
	}
	if got := len(first.request.frames); got != maxWriteBatchFrames {
		c.mu.Unlock()
		t.Fatalf("first advisory chunk frames = %d, want %d", got, maxWriteBatchFrames)
	}
	if got := testPendingPriorityUpdateCount(c); got != 1 {
		c.mu.Unlock()
		t.Fatalf("pendingPriorityUpdate after first chunk = %d, want 1", got)
	}
	if c.pending.priorityBytes == 0 {
		c.mu.Unlock()
		t.Fatal("pendingPriorityBytes after first advisory chunk = 0, want > 0")
	}

	second := c.takePendingPriorityUpdateRequestLocked()
	if c.pending.priorityBytes != 0 {
		c.mu.Unlock()
		t.Fatalf("pendingPriorityBytes after second advisory chunk = %d, want 0", c.pending.priorityBytes)
	}
	c.mu.Unlock()
	if second.err != nil {
		t.Fatalf("second takePendingPriorityUpdateRequestLocked() err = %v", second.err)
	}
	if !second.hasRequest() {
		t.Fatal("second takePendingPriorityUpdateRequestLocked() returned !ok")
	}
	if got := len(second.request.frames); got != 1 {
		t.Fatalf("second advisory chunk frames = %d, want 1", got)
	}
}

func TestWriteAllRetriesPartialWrites(t *testing.T) {
	t.Parallel()

	w := &partialWriteCloser{chunks: []int{2, 1, 3, 8}}
	payload := []byte("abcdefghij")

	if err := rt.WriteAll(w, payload); err != nil {
		t.Fatalf("writeAll err = %v, want nil", err)
	}
	if !bytes.Equal(w.bytes(), payload) {
		t.Fatalf("written bytes = %q, want %q", w.bytes(), payload)
	}
	if w.writeCount() < 2 {
		t.Fatalf("underlying write count = %d, want multiple partial writes", w.writeCount())
	}
}

func TestWriteAllReturnsNoProgressOnZeroWrite(t *testing.T) {
	t.Parallel()

	err := rt.WriteAll(&zeroProgressWriteCloser{}, []byte("abc"))
	if !errors.Is(err, io.ErrNoProgress) {
		t.Fatalf("writeAll err = %v, want %v", err, io.ErrNoProgress)
	}
}

func TestClassifyWriteRequestSingleFrameDataFIN(t *testing.T) {
	t.Parallel()

	frame := Frame{Type: FrameTypeDATA, StreamID: 4, Flags: FrameFlagFIN, Payload: []byte("x")}
	req := &writeRequest{frames: testTxFramesFrom([]Frame{frame})}

	classifyWriteRequest(req)

	if !req.requestMetaReady {
		t.Fatal("requestMetaReady = false, want true")
	}
	if !req.requestStreamScoped || !req.requestStreamIDKnown || req.requestStreamID != 4 {
		t.Fatalf("stream meta = (%v,%v,%d), want (true,true,4)", req.requestStreamScoped, req.requestStreamIDKnown, req.requestStreamID)
	}
	if req.requestAllUrgent {
		t.Fatal("requestAllUrgent = true, want false")
	}
	if !req.terminalDataPriority || !req.terminalHasFIN {
		t.Fatalf("terminal data flags = (%v,%v), want (true,true)", req.terminalDataPriority, req.terminalHasFIN)
	}
	if req.terminalResetOnly || req.terminalAbortOnly {
		t.Fatalf("terminal reset/abort flags = (%v,%v), want (false,false)", req.terminalResetOnly, req.terminalAbortOnly)
	}
	if req.requestBufferedBytes != rt.FrameBufferedBytes(frame) {
		t.Fatalf("requestBufferedBytes = %d, want %d", req.requestBufferedBytes, rt.FrameBufferedBytes(frame))
	}
	if req.requestCost != int64(rt.FrameBufferedBytes(frame)) {
		t.Fatalf("requestCost = %d, want %d", req.requestCost, int64(rt.FrameBufferedBytes(frame)))
	}
}

func TestClassifyWriteRequestSingleFramePriorityUpdate(t *testing.T) {
	t.Parallel()

	frame := Frame{Type: FrameTypeEXT, StreamID: 9, Payload: mustEncodeVarint(uint64(EXTPriorityUpdate))}
	req := &writeRequest{frames: testTxFramesFrom([]Frame{frame})}

	classifyWriteRequest(req)

	if !req.requestStreamScoped || !req.requestStreamIDKnown || req.requestStreamID != 9 {
		t.Fatalf("stream meta = (%v,%v,%d), want (true,true,9)", req.requestStreamScoped, req.requestStreamIDKnown, req.requestStreamID)
	}
	if !req.requestIsPriorityUpdate {
		t.Fatal("requestIsPriorityUpdate = false, want true")
	}
	if !req.terminalDataPriority {
		t.Fatal("terminalDataPriority = false, want true for PRIORITY_UPDATE")
	}
	if req.terminalResetOnly || req.terminalAbortOnly || req.terminalHasFIN {
		t.Fatalf("terminal flags = (%v,%v,%v), want (false,false,false)", req.terminalResetOnly, req.terminalAbortOnly, req.terminalHasFIN)
	}
}

func TestClassifyWriteRequestSingleFrameReset(t *testing.T) {
	t.Parallel()

	frame := Frame{Type: FrameTypeRESET, StreamID: 7, Payload: mustEncodeVarint(uint64(CodeCancelled))}
	req := &writeRequest{frames: testTxFramesFrom([]Frame{frame})}

	classifyWriteRequest(req)

	if !req.requestAllUrgent {
		t.Fatal("requestAllUrgent = false, want true")
	}
	if !req.requestStreamScoped || !req.requestStreamIDKnown || req.requestStreamID != 7 {
		t.Fatalf("stream meta = (%v,%v,%d), want (true,true,7)", req.requestStreamScoped, req.requestStreamIDKnown, req.requestStreamID)
	}
	if req.terminalDataPriority {
		t.Fatal("terminalDataPriority = true, want false")
	}
	if !req.terminalResetOnly || req.terminalAbortOnly {
		t.Fatalf("terminal reset/abort flags = (%v,%v), want (true,false)", req.terminalResetOnly, req.terminalAbortOnly)
	}
}

func TestClassifyWriteRequestSingleFrameSessionControl(t *testing.T) {
	t.Parallel()

	frame := Frame{Type: FrameTypePING, Payload: []byte{1}}
	req := &writeRequest{frames: testTxFramesFrom([]Frame{frame})}

	classifyWriteRequest(req)

	if !req.requestAllUrgent {
		t.Fatal("requestAllUrgent = false, want true")
	}
	if req.requestStreamScoped || req.requestStreamIDKnown {
		t.Fatalf("stream meta = (%v,%v), want (false,false)", req.requestStreamScoped, req.requestStreamIDKnown)
	}
	if req.terminalDataPriority || req.terminalResetOnly || req.terminalAbortOnly || req.terminalHasFIN {
		t.Fatalf("terminal flags = (%v,%v,%v,%v), want all false", req.terminalDataPriority, req.terminalResetOnly, req.terminalAbortOnly, req.terminalHasFIN)
	}
}

func TestBuildFramesLaneRequestSingleFrameMarksMetaReady(t *testing.T) {
	t.Parallel()

	settings := DefaultSettings()
	c := &Conn{

		lifecycle: connLifecycleState{closedCh: make(chan struct{})}, config: connConfigState{local: Preface{Settings: settings},
			peer: Preface{Settings: settings}},
	}
	frame := Frame{Type: FrameTypeDATA, StreamID: 4, Payload: []byte("x")}

	opts := frameLaneRequestOptions{origin: writeRequestOriginStream}
	req, err := testBuildFrameLaneRequest(c, []Frame{frame}, opts)
	if err != nil {
		t.Fatalf("buildFramesLaneRequest() err = %v", err)
	}
	if !req.requestMetaReady {
		t.Fatal("requestMetaReady = false, want true")
	}
	if !req.requestStreamScoped || !req.requestStreamIDKnown || req.requestStreamID != 4 {
		t.Fatalf("stream meta = (%v,%v,%d), want (true,true,4)", req.requestStreamScoped, req.requestStreamIDKnown, req.requestStreamID)
	}
	if req.requestBufferedBytes != rt.FrameBufferedBytes(frame) {
		t.Fatalf("requestBufferedBytes = %d, want %d", req.requestBufferedBytes, rt.FrameBufferedBytes(frame))
	}
}

func TestClassifyWriteRequestMultiFramePriorityUpdateWithData(t *testing.T) {
	t.Parallel()

	frames := []Frame{
		{Type: FrameTypeEXT, StreamID: 9, Payload: mustEncodeVarint(uint64(EXTPriorityUpdate))},
		{Type: FrameTypeDATA, StreamID: 9, Payload: []byte("x")},
	}
	req := &writeRequest{frames: testTxFramesFrom(frames)}

	classifyWriteRequest(req)

	if !req.requestStreamScoped || !req.requestStreamIDKnown || req.requestStreamID != 9 {
		t.Fatalf("stream meta = (%v,%v,%d), want (true,true,9)", req.requestStreamScoped, req.requestStreamIDKnown, req.requestStreamID)
	}
	if !req.terminalDataPriority {
		t.Fatal("terminalDataPriority = false, want true")
	}
	if req.terminalResetOnly || req.terminalAbortOnly || req.terminalHasFIN {
		t.Fatalf("terminal flags = (%v,%v,%v), want (false,false,false)", req.terminalResetOnly, req.terminalAbortOnly, req.terminalHasFIN)
	}
	if got, want := req.requestBufferedBytes, rt.FramesBufferedBytes(frames); got != want {
		t.Fatalf("requestBufferedBytes = %d, want %d", got, want)
	}
}

func TestClassifyWriteRequestMultiFrameDataAfterFINClearsTerminalPriority(t *testing.T) {
	t.Parallel()

	frames := []Frame{
		{Type: FrameTypeDATA, StreamID: 4, Flags: FrameFlagFIN, Payload: []byte("x")},
		{Type: FrameTypeDATA, StreamID: 4, Payload: []byte("y")},
	}
	req := &writeRequest{frames: testTxFramesFrom(frames)}

	classifyWriteRequest(req)

	if !req.requestStreamScoped || !req.requestStreamIDKnown || req.requestStreamID != 4 {
		t.Fatalf("stream meta = (%v,%v,%d), want (true,true,4)", req.requestStreamScoped, req.requestStreamIDKnown, req.requestStreamID)
	}
	if req.terminalDataPriority {
		t.Fatal("terminalDataPriority = true, want false")
	}
	if !req.terminalHasFIN {
		t.Fatal("terminalHasFIN = false, want true")
	}
	if req.terminalResetOnly || req.terminalAbortOnly {
		t.Fatalf("terminal reset/abort flags = (%v,%v), want (false,false)", req.terminalResetOnly, req.terminalAbortOnly)
	}
}

func TestClassifyWriteRequestMultiFrameMixedStreamsClearsTerminalFlags(t *testing.T) {
	t.Parallel()

	frames := []Frame{
		{Type: FrameTypeDATA, StreamID: 4, Flags: FrameFlagFIN, Payload: []byte("x")},
		{Type: FrameTypeDATA, StreamID: 5, Payload: []byte("y")},
	}
	req := &writeRequest{frames: testTxFramesFrom(frames)}

	classifyWriteRequest(req)

	if req.requestStreamScoped || req.requestStreamIDKnown {
		t.Fatalf("stream meta = (%v,%v), want (false,false)", req.requestStreamScoped, req.requestStreamIDKnown)
	}
	if req.terminalDataPriority || req.terminalResetOnly || req.terminalAbortOnly || req.terminalHasFIN {
		t.Fatalf("terminal flags = (%v,%v,%v,%v), want all false", req.terminalDataPriority, req.terminalResetOnly, req.terminalAbortOnly, req.terminalHasFIN)
	}
	if got, want := req.requestBufferedBytes, rt.FramesBufferedBytes(frames); got != want {
		t.Fatalf("requestBufferedBytes = %d, want %d", got, want)
	}
}

func TestClassifyWriteRequestClearsStaleDerivedState(t *testing.T) {
	t.Parallel()

	frames := []Frame{
		{Type: FrameTypeDATA, StreamID: 4, Payload: []byte("ab")},
		{Type: FrameTypeDATA, StreamID: 4, Flags: FrameFlagFIN, Payload: []byte("cd")},
	}
	req := &writeRequest{
		frames:                  testTxFramesFrom(frames),
		requestStreamID:         99,
		requestStreamIDKnown:    true,
		requestStreamScoped:     true,
		requestUrgencyRank:      -1,
		requestCost:             1234,
		requestBufferedBytes:    4321,
		requestIsPriorityUpdate: true,
		requestAllUrgent:        true,
		terminalDataPriority:    false,
		terminalResetOnly:       true,
		terminalAbortOnly:       true,
		terminalHasFIN:          false,
	}

	classifyWriteRequest(req)

	if !req.requestMetaReady {
		t.Fatal("requestMetaReady = false, want true")
	}
	if !req.requestStreamScoped || !req.requestStreamIDKnown || req.requestStreamID != 4 {
		t.Fatalf("stream meta = (%v,%v,%d), want (true,true,4)", req.requestStreamScoped, req.requestStreamIDKnown, req.requestStreamID)
	}
	if req.requestIsPriorityUpdate {
		t.Fatal("requestIsPriorityUpdate = true, want false")
	}
	if req.requestAllUrgent {
		t.Fatal("requestAllUrgent = true, want false")
	}
	if !req.terminalDataPriority || !req.terminalHasFIN {
		t.Fatalf("terminal flags = (%v,%v), want (true,true)", req.terminalDataPriority, req.terminalHasFIN)
	}
	if req.terminalResetOnly || req.terminalAbortOnly {
		t.Fatalf("terminal reset/abort flags = (%v,%v), want (false,false)", req.terminalResetOnly, req.terminalAbortOnly)
	}
	if got, want := req.requestBufferedBytes, rt.FramesBufferedBytes(frames); got != want {
		t.Fatalf("requestBufferedBytes = %d, want %d", got, want)
	}
	if got, want := req.requestCost, int64(rt.FramesBufferedBytes(frames)); got != want {
		t.Fatalf("requestCost = %d, want %d", got, want)
	}
}

func TestCloneWriteRequestFramesIfNeededSingleFrame(t *testing.T) {
	t.Parallel()

	payload := []byte("12345678")
	frames := []Frame{{Type: FrameTypePING, Payload: payload}}
	sourceTx := testTxFramesFrom(frames)
	req := &writeRequest{
		frames:                sourceTx,
		cloneFramesBeforeSend: true,
	}

	req.frames, req.cloneFramesBeforeSend = cloneTxFramesIfNeeded(req.frames, req.cloneFramesBeforeSend)
	if req.cloneFramesBeforeSend {
		t.Fatal("cloneFramesBeforeSend = true, want false")
	}
	if len(req.frames) != 1 {
		t.Fatalf("len(req.frames) = %d, want 1", len(req.frames))
	}
	if &req.frames[0] == &sourceTx[0] {
		t.Fatal("single-frame request reused source backing array")
	}
	payload[0] = 'x'
	if req.frames[0].Payload[0] != '1' {
		t.Fatal("single-frame request reused source payload backing array")
	}
}

func TestCloneWriteRequestFramesIfNeededMultiFrame(t *testing.T) {
	t.Parallel()

	first := []byte("12345678")
	second := []byte("abcdefgh")
	frames := []Frame{
		{Type: FrameTypePING, Payload: first},
		{Type: FrameTypePONG, Payload: second},
	}
	sourceTx := testTxFramesFrom(frames)
	req := &writeRequest{
		frames:                sourceTx,
		cloneFramesBeforeSend: true,
	}

	req.frames, req.cloneFramesBeforeSend = cloneTxFramesIfNeeded(req.frames, req.cloneFramesBeforeSend)
	if req.cloneFramesBeforeSend {
		t.Fatal("cloneFramesBeforeSend = true, want false")
	}
	if len(req.frames) != len(frames) {
		t.Fatalf("len(req.frames) = %d, want %d", len(req.frames), len(frames))
	}
	if &req.frames[0] == &sourceTx[0] {
		t.Fatal("multi-frame request reused source backing array")
	}
	first[0] = 'x'
	second[0] = 'y'
	if req.frames[0].Payload[0] != '1' {
		t.Fatal("multi-frame request reused first payload backing array")
	}
	if req.frames[1].Payload[0] != 'a' {
		t.Fatal("multi-frame request reused second payload backing array")
	}
}

func TestEnsureWriteRequestDoneAllocatesOnce(t *testing.T) {
	t.Parallel()

	req := &writeRequest{}
	req.ensureDoneChan()
	if req.done == nil {
		t.Fatal("req.done = nil, want allocated channel")
	}
	first := req.done
	req.ensureDoneChan()
	if req.done != first {
		t.Fatal("ensureWriteRequestDone replaced existing channel")
	}
}

func TestClearRetainedRefsClearsCachedRequestState(t *testing.T) {
	t.Parallel()

	stream := testBuildDetachedStream(nil, 4)
	req := &writeRequest{
		frames:                   testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: 4, Payload: []byte("x")}}),
		done:                     make(chan error, 1),
		queueReserved:            true,
		queuedBytes:              7,
		reservedStream:           stream,
		urgentReserved:           true,
		advisoryReserved:         true,
		cloneFramesBeforeSend:    true,
		requestMetaReady:         true,
		requestStreamID:          4,
		requestStreamIDKnown:     true,
		requestStreamScoped:      true,
		requestUrgencyRank:       3,
		requestCost:              9,
		requestBufferedBytes:     11,
		requestIsPriorityUpdate:  true,
		requestAllUrgent:         true,
		terminalDataPriority:     true,
		terminalResetOnly:        true,
		terminalAbortOnly:        true,
		terminalHasFIN:           true,
		preparedSendBytes:        13,
		preparedSendFin:          true,
		preparedOpenerVisibility: openerVisibilityPeerVisible,
		preparedPriorityStreamID: 4,
		preparedPriorityPayload:  []byte("prio"),
		preparedPriorityBytes:    15,
		preparedPriorityQueued:   true,
	}

	req.clearRetainedRefs()

	if req.frames != nil || req.done != nil || req.reservedStream != nil {
		t.Fatal("retained request refs not cleared")
	}
	if req.queueReserved || req.queuedBytes != 0 || req.urgentReserved || req.advisoryReserved {
		t.Fatalf("queue state retained = (%v,%d,%v,%v), want cleared", req.queueReserved, req.queuedBytes, req.urgentReserved, req.advisoryReserved)
	}
	if req.cloneFramesBeforeSend {
		t.Fatal("cloneFramesBeforeSend = true, want false")
	}
	if req.requestMetaReady || req.requestStreamID != 0 || req.requestStreamIDKnown || req.requestStreamScoped ||
		req.requestUrgencyRank != 0 || req.requestCost != 0 || req.requestBufferedBytes != 0 || req.requestIsPriorityUpdate ||
		req.requestAllUrgent || req.terminalDataPriority || req.terminalResetOnly || req.terminalAbortOnly || req.terminalHasFIN {
		t.Fatalf("cached request classification retained = %+v", *req)
	}
	if req.preparedSendBytes != 0 || req.preparedSendFin || req.preparedOpenerVisibility != openerVisibilityUnchanged ||
		req.preparedPriorityStreamID != 0 || req.preparedPriorityPayload != nil || req.preparedPriorityBytes != 0 || req.preparedPriorityQueued {
		t.Fatalf("prepared request state retained = (%d,%v,%v,%d,%x,%d,%v), want cleared",
			req.preparedSendBytes,
			req.preparedSendFin,
			req.preparedOpenerVisibility,
			req.preparedPriorityStreamID,
			req.preparedPriorityPayload,
			req.preparedPriorityBytes,
			req.preparedPriorityQueued,
		)
	}
}

func TestEnqueuePreparedQueueRequestSkipsPrepareWhenAlreadyClosed(t *testing.T) {
	t.Parallel()

	c := &Conn{
		lifecycle: connLifecycleState{closedCh: make(chan struct{})},
		writer:    connWriterRuntimeState{advisoryWriteCh: make(chan writeRequest, 1)},
	}
	close(c.lifecycle.closedCh)

	payload := []byte("12345678")
	req := &writeRequest{
		frames: testTxFramesFrom([]Frame{{
			Type:    FrameTypePING,
			Payload: payload,
		}}),
		cloneFramesBeforeSend: true,
	}
	framePtr := &req.frames[0]
	payloadPtr := &req.frames[0].Payload[0]

	err := c.enqueuePreparedQueueRequest(req, preparedQueueDispatchOptions{
		lane: writeLaneAdvisory,
	})
	if !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("enqueuePreparedQueueRequest err = %v, want %v", err, ErrSessionClosed)
	}
	if req.done != nil {
		t.Fatal("req.done allocated even though request never reached writer admission")
	}
	if req.advisoryReserved {
		t.Fatal("req.advisoryReserved = true, want released on closed session")
	}
	if got := c.flow.advisoryQueuedBytes; got != 0 {
		t.Fatalf("advisoryQueuedBytes = %d, want 0 after closed-session rejection", got)
	}
	if &req.frames[0] != framePtr {
		t.Fatal("request frame slice was cloned even though session was already closed")
	}
	if &req.frames[0].Payload[0] != payloadPtr {
		t.Fatal("request payload was cloned even though session was already closed")
	}
	select {
	case queued := <-c.writer.advisoryWriteCh:
		t.Fatalf("unexpected queued request after closed-session rejection: %+v", queued)
	default:
	}
}

func TestDispatchPreparedQueueRequestClearsPreparedStateOnClosedBeforeAdmission(t *testing.T) {
	t.Parallel()

	c := &Conn{
		lifecycle: connLifecycleState{closedCh: make(chan struct{})},
		writer:    connWriterRuntimeState{writeCh: make(chan writeRequest)},
	}

	req := &writeRequest{
		preparedNotify: make(chan struct{}),
		frames: testTxFramesFrom([]Frame{{
			Type:    FrameTypePING,
			Payload: []byte("payload"),
		}}),
	}
	preparedCh := req.preparedNotify

	errCh := make(chan error, 1)
	go func() {
		errCh <- c.dispatchPreparedQueueRequest(req, preparedQueueDispatchOptions{})
	}()

	select {
	case <-preparedCh:
	case <-time.After(testSignalTimeout):
		t.Fatal("request was not prepared before close")
	}

	close(c.lifecycle.closedCh)

	select {
	case err := <-errCh:
		if !errors.Is(err, ErrSessionClosed) {
			t.Fatalf("dispatchPreparedQueueRequest err = %v, want %v", err, ErrSessionClosed)
		}
	case <-time.After(testSignalTimeout):
		t.Fatal("dispatchPreparedQueueRequest did not return after closedCh close")
	}

	if req.frames != nil || req.done != nil || req.cloneFramesBeforeSend {
		t.Fatalf("prepared request state retained after closed admission: frames=%v done=%v clone=%v", req.frames, req.done, req.cloneFramesBeforeSend)
	}
}

func TestWaitPreparedQueueRequestDoesNotConsumeControlNotify(t *testing.T) {
	t.Parallel()

	c := &Conn{
		pending:   connPendingControlState{controlNotify: make(chan struct{}, 1)},
		lifecycle: connLifecycleState{closedCh: make(chan struct{}), terminalCh: make(chan struct{})},
	}
	req := writeRequest{
		frames: testTxFramesFrom([]Frame{{
			Type:    FrameTypePING,
			Payload: []byte("payload"),
		}}),
		done: make(chan error, 1),
	}

	c.pending.controlNotify <- struct{}{}
	req.done <- nil

	if err := c.waitPreparedQueueRequest(&req); err != nil {
		t.Fatalf("waitPreparedQueueRequest err = %v, want nil", err)
	}

	select {
	case <-c.pending.controlNotify:
	default:
		t.Fatal("waitPreparedQueueRequest consumed conn.controlNotify, want prepared-request wait to leave control-flush signal intact")
	}
}

func TestDispatchPreparedQueueRequestUrgentWaitUsesUrgentBroadcastWake(t *testing.T) {
	t.Parallel()

	c := &Conn{
		lifecycle: connLifecycleState{closedCh: make(chan struct{})},
		pending:   connPendingControlState{controlNotify: make(chan struct{}, 1)},
		writer:    connWriterRuntimeState{urgentWriteCh: make(chan writeRequest, 1)},
	}

	req := &writeRequest{
		frames: testTxFramesFrom([]Frame{{
			Type:    FrameTypePING,
			Payload: []byte("payload"),
		}}),
	}
	blockedBytes := c.urgentLaneCapLocked()
	if blockedBytes == 0 {
		blockedBytes = 1
	}

	c.mu.Lock()
	c.flow.urgentQueuedBytes = blockedBytes
	c.mu.Unlock()

	errCh := make(chan error, 1)
	go func() {
		errCh <- c.dispatchPreparedQueueRequest(req, preparedQueueDispatchOptions{lane: writeLaneUrgent})
	}()

	deadline := time.Now().Add(testSignalTimeout)
	for {
		c.mu.Lock()
		armed := c.signals.urgentWakeCh != nil
		c.mu.Unlock()
		if armed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("urgent request did not arm urgent wake while blocked")
		}
		time.Sleep(time.Millisecond)
	}

	c.mu.Lock()
	c.flow.urgentQueuedBytes = 0
	c.broadcastUrgentWakeLocked()
	c.mu.Unlock()

	select {
	case queued := <-c.writer.urgentWriteCh:
		if len(queued.frames) != 1 || queued.frames[0].Type != FrameTypePING {
			t.Fatalf("queued urgent request = %+v, want single PING frame", queued)
		}
	case <-time.After(testSignalTimeout):
		t.Fatal("urgent request did not resume after urgent wake broadcast")
	}

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("dispatchPreparedQueueRequest err = %v, want nil", err)
		}
	case <-time.After(testSignalTimeout):
		t.Fatal("dispatchPreparedQueueRequest did not finish after urgent wake broadcast")
	}
}

func TestDispatchPreparedQueueRequestsClearsTailAfterWaitError(t *testing.T) {
	t.Parallel()

	c := &Conn{
		lifecycle: connLifecycleState{closedCh: make(chan struct{})},
		writer:    connWriterRuntimeState{writeCh: make(chan writeRequest, 2)},
	}
	reqs := []writeRequest{
		{frames: testTxFramesFrom([]Frame{{Type: FrameTypePING, Payload: []byte("first")}})},
		{frames: testTxFramesFrom([]Frame{{Type: FrameTypePING, Payload: []byte("second")}})},
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- c.dispatchPreparedQueueRequests(reqs, preparedQueueDispatchOptions{})
	}()

	var queued [2]writeRequest
	for i := range queued {
		select {
		case queued[i] = <-c.writer.writeCh:
		case <-time.After(testSignalTimeout):
			t.Fatalf("timed out waiting for queued request %d", i)
		}
	}

	queued[0].done <- errors.New("boom")
	queued[1].done <- nil

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("dispatchPreparedQueueRequests err = nil, want propagated wait error")
		}
	case <-time.After(testSignalTimeout):
		t.Fatal("dispatchPreparedQueueRequests did not return after first wait error")
	}

	if reqs[0].frames != nil || reqs[0].done != nil {
		t.Fatalf("first request retained refs after wait error: frames=%v done=%v", reqs[0].frames, reqs[0].done)
	}
	if reqs[1].frames != nil || reqs[1].done != nil || reqs[1].cloneFramesBeforeSend {
		t.Fatalf("tail request retained refs after early wait error: frames=%v done=%v clone=%v", reqs[1].frames, reqs[1].done, reqs[1].cloneFramesBeforeSend)
	}
}

func TestQueueAdvisoryFramesSplitsBatchesAtFrameCountLimit(t *testing.T) {
	caps := CapabilityPriorityUpdate | CapabilityPriorityHints
	payload, err := buildPriorityUpdatePayload(
		caps,
		MetadataUpdate{Priority: invalidUint64Ptr(7)},
		4096,
	)
	if err != nil {
		t.Fatalf("build priority update payload: %v", err)
	}

	c := &Conn{
		lifecycle: connLifecycleState{closedCh: make(chan struct{})},
		writer:    connWriterRuntimeState{writeCh: make(chan writeRequest, 1), advisoryWriteCh: make(chan writeRequest, 2), urgentWriteCh: make(chan writeRequest, 1)}, config: connConfigState{peer: Preface{
			Settings: Settings{
				MaxExtensionPayloadBytes: 4096,
			},
		}},
	}

	frames := make([]Frame, maxWriteBatchFrames+1)
	for i := range frames {
		frames[i] = Frame{
			Type:     FrameTypeEXT,
			StreamID: uint64(i + 1),
			Payload:  payload,
		}
	}

	reqCh := make(chan writeRequest, 2)
	go func() {
		for i := 0; i < 2; i++ {
			req := <-c.writer.advisoryWriteCh
			reqCh <- req
			req.done <- nil
		}
	}()

	if err := testQueueAdvisoryFrames(c, frames); err != nil {
		t.Fatalf("queueAdvisoryFrames err = %v, want nil", err)
	}

	first := <-reqCh
	second := <-reqCh
	if got := len(first.frames); got != maxWriteBatchFrames {
		t.Fatalf("first advisory chunk frames = %d, want %d", got, maxWriteBatchFrames)
	}
	if got := len(second.frames); got != 1 {
		t.Fatalf("second advisory chunk frames = %d, want 1", got)
	}
}

func TestQueueAdvisoryFramesDispatchesSecondChunkBeforeFirstCompletion(t *testing.T) {
	caps := CapabilityPriorityUpdate | CapabilityPriorityHints
	payload, err := buildPriorityUpdatePayload(
		caps,
		MetadataUpdate{Priority: invalidUint64Ptr(7)},
		4096,
	)
	if err != nil {
		t.Fatalf("build priority update payload: %v", err)
	}

	c := &Conn{
		lifecycle: connLifecycleState{closedCh: make(chan struct{})},
		writer:    connWriterRuntimeState{writeCh: make(chan writeRequest, 1), advisoryWriteCh: make(chan writeRequest, 2), urgentWriteCh: make(chan writeRequest, 1)}, config: connConfigState{peer: Preface{
			Settings: Settings{
				MaxExtensionPayloadBytes: 4096,
			},
		}},
	}

	frames := make([]Frame, maxWriteBatchFrames+1)
	for i := range frames {
		frames[i] = Frame{
			Type:     FrameTypeEXT,
			StreamID: uint64(i + 1),
			Payload:  payload,
		}
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- testQueueAdvisoryFrames(c, frames)
	}()

	var first, second writeRequest
	select {
	case first = <-c.writer.advisoryWriteCh:
	case <-time.After(testSignalTimeout):
		t.Fatal("first advisory chunk was not dispatched")
	}
	select {
	case second = <-c.writer.advisoryWriteCh:
	case <-time.After(testSignalTimeout):
		t.Fatal("second advisory chunk was not dispatched before first completion")
	}

	if got := len(first.frames); got != maxWriteBatchFrames {
		t.Fatalf("first advisory chunk frames = %d, want %d", got, maxWriteBatchFrames)
	}
	if got := len(second.frames); got != 1 {
		t.Fatalf("second advisory chunk frames = %d, want 1", got)
	}

	select {
	case err := <-errCh:
		t.Fatalf("queueAdvisoryFrames returned early with %v", err)
	default:
	}

	first.done <- nil

	select {
	case err := <-errCh:
		t.Fatalf("queueAdvisoryFrames returned before second completion with %v", err)
	default:
	}

	second.done <- nil

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("queueAdvisoryFrames err = %v, want nil", err)
		}
	case <-time.After(testSignalTimeout):
		t.Fatal("queueAdvisoryFrames did not finish after both chunks completed")
	}
}

func TestQueueFrameRejectsOversizedDataPayloadForPeerLimit(t *testing.T) {
	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()

	c.config.peer.Settings.MaxFramePayload = 64
	c.config.negotiated.PeerSettings = c.config.peer.Settings

	err := testQueueFrame(c, Frame{
		Type:     FrameTypeDATA,
		StreamID: state.FirstLocalStreamID(c.config.negotiated.LocalRole, true),
		Payload:  make([]byte, 65),
	})
	if !IsErrorCode(err, CodeFrameSize) {
		t.Fatalf("queue oversized DATA err = %v, want %s", err, CodeFrameSize)
	}
	assertNoQueuedFrame(t, frames)
}

func TestQueueFrameRejectsOversizedControlPayloadForPeerLimit(t *testing.T) {
	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()

	c.config.peer.Settings.MaxControlPayloadBytes = 32
	c.config.negotiated.PeerSettings = c.config.peer.Settings

	err := testQueueFrame(c, Frame{
		Type:     FrameTypePONG,
		StreamID: 0,
		Payload:  make([]byte, 33),
	})
	if !IsErrorCode(err, CodeFrameSize) {
		t.Fatalf("queue oversized PONG err = %v, want %s", err, CodeFrameSize)
	}
	assertNoQueuedFrame(t, frames)
}

func TestQueueFrameRejectsOversizedEXTPayloadForPeerLimit(t *testing.T) {
	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()

	c.config.peer.Settings.MaxExtensionPayloadBytes = 16
	c.config.negotiated.PeerSettings = c.config.peer.Settings

	err := testQueueFrame(c, Frame{
		Type:     FrameTypeEXT,
		StreamID: 0,
		Payload:  append([]byte{0x00}, make([]byte, 16)...),
	})
	if !IsErrorCode(err, CodeFrameSize) {
		t.Fatalf("queue oversized EXT err = %v, want %s", err, CodeFrameSize)
	}
	assertNoQueuedFrame(t, frames)
}

func TestQueueFramePingUsesSmallerOfLocalAndPeerControlLimits(t *testing.T) {
	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()

	c.config.local.Settings.MaxControlPayloadBytes = 128
	c.config.peer.Settings.MaxControlPayloadBytes = 64
	c.config.negotiated.PeerSettings = c.config.peer.Settings

	err := testQueueFrame(c, Frame{
		Type:     FrameTypePING,
		StreamID: 0,
		Payload:  make([]byte, 65),
	})
	if !IsErrorCode(err, CodeFrameSize) {
		t.Fatalf("queue oversized PING err = %v, want %s", err, CodeFrameSize)
	}
	assertNoQueuedFrame(t, frames)

	if err := testQueueFrame(c, Frame{
		Type:     FrameTypePING,
		StreamID: 0,
		Payload:  make([]byte, 64),
	}); err != nil {
		t.Fatalf("queue exact-limit PING: %v", err)
	}
	frame := awaitQueuedFrame(t, frames)
	if frame.Type != FrameTypePING || len(frame.Payload) != 64 {
		t.Fatalf("queued frame = %+v, want PING with 64-byte payload", frame)
	}
}

func TestWriteBurstQueuesMultipleDataFramesInSingleRequest(t *testing.T) {
	t.Parallel()

	stream := newQueueBackpressureTestStream()
	stream.conn.config.peer.Settings.MaxFramePayload = 16
	payload := bytes.Repeat([]byte("x"), 33)

	type writeResult struct {
		n   int
		err error
	}
	resultCh := make(chan writeResult, 1)
	go func() {
		n, err := stream.Write(payload)
		resultCh <- writeResult{n: n, err: err}
	}()

	var req writeRequest
	select {
	case req = <-stream.conn.writer.writeCh:
	case <-time.After(testSignalTimeout):
		t.Fatal("timed out waiting for burst write request")
	}

	if got, want := len(req.frames), 3; got != want {
		t.Fatalf("queued frame count = %d, want %d", got, want)
	}
	if got, want := req.queuedBytes, txFramesBufferedBytes(req.frames); got != want {
		t.Fatalf("queued bytes = %d, want %d", got, want)
	}

	gotPayload := make([]byte, 0, len(payload))
	for i, frame := range req.frames {
		if frame.Type != FrameTypeDATA {
			t.Fatalf("frame %d type = %v, want %v", i, frame.Type, FrameTypeDATA)
		}
		if frame.StreamID != stream.id {
			t.Fatalf("frame %d stream = %d, want %d", i, frame.StreamID, stream.id)
		}
		if frame.Flags&FrameFlagFIN != 0 {
			t.Fatalf("frame %d unexpectedly carried FIN", i)
		}
		gotPayload = frame.appendPayload(gotPayload)
	}
	if !bytes.Equal(gotPayload, payload) {
		t.Fatalf("queued payload = %q, want %q", gotPayload, payload)
	}

	req.done <- nil

	select {
	case result := <-resultCh:
		if result.err != nil {
			t.Fatalf("Write err = %v, want nil", result.err)
		}
		if result.n != len(payload) {
			t.Fatalf("Write n = %d, want %d", result.n, len(payload))
		}
	case <-time.After(testSignalTimeout):
		t.Fatal("timed out waiting for Write result")
	}

	select {
	case extra := <-stream.conn.writer.writeCh:
		t.Fatalf("unexpected extra write request: %+v", extra)
	default:
	}
}

func TestWriteSingleFrameCarriesPreparedRollbackBytes(t *testing.T) {
	t.Parallel()

	stream := newQueueBackpressureTestStream()
	payload := []byte("hello")

	type writeResult struct {
		n   int
		err error
	}
	resultCh := make(chan writeResult, 1)
	go func() {
		n, err := stream.Write(payload)
		resultCh <- writeResult{n: n, err: err}
	}()

	var req writeRequest
	select {
	case req = <-stream.conn.writer.writeCh:
	case <-time.After(testSignalTimeout):
		t.Fatal("timed out waiting for single-frame write request")
	}

	if got, want := len(req.frames), 1; got != want {
		t.Fatalf("queued frame count = %d, want %d", got, want)
	}
	if got, want := req.queuedBytes, txFramesBufferedBytes(req.frames); got != want {
		t.Fatalf("queued bytes = %d, want %d", got, want)
	}
	if got, want := req.preparedSendBytes, uint64(len(payload)); got != want {
		t.Fatalf("preparedSendBytes = %d, want %d", got, want)
	}
	if req.preparedSendFin {
		t.Fatal("preparedSendFin = true, want false")
	}

	req.done <- nil

	select {
	case result := <-resultCh:
		if result.err != nil {
			t.Fatalf("Write err = %v, want nil", result.err)
		}
		if result.n != len(payload) {
			t.Fatalf("Write n = %d, want %d", result.n, len(payload))
		}
	case <-time.After(testSignalTimeout):
		t.Fatal("timed out waiting for Write result")
	}
}

func TestReserveWriteChunkLockedSaturatesCommittedCreditCounters(t *testing.T) {
	t.Parallel()

	stream := newQueueBackpressureTestStream()

	stream.conn.mu.Lock()
	stream.sendSent = ^uint64(0) - 1
	stream.conn.flow.sendSessionUsed = ^uint64(0) - 1
	stream.reserveWriteChunkLocked(8)
	gotSend := stream.sendSent
	gotSession := stream.conn.flow.sendSessionUsed
	stream.conn.mu.Unlock()

	if gotSend != ^uint64(0) {
		t.Fatalf("stream.sendSent = %d, want saturation to %d", gotSend, ^uint64(0))
	}
	if gotSession != ^uint64(0) {
		t.Fatalf("conn.flow.sendSessionUsed = %d, want saturation to %d", gotSession, ^uint64(0))
	}
}

func TestResolveWritePreparedBurstPropagatesQueueErrWithoutProgress(t *testing.T) {
	t.Parallel()

	stream := newQueueBackpressureTestStream()
	if err := stream.SetWriteDeadline(time.Now().Add(-time.Second)); err != nil {
		t.Fatalf("SetWriteDeadline: %v", err)
	}

	frame := Frame{Type: FrameTypeEXT, StreamID: stream.id, Payload: mustEncodeVarint(uint64(EXTPriorityUpdate))}
	burst := writeBurstState{
		frames:      testTxFramesFrom([]Frame{frame}),
		queuedBytes: rt.FrameBufferedBytes(frame),
	}

	result := stream.flushWriteBurst(burst, writeChunkStreaming, writeBurstFlushPrepared, nil)
	if result.progress != 0 {
		t.Fatalf("progress = %d, want 0", result.progress)
	}
	if result.stop {
		t.Fatal("stop = true, want false")
	}
	if !errors.Is(result.err, os.ErrDeadlineExceeded) {
		t.Fatalf("err = %v, want deadline exceeded", result.err)
	}
}

func TestWriteFinalBurstQueuesMultipleDataFramesWithTrailingFin(t *testing.T) {
	t.Parallel()

	stream := newQueueBackpressureTestStream()
	stream.conn.config.peer.Settings.MaxFramePayload = 16
	payload := bytes.Repeat([]byte("x"), 33)

	type writeResult struct {
		n   int
		err error
	}
	resultCh := make(chan writeResult, 1)
	go func() {
		n, err := stream.WriteFinal(payload)
		resultCh <- writeResult{n: n, err: err}
	}()

	var req writeRequest
	select {
	case req = <-stream.conn.writer.writeCh:
	case <-time.After(testSignalTimeout):
		t.Fatal("timed out waiting for final burst write request")
	}

	if !req.terminalPolicy.allowsTerminal() {
		t.Fatal("queued final request did not allow terminal frames")
	}
	if got, want := len(req.frames), 3; got != want {
		t.Fatalf("queued frame count = %d, want %d", got, want)
	}
	if got, want := req.queuedBytes, txFramesBufferedBytes(req.frames); got != want {
		t.Fatalf("queued bytes = %d, want %d", got, want)
	}

	gotPayload := make([]byte, 0, len(payload))
	for i, frame := range req.frames {
		if frame.Type != FrameTypeDATA {
			t.Fatalf("frame %d type = %v, want %v", i, frame.Type, FrameTypeDATA)
		}
		if frame.StreamID != stream.id {
			t.Fatalf("frame %d stream = %d, want %d", i, frame.StreamID, stream.id)
		}
		if i < len(req.frames)-1 && frame.Flags&FrameFlagFIN != 0 {
			t.Fatalf("frame %d unexpectedly carried FIN", i)
		}
		if i == len(req.frames)-1 && frame.Flags&FrameFlagFIN == 0 {
			t.Fatalf("last frame flags = %v, want FIN", frame.Flags)
		}
		gotPayload = frame.appendPayload(gotPayload)
	}
	if !bytes.Equal(gotPayload, payload) {
		t.Fatalf("queued payload = %q, want %q", gotPayload, payload)
	}

	req.done <- nil

	select {
	case result := <-resultCh:
		if result.err != nil {
			t.Fatalf("WriteFinal err = %v, want nil", result.err)
		}
		if result.n != len(payload) {
			t.Fatalf("WriteFinal n = %d, want %d", result.n, len(payload))
		}
	case <-time.After(testSignalTimeout):
		t.Fatal("timed out waiting for WriteFinal result")
	}

	if got := stream.sendHalfState(); got != state.SendHalfFin {
		t.Fatalf("sendHalf = %v, want %v", got, state.SendHalfFin)
	}

	select {
	case extra := <-stream.conn.writer.writeCh:
		t.Fatalf("unexpected extra write request: %+v", extra)
	default:
	}
}

func TestWriteFinalSingleFrameCarriesPreparedRollbackFin(t *testing.T) {
	t.Parallel()

	stream := newQueueBackpressureTestStream()
	payload := []byte("hello")

	type writeResult struct {
		n   int
		err error
	}
	resultCh := make(chan writeResult, 1)
	go func() {
		n, err := stream.WriteFinal(payload)
		resultCh <- writeResult{n: n, err: err}
	}()

	var req writeRequest
	select {
	case req = <-stream.conn.writer.writeCh:
	case <-time.After(testSignalTimeout):
		t.Fatal("timed out waiting for single-frame final write request")
	}

	if got, want := len(req.frames), 1; got != want {
		t.Fatalf("queued frame count = %d, want %d", got, want)
	}
	if got, want := req.queuedBytes, txFramesBufferedBytes(req.frames); got != want {
		t.Fatalf("queued bytes = %d, want %d", got, want)
	}
	if got, want := req.preparedSendBytes, uint64(len(payload)); got != want {
		t.Fatalf("preparedSendBytes = %d, want %d", got, want)
	}
	if !req.preparedSendFin {
		t.Fatal("preparedSendFin = false, want true")
	}

	req.done <- nil

	select {
	case result := <-resultCh:
		if result.err != nil {
			t.Fatalf("WriteFinal err = %v, want nil", result.err)
		}
		if result.n != len(payload) {
			t.Fatalf("WriteFinal n = %d, want %d", result.n, len(payload))
		}
	case <-time.After(testSignalTimeout):
		t.Fatal("timed out waiting for WriteFinal result")
	}
}

func TestWritevFinalBurstCoalescesAcrossPartsIntoSingleRequest(t *testing.T) {
	t.Parallel()

	stream := newQueueBackpressureTestStream()
	stream.conn.config.peer.Settings.MaxFramePayload = 16
	parts := [][]byte{
		[]byte("aaaaa"),
		[]byte("bbbbb"),
		[]byte("ccccc"),
		[]byte("ddddd"),
		[]byte("eeeee"),
		[]byte("fffff"),
		[]byte("ggg"),
	}
	payload := bytes.Join(parts, nil)

	type writeResult struct {
		n   int
		err error
	}
	resultCh := make(chan writeResult, 1)
	go func() {
		n, err := stream.WritevFinal(parts...)
		resultCh <- writeResult{n: n, err: err}
	}()

	var req writeRequest
	select {
	case req = <-stream.conn.writer.writeCh:
	case <-time.After(testSignalTimeout):
		t.Fatal("timed out waiting for writev final burst request")
	}

	if !req.terminalPolicy.allowsTerminal() {
		t.Fatal("queued writev final request did not allow terminal frames")
	}
	if got, want := len(req.frames), 3; got != want {
		t.Fatalf("queued frame count = %d, want %d", got, want)
	}

	gotPayload := make([]byte, 0, len(payload))
	for i, frame := range req.frames {
		if frame.Type != FrameTypeDATA {
			t.Fatalf("frame %d type = %v, want %v", i, frame.Type, FrameTypeDATA)
		}
		if i < len(req.frames)-1 && frame.Flags&FrameFlagFIN != 0 {
			t.Fatalf("frame %d unexpectedly carried FIN", i)
		}
		if i == len(req.frames)-1 && frame.Flags&FrameFlagFIN == 0 {
			t.Fatalf("last frame flags = %v, want FIN", frame.Flags)
		}
		gotPayload = frame.appendPayload(gotPayload)
	}
	if !bytes.Equal(gotPayload, payload) {
		t.Fatalf("queued payload = %q, want %q", gotPayload, payload)
	}

	req.done <- nil

	select {
	case result := <-resultCh:
		if result.err != nil {
			t.Fatalf("WritevFinal err = %v, want nil", result.err)
		}
		if result.n != len(payload) {
			t.Fatalf("WritevFinal n = %d, want %d", result.n, len(payload))
		}
	case <-time.After(testSignalTimeout):
		t.Fatal("timed out waiting for WritevFinal result")
	}

	if got := stream.sendHalfState(); got != state.SendHalfFin {
		t.Fatalf("sendHalf = %v, want %v", got, state.SendHalfFin)
	}
}

func TestWritevFinalOnConcreteLocalIDCoalescesPartsIntoOpeningFinFrame(t *testing.T) {
	t.Parallel()

	c, frames, stop := newInvalidFrameConn(t, CapabilityOpenMetadata)
	defer stop()

	prefix, err := buildOpenMetadataPrefix(
		CapabilityOpenMetadata,
		OpenOptions{OpenInfo: []byte("ssh")},
		c.config.peer.Settings.MaxFramePayload,
	)
	if err != nil {
		t.Fatalf("buildOpenMetadataPrefix err = %v", err)
	}

	c.mu.Lock()
	stream := c.newLocalStreamWithIDLocked(
		state.FirstLocalStreamID(c.config.negotiated.LocalRole, true), streamArityBidi, OpenOptions{OpenInfo: []byte("ssh")},
		prefix,
	)
	c.registry.streams[stream.id] = stream
	c.appendUnseenLocalLocked(stream)
	c.mu.Unlock()

	n, err := stream.WritevFinal([]byte("a"), []byte("b"), []byte("c"))
	if err != nil {
		t.Fatalf("WritevFinal err = %v, want nil", err)
	}
	if n != 3 {
		t.Fatalf("WritevFinal n = %d, want 3", n)
	}

	frame := awaitQueuedFrame(t, frames)
	if frame.Type != FrameTypeDATA {
		t.Fatalf("queued frame type = %v, want %v", frame.Type, FrameTypeDATA)
	}
	if frame.StreamID != stream.id {
		t.Fatalf("queued frame stream = %d, want %d", frame.StreamID, stream.id)
	}
	if frame.Flags&FrameFlagOpenMetadata == 0 {
		t.Fatal("queued frame missing OPEN_METADATA flag")
	}
	if frame.Flags&FrameFlagFIN == 0 {
		t.Fatal("queued frame missing FIN flag")
	}
	if got, want := frame.Payload, append(append([]byte(nil), prefix...), []byte("abc")...); !bytes.Equal(got, want) {
		t.Fatalf("queued payload = %x, want %x", got, want)
	}
	assertNoQueuedFrame(t, frames)

	c.mu.Lock()
	defer c.mu.Unlock()
	if !stream.localOpen.committed {
		t.Fatal("sendCommitted = false, want true after opening WritevFinal")
	}
	if !stream.isPeerVisibleLocked() {
		t.Fatal("peerVisible = false, want true after opening WritevFinal")
	}
	if !stream.sendFinReached() {
		t.Fatal("sendFinReached() = false, want true after opening WritevFinal")
	}
}

func TestResolveWriteFinalPreparedBurstPropagatesQueueErrWithoutProgress(t *testing.T) {
	t.Parallel()

	stream := newQueueBackpressureTestStream()
	if err := stream.SetWriteDeadline(time.Now().Add(-time.Second)); err != nil {
		t.Fatalf("SetWriteDeadline: %v", err)
	}

	frame := Frame{Type: FrameTypeEXT, StreamID: stream.id, Payload: mustEncodeVarint(uint64(EXTPriorityUpdate))}
	burst := writeBurstState{
		frames:      testTxFramesFrom([]Frame{frame}),
		queuedBytes: rt.FrameBufferedBytes(frame),
	}

	result := stream.flushWriteBurst(burst, writeChunkFinal, writeBurstFlushPrepared, nil)
	if result.progress != 0 {
		t.Fatalf("progress = %d, want 0", result.progress)
	}
	if result.finalState.finalized() {
		t.Fatal("finalized = true, want false")
	}
	if result.stop {
		t.Fatal("stop = true, want false")
	}
	if !errors.Is(result.err, os.ErrDeadlineExceeded) {
		t.Fatalf("err = %v, want deadline exceeded", result.err)
	}
}

func TestPrepareOwnedWriteRequestSingleFrameDataFIN(t *testing.T) {
	t.Parallel()

	stream := newQueueBackpressureTestStream()
	frame := Frame{Type: FrameTypeDATA, StreamID: stream.id, Flags: FrameFlagFIN, Payload: []byte("hello")}
	req := &writeRequest{frames: testTxFramesFrom([]Frame{frame})}

	if err := stream.prepareOwnedWriteRequest(req); err != nil {
		t.Fatalf("prepareOwnedWriteRequest() err = %v", err)
	}
	if !req.requestMetaReady {
		t.Fatal("requestMetaReady = false, want true")
	}
	if !req.requestStreamScoped || !req.requestStreamIDKnown || req.requestStreamID != stream.id {
		t.Fatalf("stream meta = (%v,%v,%d), want (true,true,%d)", req.requestStreamScoped, req.requestStreamIDKnown, req.requestStreamID, stream.id)
	}
	if req.requestBufferedBytes != rt.FrameBufferedBytes(frame) {
		t.Fatalf("requestBufferedBytes = %d, want %d", req.requestBufferedBytes, rt.FrameBufferedBytes(frame))
	}
	if req.requestCost != int64(rt.FrameBufferedBytes(frame)) {
		t.Fatalf("requestCost = %d, want %d", req.requestCost, int64(rt.FrameBufferedBytes(frame)))
	}
	if !req.terminalDataPriority || !req.terminalHasFIN {
		t.Fatalf("terminal flags = (%v,%v), want (true,true)", req.terminalDataPriority, req.terminalHasFIN)
	}
	if got, want := req.preparedSendBytes, uint64(len(frame.Payload)); got != want {
		t.Fatalf("preparedSendBytes = %d, want %d", got, want)
	}
	if !req.preparedSendFin {
		t.Fatal("preparedSendFin = false, want true")
	}
}

func TestPrepareOwnedWriteRequestMultiFrameDataTracksPreparedFin(t *testing.T) {
	t.Parallel()

	stream := newQueueBackpressureTestStream()
	frames := []Frame{
		{Type: FrameTypeDATA, StreamID: stream.id, Payload: []byte("ab")},
		{Type: FrameTypeDATA, StreamID: stream.id, Flags: FrameFlagFIN, Payload: []byte("cde")},
	}
	req := &writeRequest{frames: testTxFramesFrom(frames)}

	if err := stream.prepareOwnedWriteRequest(req); err != nil {
		t.Fatalf("prepareOwnedWriteRequest() err = %v", err)
	}
	if !req.requestMetaReady {
		t.Fatal("requestMetaReady = false, want true")
	}
	if !req.terminalDataPriority || !req.terminalHasFIN {
		t.Fatalf("terminal flags = (%v,%v), want (true,true)", req.terminalDataPriority, req.terminalHasFIN)
	}
	if got, want := req.preparedSendBytes, uint64(5); got != want {
		t.Fatalf("preparedSendBytes = %d, want %d", got, want)
	}
	if !req.preparedSendFin {
		t.Fatal("preparedSendFin = false, want true")
	}
	if got, want := req.requestBufferedBytes, rt.FramesBufferedBytes(frames); got != want {
		t.Fatalf("requestBufferedBytes = %d, want %d", got, want)
	}
}

func TestPrepareOwnedWriteRequestOpenMetadataRollbackBytesExcludePrefix(t *testing.T) {
	t.Parallel()

	stream := newQueueBackpressureTestStream()
	prefix, err := buildOpenMetadataPrefix(
		CapabilityOpenMetadata,
		OpenOptions{OpenInfo: []byte("ssh")},
		stream.conn.config.peer.Settings.MaxFramePayload,
	)
	if err != nil {
		t.Fatalf("buildOpenMetadataPrefix err = %v", err)
	}
	stream.openMetadataPrefix = prefix

	payload := append(append([]byte(nil), prefix...), []byte("hello")...)
	req := &writeRequest{frames: testTxFramesFrom([]Frame{{
		Type:     FrameTypeDATA,
		StreamID: stream.id,
		Flags:    FrameFlagOpenMetadata,
		Payload:  payload,
	}})}

	if err := stream.prepareOwnedWriteRequest(req); err != nil {
		t.Fatalf("prepareOwnedWriteRequest() err = %v", err)
	}
	if got, want := req.preparedSendBytes, uint64(5); got != want {
		t.Fatalf("preparedSendBytes = %d, want %d", got, want)
	}
	if req.preparedSendFin {
		t.Fatal("preparedSendFin = true, want false")
	}
}

func TestPrepareOwnedWriteRequestMarksPiggybackPriorityRollback(t *testing.T) {
	t.Parallel()

	stream := newQueueBackpressureTestStream()
	priorityPayload := mustEncodeVarint(uint64(EXTPriorityUpdate))
	req := &writeRequest{frames: testTxFramesFrom([]Frame{
		{Type: FrameTypeEXT, StreamID: stream.id, Payload: priorityPayload},
		{Type: FrameTypeDATA, StreamID: stream.id, Payload: []byte("x")},
	})}

	if err := stream.prepareOwnedWriteRequest(req); err != nil {
		t.Fatalf("prepareOwnedWriteRequest() err = %v", err)
	}
	if got, want := req.preparedPriorityStreamID, stream.id; got != want {
		t.Fatalf("preparedPriorityStreamID = %d, want %d", got, want)
	}
	if !bytes.Equal(req.preparedPriorityPayload, priorityPayload) {
		t.Fatalf("preparedPriorityPayload = %x, want %x", req.preparedPriorityPayload, priorityPayload)
	}
	if got, want := req.preparedPriorityBytes, txFrameBufferedBytes(req.frames[0]); got != want {
		t.Fatalf("preparedPriorityBytes = %d, want %d", got, want)
	}
	if got, want := req.preparedSendBytes, uint64(1); got != want {
		t.Fatalf("preparedSendBytes = %d, want %d", got, want)
	}
}

func TestPrepareOwnedWriteRequestClearsPiggybackPriorityRollbackOnNonDataTail(t *testing.T) {
	t.Parallel()

	stream := newQueueBackpressureTestStream()
	req := &writeRequest{frames: testTxFramesFrom([]Frame{
		{Type: FrameTypeEXT, StreamID: stream.id, Payload: mustEncodeVarint(uint64(EXTPriorityUpdate))},
		{Type: FrameTypeDATA, StreamID: stream.id, Payload: []byte("x")},
		{Type: FrameTypeRESET, StreamID: stream.id, Payload: mustEncodeVarint(uint64(CodeCancelled))},
	})}

	if err := stream.prepareOwnedWriteRequest(req); err != nil {
		t.Fatalf("prepareOwnedWriteRequest() err = %v", err)
	}
	if req.preparedPriorityStreamID != 0 {
		t.Fatalf("preparedPriorityStreamID = %d, want 0", req.preparedPriorityStreamID)
	}
	if req.preparedPriorityPayload != nil {
		t.Fatalf("preparedPriorityPayload = %x, want nil", req.preparedPriorityPayload)
	}
	if req.preparedPriorityBytes != 0 {
		t.Fatalf("preparedPriorityBytes = %d, want 0", req.preparedPriorityBytes)
	}
	if got, want := req.preparedSendBytes, uint64(1); got != want {
		t.Fatalf("preparedSendBytes = %d, want %d", got, want)
	}
}

func TestPrepareOwnedWriteRequestReclassifiesReusedRequest(t *testing.T) {
	t.Parallel()

	stream := newQueueBackpressureTestStream()
	req := &writeRequest{
		frames: testTxFramesFrom([]Frame{{
			Type:     FrameTypeDATA,
			StreamID: stream.id,
			Payload:  []byte("xyz"),
		}}),
		requestMetaReady:         true,
		requestStreamID:          77,
		requestStreamIDKnown:     true,
		requestStreamScoped:      true,
		requestUrgencyRank:       -1,
		requestCost:              999,
		requestBufferedBytes:     999,
		requestIsPriorityUpdate:  true,
		requestAllUrgent:         true,
		terminalDataPriority:     false,
		terminalResetOnly:        true,
		terminalAbortOnly:        true,
		terminalHasFIN:           true,
		preparedSendBytes:        88,
		preparedSendFin:          true,
		preparedPriorityStreamID: stream.id,
		preparedPriorityPayload:  []byte("prio"),
		preparedPriorityBytes:    55,
		preparedPriorityQueued:   true,
	}

	if err := stream.prepareOwnedWriteRequest(req); err != nil {
		t.Fatalf("prepareOwnedWriteRequest() err = %v", err)
	}

	frame := req.frames[0]
	if !req.requestMetaReady {
		t.Fatal("requestMetaReady = false, want true")
	}
	if !req.requestStreamScoped || !req.requestStreamIDKnown || req.requestStreamID != stream.id {
		t.Fatalf("stream meta = (%v,%v,%d), want (true,true,%d)", req.requestStreamScoped, req.requestStreamIDKnown, req.requestStreamID, stream.id)
	}
	if req.requestIsPriorityUpdate {
		t.Fatal("requestIsPriorityUpdate = true, want false")
	}
	if req.requestAllUrgent {
		t.Fatal("requestAllUrgent = true, want false")
	}
	if !req.terminalDataPriority || req.terminalHasFIN {
		t.Fatalf("terminal flags = (%v,%v), want (true,false)", req.terminalDataPriority, req.terminalHasFIN)
	}
	if req.terminalResetOnly || req.terminalAbortOnly {
		t.Fatalf("terminal reset/abort flags = (%v,%v), want (false,false)", req.terminalResetOnly, req.terminalAbortOnly)
	}
	if got, want := req.requestBufferedBytes, txFrameBufferedBytes(frame); got != want {
		t.Fatalf("requestBufferedBytes = %d, want %d", got, want)
	}
	if got, want := req.requestCost, int64(txFrameBufferedBytes(frame)); got != want {
		t.Fatalf("requestCost = %d, want %d", got, want)
	}
	if got, want := req.preparedSendBytes, uint64(len(frame.Payload)); got != want {
		t.Fatalf("preparedSendBytes = %d, want %d", got, want)
	}
	if req.preparedSendFin {
		t.Fatal("preparedSendFin = true, want false")
	}
	if req.preparedPriorityStreamID != 0 || req.preparedPriorityPayload != nil || req.preparedPriorityBytes != 0 || req.preparedPriorityQueued {
		t.Fatalf("prepared priority rollback retained = (%d,%x,%d,%v), want cleared", req.preparedPriorityStreamID, req.preparedPriorityPayload, req.preparedPriorityBytes, req.preparedPriorityQueued)
	}
}

func configurePendingPriorityUpdateForTest(t *testing.T, c *Conn, s *nativeStream, priority uint64) []byte {
	t.Helper()

	caps := CapabilityPriorityUpdate | CapabilityPriorityHints
	payload, err := buildPriorityUpdatePayload(caps, MetadataUpdate{Priority: &priority}, 4096)
	if err != nil {
		t.Fatalf("build priority update payload: %v", err)
	}

	c.mu.Lock()
	c.config.negotiated.Capabilities = caps
	c.config.local.Settings.MaxExtensionPayloadBytes = 4096
	c.config.peer.Settings.MaxExtensionPayloadBytes = 4096
	testSetPendingPriorityUpdate(c, s.id, append([]byte(nil), payload...))
	c.mu.Unlock()

	return payload
}

func newBlockedWriterConnWithStreams(t *testing.T) (*Conn, *nativeStream, *nativeStream, *stallingWriteCloser) {
	t.Helper()

	writer := newStallingWriteCloser()
	c := &Conn{
		io: connIOState{conn: writer},

		pending:   connPendingControlState{controlNotify: make(chan struct{}, 1)},
		lifecycle: connLifecycleState{closedCh: make(chan struct{})},

		writer: connWriterRuntimeState{writeCh: make(chan writeRequest), urgentWriteCh: make(chan writeRequest)}, config: connConfigState{peer: Preface{Settings: Settings{MaxFramePayload: 16}}}, flow: connFlowState{sendSessionMax: 1024}, registry: connRegistryState{streams: make(map[uint64]*nativeStream)},
	}

	newStream := func(id uint64) *nativeStream {
		return testVisibleBidiStream(c, id, testWithSendMax(1024))
	}

	first := newStream(4)
	second := newStream(8)
	go c.writeLoop()

	t.Cleanup(func() {
		_ = writer.Close()
		select {
		case <-c.lifecycle.closedCh:
		default:
			close(c.lifecycle.closedCh)
		}
	})

	return c, first, second, writer
}

func startBlockedFirstWrite(t *testing.T, first *nativeStream, writer *stallingWriteCloser) <-chan error {
	t.Helper()

	firstErrCh := make(chan error, 1)
	go func() {
		_, err := first.Write([]byte("hello"))
		firstErrCh <- err
	}()
	awaitWriterBlocked(t, writer)
	return firstErrCh
}

func releaseBlockedFirstWrite(t *testing.T, writer *stallingWriteCloser, firstErrCh <-chan error) error {
	t.Helper()

	_ = writer.Close()
	select {
	case err := <-firstErrCh:
		return err
	case <-time.After(testSignalTimeout):
		t.Fatal("first write did not unblock during cleanup")
		return nil
	}
}

func assertRetainedWriteRequestCleared(t *testing.T, req *writeRequest) {
	t.Helper()

	if req.frames != nil || req.done != nil || req.reservedStream != nil {
		t.Fatalf("retained refs not cleared: frames=%v done=%v reservedStream=%v", req.frames, req.done, req.reservedStream)
	}
	if req.queueReserved || req.queuedBytes != 0 || req.urgentReserved || req.advisoryReserved || req.cloneFramesBeforeSend {
		t.Fatalf("queue state retained = (%v,%d,%v,%v,%v), want cleared",
			req.queueReserved,
			req.queuedBytes,
			req.urgentReserved,
			req.advisoryReserved,
			req.cloneFramesBeforeSend,
		)
	}
	if req.requestMetaReady || req.requestStreamID != 0 || req.requestStreamIDKnown || req.requestStreamScoped ||
		req.requestUrgencyRank != 0 || req.requestCost != 0 || req.requestBufferedBytes != 0 || req.requestIsPriorityUpdate ||
		req.requestAllUrgent || req.terminalDataPriority || req.terminalResetOnly || req.terminalAbortOnly || req.terminalHasFIN {
		t.Fatalf("cached request classification retained = %+v", *req)
	}
	if req.preparedSendBytes != 0 || req.preparedSendFin || req.preparedOpenerVisibility != openerVisibilityUnchanged ||
		req.preparedPriorityStreamID != 0 || req.preparedPriorityPayload != nil || req.preparedPriorityBytes != 0 || req.preparedPriorityQueued {
		t.Fatalf("prepared request state retained = (%d,%v,%v,%d,%x,%d,%v), want cleared",
			req.preparedSendBytes,
			req.preparedSendFin,
			req.preparedOpenerVisibility,
			req.preparedPriorityStreamID,
			req.preparedPriorityPayload,
			req.preparedPriorityBytes,
			req.preparedPriorityQueued,
		)
	}
}

func TestWriteDeadlineExpiresWhileBlockedSendingToWriterQueue(t *testing.T) {
	c, first, second, writer := newBlockedWriterConnWithStreams(t)
	_ = c

	firstErrCh := startBlockedFirstWrite(t, first, writer)

	if err := second.SetWriteDeadline(time.Now().Add(30 * time.Millisecond)); err != nil {
		t.Fatalf("SetWriteDeadline: %v", err)
	}

	_, err := second.Write([]byte("world"))
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("second Write err = %v, want deadline exceeded", err)
	}

	if err := releaseBlockedFirstWrite(t, writer, firstErrCh); err != nil {
		t.Fatalf("first Write err = %v, want nil after writer release", err)
	}
}

func TestEnqueueWriteRequestDeadlineRollbackClearsRetainedRefs(t *testing.T) {
	c, first, second, writer := newBlockedWriterConnWithStreams(t)

	firstErrCh := startBlockedFirstWrite(t, first, writer)
	c.mu.Lock()
	baselineSessionUsed := c.flow.sendSessionUsed
	baselineSessionQueued := c.flow.queuedDataBytes
	baselineStreamQueued := second.queuedDataBytes
	c.mu.Unlock()

	req := writeRequest{
		frames: testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: second.id, Payload: []byte("world")}}),
		origin: writeRequestOriginStream,
	}
	if err := second.prepareOwnedWriteRequest(&req); err != nil {
		t.Fatalf("prepareOwnedWriteRequest: %v", err)
	}
	c.mu.Lock()
	second.sendSent = saturatingAdd(second.sendSent, req.preparedSendBytes)
	c.flow.sendSessionUsed = saturatingAdd(c.flow.sendSessionUsed, req.preparedSendBytes)
	c.mu.Unlock()

	err := second.enqueueWriteRequestUntilDeadline(&req, streamWriteDispatchOptions{
		lane:             writeLaneOrdinary,
		deadlineOverride: time.Now().Add(30 * time.Millisecond),
		deadlinePolicy:   writeDeadlineOverrideOnly,
	})
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("enqueueWriteRequestUntilDeadline err = %v, want deadline exceeded", err)
	}
	assertRetainedWriteRequestCleared(t, &req)

	c.mu.Lock()
	secondSendSent := second.sendSent
	sessionUsed := c.flow.sendSessionUsed
	sessionQueued := c.flow.queuedDataBytes
	streamQueued := second.queuedDataBytes
	c.mu.Unlock()

	if secondSendSent != 0 {
		t.Fatalf("second sendSent = %d, want 0 after queue-admission timeout rollback", secondSendSent)
	}
	if sessionUsed != baselineSessionUsed {
		t.Fatalf("sendSessionUsed = %d, want %d after rolling back only the timed-out request", sessionUsed, baselineSessionUsed)
	}
	if sessionQueued != baselineSessionQueued {
		t.Fatalf("queuedDataBytes = %d, want %d after queue-admission timeout rollback", sessionQueued, baselineSessionQueued)
	}
	if streamQueued != baselineStreamQueued {
		t.Fatalf("stream queuedDataBytes = %d, want %d after queue-admission timeout rollback", streamQueued, baselineStreamQueued)
	}

	if err := releaseBlockedFirstWrite(t, writer, firstErrCh); err != nil {
		t.Fatalf("first Write err = %v, want nil after writer release", err)
	}
}

func TestSetWriteDeadlineWakesWriteBlockedOnWriterQueue(t *testing.T) {
	_, first, second, writer := newBlockedWriterConnWithStreams(t)

	firstErrCh := startBlockedFirstWrite(t, first, writer)

	errCh := make(chan error, 1)
	go func() {
		_, err := second.Write([]byte("world"))
		errCh <- err
	}()

	select {
	case err := <-errCh:
		t.Fatalf("second Write completed too early with %v", err)
	default:
	}

	if err := second.SetWriteDeadline(time.Now().Add(30 * time.Millisecond)); err != nil {
		t.Fatalf("SetWriteDeadline: %v", err)
	}

	select {
	case err := <-errCh:
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("second Write err = %v, want deadline exceeded", err)
		}
	case <-time.After(2 * testSignalTimeout):
		t.Fatal("blocked queue send did not wake on deadline update")
	}

	if err := releaseBlockedFirstWrite(t, writer, firstErrCh); err != nil {
		t.Fatalf("first Write err = %v, want nil after writer release", err)
	}
}

func TestWriteDeadlineRollbackReleasesPreparedSendCreditWhileBlockedOnWriterQueue(t *testing.T) {
	c, first, second, writer := newBlockedWriterConnWithStreams(t)

	firstErrCh := startBlockedFirstWrite(t, first, writer)

	if err := second.SetWriteDeadline(time.Now().Add(30 * time.Millisecond)); err != nil {
		t.Fatalf("SetWriteDeadline: %v", err)
	}

	_, err := second.Write([]byte("world"))
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("second Write err = %v, want deadline exceeded", err)
	}

	c.mu.Lock()
	secondSendSent := second.sendSent
	sessionUsed := c.flow.sendSessionUsed
	c.mu.Unlock()

	if secondSendSent != 0 {
		t.Fatalf("second sendSent = %d, want 0 after queue-admission timeout rollback", secondSendSent)
	}
	if sessionUsed != uint64(len("hello")) {
		t.Fatalf("sendSessionUsed = %d, want %d after rolling back only the timed-out request", sessionUsed, len("hello"))
	}

	if err := releaseBlockedFirstWrite(t, writer, firstErrCh); err != nil {
		t.Fatalf("first Write err = %v, want nil after writer release", err)
	}
}

func TestWriteFinalDeadlineRollbackClearsPreparedFinWhileBlockedOnWriterQueue(t *testing.T) {
	c, first, second, writer := newBlockedWriterConnWithStreams(t)

	firstErrCh := startBlockedFirstWrite(t, first, writer)

	if err := second.SetWriteDeadline(time.Now().Add(30 * time.Millisecond)); err != nil {
		t.Fatalf("SetWriteDeadline: %v", err)
	}

	_, err := second.WriteFinal([]byte("world"))
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("second WriteFinal err = %v, want deadline exceeded", err)
	}

	c.mu.Lock()
	secondSendSent := second.sendSent
	secondSendFin := second.sendFinReached()
	sessionUsed := c.flow.sendSessionUsed
	c.mu.Unlock()

	if secondSendSent != 0 {
		t.Fatalf("second sendSent = %d, want 0 after final-write queue-admission timeout rollback", secondSendSent)
	}
	if secondSendFin {
		t.Fatal("second sendFinReached() = true, want false after final-write queue-admission timeout rollback")
	}
	if sessionUsed != uint64(len("hello")) {
		t.Fatalf("sendSessionUsed = %d, want %d after rolling back only the timed-out final request", sessionUsed, len("hello"))
	}

	if err := releaseBlockedFirstWrite(t, writer, firstErrCh); err != nil {
		t.Fatalf("first Write err = %v, want nil after writer release", err)
	}
}

func TestWriteDeadlineRestoresPendingPriorityUpdateWhileBlockedOnWriterQueue(t *testing.T) {
	c, first, second, writer := newBlockedWriterConnWithStreams(t)
	wantPayload := configurePendingPriorityUpdateForTest(t, c, second, 7)

	firstErrCh := startBlockedFirstWrite(t, first, writer)

	if err := second.SetWriteDeadline(time.Now().Add(30 * time.Millisecond)); err != nil {
		t.Fatalf("SetWriteDeadline: %v", err)
	}

	_, err := second.Write([]byte("world"))
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("second Write err = %v, want deadline exceeded", err)
	}

	c.mu.Lock()
	gotPayload, ok := testPendingPriorityUpdatePayload(c, second.id)
	gotPrepared := c.pending.preparedPriorityBytes
	c.mu.Unlock()

	if !ok {
		t.Fatal("pending priority update missing after writer-queue deadline rollback")
	}
	if string(gotPayload) != string(wantPayload) {
		t.Fatalf("restored priority payload = %x, want %x", gotPayload, wantPayload)
	}
	if gotPrepared != 0 {
		t.Fatalf("preparedPriorityBytes = %d, want 0 after rollback", gotPrepared)
	}
	select {
	case <-c.pending.controlNotify:
	case <-time.After(testSignalTimeout):
		t.Fatal("restored pending priority update did not notify control flush")
	}

	if err := releaseBlockedFirstWrite(t, writer, firstErrCh); err != nil {
		t.Fatalf("first Write err = %v, want nil after writer release", err)
	}
}

func TestCloseWriteDeadlineRestoresPendingPriorityUpdateWhileBlockedOnWriterQueue(t *testing.T) {
	c, first, second, writer := newBlockedWriterConnWithStreams(t)
	wantPayload := configurePendingPriorityUpdateForTest(t, c, second, 9)

	firstErrCh := startBlockedFirstWrite(t, first, writer)

	if err := second.SetWriteDeadline(time.Now().Add(30 * time.Millisecond)); err != nil {
		t.Fatalf("SetWriteDeadline: %v", err)
	}

	err := second.CloseWrite()
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("CloseWrite err = %v, want deadline exceeded", err)
	}

	c.mu.Lock()
	gotPayload, ok := testPendingPriorityUpdatePayload(c, second.id)
	gotPrepared := c.pending.preparedPriorityBytes
	sendFin := second.sendFinReached()
	c.mu.Unlock()

	if !ok {
		t.Fatal("pending priority update missing after CloseWrite deadline rollback")
	}
	if string(gotPayload) != string(wantPayload) {
		t.Fatalf("restored priority payload = %x, want %x", gotPayload, wantPayload)
	}
	if gotPrepared != 0 {
		t.Fatalf("preparedPriorityBytes = %d, want 0 after CloseWrite rollback", gotPrepared)
	}
	if sendFin {
		t.Fatal("sendFinReached() = true, want false after CloseWrite queue-admission timeout rollback")
	}
	select {
	case <-c.pending.controlNotify:
	case <-time.After(testSignalTimeout):
		t.Fatal("restored pending priority update after CloseWrite did not notify control flush")
	}

	if err := releaseBlockedFirstWrite(t, writer, firstErrCh); err != nil {
		t.Fatalf("first Write err = %v, want nil after writer release", err)
	}
}

func TestCloseReadDeadlineExpiresWhileBlockedOnWriterQueue(t *testing.T) {
	_, first, second, writer := newBlockedWriterConnWithStreams(t)

	firstErrCh := startBlockedFirstWrite(t, first, writer)

	if err := second.SetWriteDeadline(time.Now().Add(30 * time.Millisecond)); err != nil {
		t.Fatalf("SetWriteDeadline: %v", err)
	}

	err := second.CloseRead()
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("CloseRead err = %v, want deadline exceeded", err)
	}
	if !second.readStopSentLocked() {
		t.Fatal("readStopped = false, want local read stop committed even when outbound STOP_SENDING times out")
	}

	if err := releaseBlockedFirstWrite(t, writer, firstErrCh); err != nil {
		t.Fatalf("first Write err = %v, want nil after writer release", err)
	}
}

func TestSetWriteDeadlineWakesCloseReadBlockedOnWriterQueue(t *testing.T) {
	_, first, second, writer := newBlockedWriterConnWithStreams(t)

	firstErrCh := startBlockedFirstWrite(t, first, writer)

	errCh := make(chan error, 1)
	go func() {
		errCh <- second.CloseRead()
	}()

	select {
	case err := <-errCh:
		t.Fatalf("CloseRead completed too early with %v", err)
	default:
	}

	if err := second.SetWriteDeadline(time.Now().Add(30 * time.Millisecond)); err != nil {
		t.Fatalf("SetWriteDeadline: %v", err)
	}

	select {
	case err := <-errCh:
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("CloseRead err = %v, want deadline exceeded", err)
		}
	case <-time.After(2 * testSignalTimeout):
		t.Fatal("blocked CloseRead did not wake on deadline update")
	}

	if !second.readStopSentLocked() {
		t.Fatal("readStopped = false, want local read stop committed before outbound signaling completes")
	}

	if err := releaseBlockedFirstWrite(t, writer, firstErrCh); err != nil {
		t.Fatalf("first Write err = %v, want nil after writer release", err)
	}
}

func TestCloseReadCommitsLocalStopBeforeOpenerWriterDeadline(t *testing.T) {
	c, first, second, writer := newBlockedWriterConnWithStreams(t)

	c.mu.Lock()
	testResetLocalOpenVisibility(second)
	c.mu.Unlock()

	firstErrCh := startBlockedFirstWrite(t, first, writer)

	if err := second.SetWriteDeadline(time.Now().Add(30 * time.Millisecond)); err != nil {
		t.Fatalf("SetWriteDeadline: %v", err)
	}

	err := second.CloseRead()
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("CloseRead err = %v, want deadline exceeded", err)
	}

	c.mu.Lock()
	readStopped := second.readStopSentLocked()
	sendCommitted := second.localOpen.committed
	peerVisible := second.isPeerVisibleLocked()
	c.mu.Unlock()

	if !readStopped {
		t.Fatal("readStopped = false, want local read stop committed before opener queue deadline")
	}
	if !sendCommitted {
		t.Fatal("sendCommitted = false, want opener commit-visible before writer admission deadline")
	}
	if peerVisible {
		t.Fatal("peerVisible = true, want false when opener never entered the writer path")
	}

	if err := releaseBlockedFirstWrite(t, writer, firstErrCh); err != nil {
		t.Fatalf("first Write err = %v, want nil after writer release", err)
	}
}

func TestCloseReadRetryAfterOpenerWriterDeadlineContinuesSignaling(t *testing.T) {
	c, first, second, writer := newBlockedWriterConnWithStreams(t)

	c.mu.Lock()
	testResetLocalOpenVisibility(second)
	c.mu.Unlock()

	firstErrCh := startBlockedFirstWrite(t, first, writer)

	if err := second.SetWriteDeadline(time.Now().Add(30 * time.Millisecond)); err != nil {
		t.Fatalf("SetWriteDeadline: %v", err)
	}

	err := second.CloseRead()
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("first CloseRead err = %v, want deadline exceeded", err)
	}

	c.mu.Lock()
	readStopped := second.readStopSentLocked()
	peerVisible := second.isPeerVisibleLocked()
	c.mu.Unlock()

	if !readStopped {
		t.Fatal("readStopped = false, want local read stop committed after first CloseRead deadline")
	}
	if peerVisible {
		t.Fatal("peerVisible = true, want false before retry re-queues opener")
	}

	if err := second.SetWriteDeadline(time.Time{}); err != nil {
		t.Fatalf("clear SetWriteDeadline: %v", err)
	}

	closeReadErrCh := make(chan error, 1)
	go func() {
		closeReadErrCh <- second.CloseRead()
	}()

	select {
	case err := <-closeReadErrCh:
		t.Fatalf("retry CloseRead returned before writer release: %v", err)
	case <-time.After(30 * time.Millisecond):
	}

	_ = writer.Close()

	select {
	case err := <-closeReadErrCh:
		if err != nil {
			t.Fatalf("retry CloseRead err = %v, want nil", err)
		}
	case <-time.After(2 * testSignalTimeout):
		t.Fatal("retry CloseRead did not complete after writer release")
	}

	select {
	case err := <-firstErrCh:
		if err != nil {
			t.Fatalf("first Write err = %v, want nil after writer release", err)
		}
	case <-time.After(testSignalTimeout):
		t.Fatal("first write did not unblock during cleanup")
	}

	c.mu.Lock()
	peerVisible = second.isPeerVisibleLocked()
	c.mu.Unlock()

	if !peerVisible {
		t.Fatal("peerVisible = false, want true after retry CloseRead re-queued opener")
	}
}

func TestTerminalWakeFailsWriteBlockedOnWriterQueue(t *testing.T) {
	_, first, second, writer := newBlockedWriterConnWithStreams(t)

	firstErrCh := startBlockedFirstWrite(t, first, writer)

	errCh := make(chan error, 1)
	go func() {
		_, err := second.Write([]byte("world"))
		errCh <- err
	}()

	second.conn.mu.Lock()
	second.setSendStopSeen(&ApplicationError{Code: uint64(CodeCancelled)})
	second.conn.mu.Unlock()
	notify(second.writeNotify)

	select {
	case err := <-errCh:
		if !errors.Is(err, ErrWriteClosed) {
			t.Fatalf("second Write err = %v, want %v", err, ErrWriteClosed)
		}
	case <-time.After(2 * testSignalTimeout):
		t.Fatal("blocked queue send did not wake on terminal state")
	}

	if err := releaseBlockedFirstWrite(t, writer, firstErrCh); err != nil {
		t.Fatalf("first Write err = %v, want nil after writer release", err)
	}
}

func TestCloseSessionFailsWriteBlockedOnWriterQueue(t *testing.T) {
	_, first, second, writer := newBlockedWriterConnWithStreams(t)

	firstErrCh := startBlockedFirstWrite(t, first, writer)

	errCh := make(chan error, 1)
	go func() {
		_, err := second.Write([]byte("world"))
		errCh <- err
	}()

	second.conn.closeSession(nil)

	select {
	case err := <-errCh:
		if !errors.Is(err, ErrSessionClosed) {
			t.Fatalf("second Write err = %v, want %v", err, ErrSessionClosed)
		}
	case <-time.After(2 * testSignalTimeout):
		t.Fatal("blocked queue send did not wake on session close")
	}

	if err := releaseBlockedFirstWrite(t, writer, firstErrCh); !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("first Write err = %v, want %v after session close", err, ErrSessionClosed)
	}
}

func TestEnqueueWriteRequestMemoryFailureClearsRetainedRefs(t *testing.T) {
	stream := newQueueBackpressureTestStream()
	stream.conn.flow.sessionMemoryCap = 1

	req := writeRequest{
		frames: testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: stream.id, Payload: []byte("x")}}),
		origin: writeRequestOriginStream,
	}
	if err := stream.prepareOwnedWriteRequest(&req); err != nil {
		t.Fatalf("prepareOwnedWriteRequest: %v", err)
	}
	stream.conn.mu.Lock()
	stream.sendSent = saturatingAdd(stream.sendSent, req.preparedSendBytes)
	stream.conn.flow.sendSessionUsed = saturatingAdd(stream.conn.flow.sendSessionUsed, req.preparedSendBytes)
	stream.conn.mu.Unlock()

	err := stream.enqueueWriteRequestUntilDeadline(&req, streamWriteDispatchOptions{
		lane:           writeLaneOrdinary,
		deadlinePolicy: writeDeadlineOverrideOnly,
	})
	if !IsErrorCode(err, CodeInternal) {
		t.Fatalf("enqueueWriteRequestUntilDeadline err = %v, want %s", err, CodeInternal)
	}
	assertRetainedWriteRequestCleared(t, &req)

	if !IsErrorCode(stream.conn.err(), CodeInternal) {
		t.Fatalf("stored session err = %v, want %s", stream.conn.err(), CodeInternal)
	}

	stream.conn.mu.Lock()
	sendSent := stream.sendSent
	sendSessionUsed := stream.conn.flow.sendSessionUsed
	streamQueued := stream.queuedDataBytes
	sessionQueued := stream.conn.flow.queuedDataBytes
	stream.conn.mu.Unlock()

	if sendSent != 0 {
		t.Fatalf("stream sendSent = %d, want 0 after memory rollback", sendSent)
	}
	if sendSessionUsed != 0 {
		t.Fatalf("conn sendSessionUsed = %d, want 0 after memory rollback", sendSessionUsed)
	}
	if streamQueued != 0 || sessionQueued != 0 {
		t.Fatalf("queued bytes = (%d,%d), want (0,0) after memory rollback", streamQueued, sessionQueued)
	}
}

func TestWriteAfterSendTerminalReturnsExpectedLocalError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		sendHalf      string
		wantWriteStop bool
		wantCode      uint64
		wantState     state.SendHalfState
	}{
		{
			name:          "send_fin",
			sendHalf:      "send_fin",
			wantWriteStop: true,
			wantState:     state.SendHalfFin,
		},
		{
			name:          "send_stop_seen",
			sendHalf:      "send_stop_seen",
			wantWriteStop: true,
			wantState:     state.SendHalfStopSeen,
		},
		{
			name:      "send_reset",
			sendHalf:  "send_reset",
			wantCode:  uint64(CodeCancelled),
			wantState: state.SendHalfReset,
		},
		{
			name:      "send_aborted",
			sendHalf:  "send_aborted",
			wantCode:  uint64(CodeCancelled),
			wantState: state.SendHalfAborted,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c, frames, stop := newInvalidFrameConn(t, 0)
			defer stop()

			stream := seedStateFixtureStream(t, c, state.FirstLocalStreamID(c.config.negotiated.LocalRole, true), "bidi", "local_owned", stateHalfExpect{
				SendHalf: tc.sendHalf,
				RecvHalf: "recv_open",
			})
			testMarkLocalOpenCommitted(stream)

			_, err := stream.Write([]byte("x"))
			if tc.wantWriteStop {
				if !isWriteStoppedErr(err) {
					t.Fatalf("Write err = %v, want write-stopped error", err)
				}
			} else {
				var appErr *ApplicationError
				if !errors.As(err, &appErr) {
					t.Fatalf("Write err = %v, want ApplicationError", err)
				}
				if appErr.Code != tc.wantCode {
					t.Fatalf("Write code = %d, want %d", appErr.Code, tc.wantCode)
				}
			}

			if got := stream.sendHalfState(); got != tc.wantState {
				t.Fatalf("sendHalf = %v, want %v", got, tc.wantState)
			}
			assertNoQueuedFrame(t, frames)
		})
	}
}

func TestWriteFinalAfterSendTerminalReturnsExpectedLocalError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		sendHalf      string
		wantWriteStop bool
		wantCode      uint64
		wantState     state.SendHalfState
	}{
		{
			name:          "send_fin",
			sendHalf:      "send_fin",
			wantWriteStop: true,
			wantState:     state.SendHalfFin,
		},
		{
			name:      "send_reset",
			sendHalf:  "send_reset",
			wantCode:  uint64(CodeCancelled),
			wantState: state.SendHalfReset,
		},
		{
			name:      "send_aborted",
			sendHalf:  "send_aborted",
			wantCode:  uint64(CodeCancelled),
			wantState: state.SendHalfAborted,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c, frames, stop := newInvalidFrameConn(t, 0)
			defer stop()

			stream := seedStateFixtureStream(t, c, state.FirstLocalStreamID(c.config.negotiated.LocalRole, true), "bidi", "local_owned", stateHalfExpect{
				SendHalf: tc.sendHalf,
				RecvHalf: "recv_open",
			})
			testMarkLocalOpenCommitted(stream)

			_, err := stream.WriteFinal([]byte("x"))
			if tc.wantWriteStop {
				if !isWriteStoppedErr(err) {
					t.Fatalf("WriteFinal err = %v, want write-stopped error", err)
				}
			} else {
				var appErr *ApplicationError
				if !errors.As(err, &appErr) {
					t.Fatalf("WriteFinal err = %v, want ApplicationError", err)
				}
				if appErr.Code != tc.wantCode {
					t.Fatalf("WriteFinal code = %d, want %d", appErr.Code, tc.wantCode)
				}
			}

			if got := stream.sendHalfState(); got != tc.wantState {
				t.Fatalf("sendHalf = %v, want %v", got, tc.wantState)
			}
			assertNoQueuedFrame(t, frames)
		})
	}
}

func TestWriteFinalAfterSendStopSeenQueuesClosingFin(t *testing.T) {
	t.Parallel()

	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()

	stream := seedStateFixtureStream(t, c, state.FirstLocalStreamID(c.config.negotiated.LocalRole, true), "bidi", "local_owned", stateHalfExpect{
		SendHalf: "send_stop_seen",
		RecvHalf: "recv_open",
	})
	testMarkLocalOpenCommitted(stream)

	n, err := stream.WriteFinal([]byte("x"))
	if err != nil {
		t.Fatalf("WriteFinal err = %v, want nil", err)
	}
	if n != 1 {
		t.Fatalf("WriteFinal n = %d, want 1", n)
	}

	frame := awaitQueuedFrame(t, frames)
	if frame.Type != FrameTypeDATA || frame.StreamID != stream.id || frame.Flags&FrameFlagFIN == 0 || string(frame.Payload) != "x" {
		t.Fatalf("queued frame = %+v, want DATA|FIN payload x", frame)
	}

	if got := stream.sendHalfState(); got != state.SendHalfFin {
		t.Fatalf("sendHalf = %v, want %v", got, state.SendHalfFin)
	}
}

func TestWritevFinalAfterSendStopSeenAcrossMultiplePartsQueuesClosingFin(t *testing.T) {
	t.Parallel()

	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()

	stream := seedStateFixtureStream(t, c, state.FirstLocalStreamID(c.config.negotiated.LocalRole, true), "bidi", "local_owned", stateHalfExpect{
		SendHalf: "send_stop_seen",
		RecvHalf: "recv_open",
	})
	testMarkLocalOpenCommitted(stream)

	n, err := stream.WritevFinal([]byte("x"), []byte("y"))
	if err != nil {
		t.Fatalf("WritevFinal err = %v, want nil", err)
	}
	if n != 2 {
		t.Fatalf("WritevFinal n = %d, want 2", n)
	}

	frame := awaitQueuedFrame(t, frames)
	if frame.Type != FrameTypeDATA || frame.StreamID != stream.id || frame.Flags&FrameFlagFIN == 0 || string(frame.Payload) != "xy" {
		t.Fatalf("queued frame = %+v, want DATA|FIN payload xy", frame)
	}

	if got := stream.sendHalfState(); got != state.SendHalfFin {
		t.Fatalf("sendHalf = %v, want %v", got, state.SendHalfFin)
	}
}

func TestWriteFinalAfterSendStopSeenWithInsufficientCreditQueuesReset(t *testing.T) {
	t.Parallel()

	c, frames, stop := newInvalidFrameConn(t, 0)
	defer stop()

	stream := seedStateFixtureStream(t, c, state.FirstLocalStreamID(c.config.negotiated.LocalRole, true), "bidi", "local_owned", stateHalfExpect{
		SendHalf: "send_stop_seen",
		RecvHalf: "recv_open",
	})
	testMarkLocalOpenCommitted(stream)
	stream.sendMax = 0

	_, err := stream.WriteFinal([]byte("x"))
	var appErr *ApplicationError
	if !errors.As(err, &appErr) {
		t.Fatalf("WriteFinal err = %v, want ApplicationError", err)
	}
	if appErr.Code != uint64(CodeCancelled) {
		t.Fatalf("WriteFinal code = %d, want %d", appErr.Code, uint64(CodeCancelled))
	}

	assertQueuedResetCode(t, awaitQueuedFrame(t, frames), stream.id, CodeCancelled)

	if got := stream.sendHalfState(); got != state.SendHalfReset {
		t.Fatalf("sendHalf = %v, want %v", got, state.SendHalfReset)
	}
}

func TestSendFinAllowsQueuedTerminalDataFINDrain(t *testing.T) {
	t.Parallel()

	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	stream := seedStateFixtureStream(t, c, state.FirstLocalStreamID(c.config.negotiated.LocalRole, true), "bidi", "local_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})
	testMarkLocalOpenCommitted(stream)
	stream.setSendFin()

	req := writeRequest{
		frames:         testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: stream.id, Flags: FrameFlagFIN, Payload: []byte("x")}}),
		done:           make(chan error, 1),
		origin:         writeRequestOriginStream,
		queueReserved:  true,
		queuedBytes:    2,
		reservedStream: stream,
	}

	if err := stream.suppressWriteRequestErrLocked(&req); err != nil {
		t.Fatalf("suppressWriteRequestErrLocked err = %v, want nil", err)
	}
}

func TestSendFinRejectsQueuedPlainDataDrain(t *testing.T) {
	t.Parallel()

	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	stream := seedStateFixtureStream(t, c, state.FirstLocalStreamID(c.config.negotiated.LocalRole, true), "bidi", "local_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})
	testMarkLocalOpenCommitted(stream)
	stream.setSendFin()

	req := writeRequest{
		frames:         testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: stream.id, Payload: []byte("x")}}),
		done:           make(chan error, 1),
		origin:         writeRequestOriginStream,
		queueReserved:  true,
		queuedBytes:    2,
		reservedStream: stream,
	}

	if err := stream.suppressWriteRequestErrLocked(&req); !errors.Is(err, ErrWriteClosed) {
		t.Fatalf("suppressWriteRequestErrLocked err = %v, want %v", err, ErrWriteClosed)
	}
}

func TestSendFinDoesNotAllowQueuedResetDrain(t *testing.T) {
	t.Parallel()

	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	stream := seedStateFixtureStream(t, c, state.FirstLocalStreamID(c.config.negotiated.LocalRole, true), "bidi", "local_owned", stateHalfExpect{
		SendHalf: "send_open",
		RecvHalf: "recv_open",
	})
	testMarkLocalOpenCommitted(stream)
	stream.setSendFin()

	req := writeRequest{
		frames:         testTxFramesFrom([]Frame{{Type: FrameTypeRESET, StreamID: stream.id, Payload: mustEncodeVarint(uint64(CodeCancelled))}}),
		done:           make(chan error, 1),
		terminalPolicy: terminalWriteAllow,
		origin:         writeRequestOriginStream,
		queueReserved:  true,
		queuedBytes:    1,
		reservedStream: stream,
	}

	if err := stream.suppressWriteRequestErrLocked(&req); !errors.Is(err, ErrWriteClosed) {
		t.Fatalf("suppressWriteRequestErrLocked err = %v, want %v", err, ErrWriteClosed)
	}
}

func TestSendStopSeenAllowsExplicitTerminalDataRequest(t *testing.T) {
	t.Parallel()

	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	stream := seedStateFixtureStream(t, c, state.FirstLocalStreamID(c.config.negotiated.LocalRole, true), "bidi", "local_owned", stateHalfExpect{
		SendHalf: "send_stop_seen",
		RecvHalf: "recv_open",
	})
	testMarkLocalOpenCommitted(stream)

	req := writeRequest{
		frames:         testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: stream.id, Payload: []byte("x")}}),
		done:           make(chan error, 1),
		terminalPolicy: terminalWriteAllow,
		origin:         writeRequestOriginStream,
		queuedBytes:    2,
		reservedStream: stream,
	}

	if err := stream.suppressWriteRequestErrLocked(&req); err != nil {
		t.Fatalf("suppressWriteRequestErrLocked err = %v, want nil", err)
	}
}

func TestSendResetAllowsExplicitTerminalResetRequest(t *testing.T) {
	t.Parallel()

	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	stream := seedStateFixtureStream(t, c, state.FirstLocalStreamID(c.config.negotiated.LocalRole, true), "bidi", "local_owned", stateHalfExpect{
		SendHalf: "send_reset",
		RecvHalf: "recv_open",
	})
	testMarkLocalOpenCommitted(stream)

	req := writeRequest{
		frames:         testTxFramesFrom([]Frame{{Type: FrameTypeRESET, StreamID: stream.id, Payload: mustEncodeVarint(uint64(CodeCancelled))}}),
		done:           make(chan error, 1),
		terminalPolicy: terminalWriteAllow,
		origin:         writeRequestOriginStream,
		queuedBytes:    1,
		reservedStream: stream,
	}

	if err := stream.suppressWriteRequestErrLocked(&req); err != nil {
		t.Fatalf("suppressWriteRequestErrLocked err = %v, want nil", err)
	}
}

func TestSendAbortedAllowsExplicitTerminalAbortRequest(t *testing.T) {
	t.Parallel()

	c, _, stop := newInvalidFrameConn(t, 0)
	defer stop()

	stream := seedStateFixtureStream(t, c, state.FirstLocalStreamID(c.config.negotiated.LocalRole, true), "bidi", "local_owned", stateHalfExpect{
		SendHalf: "send_aborted",
		RecvHalf: "recv_open",
	})
	testMarkLocalOpenCommitted(stream)

	req := writeRequest{
		frames:         testTxFramesFrom([]Frame{{Type: FrameTypeABORT, StreamID: stream.id, Payload: mustEncodeVarint(uint64(CodeCancelled))}}),
		done:           make(chan error, 1),
		terminalPolicy: terminalWriteAllow,
		origin:         writeRequestOriginStream,
		queuedBytes:    1,
		reservedStream: stream,
	}

	if err := stream.suppressWriteRequestErrLocked(&req); err != nil {
		t.Fatalf("suppressWriteRequestErrLocked err = %v, want nil", err)
	}
}

func newUrgentBackpressureConn() *Conn {
	settings := DefaultSettings()
	return &Conn{

		lifecycle: connLifecycleState{closedCh: make(chan struct{})},
		pending:   connPendingControlState{controlNotify: make(chan struct{}, 1)},
		writer:    connWriterRuntimeState{writeCh: make(chan writeRequest, 1), urgentWriteCh: make(chan writeRequest, 1)},
		config: connConfigState{local: Preface{Settings: settings},
			peer: Preface{Settings: settings}}, registry: connRegistryState{streams: make(map[uint64]*nativeStream)},
	}
}

func newQueueBackpressureTestStream() *nativeStream {
	c := &Conn{

		lifecycle: connLifecycleState{closedCh: make(chan struct{})},

		writer: connWriterRuntimeState{writeCh: make(chan writeRequest, 1), urgentWriteCh: make(chan writeRequest, 1)}, config: connConfigState{peer: Preface{Settings: Settings{MaxFramePayload: 16384}}}, flow: connFlowState{sendSessionMax: 1 << 20}, registry: connRegistryState{streams: make(map[uint64]*nativeStream)},
	}
	return testVisibleBidiStream(c, 4, testWithSendMax(1<<20))
}

func TestUrgentFrameBypassesOrdinaryQueuedDataWatermark(t *testing.T) {
	c := newUrgentBackpressureConn()
	c.flow.queuedDataBytes = c.sessionDataHWMLocked()

	errCh := make(chan error, 1)
	go func() {
		errCh <- testQueueFrame(c, Frame{Type: FrameTypePING, Payload: make([]byte, 8)})
	}()

	select {
	case req := <-c.writer.urgentWriteCh:
		req.done <- nil
	case <-time.After(testSignalTimeout):
		t.Fatal("urgent frame did not bypass ordinary queued-data watermark")
	}

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("queueFrame err = %v, want nil", err)
		}
	case <-time.After(testSignalTimeout):
		t.Fatal("queueFrame did not complete after urgent send")
	}
}

func TestUrgentFrameBlocksAtUrgentLaneCapUntilRelease(t *testing.T) {
	c := newUrgentBackpressureConn()
	reserved := writeRequest{
		queuedBytes:    c.urgentLaneCapLocked(),
		urgentReserved: true,
	}
	c.flow.urgentQueuedBytes = reserved.queuedBytes

	errCh := make(chan error, 1)
	go func() {
		errCh <- testQueueFrame(c, Frame{Type: FrameTypePING, Payload: make([]byte, 8)})
	}()

	select {
	case err := <-errCh:
		t.Fatalf("queueFrame completed too early with %v", err)
	case <-time.After(25 * time.Millisecond):
	}

	go func() {
		req := <-c.writer.urgentWriteCh
		req.done <- nil
	}()

	c.releaseUrgentQueueReservation(&reserved)

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("queueFrame err = %v, want nil", err)
		}
	case <-time.After(2 * testSignalTimeout):
		t.Fatal("urgent frame did not resume after urgent-lane release")
	}
}

func TestUrgentFrameBlocksAtUrgentLaneCapUntilBatchRelease(t *testing.T) {
	c := newUrgentBackpressureConn()
	batch := []writeRequest{{
		queuedBytes:    c.urgentLaneCapLocked(),
		urgentReserved: true,
	}}
	c.flow.urgentQueuedBytes = batch[0].queuedBytes

	errCh := make(chan error, 1)
	go func() {
		errCh <- testQueueFrame(c, Frame{Type: FrameTypePING, Payload: make([]byte, 8)})
	}()

	select {
	case err := <-errCh:
		t.Fatalf("queueFrame completed too early with %v", err)
	case <-time.After(25 * time.Millisecond):
	}

	go func() {
		req := <-c.writer.urgentWriteCh
		req.done <- nil
	}()

	c.releaseBatchReservations(batch)

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("queueFrame err = %v, want nil", err)
		}
	case <-time.After(2 * testSignalTimeout):
		t.Fatal("urgent frame did not resume after batch urgent-lane release")
	}
}

func TestUrgentFrameClosesSessionWhenTrackedMemoryCapWouldBeExceeded(t *testing.T) {
	c := newUrgentBackpressureConn()
	c.flow.recvSessionUsed = 8
	c.flow.sessionMemoryCap = 8

	err := testQueueFrame(c, Frame{Type: FrameTypePING, Payload: make([]byte, 8)})
	if !IsErrorCode(err, CodeInternal) {
		t.Fatalf("queueFrame err = %v, want %s", err, CodeInternal)
	}
	if !IsErrorCode(c.err(), CodeInternal) {
		t.Fatalf("stored session err = %v, want %s", c.err(), CodeInternal)
	}
}

func TestQueueUrgentFramesSplitsBatchesToRespectUrgentLaneCap(t *testing.T) {
	c := newUrgentBackpressureConn()
	c.flow.urgentQueueCap = 10

	frames := []Frame{
		{Type: FrameTypePING, Payload: make([]byte, 8)},
		{Type: FrameTypePING, Payload: make([]byte, 8)},
		{Type: FrameTypePING, Payload: make([]byte, 8)},
	}

	reqCh := make(chan writeRequest, len(frames))
	go func() {
		for i := 0; i < len(frames); i++ {
			req := <-c.writer.urgentWriteCh
			reqCh <- req
			c.releaseUrgentQueueReservation(&req)
			req.done <- nil
		}
	}()

	if err := testQueueUrgentFrames(c, frames); err != nil {
		t.Fatalf("queueUrgentFrames err = %v, want nil", err)
	}

	totalFrames := 0
	for i := 0; i < len(frames); i++ {
		req := <-reqCh
		if req.queuedBytes > c.flow.urgentQueueCap {
			t.Fatalf("queuedBytes = %d, want <= %d", req.queuedBytes, c.flow.urgentQueueCap)
		}
		totalFrames += len(req.frames)
	}
	if totalFrames != len(frames) {
		t.Fatalf("queued frame count = %d, want %d", totalFrames, len(frames))
	}
}

func TestQueueUrgentFramesSplitsBatchesAtFrameCountLimit(t *testing.T) {
	c := newUrgentBackpressureConn()

	frames := make([]Frame, maxWriteBatchFrames+1)
	for i := range frames {
		frames[i] = Frame{Type: FrameTypePING, Payload: make([]byte, 8)}
	}

	reqCh := make(chan writeRequest, 2)
	go func() {
		for i := 0; i < 2; i++ {
			req := <-c.writer.urgentWriteCh
			reqCh <- req
			c.releaseUrgentQueueReservation(&req)
			req.done <- nil
		}
	}()

	if err := testQueueUrgentFrames(c, frames); err != nil {
		t.Fatalf("queueUrgentFrames err = %v, want nil", err)
	}

	first := <-reqCh
	second := <-reqCh
	if got := len(first.frames); got != maxWriteBatchFrames {
		t.Fatalf("first urgent chunk frames = %d, want %d", got, maxWriteBatchFrames)
	}
	if got := len(second.frames); got != 1 {
		t.Fatalf("second urgent chunk frames = %d, want 1", got)
	}
}

func TestQueueUrgentFramesDispatchesSecondChunkBeforeFirstCompletion(t *testing.T) {
	c := newUrgentBackpressureConn()
	c.writer.urgentWriteCh = make(chan writeRequest, 2)
	c.flow.urgentQueueCap = 1 << 20

	frames := make([]Frame, maxWriteBatchFrames+1)
	for i := range frames {
		frames[i] = Frame{Type: FrameTypePING, Payload: make([]byte, 8)}
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- testQueueUrgentFrames(c, frames)
	}()

	var first, second writeRequest
	select {
	case first = <-c.writer.urgentWriteCh:
	case <-time.After(testSignalTimeout):
		t.Fatal("first urgent chunk was not dispatched")
	}
	select {
	case second = <-c.writer.urgentWriteCh:
	case <-time.After(testSignalTimeout):
		t.Fatal("second urgent chunk was not dispatched before first completion")
	}

	if got := len(first.frames); got != maxWriteBatchFrames {
		t.Fatalf("first urgent chunk frames = %d, want %d", got, maxWriteBatchFrames)
	}
	if got := len(second.frames); got != 1 {
		t.Fatalf("second urgent chunk frames = %d, want 1", got)
	}

	select {
	case err := <-errCh:
		t.Fatalf("queueUrgentFrames returned early with %v", err)
	default:
	}

	c.releaseUrgentQueueReservation(&first)
	first.done <- nil

	select {
	case err := <-errCh:
		t.Fatalf("queueUrgentFrames returned before second completion with %v", err)
	default:
	}

	c.releaseUrgentQueueReservation(&second)
	second.done <- nil

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("queueUrgentFrames err = %v, want nil", err)
		}
	case <-time.After(testSignalTimeout):
		t.Fatal("queueUrgentFrames did not finish after both chunks completed")
	}
}

func TestQueueFramesUntilDeadlineAllowsTerminalUrgentControlUsesReservedStream(t *testing.T) {
	stream := newQueueBackpressureTestStream()
	payload := mustEncodeVarint(uint64(CodeCancelled))

	errCh := make(chan error, 1)
	go func() {
		errCh <- stream.queueFramesUntilDeadlineAndOptionsOwned(testTxFrames(
			Frame{
				Type:     FrameTypeRESET,
				StreamID: stream.id,
				Payload:  payload,
			}), queuedWriteOptions{
			terminalPolicy: terminalWriteAllow,
			ownership:      frameOwned,
		})
	}()

	select {
	case req := <-stream.conn.writer.urgentWriteCh:
		if req.reservedStream != stream {
			t.Fatalf("reservedStream = %p, want %p", req.reservedStream, stream)
		}
		req.done <- nil
	case <-time.After(testSignalTimeout):
		t.Fatal("queueFramesUntilDeadlineAndOptionsOwned did not queue urgent request")
	}

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("queueFramesUntilDeadlineAndOptionsOwned err = %v, want nil", err)
		}
	case <-time.After(testSignalTimeout):
		t.Fatal("queueFramesUntilDeadlineAndOptionsOwned did not complete")
	}
}

func TestQueueFramesUntilDeadlineCachesReservedStream(t *testing.T) {
	stream := newQueueBackpressureTestStream()

	errCh := make(chan error, 1)
	go func() {
		errCh <- stream.queueFramesUntilDeadlineAndOptionsOwned(testTxFrames(
			Frame{
				Type:     FrameTypeDATA,
				StreamID: stream.id,
				Payload:  []byte("x"),
			}), queuedWriteOptions{
			ownership: frameOwned,
		})
	}()

	select {
	case req := <-stream.conn.writer.writeCh:
		if req.reservedStream != stream {
			t.Fatalf("reservedStream = %p, want %p", req.reservedStream, stream)
		}
		req.done <- nil
	case <-time.After(testSignalTimeout):
		t.Fatal("queueFramesUntilDeadlineAndOptionsOwned did not queue request")
	}

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("queueFramesUntilDeadlineAndOptionsOwned err = %v, want nil", err)
		}
	case <-time.After(testSignalTimeout):
		t.Fatal("queueFramesUntilDeadlineAndOptionsOwned did not complete")
	}
}

func TestQueueFramesUntilDeadlineRoutesUrgentControlToUrgentLane(t *testing.T) {
	stream := newQueueBackpressureTestStream()
	payload := mustEncodeVarint(uint64(CodeCancelled))

	errCh := make(chan error, 1)
	go func() {
		errCh <- stream.queueFramesUntilDeadlineAndOptionsOwned(testTxFrames(
			Frame{
				Type:     FrameTypeStopSending,
				StreamID: stream.id,
				Payload:  payload,
			},
		), queuedWriteOptions{
			ownership: frameOwned,
		})
	}()

	select {
	case req := <-stream.conn.writer.urgentWriteCh:
		if req.reservedStream != stream {
			t.Fatalf("reservedStream = %p, want %p", req.reservedStream, stream)
		}
		if len(req.frames) != 1 || req.frames[0].Type != FrameTypeStopSending {
			t.Fatalf("queued frames = %+v, want single STOP_SENDING", req.frames)
		}
		req.done <- nil
	case <-time.After(testSignalTimeout):
		t.Fatal("owned urgent control did not route to urgent lane")
	}

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("queueFramesUntilDeadlineAndOptionsOwned err = %v, want nil", err)
		}
	case <-time.After(testSignalTimeout):
		t.Fatal("owned urgent control queue did not complete")
	}
}

func TestWriteDeadlineExpiresWhileBlockedBySessionQueuedDataWatermark(t *testing.T) {
	stream := newQueueBackpressureTestStream()
	stream.conn.flow.queuedDataBytes = stream.conn.sessionDataHWMLocked()

	if err := stream.SetWriteDeadline(time.Now().Add(30 * time.Millisecond)); err != nil {
		t.Fatalf("SetWriteDeadline: %v", err)
	}

	_, err := stream.Write([]byte("x"))
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("Write err = %v, want deadline exceeded", err)
	}

	stream.conn.mu.Lock()
	sendSent := stream.sendSent
	sendSessionUsed := stream.conn.flow.sendSessionUsed
	stream.conn.mu.Unlock()

	if sendSent != 0 {
		t.Fatalf("stream.sendSent = %d, want 0 after queue-watermark timeout rollback", sendSent)
	}
	if sendSessionUsed != 0 {
		t.Fatalf("conn.flow.sendSessionUsed = %d, want 0 after queue-watermark timeout rollback", sendSessionUsed)
	}
}

func TestWriteDeadlineExpiresWhileBlockedByPerStreamQueuedDataWatermark(t *testing.T) {
	stream := newQueueBackpressureTestStream()
	stream.queuedDataBytes = stream.conn.perStreamDataHWMLocked()

	if err := stream.SetWriteDeadline(time.Now().Add(30 * time.Millisecond)); err != nil {
		t.Fatalf("SetWriteDeadline: %v", err)
	}

	_, err := stream.Write([]byte("x"))
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("Write err = %v, want deadline exceeded", err)
	}

	stream.conn.mu.Lock()
	sendSent := stream.sendSent
	sendSessionUsed := stream.conn.flow.sendSessionUsed
	stream.conn.mu.Unlock()

	if sendSent != 0 {
		t.Fatalf("stream.sendSent = %d, want 0 after per-stream queue-watermark timeout rollback", sendSent)
	}
	if sendSessionUsed != 0 {
		t.Fatalf("conn.flow.sendSessionUsed = %d, want 0 after per-stream queue-watermark timeout rollback", sendSessionUsed)
	}
}

func TestReleaseWriteQueueReservationWakesBlockedWriteAtLowWatermark(t *testing.T) {
	stream := newQueueBackpressureTestStream()
	holder := testBuildStream(stream.conn, 8, testWithLocalSend(), testWithWriteNotify())
	stream.conn.registry.streams[holder.id] = holder

	sessionHWM := stream.conn.sessionDataHWMLocked()
	sessionLWM := stream.conn.sessionDataLWMLocked()
	reserved := sessionHWM - sessionLWM

	holder.queuedDataBytes = reserved
	stream.conn.flow.queuedDataBytes = sessionHWM

	req := writeRequest{
		frames:         testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: holder.id, Payload: []byte("held")}}),
		done:           make(chan error, 1),
		origin:         writeRequestOriginStream,
		queueReserved:  true,
		queuedBytes:    reserved,
		reservedStream: holder,
	}

	done := make(chan error, 1)
	go func() {
		_, err := stream.Write([]byte("x"))
		done <- err
	}()
	awaitStreamWriteWaiter(t, stream, testSignalTimeout, "blocked write did not enter wait state")

	go func() {
		queued := <-stream.conn.writer.writeCh
		queued.done <- nil
	}()

	stream.conn.releaseWriteQueueReservation(&req)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Write err = %v, want nil", err)
		}
	case <-time.After(2 * testSignalTimeout):
		t.Fatal("blocked write did not wake after queued bytes crossed low watermark")
	}
}

func TestReleaseBatchReservationsClearsInflightQueued(t *testing.T) {
	stream := newQueueBackpressureTestStream()
	stream.queuedDataBytes = 7
	stream.inflightQueued = 7

	batch := []writeRequest{{
		frames:         testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: stream.id, Payload: []byte("held")}}),
		done:           make(chan error, 1),
		origin:         writeRequestOriginStream,
		queueReserved:  true,
		queuedBytes:    7,
		reservedStream: stream,
	}}

	stream.conn.releaseBatchReservations(batch)

	stream.conn.mu.Lock()
	defer stream.conn.mu.Unlock()
	if stream.inflightQueued != 0 {
		t.Fatalf("stream.inflightQueued = %d, want 0", stream.inflightQueued)
	}
	if stream.queuedDataBytes != 0 {
		t.Fatalf("stream.queuedDataBytes = %d, want 0", stream.queuedDataBytes)
	}
}

func TestReleaseBatchReservationsWakesDistinctStreamsCrossingLowWatermark(t *testing.T) {
	stream := newQueueBackpressureTestStream()
	other := testVisibleBidiStream(stream.conn, 8, testWithSendMax(1<<20))

	released := stream.conn.perStreamDataLWMLocked() + 1
	stream.queuedDataBytes = released
	stream.inflightQueued = released
	other.queuedDataBytes = released
	other.inflightQueued = released
	stream.conn.flow.queuedDataBytes = stream.conn.sessionDataLWMLocked() + released*2 + 1

	batch := []writeRequest{
		{
			frames:         testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: stream.id, Payload: []byte("held")}}),
			done:           make(chan error, 1),
			origin:         writeRequestOriginStream,
			queueReserved:  true,
			queuedBytes:    released,
			reservedStream: stream,
		},
		{
			frames:         testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: other.id, Payload: []byte("held")}}),
			done:           make(chan error, 1),
			origin:         writeRequestOriginStream,
			queueReserved:  true,
			queuedBytes:    released,
			reservedStream: other,
		},
	}

	stream.conn.releaseBatchReservations(batch)

	stream.conn.mu.Lock()
	if stream.inflightQueued != 0 {
		stream.conn.mu.Unlock()
		t.Fatalf("stream.inflightQueued = %d, want 0", stream.inflightQueued)
	}
	if other.inflightQueued != 0 {
		stream.conn.mu.Unlock()
		t.Fatalf("other.inflightQueued = %d, want 0", other.inflightQueued)
	}
	stream.conn.mu.Unlock()

	select {
	case <-stream.writeNotify:
	default:
		t.Fatal("stream writeNotify not signaled")
	}
	select {
	case <-other.writeNotify:
	default:
		t.Fatal("other writeNotify not signaled")
	}
}

func TestSuppressWriteBatchMarksInflightQueuedForAcceptedRequests(t *testing.T) {
	stream := newQueueBackpressureTestStream()
	req := writeRequest{
		frames:         testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: stream.id, Payload: []byte("held")}}),
		done:           make(chan error, 1),
		origin:         writeRequestOriginStream,
		queueReserved:  true,
		queuedBytes:    7,
		reservedStream: stream,
	}

	filtered := stream.conn.suppressWriteBatch([]writeRequest{req})

	if len(filtered) != 1 {
		t.Fatalf("filtered batch len = %d, want 1", len(filtered))
	}
	stream.conn.mu.Lock()
	defer stream.conn.mu.Unlock()
	if stream.inflightQueued != 7 {
		t.Fatalf("stream.inflightQueued = %d, want 7", stream.inflightQueued)
	}
}

func TestSuppressWriteBatchAggregatesInflightQueuedAcrossStreams(t *testing.T) {
	stream := newQueueBackpressureTestStream()
	other := testLocalSendStream(stream.conn, 8)

	batch := []writeRequest{
		{
			frames:         testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: stream.id, Payload: []byte("a")}}),
			done:           make(chan error, 1),
			origin:         writeRequestOriginStream,
			queueReserved:  true,
			queuedBytes:    3,
			reservedStream: stream,
		},
		{
			frames:         testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: stream.id, Payload: []byte("b")}}),
			done:           make(chan error, 1),
			origin:         writeRequestOriginStream,
			queueReserved:  true,
			queuedBytes:    5,
			reservedStream: stream,
		},
		{
			frames:         testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: other.id, Payload: []byte("c")}}),
			done:           make(chan error, 1),
			origin:         writeRequestOriginStream,
			queueReserved:  true,
			queuedBytes:    7,
			reservedStream: other,
		},
	}
	filtered := stream.conn.suppressWriteBatch(batch)

	if len(filtered) != len(batch) {
		t.Fatalf("filtered batch len = %d, want %d", len(filtered), len(batch))
	}
	stream.conn.mu.Lock()
	defer stream.conn.mu.Unlock()
	if stream.inflightQueued != 8 {
		t.Fatalf("stream.inflightQueued = %d, want 8", stream.inflightQueued)
	}
	if other.inflightQueued != 7 {
		t.Fatalf("other.inflightQueued = %d, want 7", other.inflightQueued)
	}
}

func TestSuppressWriteBatchBatchReleasesRejectedPreparedRequests(t *testing.T) {
	stream := newQueueBackpressureTestStream()
	other := testLocalSendStream(stream.conn, 8)
	stream.setSendStopSeen(&ApplicationError{Code: uint64(CodeCancelled)})
	other.setSendStopSeen(&ApplicationError{Code: uint64(CodeCancelled)})

	stream.sendSent = 3
	stream.queuedDataBytes = 4
	other.sendSent = 5
	other.queuedDataBytes = 6
	stream.conn.flow.sendSessionUsed = 8
	stream.conn.flow.queuedDataBytes = 10

	batch := []writeRequest{
		{
			frames:            testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: stream.id, Payload: []byte("a")}}),
			done:              make(chan error, 1),
			origin:            writeRequestOriginStream,
			queueReserved:     true,
			queuedBytes:       4,
			reservedStream:    stream,
			preparedSendBytes: 3,
		},
		{
			frames:            testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: other.id, Payload: []byte("b")}}),
			done:              make(chan error, 1),
			origin:            writeRequestOriginStream,
			queueReserved:     true,
			queuedBytes:       6,
			reservedStream:    other,
			preparedSendBytes: 5,
		},
	}
	doneChs := []<-chan error{batch[0].done, batch[1].done}

	filtered := stream.conn.suppressWriteBatch(batch)

	if len(filtered) != 0 {
		t.Fatalf("filtered batch len = %d, want 0", len(filtered))
	}
	for i := range doneChs {
		if err := <-doneChs[i]; !errors.Is(err, ErrWriteClosed) {
			t.Fatalf("batch[%d] done err = %v, want %v", i, err, ErrWriteClosed)
		}
	}

	stream.conn.mu.Lock()
	defer stream.conn.mu.Unlock()
	if stream.sendSent != 0 || other.sendSent != 0 {
		t.Fatalf("sendSent = (%d,%d), want (0,0)", stream.sendSent, other.sendSent)
	}
	if stream.conn.flow.sendSessionUsed != 0 {
		t.Fatalf("sendSessionUsed = %d, want 0", stream.conn.flow.sendSessionUsed)
	}
	if stream.conn.flow.queuedDataBytes != 0 {
		t.Fatalf("queuedDataBytes = %d, want 0", stream.conn.flow.queuedDataBytes)
	}
	if stream.queuedDataBytes != 0 || other.queuedDataBytes != 0 {
		t.Fatalf("stream queued bytes = (%d,%d), want (0,0)", stream.queuedDataBytes, other.queuedDataBytes)
	}
}

func TestSuppressWriteBatchIgnoresRejectedRequestWithoutDoneChan(t *testing.T) {
	stream := newQueueBackpressureTestStream()
	stream.setSendStopSeen(&ApplicationError{Code: uint64(CodeCancelled)})
	stream.sendSent = 3
	stream.queuedDataBytes = 4
	stream.conn.flow.sendSessionUsed = 3
	stream.conn.flow.queuedDataBytes = 4

	batch := []writeRequest{
		{
			frames:            testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: stream.id, Payload: []byte("a")}}),
			origin:            writeRequestOriginStream,
			queueReserved:     true,
			queuedBytes:       4,
			reservedStream:    stream,
			preparedSendBytes: 3,
		},
	}

	filtered := stream.conn.suppressWriteBatch(batch)

	if len(filtered) != 0 {
		t.Fatalf("filtered batch len = %d, want 0", len(filtered))
	}
	stream.conn.mu.Lock()
	defer stream.conn.mu.Unlock()
	if stream.sendSent != 0 {
		t.Fatalf("stream sendSent = %d, want 0", stream.sendSent)
	}
	if stream.conn.flow.sendSessionUsed != 0 {
		t.Fatalf("sendSessionUsed = %d, want 0", stream.conn.flow.sendSessionUsed)
	}
	if stream.conn.flow.queuedDataBytes != 0 || stream.queuedDataBytes != 0 {
		t.Fatalf("queued bytes = (%d,%d), want (0,0)", stream.conn.flow.queuedDataBytes, stream.queuedDataBytes)
	}
}

func TestWriteClosesSessionWhenTrackedMemoryCapWouldBeExceeded(t *testing.T) {
	stream := newQueueBackpressureTestStream()
	stream.conn.flow.recvSessionUsed = 8
	stream.conn.flow.sessionMemoryCap = 8

	_, err := stream.Write([]byte("x"))
	if !IsErrorCode(err, CodeInternal) {
		t.Fatalf("Write err = %v, want %s", err, CodeInternal)
	}
	if !IsErrorCode(stream.conn.err(), CodeInternal) {
		t.Fatalf("stored session err = %v, want %s", stream.conn.err(), CodeInternal)
	}
}

func TestPrepareWriteBurstBatchCapsSingleRequestAtPerStreamHWM(t *testing.T) {
	stream := newQueueBackpressureTestStream()
	payload := make([]byte, int(stream.conn.perStreamDataHWMLocked())*2)

	var parts [1][]byte
	parts[0] = payload
	prepared := stream.prepareWritePartsBurstBatch(parts[:], 0, 0, len(payload), writeChunkStreaming)

	if !prepared.handled {
		t.Fatal("prepareWriteBurstBatch handled = false, want true")
	}
	if prepared.err != nil {
		t.Fatalf("prepareWriteBurstBatch err = %v, want nil", prepared.err)
	}
	if prepared.progress == 0 || len(prepared.frames) == 0 {
		t.Fatalf("prepareWriteBurstBatch returned progress=%d frames=%d, want >0", prepared.progress, len(prepared.frames))
	}
	if got := saturatingAdd(prepared.start.priority.frameBytes, prepared.queuedBytes); got > stream.conn.perStreamDataHWMLocked() {
		t.Fatalf("queued bytes = %d, want <= %d", got, stream.conn.perStreamDataHWMLocked())
	}
}

func TestPrepareWriteFinalBurstBatchCapsSingleRequestAtPerStreamHWM(t *testing.T) {
	stream := newQueueBackpressureTestStream()
	payload := make([]byte, int(stream.conn.perStreamDataHWMLocked())*2)

	prepared := stream.prepareWritePartsBurstBatch([][]byte{payload}, 0, 0, len(payload), writeChunkFinal)

	if !prepared.handled {
		t.Fatal("prepareWriteFinalBurstBatch handled = false, want true")
	}
	if prepared.err != nil {
		t.Fatalf("prepareWriteFinalBurstBatch err = %v, want nil", prepared.err)
	}
	if prepared.finalState.finalized() {
		t.Fatal("prepareWriteFinalBurstBatch finalized = true, want false for oversized payload")
	}
	if prepared.progress == 0 || len(prepared.frames) == 0 {
		t.Fatalf("prepareWriteFinalBurstBatch returned progress=%d frames=%d, want >0", prepared.progress, len(prepared.frames))
	}
	if got := saturatingAdd(prepared.start.priority.frameBytes, prepared.queuedBytes); got > stream.conn.perStreamDataHWMLocked() {
		t.Fatalf("queued bytes = %d, want <= %d", got, stream.conn.perStreamDataHWMLocked())
	}
}

func TestPrepareWriteBurstBatchUsesSmallerSessionQueueCap(t *testing.T) {
	stream := newQueueBackpressureTestStream()
	stream.conn.flow.sessionDataHWM = 64 * 1024
	payload := make([]byte, int(stream.conn.perStreamDataHWMLocked())*2)

	var parts [1][]byte
	parts[0] = payload
	prepared := stream.prepareWritePartsBurstBatch(parts[:], 0, 0, len(payload), writeChunkStreaming)

	if !prepared.handled {
		t.Fatal("prepareWriteBurstBatch handled = false, want true")
	}
	if prepared.err != nil {
		t.Fatalf("prepareWriteBurstBatch err = %v, want nil", prepared.err)
	}
	if prepared.progress == 0 || len(prepared.frames) == 0 {
		t.Fatalf("prepareWriteBurstBatch returned progress=%d frames=%d, want >0", prepared.progress, len(prepared.frames))
	}
	if got := saturatingAdd(prepared.start.priority.frameBytes, prepared.queuedBytes); got > stream.conn.sessionDataHWMLocked() {
		t.Fatalf("queued bytes = %d, want <= session cap %d", got, stream.conn.sessionDataHWMLocked())
	}
}

func TestReserveWriteQueueTracksOnlyQueuedStreams(t *testing.T) {
	stream := newQueueBackpressureTestStream()
	req := writeRequest{
		frames: testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: stream.id, Payload: []byte("held")}}),
		done:   make(chan error, 1),
		origin: writeRequestOriginStream,
	}

	stream.conn.mu.Lock()
	reservation := stream.conn.reserveWriteQueueLocked(stream, &req, writeLaneOrdinary)
	stream.conn.mu.Unlock()

	if reservation.blocked() {
		t.Fatal("reserveWriteQueueLocked unexpectedly blocked")
	}
	if reservation.memoryErr != nil {
		t.Fatalf("reserveWriteQueueLocked err = %v", reservation.memoryErr)
	}
	if req.reservedStream != stream || !req.queueReserved {
		t.Fatalf("request reservation = (%v,%v), want reserved for stream", req.queueReserved, req.reservedStream)
	}
	if got := len(stream.conn.flow.queuedDataStreams); got != 1 {
		t.Fatalf("len(queuedDataStreams) = %d, want 1", got)
	}
	if _, ok := stream.conn.flow.queuedDataStreams[stream]; !ok {
		t.Fatal("tracked queued-data stream missing reserved stream")
	}
}

func TestReleaseWriteQueueReservationUntracksDrainedStream(t *testing.T) {
	stream := newQueueBackpressureTestStream()
	req := writeRequest{
		frames: testTxFramesFrom([]Frame{{Type: FrameTypeDATA, StreamID: stream.id, Payload: []byte("held")}}),
		done:   make(chan error, 1),
		origin: writeRequestOriginStream,
	}

	stream.conn.mu.Lock()
	reservation := stream.conn.reserveWriteQueueLocked(stream, &req, writeLaneOrdinary)
	stream.conn.mu.Unlock()
	if reservation.memoryErr != nil {
		t.Fatalf("reserveWriteQueueLocked err = %v", reservation.memoryErr)
	}

	stream.conn.releaseWriteQueueReservation(&req)

	stream.conn.mu.Lock()
	defer stream.conn.mu.Unlock()
	if stream.queuedDataBytes != 0 {
		t.Fatalf("stream.queuedDataBytes = %d, want 0", stream.queuedDataBytes)
	}
	if stream.conn.flow.queuedDataStreams != nil {
		if _, ok := stream.conn.flow.queuedDataStreams[stream]; ok {
			t.Fatal("drained stream still tracked in queuedDataStreams")
		}
	}
}

func TestClearWriteQueueReservationsLockedUsesTrackedSet(t *testing.T) {
	stream := newQueueBackpressureTestStream()
	other := testLocalSendStream(stream.conn, 8)

	stream.conn.mu.Lock()
	stream.queuedDataBytes = 11
	stream.inflightQueued = 5
	other.queuedDataBytes = 0
	other.inflightQueued = 7
	stream.conn.flow.queuedDataStreams = map[*nativeStream]struct{}{
		stream: {},
	}
	stream.conn.clearWriteQueueReservationsLocked()
	stream.conn.mu.Unlock()

	if stream.queuedDataBytes != 0 {
		t.Fatalf("tracked stream queuedDataBytes = %d, want 0", stream.queuedDataBytes)
	}
	if stream.inflightQueued != 0 {
		t.Fatalf("tracked stream inflightQueued = %d, want 0", stream.inflightQueued)
	}
	if other.queuedDataBytes != 0 {
		t.Fatalf("untracked stream queuedDataBytes = %d, want 0", other.queuedDataBytes)
	}
	if other.inflightQueued != 0 {
		t.Fatalf("untracked stream inflightQueued = %d, want 0", other.inflightQueued)
	}
	if stream.conn.flow.queuedDataStreams != nil {
		t.Fatalf("queuedDataStreams = %#v, want nil", stream.conn.flow.queuedDataStreams)
	}
}

func TestClearWriteQueueReservationsLockedRebuildsTrackedSetFromSeededStreams(t *testing.T) {
	stream := newQueueBackpressureTestStream()
	provisional := testBuildStream(
		stream.conn,
		0,
		testWithBidi(),
		testWithLocalOpen(testLocalOpenOpenedState()),
		testWithLocalSend(),
		testWithLocalReceive(),
		testWithNotifications(),
	)
	provisional.provisionalIndex = 0

	stream.conn.mu.Lock()
	stream.queuedDataBytes = 11
	stream.inflightQueued = 4
	provisional.queuedDataBytes = 7
	provisional.inflightQueued = 6
	stream.conn.flow.queuedDataBytes = 18
	stream.conn.queues.provisionalBidi.items = []*nativeStream{provisional}
	stream.conn.clearWriteQueueReservationsLocked()
	stream.conn.mu.Unlock()

	if stream.queuedDataBytes != 0 {
		t.Fatalf("live stream queuedDataBytes = %d, want 0", stream.queuedDataBytes)
	}
	if stream.inflightQueued != 0 {
		t.Fatalf("live stream inflightQueued = %d, want 0", stream.inflightQueued)
	}
	if provisional.queuedDataBytes != 0 {
		t.Fatalf("provisional queuedDataBytes = %d, want 0", provisional.queuedDataBytes)
	}
	if provisional.inflightQueued != 0 {
		t.Fatalf("provisional inflightQueued = %d, want 0", provisional.inflightQueued)
	}
	if stream.conn.flow.queuedDataStreams != nil {
		t.Fatalf("queuedDataStreams = %#v, want nil after clear", stream.conn.flow.queuedDataStreams)
	}
}

func TestCloseStreamOnSessionReleasesQueuedBytesAndWakesBlockedWriters(t *testing.T) {
	stream := newQueueBackpressureTestStream()
	holder := testVisibleBidiStream(stream.conn, 8, testWithSendMax(1<<20))

	sessionHWM := stream.conn.sessionDataHWMLocked()
	sessionLWM := stream.conn.sessionDataLWMLocked()
	reserved := sessionHWM - sessionLWM

	stream.conn.mu.Lock()
	holder.queuedDataBytes = reserved
	stream.conn.flow.queuedDataBytes = sessionHWM
	stream.conn.trackQueuedDataStreamLocked(holder)
	stream.conn.mu.Unlock()

	done := make(chan error, 1)
	go func() {
		_, err := stream.Write([]byte("x"))
		done <- err
	}()
	awaitStreamWriteWaiter(t, stream, testSignalTimeout, "blocked write did not enter wait state")

	go func() {
		queued := <-stream.conn.writer.writeCh
		queued.done <- nil
	}()

	stream.conn.mu.Lock()
	stream.conn.closeStreamOnSessionWithOptionsLocked(holder, refusedStreamAppErr(), sessionCloseOptions{
		abortSource: terminalAbortLocal,
		finalize:    true,
	})
	gotSessionQueued := stream.conn.flow.queuedDataBytes
	gotHolderQueued := holder.queuedDataBytes
	_, tracked := stream.conn.flow.queuedDataStreams[holder]
	stream.conn.mu.Unlock()

	if gotSessionQueued != sessionLWM {
		t.Fatalf("conn queuedDataBytes = %d, want %d after session-close release", gotSessionQueued, sessionLWM)
	}
	if gotHolderQueued != 0 {
		t.Fatalf("holder queuedDataBytes = %d, want 0 after session-close release", gotHolderQueued)
	}
	if tracked {
		t.Fatal("closed stream still tracked in queuedDataStreams")
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Write err = %v, want nil", err)
		}
	case <-time.After(2 * testSignalTimeout):
		t.Fatal("blocked write did not wake after session-close queue release")
	}
}

func TestCloseStreamOnSessionReleasesSessionSendCreditAndWakesBlockedWriters(t *testing.T) {
	stream := newQueueBackpressureTestStream()
	holder := testVisibleBidiStream(stream.conn, 8, testWithSendMax(1))
	holder.sendSent = 1

	stream.conn.mu.Lock()
	stream.conn.flow.sendSessionMax = 1
	stream.conn.flow.sendSessionUsed = 1
	stream.conn.mu.Unlock()

	done := make(chan error, 1)
	go func() {
		_, err := stream.Write([]byte("x"))
		done <- err
	}()
	awaitStreamWriteWaiter(t, stream, testSignalTimeout, "blocked write did not enter session-credit wait state")

	go func() {
		queued := <-stream.conn.writer.writeCh
		queued.done <- nil
	}()

	stream.conn.mu.Lock()
	stream.conn.closeStreamOnSessionWithOptionsLocked(holder, refusedStreamAppErr(), sessionCloseOptions{
		abortSource: terminalAbortLocal,
		finalize:    true,
	})
	gotSessionUsed := stream.conn.flow.sendSessionUsed
	gotHolderSent := holder.sendSent
	stream.conn.mu.Unlock()

	if gotSessionUsed != 0 {
		t.Fatalf("conn sendSessionUsed = %d, want 0 after session-close send-credit release", gotSessionUsed)
	}
	if gotHolderSent != 0 {
		t.Fatalf("holder sendSent = %d, want 0 after session-close send-credit release", gotHolderSent)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Write err = %v, want nil", err)
		}
	case <-time.After(2 * testSignalTimeout):
		t.Fatal("blocked write did not wake after session-close send-credit release")
	}
}

func TestCloseStreamOnSessionClearsPendingSessionBlockedAfterSendCreditRelease(t *testing.T) {
	stream := newQueueBackpressureTestStream()
	holder := testVisibleBidiStream(stream.conn, 8, testWithSendMax(1))
	holder.sendSent = 1

	stream.conn.mu.Lock()
	stream.conn.flow.sendSessionMax = 1
	stream.conn.flow.sendSessionUsed = 1
	stream.conn.mu.Unlock()

	done := make(chan error, 1)
	go func() {
		_, err := stream.Write([]byte("x"))
		done <- err
	}()
	awaitStreamWriteWaiter(t, stream, testSignalTimeout, "blocked write did not enter session-credit wait state")

	stream.conn.mu.Lock()
	if !stream.conn.pending.hasSessionBlocked {
		stream.conn.mu.Unlock()
		t.Fatal("expected blocked write to queue pending session BLOCKED")
	}
	stream.conn.mu.Unlock()

	go func() {
		queued := <-stream.conn.writer.writeCh
		queued.done <- nil
	}()

	stream.conn.mu.Lock()
	stream.conn.closeStreamOnSessionWithOptionsLocked(holder, refusedStreamAppErr(), sessionCloseOptions{
		abortSource: terminalAbortLocal,
		finalize:    true,
	})
	staleBlocked := stream.conn.pending.hasSessionBlocked
	stream.conn.mu.Unlock()

	if staleBlocked {
		t.Fatal("pending session BLOCKED remained queued after session send credit was released")
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Write err = %v, want nil", err)
		}
	case <-time.After(2 * testSignalTimeout):
		t.Fatal("blocked write did not wake after session-close send-credit release")
	}
}

func TestReleasePreparedWriteRequestClearsPendingSessionBlockedAfterCreditReturns(t *testing.T) {
	stream := newQueueBackpressureTestStream()
	holder := testVisibleBidiStream(stream.conn, 8, testWithSendMax(1))
	holder.sendSent = 1

	stream.conn.mu.Lock()
	stream.conn.flow.sendSessionMax = 1
	stream.conn.flow.sendSessionUsed = 1
	stream.conn.mu.Unlock()

	done := make(chan error, 1)
	go func() {
		_, err := stream.Write([]byte("x"))
		done <- err
	}()
	awaitStreamWriteWaiter(t, stream, testSignalTimeout, "blocked write did not enter session-credit wait state")

	stream.conn.mu.Lock()
	if !stream.conn.pending.hasSessionBlocked {
		stream.conn.mu.Unlock()
		t.Fatal("expected pending session BLOCKED before prepared-send rollback")
	}
	stream.conn.mu.Unlock()

	go func() {
		queued := <-stream.conn.writer.writeCh
		queued.done <- nil
	}()

	req := &writeRequest{
		requestStreamIDKnown: true,
		requestStreamID:      holder.id,
		preparedSendBytes:    1,
	}
	stream.conn.releasePreparedWriteRequest(req)

	stream.conn.mu.Lock()
	staleBlocked := stream.conn.pending.hasSessionBlocked
	stream.conn.mu.Unlock()

	if staleBlocked {
		t.Fatal("pending session BLOCKED remained queued after prepared-send credit rollback")
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Write err = %v, want nil", err)
		}
	case <-time.After(2 * testSignalTimeout):
		t.Fatal("blocked write did not wake after prepared-send session credit rollback")
	}
}

func TestReleasePreparedWriteRequestClearsPendingStreamBlockedAfterCreditReturns(t *testing.T) {
	stream := newQueueBackpressureTestStream()

	stream.conn.mu.Lock()
	stream.sendMax = 1
	stream.sendSent = 1
	stream.conn.mu.Unlock()

	done := make(chan error, 1)
	go func() {
		_, err := stream.Write([]byte("x"))
		done <- err
	}()
	awaitStreamWriteWaiter(t, stream, testSignalTimeout, "blocked write did not enter stream-credit wait state")

	stream.conn.mu.Lock()
	if !testHasPendingStreamBlocked(stream.conn, stream.id) {
		stream.conn.mu.Unlock()
		t.Fatal("expected pending stream BLOCKED before prepared-send rollback")
	}
	stream.conn.mu.Unlock()

	go func() {
		queued := <-stream.conn.writer.writeCh
		queued.done <- nil
	}()

	req := &writeRequest{
		requestStreamIDKnown: true,
		requestStreamID:      stream.id,
		preparedSendBytes:    1,
	}
	stream.conn.releasePreparedWriteRequest(req)

	stream.conn.mu.Lock()
	staleBlocked := testHasPendingStreamBlocked(stream.conn, stream.id)
	stream.conn.mu.Unlock()

	if staleBlocked {
		t.Fatal("pending stream BLOCKED remained queued after prepared-send stream credit rollback")
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Write err = %v, want nil", err)
		}
	case <-time.After(2 * testSignalTimeout):
		t.Fatal("blocked write did not wake after prepared-send stream credit rollback")
	}
}

func TestWriteCarriesPendingPriorityUpdateBeforeData(t *testing.T) {
	caps := CapabilityPriorityUpdate | CapabilityPriorityHints
	payload, err := buildPriorityUpdatePayload(
		caps,
		MetadataUpdate{Priority: invalidUint64Ptr(7)},
		4096,
	)
	if err != nil {
		t.Fatalf("build priority update payload: %v", err)
	}

	c := &Conn{

		writer:    connWriterRuntimeState{writeCh: make(chan writeRequest, 1), urgentWriteCh: make(chan writeRequest, 1)},
		lifecycle: connLifecycleState{closedCh: make(chan struct{})}, config: connConfigState{negotiated: Negotiated{Capabilities: caps},
			peer: Preface{
				Settings: Settings{
					MaxFramePayload:          16384,
					MaxExtensionPayloadBytes: 4096,
				},
			}}, flow: connFlowState{sendSessionMax: 16384},
	}
	s := testVisibleBidiStream(c, 9, testWithSendMax(16384))
	testSetPendingPriorityUpdate(c, s.id, append([]byte(nil), payload...))

	gotCh := make(chan writeRequest, 1)
	go func() {
		req := <-c.writer.writeCh
		gotCh <- req
		req.done <- nil
	}()

	if _, err := s.Write([]byte("hi")); err != nil {
		t.Fatalf("write: %v", err)
	}

	req := <-gotCh
	if len(req.frames) != 2 {
		t.Fatalf("frames len = %d, want 2", len(req.frames))
	}
	if req.frames[0].Type != FrameTypeEXT {
		t.Fatalf("first frame type = %s, want EXT", req.frames[0].Type)
	}
	if req.frames[0].StreamID != s.id {
		t.Fatalf("first frame stream = %d, want %d", req.frames[0].StreamID, s.id)
	}
	if req.frames[1].Type != FrameTypeDATA {
		t.Fatalf("second frame type = %s, want DATA", req.frames[1].Type)
	}
	if testHasPendingPriorityUpdate(c, s.id) {
		t.Fatal("pending priority update not cleared")
	}
}

func TestWriteCarriesPendingPriorityUpdateAfterOpeningFrame(t *testing.T) {
	caps := CapabilityPriorityUpdate | CapabilityPriorityHints
	payload, err := buildPriorityUpdatePayload(
		caps,
		MetadataUpdate{Priority: invalidUint64Ptr(7)},
		4096,
	)
	if err != nil {
		t.Fatalf("build priority update payload: %v", err)
	}

	c, frames, stop := newHandlerTestConn(t)
	defer stop()
	c.config.negotiated.Capabilities = caps

	streamID := state.FirstLocalStreamID(c.config.negotiated.LocalRole, true)
	stream := testBuildStream(
		c,
		streamID,
		testWithBidi(),
		testWithLocalOpen(testLocalOpenOpenedState()),
		testWithLocalSend(),
		testWithLocalReceive(),
		testWithNotifications(),
		testWithSendMax(c.flow.sendSessionMax),
	)
	c.mu.Lock()
	testSetPendingPriorityUpdate(c, streamID, append([]byte(nil), payload...))
	c.mu.Unlock()

	if _, err := stream.Write([]byte("x")); err != nil {
		t.Fatalf("write first byte: %v", err)
	}
	first := awaitQueuedFrame(t, frames)
	if first.Type != FrameTypeDATA || first.StreamID != streamID {
		t.Fatalf("first write first frame = %+v, want DATA for stream %d", first, streamID)
	}
	c.mu.Lock()
	if !stream.localOpen.committed {
		c.mu.Unlock()
		t.Fatalf("stream sendCommitted = %v, want true", stream.localOpen.committed)
	}
	if !testHasPendingPriorityUpdate(c, streamID) {
		c.mu.Unlock()
		t.Fatalf("pending priority update removed before open frame")
	}
	c.mu.Unlock()

	if _, err := stream.Write([]byte("y")); err != nil {
		t.Fatalf("write second byte: %v", err)
	}
	second := awaitQueuedFrame(t, frames)
	if second.Type != FrameTypeEXT || second.StreamID != streamID {
		t.Fatalf("second write first frame = %+v, want EXT on stream %d", second, streamID)
	}
	third := awaitQueuedFrame(t, frames)
	if third.Type != FrameTypeDATA || third.StreamID != streamID {
		t.Fatalf("second write second frame = %+v, want DATA on stream %d", third, streamID)
	}
	c.mu.Lock()
	if testHasPendingPriorityUpdate(c, streamID) {
		c.mu.Unlock()
		t.Fatalf("pending priority update should be cleared after send")
	}
	c.mu.Unlock()
}

func TestStreamPriorityAffectsLocalBurstAndFragmentPolicy(t *testing.T) {
	serverCfg := DefaultConfig()
	serverCfg.Settings.MaxFramePayload = 16384

	client, _ := newConnPairWithConfig(t, nil, serverCfg)
	ctx, cancel := testContext(t)
	defer cancel()

	mild := uint64(2)
	strong := uint64(6)
	saturated := uint64(20)

	mildStreamRaw, err := client.OpenStreamWithOptions(ctx, OpenOptions{InitialPriority: &mild})
	if err != nil {
		t.Fatalf("open mild stream: %v", err)
	}
	mildStream := requireNativeStreamImpl(t, mildStreamRaw)
	strongStreamRaw, err := client.OpenStreamWithOptions(ctx, OpenOptions{InitialPriority: &strong})
	if err != nil {
		t.Fatalf("open strong stream: %v", err)
	}
	strongStream := requireNativeStreamImpl(t, strongStreamRaw)
	saturatedStreamRaw, err := client.OpenStreamWithOptions(ctx, OpenOptions{InitialPriority: &saturated})
	if err != nil {
		t.Fatalf("open saturated stream: %v", err)
	}
	saturatedStream := requireNativeStreamImpl(t, saturatedStreamRaw)

	client.mu.Lock()
	defer client.mu.Unlock()

	if got := mildStream.writeBurstLimitLocked(); got != mildWriteBurstFrames {
		t.Fatalf("mild burst limit = %d, want %d", got, mildWriteBurstFrames)
	}
	if got := strongStream.writeBurstLimitLocked(); got != strongWriteBurstFrames {
		t.Fatalf("strong burst limit = %d, want %d", got, strongWriteBurstFrames)
	}
	if got := saturatedStream.writeBurstLimitLocked(); got != saturatedWriteBurstFrames {
		t.Fatalf("saturated burst limit = %d, want %d", got, saturatedWriteBurstFrames)
	}

	if got := mildStream.txFragmentCapLocked(0); got != 12288 {
		t.Fatalf("mild fragment cap = %d, want 12288", got)
	}
	if got := strongStream.txFragmentCapLocked(0); got != 8192 {
		t.Fatalf("strong fragment cap = %d, want 8192", got)
	}
	if got := saturatedStream.txFragmentCapLocked(0); got != 4096 {
		t.Fatalf("saturated fragment cap = %d, want 4096", got)
	}
}

func TestSchedulerLatencyHintBiasesDefaultBurstAndFragmentPolicy(t *testing.T) {
	serverCfg := DefaultConfig()
	serverCfg.Settings.MaxFramePayload = 16384
	serverCfg.Settings.SchedulerHints = SchedulerLatency

	client, _ := newConnPairWithConfig(t, nil, serverCfg)
	ctx, cancel := testContext(t)
	defer cancel()

	streamRaw, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	stream := requireNativeStreamImpl(t, streamRaw)

	client.mu.Lock()
	defer client.mu.Unlock()

	if got := stream.writeBurstLimitLocked(); got != mildWriteBurstFrames {
		t.Fatalf("latency-hint burst limit = %d, want %d", got, mildWriteBurstFrames)
	}
	if got := stream.txFragmentCapLocked(0); got != 8192 {
		t.Fatalf("latency-hint fragment cap = %d, want 8192", got)
	}
}

func TestPriorityPolicyRespectsOpeningMetadataPrefix(t *testing.T) {
	serverCfg := DefaultConfig()
	serverCfg.Settings.MaxFramePayload = 16384

	clientCfg := DefaultConfig()
	clientCfg.Capabilities |= CapabilityOpenMetadata | CapabilityPriorityHints
	serverCfg.Capabilities |= CapabilityOpenMetadata | CapabilityPriorityHints

	client, _ := newConnPairWithConfig(t, clientCfg, serverCfg)
	ctx, cancel := testContext(t)
	defer cancel()

	priority := uint64(20)
	streamRaw, err := client.OpenStreamWithOptions(ctx, OpenOptions{
		InitialPriority: &priority,
		OpenInfo:        []byte("ssh"),
	})
	if err != nil {
		t.Fatalf("open stream with metadata: %v", err)
	}
	stream := requireNativeStreamImpl(t, streamRaw)

	client.mu.Lock()
	defer client.mu.Unlock()

	prefixLen := uint64(len(stream.openMetadataPrefix))
	if prefixLen == 0 {
		t.Fatal("expected non-empty opening metadata prefix")
	}
	got := stream.txFragmentCapLocked(prefixLen)
	want := scaledFragmentCap(serverCfg.Settings.MaxFramePayload-prefixLen, 1, 4)
	if got != want {
		t.Fatalf("fragment cap with prefix = %d, want %d", got, want)
	}
}

func TestFragmentCapUsesSlowSendRateEstimate(t *testing.T) {
	c := &Conn{

		metrics: connRuntimeMetricsState{
			sendRateEstimate: 1000,
		}, config: connConfigState{peer: Preface{
			Settings: Settings{
				MaxFramePayload: 16384,
			},
		}},
	}
	stream := testBuildDetachedStream(c, 0)

	if got := stream.txFragmentCapLocked(0); got != 200 {
		t.Fatalf("fragment cap with slow send-rate estimate = %d, want 200", got)
	}
}

func TestFragmentCapKeepsStaticLimitWhenRateEstimateIsHigher(t *testing.T) {
	c := &Conn{

		metrics: connRuntimeMetricsState{
			sendRateEstimate: 1 << 20,
		}, config: connConfigState{peer: Preface{
			Settings: Settings{
				MaxFramePayload: 16384,
			},
		}},
	}
	stream := testBuildDetachedStream(c, 0)
	stream.priority = 20

	if got := stream.txFragmentCapLocked(0); got != 4096 {
		t.Fatalf("fragment cap with fast send-rate estimate = %d, want 4096", got)
	}
}
