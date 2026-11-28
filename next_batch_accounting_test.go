package zmux

import (
	"context"
	"errors"
	"math"
	"net"
	"testing"
	"testing/synctest"
)

func TestNextBatchHAccountingSegments(t *testing.T) {
	read := CarrierHalfSource{Route: "receive"}
	send := CarrierHalfSource{Route: "send"}
	policy := AccountingPolicy{Enabled: true, SourceLimit: 2}
	a := newPipeAccounting(2, policy)
	first, second := a.startSegment(), a.startSegment()
	r, w := first.startHalf(read, false, 0), second.startHalf(send, true, 0)
	r.add(RecordTotals{Bytes: 7, Records: 1}, false)
	w.add(RecordTotals{Bytes: 15, Records: 2}, false)
	live := a.Snapshot()
	if live.Send[0].Active.Bytes != 15 || live.Send[0].Retired.Bytes != 0 || live.Receive[0].Active.Bytes != 7 {
		t.Fatalf("active totals %+v", live)
	}
	w.add(RecordTotals{PartialBytes: 3}, true)
	second.finish()
	mixed := a.Snapshot()
	wantSend := HalfTotals{Source: send, Retired: RecordTotals{Bytes: 15, Records: 2, PartialBytes: 3}}
	if mixed.Send[0] != wantSend || mixed.Receive[0].Active.Bytes != 7 {
		t.Fatalf("retired lane totals %+v", mixed)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := a.Wait(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("ticket completed before last segment: %v", err)
	}
	first.finish()
	got, err := a.Wait(t.Context())
	if err != nil || len(got.Receive) != 1 || len(got.Send) != 1 {
		t.Fatalf("accounting %+v, %v", got, err)
	}
	wantReceive := HalfTotals{Source: read, Retired: RecordTotals{Bytes: 7, Records: 1}}
	if got.Receive[0] != wantReceive || got.Send[0] != wantSend {
		t.Fatalf("source attribution %+v", got)
	}
	got.Receive[0] = HalfTotals{}
	again, _ := a.Wait(t.Context())
	if again.Receive[0] != wantReceive {
		t.Fatal("waiter changed the final result")
	}
	policy.SourceLimit, policy.Strict = 1, true
	strict := newPipeAccounting(2, policy)
	if err := strict.checkSources(read, send, true, true); !errors.Is(err, ErrAccountingSources) {
		t.Fatalf("strict ledger with two sources: %v", err)
	}
	if err := strict.checkSources(read, CarrierHalfSource{}, true, true); !errors.Is(err, ErrAccountingSourceMissing) {
		t.Fatalf("strict ledger with a missing split source: %v", err)
	}
}

func TestAccountingFoldsSameSourceLaneOnce(t *testing.T) {
	source := CarrierHalfSource{Route: "shared"}
	a := newPipeAccounting(2, AccountingPolicy{Enabled: true, SourceLimit: 1})
	segment := a.startSegment()
	first, second := segment.startHalf(source, true, 0), segment.startHalf(source, true, 1)
	first.add(RecordTotals{Bytes: 15, Records: 2}, false)
	second.add(RecordTotals{Bytes: 7, Records: 1}, false)
	first.retire()
	// The first lane's carrier has returned the final write prefix after the
	// protocol retired it, but before its physical I/O worker is released.
	first.add(RecordTotals{PartialBytes: 3}, true)
	overflow := segment.startHalf(CarrierHalfSource{Route: "overflow"}, false, 0)
	overflow.add(RecordTotals{Bytes: 5, Records: 1, Imprecise: true}, true)
	want := HalfTotals{Source: source, Active: RecordTotals{Bytes: 7, Records: 1},
		Retired: RecordTotals{Bytes: 15, Records: 2, PartialBytes: 3}}
	got := a.Snapshot()
	if len(got.Send) != 1 || got.Send[0] != want || len(got.Receive) != 1 ||
		got.Receive[0] != (HalfTotals{SourceOverflow: true, Retired: RecordTotals{Bytes: 5, Records: 1, Imprecise: true}}) {
		t.Fatalf("split accounting: %+v", got)
	}
	if len(got.ActiveLanes) != 1 || got.ActiveLanes[0] != (LaneAccounting{Source: source, Totals: want.Active, Lane: 1, Send: true}) {
		t.Fatalf("active lane projection: %+v", got.ActiveLanes)
	}
	segment.finish()
	final, err := a.Wait(t.Context())
	if err != nil || final.Send[0].Active != (RecordTotals{}) ||
		final.Send[0].Retired != (RecordTotals{Bytes: 22, Records: 3, PartialBytes: 3}) {
		t.Fatalf("final accounting: %+v, %v", final, err)
	}
}

func TestAccountingSaturatesOnRetirement(t *testing.T) {
	a := newPipeAccounting(2, AccountingPolicy{Enabled: true})
	seg := a.startSegment()
	src := CarrierHalfSource{Route: "shared"}
	first, second := seg.startHalf(src, true, 0), seg.startHalf(src, true, 1)
	saturated := RecordTotals{Bytes: math.MaxUint64, Records: math.MaxUint64, PartialBytes: math.MaxUint64,
		ChannelBytes: math.MaxUint64, ChannelDatagrams: math.MaxUint64}
	first.add(saturated, true)
	second.add(RecordTotals{Bytes: 1, Records: 1, PartialBytes: 1, ChannelBytes: 1, ChannelDatagrams: 1}, true)
	seg.finish()
	got, err := a.Wait(t.Context())
	if err != nil || len(got.Send) != 1 || len(got.ActiveLanes) != 0 || got.Send[0].Retired != saturated {
		t.Fatalf("saturated totals: %+v, %v", got, err)
	}
}

func TestNextBatchHBootstrapAccounting(t *testing.T) {
	a, b := net.Pipe()
	cfg := &Config{Accounting: AccountingPolicy{Enabled: true}}
	client, server := pair(t, a, b, cfg, cfg)
	first, _ := client.Pipe(0)
	last, _ := server.Pipe(0)
	x, err := first.Accounting()
	if err != nil {
		t.Fatal(err)
	}
	y, err := last.Accounting()
	if err != nil {
		t.Fatal(err)
	}
	echoOnce(t, t.Context(), client, server)
	client.Abort(0, "")
	server.Abort(0, "")
	cs, err := x.Wait(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	ss, err := y.Wait(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(cs.Send) != 1 || len(ss.Receive) != 1 || cs.Send[0].Retired.Records == 0 || ss.Receive[0].Retired.Records == 0 {
		t.Fatalf("bootstrap totals %+v / %+v", cs, ss)
	}
	after, err := first.Accounting()
	if err != nil || after != x || after.Snapshot().Send[0] != cs.Send[0] {
		t.Fatalf("retired attachment lost its accounting ticket: %v", err)
	}
}

func TestAccountingTicketStartsAtCarrierHandoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := mpConfig()
		cfg.Accounting.Enabled = true
		server := mpConfig()
		entered := make(chan struct{})
		release := make(chan struct{})
		defer close(release)
		server.SessionPolicy.AuthorizeJoin = func(ctx context.Context, _ JoinAuthorization) (JoinVerdict, error) {
			close(entered)
			select {
			case <-release:
				return JoinPermit, nil
			case <-ctx.Done():
				return JoinDeny, ctx.Err()
			}
		}
		e := newMemMPEnv(t, cfg, server)
		tx := testChange(t, e.client, []MemberSpec{testMemberSpec("pending", Send)})
		attempt, err := tx.StartMember(0, testMemberOpener(e))
		if err != nil {
			t.Fatal(err)
		}
		<-entered
		half := attempt.Snapshot().Attachments[0]
		if !half.ReplyAt.IsZero() || half.Ticket != nil {
			t.Fatalf("pending Join before I/O handoff: %+v", half)
		}
		p, ok := e.client.Pipe(half.Pipe)
		if !ok {
			t.Fatal("pending pipe is missing")
		}
		if _, err := p.Accounting(); !errors.Is(err, ErrNotNegotiated) {
			t.Fatalf("pending pipe accounting: %v", err)
		}
		if err := attempt.Cancel(); err != nil {
			t.Fatal(err)
		}
		result, err := attempt.Wait(t.Context())
		if err != nil || len(result.Tickets) != 0 {
			t.Fatalf("cancelled before handoff: %+v, %v", result, err)
		}
	})
}
