package zmux

import (
	"errors"
	"testing"

	"github.com/zmuxio/zmux-go/internal/core"
	"github.com/zmuxio/zmux-go/internal/setting"
)

// SetLimits validates, fills defaults, reaches the slot table and trims
// the suspension list at once.
func TestListenerSetLimits(t *testing.T) {
	l := mustNewListener(&ListenerConfig{MaxSuspended: 3})
	defer func() { _ = l.Close() }()
	if got := l.Limits(); got.MaxSuspended != 3 || got.FirstRecordTimeout != DefaultFirstRecordTimeout {
		t.Fatalf("initial %+v", got)
	}
	if err := l.SetLimits(ListenerLimits{MaxPendingPerSource: -1}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("invalid: %v", err)
	}
	if l.Limits().MaxSuspended != 3 {
		t.Fatal("invalid limits changed the listener")
	}
	a, b, c := &Conn{}, &Conn{}, &Conn{}
	for _, x := range []*Conn{a, b, c} {
		l.sessionSuspended(x)
	}
	r, err := ListenerLimits{MaxPending: 7, MaxSuspended: 1, FirstRecordTimeout: -1}.resolve()
	if err != nil {
		t.Fatal(err)
	}
	// SetLimits closes these; the stand-ins have no session to close.
	if over, err := l.applyLimits(&r); err != nil || len(over) != 2 || over[0].c != a || over[1].c != b {
		t.Fatalf("over %v", over)
	}
	l.suspMu.Lock()
	n, head := l.suspN, l.suspHead
	l.suspMu.Unlock()
	if n != 1 || head != c {
		t.Fatalf("suspended %d, head %p", n, head)
	}
	l.pmu.Lock()
	total, per := l.pending.MaxTotal, l.pending.MaxPerSource
	l.pmu.Unlock()
	if total != 7 || per != core.DefaultMaxPendingPerSource || l.Limits().FirstRecordTimeout != -1 || l.lim.Load().firstRecord() != setting.NoTimeout {
		t.Fatalf("pending %d/%d, limits %+v", total, per, l.Limits())
	}
	if l.budget.Limit() != 1<<30 {
		t.Fatalf("budget %d", l.budget.Limit())
	}
}

// A lowered MemoryBudget starts pressure at once; unlimited ends it.
func TestListenerSetMemoryBudget(t *testing.T) {
	l := mustNewListener(&ListenerConfig{})
	defer func() { _ = l.Close() }()
	if !l.budget.TryCharge(4096) {
		t.Fatal("test budget rejected 4096 bytes")
	}
	original := l.Limits()
	lim := l.Limits()
	lim.MemoryBudget = 1024
	if err := l.SetLimits(lim); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("limit below commitment: %v", err)
	}
	if l.budget.Pressure() || l.Limits() != original {
		t.Fatal("rejected limits changed the listener")
	}
	lim.MemoryBudget = -1
	if err := l.SetLimits(lim); err != nil {
		t.Fatal(err)
	}
	if l.budget.Pressure() || l.Limits().MemoryBudget != -1 {
		t.Fatalf("pressure %v, limits %+v", l.budget.Pressure(), l.Limits())
	}
}

func TestListenerPendingPerSourceMinimum(t *testing.T) {
	if l, err := NewListener(&ListenerConfig{MaxPendingPerSource: 1}); !errors.Is(err, ErrInvalidConfig) {
		if l != nil {
			_ = l.Close()
		}
		t.Fatalf("source limit of one: %v", err)
	}
	l := mustNewListener(&ListenerConfig{MaxPendingPerSource: 2})
	defer func() { _ = l.Close() }()
	lim := l.Limits()
	if lim.MaxPendingPerSource != 2 {
		t.Fatalf("minimum limit changed: %+v", lim)
	}
	lim.MaxPendingPerSource = 1
	if err := l.SetLimits(lim); !errors.Is(err, ErrInvalidConfig) || l.Limits().MaxPendingPerSource != 2 {
		t.Fatalf("lowered limit committed: %+v, %v", l.Limits(), err)
	}
}
