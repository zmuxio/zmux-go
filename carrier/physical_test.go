package carrier

import (
	"errors"
	"testing"
	"time"
)

func TestPhysicalObserverReplacement(t *testing.T) {
	for _, exact := range []bool{true, false} {
		name := "gap"
		if exact {
			name = "shared-cut"
		}
		t.Run(name, func(t *testing.T) {
			id := PhysicalLegID{Line: "line", Leg: "leg", Direction: PhysicalSend}
			old := PhysicalLegConfig{ID: id, Observer: "first", Boundary: "link", Scope: "line", Classes: []string{"payload", "setup"}}
			ledger, err := NewPhysicalLedger([]PhysicalLegConfig{old})
			if err != nil {
				t.Fatal(err)
			}
			cut := time.Unix(20, 0)
			if err := ledger.Observe(id, old.Observer, PhysicalCounter{Generation: 9, At: cut, Bytes: 12, FromZero: true, Final: exact}); err != nil {
				t.Fatal(err)
			}
			next := old
			next.Observer, next.Classes = "second", []string{"setup", "payload"}
			if err := ledger.ReplaceObserver(next, PhysicalCounter{Generation: 1, At: cut, Bytes: 100}); err != nil {
				t.Fatal(err)
			}
			end := PhysicalCounter{Generation: 1, At: cut.Add(time.Second), Bytes: 130, Final: true}
			if err := ledger.Observe(id, old.Observer, end); !errors.Is(err, ErrPhysicalObserver) {
				t.Fatalf("retired observer = %v", err)
			}
			if err := ledger.Observe(id, next.Observer, end); err != nil {
				t.Fatal(err)
			}
			got := ledger.Snapshot(&id)
			want := PhysicalLowerBound
			if exact {
				want = PhysicalExact
			}
			if got.KnownBytes != 42 || got.Precision != want || !got.Final || got.Boundary != "link" {
				t.Fatalf("coverage = %+v", got)
			}
		})
	}
}
