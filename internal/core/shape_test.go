package core

import "testing"

func TestShapeWindow(t *testing.T) {
	s := New(DefaultConfig(), 0)
	sh := s.shapeSess()
	for i := range 16 {
		if !sh.allowed(Time(i)) {
			t.Fatalf("record %d refused", i)
		}
		sh.mark(Time(i))
	}
	if sh.allowed(Time(Second - 1)) {
		t.Fatal("17th record within one second")
	}
	if !sh.allowed(Time(Second)) {
		t.Fatal("record after one second refused")
	}
}
