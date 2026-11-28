package core

import "testing"

// TestAbandonWithoutStateResent: a PIPE_ABANDON for an ID without
// attachment state is sent again when the pipe that carried it is lost,
// as every PIPE_ABANDON until acknowledged (MULTIPATH.md Section 7.4).
func TestAbandonWithoutStateResent(t *testing.T) {
	s, _ := statsMP()
	id := uint64(3)
	s.lost(&frameDesc{kind: dAbandonID, id: id, val: 9})
	if code, ok := s.mp.pendAbandon[id]; !ok || code.code != 9 {
		t.Fatalf("not owed again: %v %v", code, ok)
	}
}
