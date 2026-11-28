package testpeer

import "testing"

func TestExpectOutOfOrder(t *testing.T) {
	lines := make(chan string, 2)
	lines <- "KEYS 00ff"
	lines <- "ESTABLISHED"
	close(lines)
	p := Output{Lines: lines}
	if p.Expect(t, "ESTABLISHED") != "" || p.Expect(t, "KEYS ") != "00ff" {
		t.Fatal("peer output was reordered or lost")
	}
}
