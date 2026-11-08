package wire

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestPhraseCutsAtCharacterBoundary(t *testing.T) {
	r := strings.Repeat("a", 1023) + "é" + "tail"
	got := Phrase(r, "")
	if got != strings.Repeat("a", 1023) {
		t.Fatalf("len %d, valid %v", len(got), utf8.ValidString(got))
	}
	if got := Phrase("x", "k=v"); got != "x\nk=v" {
		t.Fatalf("got %q", got)
	}
	if got := Phrase(strings.Repeat("b", 2000), ""); len(got) != 1024 {
		t.Fatalf("len %d", len(got))
	}
}
