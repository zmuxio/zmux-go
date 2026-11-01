package core

import (
	"testing"

	"github.com/zmuxio/zmux-go/shaping"
)

// TestShapeAllowPadNegative: a negative PADDING request is none.
func TestShapeAllowPadNegative(t *testing.T) {
	s, _, _ := injectSession(t, shaping.Limits{MaxPaddingBytes: 1 << 20}, 0)
	before := s.shp.stats.PaddingBytes
	if n := s.allowPad(-5); n != 0 || s.shp.stats.PaddingBytes != before {
		t.Fatalf("allowPad(-5) = %d, padding %d", n, s.shp.stats.PaddingBytes)
	}
}
