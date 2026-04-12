package runtime

import (
	"math"
	"testing"

	"github.com/zmuxio/zmux-go/internal/wire"
)

func TestQuarterThreshold(t *testing.T) {
	t.Parallel()

	cases := map[uint64]uint64{
		0: 1,
		1: 1,
		4: 1,
		8: 2,
	}
	for input, want := range cases {
		if got := QuarterThreshold(input); got != want {
			t.Fatalf("QuarterThreshold(%d) = %d, want %d", input, got, want)
		}
	}
}

func TestNegotiatedFramePayload(t *testing.T) {
	t.Parallel()

	local := wire.Settings{MaxFramePayload: 4096}
	peer := wire.Settings{MaxFramePayload: 2048}
	if got := NegotiatedFramePayload(local, peer); got != 2048 {
		t.Fatalf("NegotiatedFramePayload(min) = %d, want 2048", got)
	}

	if got := NegotiatedFramePayload(wire.Settings{}, wire.Settings{}); got != wire.DefaultSettings().MaxFramePayload {
		t.Fatalf("NegotiatedFramePayload(default) = %d, want %d", got, wire.DefaultSettings().MaxFramePayload)
	}
}

func TestSaturatingMath(t *testing.T) {
	t.Parallel()

	if got := SaturatingAdd(math.MaxUint64, 1); got != math.MaxUint64 {
		t.Fatalf("SaturatingAdd overflow = %d, want %d", got, uint64(math.MaxUint64))
	}
	if got := SaturatingMul(math.MaxUint64, 2); got != math.MaxUint64 {
		t.Fatalf("SaturatingMul overflow = %d, want %d", got, uint64(math.MaxUint64))
	}
}
