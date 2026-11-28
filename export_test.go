package zmux

import (
	"testing"
	"testing/synctest"

	"github.com/zmuxio/zmux-go/shaping"
)

// ShapedRecordSizes sends n stream bytes from a client shaped by f and
// returns the sizes of the records it wrote.
func ShapedRecordSizes(t *testing.T, f ShaperFactory, n int) []int {
	t.Helper()
	var sizes []int
	synctest.Test(t, func(t *testing.T) {
		c, s, rc := fundedShapePair(t, shaping.Limits{}, false, 4096)
		mustSetShaper(t, c, f, nil)
		rc.start()
		transfer(t, c, s, n)
		sizes, _ = rc.records(true)
	})
	if len(sizes) == 0 {
		t.Fatal("no stream records")
	}
	return sizes
}
