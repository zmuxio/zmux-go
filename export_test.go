package zmux

import "testing"

// ShapedRecordSizes sends n stream bytes from a client shaped by f and
// returns the sizes of the records it wrote.
func ShapedRecordSizes(t *testing.T, f ShaperFactory, n int) []int {
	t.Helper()
	c, s, rc := shapedPair(t, nil)
	c.SetShaper(f, nil)
	rc.start()
	transfer(t, c, s, n)
	sizes, _ := rc.records()
	return sizes
}
