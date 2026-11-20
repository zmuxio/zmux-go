package seal

import (
	"io"
	"testing"

	"github.com/zmuxio/zmux-go"
)

type reportingHalf struct {
	io.ReadWriteCloser
	reports bool
}

func (h reportingHalf) ReportsFailure() bool { return h.reports }

// A sealed carrier reports its own failure exactly as the carrier below
// does, half by half.
func TestSealedHalvesForwardFailureReporting(t *testing.T) {
	for _, reports := range []bool{true, false} {
		h := reportingHalf{reports: reports}
		c := rawCommon{base: zmux.PipeIO{R: h, W: h}}
		r := &rawReader{rawCommon: c}
		w := &rawWriter{rawCommon: c}
		if r.ReportsFailure() != reports || w.ReportsFailure() != reports {
			t.Fatalf("base %v: reader %v, writer %v", reports, r.ReportsFailure(), w.ReportsFailure())
		}
	}
	plain := zmux.PipeIO{R: io.NopCloser(nil), W: nil}
	if (&rawReader{base: plain}).ReportsFailure() {
		t.Fatal("a plain carrier reports its own failure")
	}
}
