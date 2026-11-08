package zmux_test

import (
	"testing"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/shaping/machine"
	"github.com/zmuxio/zmux-go/varint"
)

func TestShapingMachineDefinition(t *testing.T) {
	f, err := machine.NewFactory([]byte(`{"machines":[{"start":0,"states":[{"record":{"pad_to":200}}]}]}`),
		machine.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range zmux.ShapedRecordSizes(t, f, 10) {
		if n+varint.SizeVarint(uint64(n)) < 200 {
			t.Fatalf("record of %d frame bytes", n)
		}
	}
}
