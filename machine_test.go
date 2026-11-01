package zmux_test

import (
	"testing"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/shaping/machine"
)

func TestShapingMachineDefinition(t *testing.T) {
	f, err := machine.NewFactory([]byte(`{"version":1,"machines":[{"states":[{"record":{"pad_to":{"const":200}}}]}]}`),
		machine.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range zmux.ShapedRecordSizes(t, f, 10) {
		if n < 200 {
			t.Fatalf("record of %d bytes", n)
		}
	}
}
