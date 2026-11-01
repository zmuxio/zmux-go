package wire

import "testing"

func TestJoinLen(t *testing.T) {
	for _, j := range []*Join{
		{Token: make([]byte, 16)},
		{Locator: make([]byte, 255), PipeID: 1 << 40, Dir: 2, CarrierBound: true, Standby: true,
			FateGroup: 1 << 20, Priority: 300, Token: make([]byte, 16)},
	} {
		if got, want := JoinLen(j), len(AppendJoin(nil, j)); got != want {
			t.Fatalf("JoinLen %d, encoded %d", got, want)
		}
	}
}
