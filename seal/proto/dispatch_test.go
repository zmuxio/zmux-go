package proto

import (
	"testing"

	"github.com/zmuxio/zmux-go/internal/join"
)

// The first-byte dispatch of a carrier.
func TestDispatch(t *testing.T) {
	if Dispatch(nil, Stream) != join.ClassIncomplete || Dispatch(nil, Message) != join.ClassNotHello {
		t.Fatal("empty")
	}
	for b, want := range map[byte]join.Class{0: join.ClassNotHello, 1: join.ClassEstablish, 2: join.ClassJoin, 3: join.ClassEstablish, 4: join.ClassEstablish, 5: join.ClassEstablish, 6: join.ClassReserved, 7: join.ClassReserved, 8: join.ClassEstablish, 9: join.ClassNotHello, 0xff: join.ClassNotHello} {
		if Dispatch([]byte{b}, Stream) != want {
			t.Fatal(b)
		}
	}
}
