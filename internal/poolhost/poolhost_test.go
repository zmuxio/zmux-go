package poolhost

import "testing"

// The order in which a shrinking pool closes its members: retired by the
// policy, then the longest idle, the fewest pipes, the least age, the
// highest key.
func TestBeforeOrder(t *testing.T) {
	for _, c := range []struct{ a, b Member }{
		{Member{Retire: true}, Member{Idle: 3600, Pipes: 1}},
		{Member{Idle: 2, Pipes: 3}, Member{Idle: 1, Pipes: 1}},
		{Member{Idle: 1, Pipes: 1, Age: 9}, Member{Idle: 1, Pipes: 2, Age: 1}},
		{Member{Pipes: 1, Age: 1, Key: 0}, Member{Pipes: 1, Age: 2, Key: 4}},
		{Member{Pipes: 1, Age: 1, Key: 4}, Member{Pipes: 1, Age: 1, Key: 2}},
	} {
		if !Before(c.a, c.b) || Before(c.b, c.a) {
			t.Fatalf("%+v should close before %+v", c.a, c.b)
		}
	}
}
