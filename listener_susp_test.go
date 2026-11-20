package zmux

import "testing"

// The suspension list closes the oldest beyond MaxSuspended and drops any
// member in constant time.
func TestListenerSuspendedList(t *testing.T) {
	l := &Listener{}
	l.lim.Store(&ListenerLimits{MaxSuspended: 2})
	a, b, c, d := &Conn{}, &Conn{}, &Conn{}, &Conn{}
	l.sessionSuspended(a)
	l.sessionSuspended(b)
	if over := l.sessionSuspended(c); len(over) != 1 || over[0] != a || a.inSusp {
		t.Fatalf("over %v", over)
	}
	l.sessionResumed(b)
	l.sessionSuspended(d)
	l.sessionSuspended(c) // again: moves to the end
	var got []*Conn
	for x := l.suspHead; x != nil; x = x.suspNext {
		got = append(got, x)
	}
	if len(got) != 2 || got[0] != d || got[1] != c || l.suspN != 2 || l.suspTail != c {
		t.Fatalf("list %v", got)
	}
}

func BenchmarkListenerSuspendResume(b *testing.B) {
	for _, n := range []int{1, 10000} {
		b.Run(map[int]string{1: "1", 10000: "10000"}[n], func(b *testing.B) {
			l := &Listener{}
			l.lim.Store(&ListenerLimits{MaxSuspended: n + 1})
			for range n {
				l.sessionSuspended(&Conn{})
			}
			c := l.suspHead
			b.ReportAllocs()
			for b.Loop() {
				l.sessionResumed(c)
				l.sessionSuspended(c)
			}
		})
	}
}
