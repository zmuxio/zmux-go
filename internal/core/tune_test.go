package core

import "testing"

// TestTuneOutOfOrder receives a stream's data out of order, as multipath
// delivers it: a frame ending below the highest offset does not move the
// tuners back, so the rate samples stay that of the data and the windows
// stay bounded by it.
func TestTuneOutOfOrder(t *testing.T) {
	s := &Session{}
	s.tn.loopMin.update(100*Millisecond, 0)
	st := &Stream{}
	r := &st.rcv
	r.window = 1 << 10
	s.conn.win = 1 << 10
	recv := func(at Duration, end uint64) {
		s.now = Time(at)
		if end > r.highest {
			s.conn.recv += end - r.highest
			r.highest = end
		}
		s.tuneData(st)
	}
	recv(0, 1<<10)
	recv(150*Millisecond, 64<<10)
	recv(300*Millisecond, 32<<10) // below the highest, a rate interval later
	recv(450*Millisecond, 96<<10)
	for _, tu := range []*tuner{r.tune, s.conn.tune} {
		for _, x := range tu.rates {
			if x < 0 || x > 1<<20 {
				t.Fatalf("rate sample %g", x)
			}
		}
		if w := s.tuneGrow(tu, 1<<10, 0, ^uint64(0)); w > 1<<20 {
			t.Fatalf("window %d", w)
		}
	}
}
