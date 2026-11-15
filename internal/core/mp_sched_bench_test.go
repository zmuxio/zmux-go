package core

import (
	"strconv"
	"testing"
)

// mpBenchSession is a multipath session with n active, measured outbound
// pipes of one tier, with distinct RTTs and rates.
func mpBenchSession(n int) (*Session, *gate) {
	s, _ := statsMP()
	for len(s.pipes) < n {
		capacityExtra(s)
	}
	for i, p := range s.pipes {
		p.id = uint64(2 * i)
		a := p.mp
		a.rtt = rttEst{has: true, min: Duration(10+i) * Millisecond, smoothed: Duration(12+i) * Millisecond}
		measuredRate(p, float64(1e6*(i+1)))
		p.out.snd.re.measured = i%2 == 0
	}
	g := s.buildGate(&s.mp.gateBuf)
	return s, g
}

func BenchmarkMPEarliestTwo(b *testing.B) {
	for _, n := range []int{2, 4, 16, 64} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			s, g := mpBenchSession(n)
			b.ReportAllocs()
			for b.Loop() {
				s.earliestTwo(g, 0, 16384, nil)
			}
		})
	}
}

func BenchmarkMPPrimary(b *testing.B) {
	for _, n := range []int{2, 4, 16, 64} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			s, _ := mpBenchSession(n)
			b.ReportAllocs()
			for b.Loop() {
				s.primary()
			}
		})
	}
}

func BenchmarkMPBuildGate(b *testing.B) {
	for _, n := range []int{2, 4, 16, 64} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			s, _ := mpBenchSession(n)
			b.ReportAllocs()
			for b.Loop() {
				s.buildGate(&s.mp.gateBuf)
			}
		})
	}
}
