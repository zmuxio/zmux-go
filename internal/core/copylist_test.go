package core

import "testing"

// The copy list and the control list unlink any member in O(1) and keep
// the others in order.
func TestStreamListsUnlink(t *testing.T) {
	s := &Session{}
	st := []*Stream{{id: 0}, {id: 4}, {id: 8}}
	for _, x := range st {
		s.markCopyMSD(x)
		s.queueCtl(x, ctlMaxStreamData)
	}
	s.markCopyMSD(st[0]) // listed once
	s.unlinkCopyMSD(st[1])
	s.unlinkCtl(st[1])
	var copies, ctl []uint64
	for x := s.copyHead; x != nil; x = x.copyNext {
		copies = append(copies, x.id)
	}
	for x := s.ctlHead; x != nil; x = x.ctlNext {
		ctl = append(ctl, x.id)
	}
	if len(copies) != 2 || copies[0] != 8 || copies[1] != 0 || len(ctl) != 2 || ctl[0] != 0 || ctl[1] != 8 || s.ctlTail != st[2] {
		t.Fatalf("copies %v, control %v", copies, ctl)
	}
	s.unlinkCtl(st[2])
	s.unlinkCtl(st[0])
	s.unlinkCopyMSD(st[0])
	s.unlinkCopyMSD(st[2])
	if s.ctlHead != nil || s.ctlTail != nil || s.copyHead != nil {
		t.Fatal("lists not empty")
	}
}

// Unlinking from the control list costs the same with many members.
func BenchmarkCtlUnlink(b *testing.B) {
	for _, n := range []int{1, 10000} {
		b.Run(map[int]string{1: "1", 10000: "10000"}[n], func(b *testing.B) {
			s := &Session{}
			for i := range n {
				s.queueCtl(&Stream{id: uint64(4 * i)}, ctlMaxStreamData)
			}
			x := s.ctlTail
			b.ReportAllocs()
			for b.Loop() {
				s.unlinkCtl(x)
				s.queueCtl(x, ctlMaxStreamData)
			}
		})
	}
}

// The gauges cost the same with many streams.
func BenchmarkGauges(b *testing.B) {
	for _, n := range []int{1, 10000} {
		b.Run(map[int]string{1: "1", 10000: "10000"}[n], func(b *testing.B) {
			s := capacitySession()
			for i := range n {
				s.streams[uint64(4*i)] = &Stream{id: uint64(4 * i)}
			}
			b.ReportAllocs()
			for b.Loop() {
				_ = s.Gauges()
			}
		})
	}
}
