package core

import "testing"

func highBDPSendSession() *Session {
	s := capacityMP()
	measuredRate(s.pipes[0], 512*(1<<20))
	s.pipes[0].mp.rtt.smoothed = Second
	return s
}
func TestSendBufferGrowsWithMultipathBudget(t *testing.T) {
	s := highBDPSendSession()
	s.cfg.MemoryBudget = 256 << 20
	if b := s.retransmitBudget(); b != 256<<20 {
		t.Fatalf("auto retransmit budget: %d", b)
	}
	if room := s.sendRoom(&Stream{}); room != 256<<20 {
		t.Fatalf("effective send buffer: %d", room)
	}
	s.mp = nil
	if room := s.sendRoom(&Stream{}); room != s.cfg.SendBuffer {
		t.Fatalf("single pipe buffer changed: %d", room)
	}
}
func TestSendBufferFixedMaximumCannotUndercutInitial(t *testing.T) {
	s := highBDPSendSession()
	s.cfg.SendBuffer, s.cfg.SendBufferMax = 8<<20, 2<<20
	if b := s.retransmitBudget(); b != 8<<20 {
		t.Fatalf("fixed maximum under initial: %d", b)
	}
}
func TestSendBufferSharedHeadroomIncludesUnchargedGrowth(t *testing.T) {
	s := highBDPSendSession()
	s.cfg.MemoryBudget = 1
	b := NewBudget(512 << 20)
	b.Add(400 << 20)
	s.cfg.Budget = b
	s.conn.sendHeld, s.mp.retained = 8<<20, 8<<20
	if n := s.retransmitBudget(); n != 112<<20 {
		t.Fatalf("shared uncharged grant: %d", n)
	}
	s.ChargeBudget()
	if n := s.retransmitBudget(); n != 112<<20 {
		t.Fatalf("double counted own charge: %d", n)
	}
	b.Add(64 << 20)
	if n := s.retransmitBudget(); n != 64<<20 {
		t.Fatalf("64 MiB floor lost: %d", n)
	}
}
func TestSendBufferDoesNotCountUnsentDataTwice(t *testing.T) {
	s := highBDPSendSession()
	s.cfg.MemoryBudget = 128 << 20
	s.conn.sendHeld = 32 << 20
	if n := s.retransmitBudget(); n != 128<<20 {
		t.Fatalf("unsent data reduced the absolute cap twice: %d", n)
	}
}
func TestSendBufferTotalKeepsOnlyTheSmallDirectionException(t *testing.T) {
	s := capacitySession()
	bulk := &Stream{}
	before := s.heldEnter(bulk)
	bulk.snd.q.end = s.cfg.SendBuffer
	s.heldLeave(bulk, before)
	small := &Stream{}
	if n := s.sendRoom(small); n != smallHeld {
		t.Fatalf("new writer exceeded its full-buffer floor: %d", n)
	}
}
func TestSendBufferHonorsExplicitSmallStreamLimit(t *testing.T) {
	s := capacitySession()
	st := &Stream{}
	s.SetSendBuffer(st, 32<<10)
	if n := s.sendRoom(st); n != 32<<10 {
		t.Fatalf("explicit limit ignored: %d", n)
	}
}
