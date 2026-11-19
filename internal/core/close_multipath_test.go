package core

import "testing"

// In a multipath session the peer closes on the first CONNECTION_CLOSE it
// reads and discards what arrives afterwards, so an application close waits
// while data written on any lane is unacknowledged.
func TestMultipathCloseWaitsForLaneData(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Test.RTT = Second
	s := New(cfg, 0)
	info := CarrierInfo{Lanes: true}
	p := s.AttachBootstrap(info, info, nil)
	s.peerSeen = true
	s.state = StateActive
	s.feat = Features{PipeAck: true, LanesOut: true, LanesIn: true, Multipath: true}
	p.out.lanes = append(p.out.lanes, &outLane{lane: 1, info: info, confirmed: true, pos: 10, elicitPos: 10})
	s.Close(0x10, nil)
	if !s.closeFr.waitLanes {
		t.Fatal("CONNECTION_CLOSE does not wait for 10 unacknowledged bytes on lane 1")
	}
}
