package core

// Framing is a carrier's record framing (SPEC.md 3.1, 3.4).
type Framing uint8

const (
	FramingBytes Framing = iota
	FramingRecords
)

// CarrierInfo describes a carrier half to the core.
type CarrierInfo struct {
	Framing Framing
	// TLS: build records of at most 16382 bytes of frames (SPEC.md 3.3).
	TLS bool
	// MinRTT reports whether the carrier provides a minimum RTT; without it
	// the first record carries a QX_PING request (SPEC.md 8.3 rule 6).
	MinRTT bool
	// Lanes: the carrier offers lanes (MULTIPATH.md 2.5).
	Lanes bool
	// QueueBound: the carrier bounds its own queue, with a low-water mark
	// or an unsent count; otherwise the carrier queue limiter
	// paces it.
	QueueBound bool
}

// Pipe is one attachment.
type Pipe struct {
	id   uint64
	conn any     // carrier connection of lane 0 (lanes)
	mp   *attach // multipath side storage; nil outside multipath
	in   *inDir
	out  *outDir
}

// ID returns the pipe ID.
func (p *Pipe) ID() uint64 { return p.id }

type inDir struct {
	lanes []*inLane
	ended bool
}

type inLane struct {
	lane     uint64
	pos      uint64
	info     CarrierInfo
	recs     uint64
	expectTP bool // the first record must start with QX_TRANSPORT_PARAMETERS
	ended    bool
	// Acknowledgement generation (MULTIPATH.md 6.3).
	elicit     uint64 // ack-eliciting bytes since the last PIPE_ACK
	elicitAt   Time   // when the first of them arrived
	reported   uint64 // position last reported
	posAt      Time   // when pos last advanced
	expectLane bool   // the first record must start with PIPE_LANE
	skipJoin   bool   // the first record starts with an already processed PIPE_JOIN
}

type outDir struct {
	lo      laneOut
	lanes   []*outLane
	snd     pipeSend
	records uint64 // records written
	q       pipeQuality
	sig     *Signal
	failed  bool
}

type outLane struct {
	lane        uint64
	pos         uint64 // bytes produced for the carrier (sender's position)
	acked       uint64
	info        CarrierInfo
	tpSent      bool
	closeSent   bool
	shut        bool
	writing     bool // a write call is in progress (since stallAt)
	notWritable bool // the carrier reports it accepts no bytes (since stallAt)
	stallAt     Time
	log         []retained // multipath retention
	segs        []sendSeg  // unacknowledged bytes in send order
	activity    []uint64   // end positions of records with activity frames
	laneFirst   bool       // the PIPE_LANE record is still to be written
	confirmed   bool
	refused     bool
	boundSt     []*Stream // stream directions bound to the lane
	served      bool      // took a data batch this round
	waiting     bool      // was refused data for its turn
	sig         *Signal   // wakes the lane's writer (lanes above 0; lane 0 uses the pipe's)
	openedAt    Time
}

// AttachBootstrap attaches pipe 0 (both directions) and returns it. The
// driver then polls it for the first record (QX_TRANSPORT_PARAMETERS).
func (s *Session) AttachBootstrap(in, out CarrierInfo, writer *Signal) *Pipe {
	p := &Pipe{id: 0}
	p.in = &inDir{lanes: []*inLane{{info: in, expectTP: true}}}
	p.out = &outDir{lanes: []*outLane{{info: out, confirmed: true}}, sig: writer}
	p.out.lo.offered = out.Lanes
	s.pipes = append(s.pipes, p)
	s.wake(writer)
	return p
}

// pipe returns the attachment with id.
func (s *Session) pipe(id uint64) *Pipe {
	for _, p := range s.pipes {
		if p.id == id {
			return p
		}
	}
	return nil
}

func (d *outDir) lane(l uint64) *outLane {
	for _, x := range d.lanes {
		if x.lane == l {
			return x
		}
	}
	return nil
}

func (d *inDir) lane(l uint64) *inLane {
	for _, x := range d.lanes {
		if x.lane == l {
			return x
		}
	}
	return nil
}

// SetWriterSignal sets the signal that wakes the writer of an outbound pipe.
func (s *Session) SetWriterSignal(pipe uint64, sig *Signal) {
	if p := s.pipe(pipe); p != nil && p.out != nil {
		p.out.sig = sig
	}
}

// SetWritable records whether a carrier half accepts bytes now; a carrier
// that accepts none for T_suspect makes the pipe suspect (MULTIPATH.md 7.1).
func (s *Session) SetWritable(pipe, lane uint64, writable bool, now Time) {
	s.SetNow(now)
	p := s.pipe(pipe)
	if p == nil || p.out == nil {
		return
	}
	ol := p.out.lane(lane)
	if ol == nil || ol.notWritable == !writable {
		return
	}
	ol.notWritable = !writable
	if !writable && !ol.writing {
		ol.stallAt = now
	}
	if s.ack != nil {
		e := &p.out.snd.re
		e.blockedNow = !writable
		if !writable && s.dataPending(ol) {
			e.blocked()
		}
	}
	if writable {
		s.wakePipe(p)
	}
}

// stalled reports a lane that has bytes to hand over but whose carrier
// takes none.
func (ol *outLane) stalled() bool { return ol.writing || ol.notWritable }
