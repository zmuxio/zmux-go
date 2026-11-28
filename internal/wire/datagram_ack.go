package wire

import "github.com/zmuxio/zmux-go/varint"

const (
	DatagramAckMaxRanges = 4090
	DatagramAckMaxSize   = 8192
)

// DatagramAck holds a validated, allocation-free report of path arrivals.
// Ranges contains additional Gap/Length pairs and aliases the frame buffer.
type DatagramAck struct {
	Ranges                             []byte
	PipeID, Largest, Delay, FirstRange uint64
	RangeCount                         int
	Channel                            uint8
}

func decodeDatagramAck(r *reader) (DatagramAck, *Error) {
	a := DatagramAck{PipeID: r.varint(), Channel: r.byte(), Largest: r.varint(), Delay: r.varint()}
	count, first := r.varint(), r.varint()
	a.FirstRange = first
	if r.bad || a.Channel > 1 || count > DatagramAckMaxRanges || first > a.Largest {
		return a, ErrFrame("invalid DATAGRAM_ACK header")
	}
	a.RangeCount = int(count)
	start, lo := r.i, a.Largest-first
	for range a.RangeCount {
		gap, length := r.varint(), r.varint()
		if r.bad || r.i > DatagramAckMaxSize || lo < 2 || gap > lo-2 || length > lo-gap-2 {
			return a, ErrFrame("invalid DATAGRAM_ACK range")
		}
		lo -= gap + 2 + length
	}
	if r.i > DatagramAckMaxSize {
		return a, ErrFrame("DATAGRAM_ACK larger than 8192 bytes")
	}
	a.Ranges = r.rangeOf(start, r.i).Contiguous()
	return a, nil
}

// DatagramAckIter walks the inclusive ranges in descending order.
type DatagramAckIter struct {
	ranges  []byte
	lo, hi  uint64
	initial bool
}

func (a *DatagramAck) Iter() DatagramAckIter {
	return DatagramAckIter{ranges: a.Ranges, lo: a.Largest - a.FirstRange, hi: a.Largest, initial: true}
}

func (it *DatagramAckIter) Next() (lo, hi uint64, ok bool) {
	if it.initial {
		it.initial = false
		return it.lo, it.hi, true
	}
	if len(it.ranges) == 0 {
		return 0, 0, false
	}
	gap, n := varint.ConsumeVarint(it.ranges)
	length, m := varint.ConsumeVarint(it.ranges[n:])
	it.ranges = it.ranges[n+m:]
	it.hi = it.lo - gap - 2
	it.lo = it.hi - length
	return it.lo, it.hi, true
}

func AppendDatagramAck(b []byte, a DatagramAck) []byte {
	b = varint.AppendVarint(b, TypeDatagramAck)
	b = varint.AppendVarint(b, a.PipeID)
	b = append(b, a.Channel)
	b = varint.AppendVarint(b, a.Largest)
	b = varint.AppendVarint(b, a.Delay)
	b = varint.AppendVarint(b, uint64(a.RangeCount))
	b = varint.AppendVarint(b, a.FirstRange)
	return append(b, a.Ranges...)
}
