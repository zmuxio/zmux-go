package core

import "github.com/zmuxio/zmux-go/varint"

const datagramSequenceSpace = varint.MaxVarint + 1

// datagramWindow uses a circular bitmap larger than or equal to the logical
// acceptance window. Advancing clears the slots that enter the new window.
type datagramWindow struct {
	words   []uint64
	highest uint64
	width   uint64
}

func (w *datagramWindow) advance(seq uint64, width int) {
	if w.words == nil {
		w.width = uint64(width)
		w.words = make([]uint64, (width+63)/64)
		w.highest = seq
		return
	}
	if seq <= w.highest {
		return
	}
	if seq-w.highest >= w.width {
		clear(w.words)
	} else {
		for first := w.highest + 1; first <= seq; {
			word, bit := w.index(first)
			count := min(64-bit, seq-first+1)
			w.words[word] &^= (1<<count - 1) << bit
			first += count
		}
	}
	w.highest = seq
}

func (w *datagramWindow) index(seq uint64) (word, bit uint64) {
	return seq / 64 % uint64(len(w.words)), seq % 64
}

func (w *datagramWindow) stale(seq uint64) bool {
	return seq <= w.highest && w.highest-seq >= w.width
}

func (w *datagramWindow) set(seq uint64, keep bool) {
	if w.stale(seq) {
		return
	}
	word, bit := w.index(seq)
	if keep {
		w.words[word] |= 1 << bit
	} else {
		w.words[word] &^= 1 << bit
	}
}

func (s *Session) duplicateDatagram(seq uint64) bool {
	w := &s.dg.window
	if w.words == nil || seq > w.highest {
		return false
	}
	if w.stale(seq) {
		s.stats.DatagramStaleDropped++
		return true
	}
	word, bit := w.index(seq)
	if w.words[word]&(1<<bit) != 0 {
		s.stats.DatagramDuplicatesDropped++
		return true
	}
	return false
}

func (s *Session) datagramSequenceWidth() int {
	if s.feat.DatagramFamily {
		var next uint64
		if s.dg != nil {
			next = s.dg.nextSequence
		}
		if next < datagramSequenceSpace {
			return varint.SizeVarint(next)
		}
	}
	return 0
}

func (s *Session) datagramData(o *outDgram) dgramView {
	if !s.feat.DatagramFamily {
		return o.data
	}
	n := s.datagramSequenceWidth()
	if n == 0 {
		return dgramView{}
	}
	data := o.data
	b := data.head[8-n:]
	data.head = varint.AppendVarint(b[:0], s.dg.nextSequence)[:len(b)]
	return data
}

func (s *Session) datagramDispatched() {
	if s.feat.DatagramFamily {
		d := s.dg
		d.nextSequence++
		switch d.nextSequence {
		case 1 << 6, 1 << 14, 1 << 30, datagramSequenceSpace:
			s.noteDatagramPath()
		}
	}
}

func (s *Session) datagramExhausted() bool {
	return s.feat.DatagramFamily && s.dg != nil && s.dg.nextSequence == datagramSequenceSpace
}

func (s *Session) datagramDataLen(o *outDgram) int {
	if !s.feat.DatagramFamily {
		return o.data.len()
	}
	return o.data.len() - 8 + s.datagramSequenceWidth()
}
