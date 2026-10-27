package wire

// StreamHeaderLen is the length of a STREAM frame header.
func StreamHeaderLen(id, off uint64, n int, withLen bool) int {
	l := 1 + VarintLen(id)
	if off > 0 {
		l += VarintLen(off)
	}
	if withLen {
		l += VarintLen(uint64(n))
	}
	return l
}

// AppendStreamHeader appends a STREAM frame header; the data follows it.
// The OFF bit is set only for a non-zero offset.
func AppendStreamHeader(b []byte, id, off uint64, n int, fin, withLen bool) []byte {
	t := TypeStream
	if off > 0 {
		t |= StreamBitOff
	}
	if withLen {
		t |= StreamBitLen
	}
	if fin {
		t |= StreamBitFin
	}
	b = append(b, byte(t))
	b = AppendVarint(b, id)
	if off > 0 {
		b = AppendVarint(b, off)
	}
	if withLen {
		b = AppendVarint(b, uint64(n))
	}
	return b
}

func AppendResetStream(b []byte, id, code, final uint64) []byte {
	b = append(b, byte(TypeResetStream))
	return AppendVarint(AppendVarint(AppendVarint(b, id), code), final)
}

func AppendResetStreamAt(b []byte, id, code, final, reliable uint64) []byte {
	b = append(b, byte(TypeResetStreamAt))
	return AppendVarint(AppendVarint(AppendVarint(AppendVarint(b, id), code), final), reliable)
}

func AppendStopSending(b []byte, id, code uint64) []byte {
	return AppendVarint(AppendVarint(append(b, byte(TypeStopSending)), id), code)
}

func AppendMaxData(b []byte, v uint64) []byte {
	return AppendVarint(append(b, byte(TypeMaxData)), v)
}

func AppendMaxStreamData(b []byte, id, v uint64) []byte {
	return AppendVarint(AppendVarint(append(b, byte(TypeMaxStreamData)), id), v)
}

func AppendMaxStreams(b []byte, bidi bool, v uint64) []byte {
	t := TypeMaxStreamsUni
	if bidi {
		t = TypeMaxStreamsBidi
	}
	return AppendVarint(append(b, byte(t)), v)
}

func AppendDataBlocked(b []byte, v uint64) []byte {
	return AppendVarint(append(b, byte(TypeDataBlocked)), v)
}

func AppendStreamDataBlocked(b []byte, id, v uint64) []byte {
	return AppendVarint(AppendVarint(append(b, byte(TypeStreamDataBlocked)), id), v)
}

func AppendStreamsBlocked(b []byte, bidi bool, v uint64) []byte {
	t := TypeStreamsBlockedUni
	if bidi {
		t = TypeStreamsBlockedBi
	}
	return AppendVarint(append(b, byte(t)), v)
}

// AppendClose appends CONNECTION_CLOSE; frameType is used only for 0x1c.
func AppendClose(b []byte, app bool, code, frameType uint64, reason []byte) []byte {
	if app {
		b = AppendVarint(append(b, byte(TypeCloseApp)), code)
	} else {
		b = AppendVarint(AppendVarint(append(b, byte(TypeCloseTransport)), code), frameType)
	}
	b = AppendVarint(b, uint64(len(reason)))
	return append(b, reason...)
}

// CloseLen is the encoded length of a CONNECTION_CLOSE frame.
func CloseLen(app bool, code, frameType uint64, reasonLen int) int {
	l := 1 + VarintLen(code) + VarintLen(uint64(reasonLen)) + reasonLen
	if !app {
		l += VarintLen(frameType)
	}
	return l
}

// AppendDatagramHeader appends a DATAGRAM header for n bytes of data.
func AppendDatagramHeader(b []byte, n int, withLen bool) []byte {
	if withLen {
		return AppendVarint(append(b, byte(TypeDatagramLen)), uint64(n))
	}
	return append(b, byte(TypeDatagram))
}

func AppendTransportParams(b []byte, block []byte) []byte {
	b = AppendVarint(b, TypeTransportParams)
	b = AppendVarint(b, uint64(len(block)))
	return append(b, block...)
}

func AppendPing(b []byte, response bool, seq uint64) []byte {
	t := TypePingRequest
	if response {
		t = TypePingResponse
	}
	return AppendVarint(AppendVarint(b, t), seq)
}

func AppendGoAway(b []byte, maxBidi, maxUni, code uint64) []byte {
	b = AppendVarint(b, TypeGoAway)
	return AppendVarint(AppendVarint(AppendVarint(b, maxBidi), maxUni), code)
}

// PriorityByte encodes urgency and incremental (EXTENSIONS.md 5.2).
func PriorityByte(urgency uint8, incr bool) byte {
	p := urgency << 5
	if incr {
		p |= 0x10
	}
	return p
}

func AppendPriorityUpdate(b []byte, id, seq uint64, urgency uint8, incr bool) []byte {
	b = AppendVarint(b, TypePriorityUpdate)
	b = AppendVarint(AppendVarint(b, id), seq)
	return append(b, PriorityByte(urgency, incr))
}

// AppendJoinMAC appends PIPE_JOIN from Type through Priority (the MAC input).
func AppendJoinMAC(b []byte, j *Join) []byte {
	b = AppendVarint(b, TypePipeJoin)
	b = append(b, byte(len(j.Locator)))
	b = append(b, j.Locator...)
	b = AppendVarint(b, j.PipeID)
	fl := j.Dir & 3
	if j.CarrierBound {
		fl |= 0x04
	}
	if j.Standby {
		fl |= 0x08
	}
	b = append(b, fl)
	b = AppendVarint(b, j.FateGroup)
	return AppendVarint(b, j.Priority)
}

// AppendJoin appends the whole PIPE_JOIN frame.
func AppendJoin(b []byte, j *Join) []byte {
	return append(AppendJoinMAC(b, j), j.Token...)
}

// AppendPipeAckHeader appends the type and entry count; entries follow.
func AppendPipeAckHeader(b []byte, count int) []byte {
	return AppendVarint(AppendVarint(b, TypePipeAck), uint64(count))
}

func AppendAckEntry(b []byte, e AckEntry) []byte {
	return AppendVarint(AppendVarint(AppendVarint(AppendVarint(b, e.PipeID), e.Lane), e.Position), e.Delay)
}

// AckEntryLen is the encoded length of an entry.
func AckEntryLen(e AckEntry) int {
	return VarintLen(e.PipeID) + VarintLen(e.Lane) + VarintLen(e.Position) + VarintLen(e.Delay)
}

func AppendPipeLane(b []byte, key []byte, pipe, lane uint64) []byte {
	b = AppendVarint(b, TypePipeLane)
	b = append(b, key...)
	return AppendVarint(AppendVarint(b, pipe), lane)
}

func AppendPipePing(b []byte) []byte { return AppendVarint(b, TypePipePing) }

func AppendPipeAbandon(b []byte, pipe, code uint64) []byte {
	return AppendVarint(AppendVarint(AppendVarint(b, TypePipeAbandon), pipe), code)
}

func AppendPipeStatus(b []byte, pipe, seq, status, prio uint64) []byte {
	b = AppendVarint(b, TypePipeStatus)
	return AppendVarint(AppendVarint(AppendVarint(AppendVarint(b, pipe), seq), status), prio)
}

// AppendFrame re-encodes a decoded frame in its shortest form. It is used by
// tests and tools; the core builds frames with the functions above.
func AppendFrame(b []byte, f *Frame) []byte {
	switch f.Kind {
	case KindPadding:
		for range f.Count {
			b = append(b, 0)
		}
		return b
	case KindStream:
		if f.HasOff && f.Offset == 0 {
			t := TypeStream | StreamBitOff
			if f.HasLen {
				t |= StreamBitLen
			}
			if f.Fin {
				t |= StreamBitFin
			}
			b = AppendVarint(append(b, byte(t)), f.StreamID)
			b = append(b, 0)
			if f.HasLen {
				b = AppendVarint(b, uint64(len(f.Data)))
			}
			return append(b, f.Data...)
		}
		return append(AppendStreamHeader(b, f.StreamID, f.Offset, len(f.Data), f.Fin, f.HasLen), f.Data...)
	case KindResetStream:
		return AppendResetStream(b, f.StreamID, f.Code, f.FinalSize)
	case KindResetStreamAt:
		return AppendResetStreamAt(b, f.StreamID, f.Code, f.FinalSize, f.Reliable)
	case KindStopSending:
		return AppendStopSending(b, f.StreamID, f.Code)
	case KindMaxData:
		return AppendMaxData(b, f.Value)
	case KindMaxStreamData:
		return AppendMaxStreamData(b, f.StreamID, f.Value)
	case KindMaxStreams:
		return AppendMaxStreams(b, f.Bidi, f.Value)
	case KindDataBlocked:
		return AppendDataBlocked(b, f.Value)
	case KindStreamDataBlocked:
		return AppendStreamDataBlocked(b, f.StreamID, f.Value)
	case KindStreamsBlocked:
		return AppendStreamsBlocked(b, f.Bidi, f.Value)
	case KindClose:
		return AppendClose(b, f.App, f.Code, f.ErrFrame, f.Data)
	case KindDatagram:
		return append(AppendDatagramHeader(b, len(f.Data), f.HasLen), f.Data...)
	case KindTransportParams:
		return AppendTransportParams(b, f.Data)
	case KindPing:
		return AppendPing(b, f.Response, f.Value)
	case KindGoAway:
		return AppendGoAway(b, f.MaxBidi, f.MaxUni, f.Code)
	case KindPriorityUpdate:
		return AppendPriorityUpdate(b, f.StreamID, f.Seq, f.Urgency, f.Incr)
	case KindPipeJoin:
		return AppendJoin(b, &f.Join)
	case KindPipeAck:
		b = AppendPipeAckHeader(b, f.Count)
		it := f.Iter()
		for e, ok := it.Next(); ok; e, ok = it.Next() {
			b = AppendAckEntry(b, e)
		}
		return b
	case KindPipeLane:
		return AppendPipeLane(b, f.LaneKey, f.PipeID, f.Lane)
	case KindPipePing:
		return AppendPipePing(b)
	case KindPipeAbandon:
		return AppendPipeAbandon(b, f.PipeID, f.Code)
	case KindPipeStatus:
		return AppendPipeStatus(b, f.PipeID, f.Seq, f.Status, f.Priority)
	}
	return b
}
