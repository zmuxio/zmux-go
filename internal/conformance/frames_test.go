package conformance

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strconv"

	"github.com/zmuxio/zmux-go/internal/wire"
)

// ---- parameters by name ----

var paramIDs = map[string]uint64{
	"max_idle_timeout": wire.ParamMaxIdleTimeout, "initial_max_data": wire.ParamInitialMaxData,
	"initial_max_stream_data_bidi_local":  wire.ParamMaxStreamDataBidiLoc,
	"initial_max_stream_data_bidi_remote": wire.ParamMaxStreamDataBidiRem,
	"initial_max_stream_data_uni":         wire.ParamMaxStreamDataUni,
	"initial_max_streams_bidi":            wire.ParamMaxStreamsBidi, "initial_max_streams_uni": wire.ParamMaxStreamsUni,
	"reset_stream_at": wire.ParamResetStreamAt, "max_datagram_frame_size": wire.ParamMaxDatagramFrameSize,
	"max_record_size": wire.ParamMaxRecordSize, "zmux_role": wire.ParamRole, "zmux_goaway": wire.ParamGoAway,
	"zmux_preamble": wire.ParamPreamble, "zmux_priority": wire.ParamPriority,
	"zmux_datagram_flows": wire.ParamDatagramFlows, "zmux_multipath": wire.ParamMultipath,
	"zmux_ack_threshold": wire.ParamAckThreshold, "zmux_pipe_ack": wire.ParamPipeAck,
	"zmux_max_ack_delay": wire.ParamMaxAckDelay, "zmux_max_lanes": wire.ParamMaxLanes,
	"zmux_resume": wire.ParamResume,
}

var emptyParams = map[string]bool{"reset_stream_at": true, "zmux_goaway": true, "zmux_priority": true,
	"zmux_datagram_flows": true, "zmux_pipe_ack": true}

func hexb(v any) []byte {
	s, _ := v.(string)
	b, err := hex.DecodeString(s)
	if err != nil {
		panic("bad hex " + s)
	}
	return b
}

func num(v any) uint64 {
	switch x := v.(type) {
	case float64:
		return uint64(x)
	case json.Number:
		n, err := strconv.ParseUint(string(x), 10, 64)
		if err != nil {
			panic(err)
		}
		return n
	case int:
		return uint64(x)
	case uint64:
		return x
	case bool:
		if x {
			return 1
		}
		return 0
	}
	panic(fmt.Sprintf("not a number: %T %v", v, v))
}

// encodeParamValue encodes a parameter value from its JSON form.
func encodeParamValue(name string, v any) []byte {
	if emptyParams[name] {
		return nil
	}
	switch name {
	case "zmux_role":
		m := v.(map[string]any)
		role := map[string]uint64{"initiator": 0, "responder": 1, "auto": 2}[m["role"].(string)]
		b := wire.AppendVarint(nil, role)
		if n, ok := m["nonce"]; ok {
			x := num(n)
			for i := 7; i >= 0; i-- {
				b = append(b, byte(x>>(8*i)))
			}
		}
		return b
	case "zmux_multipath":
		m := v.(map[string]any)
		b := []byte{byte(num(m["flags"]))}
		b = append(b, hexb(m["nonce"])...)
		loc := hexb(m["locator"])
		b = append(b, byte(len(loc)))
		return append(b, loc...)
	case "zmux_max_lanes":
		m := v.(map[string]any)
		b := wire.AppendVarint(nil, num(m["max_lanes"]))
		return append(b, hexb(m["lane_key"])...)
	}
	return wire.AppendVarint(nil, num(v))
}

// paramsFromMap builds wire.Params from a profile's tp map without
// validation (profiles may hold values below the API's floors).
func paramsFromMap(tp map[string]any) wire.Params {
	var p wire.Params
	p.MaxRecordSize = wire.DefaultMaxRecordSize
	for n, v := range tp {
		switch n {
		case "max_idle_timeout":
			p.Has |= wire.HasIdle
			p.MaxIdleTimeout = num(v)
		case "initial_max_data":
			p.Has |= wire.HasMaxData
			p.InitialMaxData = num(v)
		case "initial_max_stream_data_bidi_local":
			p.Has |= wire.HasBidiLocal
			p.MaxStreamDataBidiLoc = num(v)
		case "initial_max_stream_data_bidi_remote":
			p.Has |= wire.HasBidiRemote
			p.MaxStreamDataBidiRem = num(v)
		case "initial_max_stream_data_uni":
			p.Has |= wire.HasUni
			p.MaxStreamDataUni = num(v)
		case "initial_max_streams_bidi":
			p.Has |= wire.HasStreamsBidi
			p.MaxStreamsBidi = num(v)
		case "initial_max_streams_uni":
			p.Has |= wire.HasStreamsUni
			p.MaxStreamsUni = num(v)
		case "reset_stream_at":
			p.Has |= wire.HasResetAt
		case "max_datagram_frame_size":
			p.Has |= wire.HasDatagram
			p.MaxDatagramFrameSize = num(v)
		case "max_record_size":
			p.Has |= wire.HasRecordSize
			p.MaxRecordSize = num(v)
		case "zmux_role":
			m := v.(map[string]any)
			p.Has |= wire.HasRole
			p.Role = map[string]uint64{"initiator": 0, "responder": 1, "auto": 2}[m["role"].(string)]
			if x, ok := m["nonce"]; ok {
				p.RoleNonce = num(x)
			}
		case "zmux_goaway":
			p.Has |= wire.HasGoAway
		case "zmux_preamble":
			p.Has |= wire.HasPreamble
			p.Preamble = num(v)
		case "zmux_priority":
			p.Has |= wire.HasPriority
		case "zmux_datagram_flows":
			p.Has |= wire.HasFlows
		case "zmux_multipath":
			m := v.(map[string]any)
			p.Has |= wire.HasMultipath
			p.SecretFlags = byte(num(m["flags"]))
			copy(p.SessionNonce[:], hexb(m["nonce"]))
			p.Locator = hexb(m["locator"])
		case "zmux_ack_threshold":
			p.Has |= wire.HasAckThreshold
			p.AckThreshold = num(v)
		case "zmux_pipe_ack":
			p.Has |= wire.HasPipeAck
		case "zmux_max_ack_delay":
			p.Has |= wire.HasAckDelay
			p.MaxAckDelay = num(v)
		case "zmux_max_lanes":
			m := v.(map[string]any)
			p.Has |= wire.HasLanes
			p.MaxLanes = num(m["max_lanes"])
			copy(p.LaneKey[:], hexb(m["lane_key"]))
		case "zmux_resume":
			p.Has |= wire.HasResume
			p.ResumeGrace = num(v)
		default:
			panic("unknown parameter " + n)
		}
	}
	return p
}

// ---- peer frames from JSON ----

type encodeCtx struct {
	sent func(pipe, lane uint64) uint64
}

func (c *encodeCtx) intField(v any) (uint64, bool) {
	if m, ok := v.(map[string]any); ok {
		if p, ok := m["$sent"]; ok {
			var lane uint64
			if l, ok := m["lane"]; ok {
				lane = num(l)
			}
			var plus uint64
			if pl, ok := m["plus"]; ok {
				plus = num(pl)
			}
			return c.sent(num(p), lane) + plus, true
		}
	}
	return num(v), false
}

func get(m map[string]any, k string) any { return m[k] }

func getU(m map[string]any, k string) uint64 {
	if v, ok := m[k]; ok && v != nil {
		return num(v)
	}
	return 0
}

func getB(m map[string]any, k string) bool {
	v, _ := m[k].(bool)
	return v
}

// encodeFrame appends a JSON frame (TRACES.md 6).
func (c *encodeCtx) encodeFrame(b []byte, f map[string]any) []byte {
	switch f["type"].(string) {
	case "PADDING":
		n := 1
		if v, ok := f["count"]; ok {
			n = int(num(v))
		}
		for range n {
			b = append(b, 0)
		}
		return b
	case "RAW":
		return append(b, hexb(f["bytes"])...)
	case "REPEAT":
		n := int(num(f["count"]))
		fr := f["frame"].(map[string]any)
		for range n {
			b = c.encodeFrame(b, fr)
		}
		return b
	case "STREAM":
		data := hexb(f["data"])
		off := getU(f, "off")
		hasOff := off > 0
		if v, ok := f["has_off"]; ok {
			hasOff = v.(bool)
		}
		hasLen := true
		if v, ok := f["has_len"]; ok {
			hasLen = v.(bool)
		}
		t := byte(0x08)
		if hasOff {
			t |= 4
		}
		if hasLen {
			t |= 2
		}
		if getB(f, "fin") {
			t |= 1
		}
		b = append(b, t)
		b = wire.AppendVarint(b, getU(f, "id"))
		if hasOff {
			b = wire.AppendVarint(b, off)
		}
		if hasLen {
			b = wire.AppendVarint(b, uint64(len(data)))
		}
		return append(b, data...)
	case "RESET_STREAM":
		return wire.AppendResetStream(b, getU(f, "id"), getU(f, "code"), getU(f, "final_size"))
	case "RESET_STREAM_AT":
		return wire.AppendResetStreamAt(b, getU(f, "id"), getU(f, "code"), getU(f, "final_size"), getU(f, "reliable_size"))
	case "STOP_SENDING":
		return wire.AppendStopSending(b, getU(f, "id"), getU(f, "code"))
	case "MAX_DATA":
		return wire.AppendMaxData(b, getU(f, "max"))
	case "MAX_STREAM_DATA":
		return wire.AppendMaxStreamData(b, getU(f, "id"), getU(f, "max"))
	case "MAX_STREAMS":
		return wire.AppendMaxStreams(b, getB(f, "bidi"), getU(f, "max"))
	case "DATA_BLOCKED":
		return wire.AppendDataBlocked(b, getU(f, "limit"))
	case "STREAM_DATA_BLOCKED":
		return wire.AppendStreamDataBlocked(b, getU(f, "id"), getU(f, "limit"))
	case "STREAMS_BLOCKED":
		return wire.AppendStreamsBlocked(b, getB(f, "bidi"), getU(f, "limit"))
	case "CONNECTION_CLOSE":
		var reason []byte
		if r, ok := f["reason"].(string); ok {
			reason = hexb(r)
		}
		return wire.AppendClose(b, getB(f, "app"), getU(f, "code"), getU(f, "frame_type"), reason)
	case "DATAGRAM":
		data := hexb(f["data"])
		hasLen := true
		if v, ok := f["has_len"]; ok {
			hasLen = v.(bool)
		}
		return append(wire.AppendDatagramHeader(b, len(data), hasLen), data...)
	case "QX_TRANSPORT_PARAMETERS":
		var block []byte
		for _, pv := range f["params"].([]any) {
			p := pv.(map[string]any)
			var id uint64
			name := ""
			switch x := p["id"].(type) {
			case string:
				name = x
				id = paramIDs[x]
			default:
				id = num(x)
			}
			var v []byte
			if raw, ok := p["raw"]; ok {
				v = hexb(raw)
			} else {
				v = encodeParamValue(name, p["value"])
			}
			block = wire.AppendVarint(block, id)
			block = wire.AppendVarint(block, uint64(len(v)))
			block = append(block, v...)
		}
		return wire.AppendTransportParams(b, block)
	case "QX_PING":
		return wire.AppendPing(b, getB(f, "response"), getU(f, "seq"))
	case "GOAWAY":
		return wire.AppendGoAway(b, getU(f, "max_bidi"), getU(f, "max_uni"), getU(f, "code"))
	case "PRIORITY_UPDATE":
		return wire.AppendPriorityUpdate(b, getU(f, "id"), getU(f, "seq"), uint8(getU(f, "urgency")), getB(f, "incremental"))
	case "PIPE_JOIN":
		dir := map[string]uint8{"up": 0, "down": 1, "bidi": 2}[f["direction"].(string)]
		j := wire.Join{Locator: hexb(f["locator"]), PipeID: getU(f, "pipe"), Dir: dir,
			CarrierBound: getB(f, "carrier_bound"), Standby: getB(f, "standby"),
			FateGroup: getU(f, "fate_group"), Priority: getU(f, "priority"), Token: hexb(f["token"])}
		return wire.AppendJoin(b, &j)
	case "PIPE_ACK":
		ents := f["entries"].([]any)
		b = wire.AppendPipeAckHeader(b, len(ents))
		for _, ev := range ents {
			e := ev.(map[string]any)
			pos, ph := c.intField(e["position"])
			b = wire.AppendVarint(b, getU(e, "pipe"))
			b = wire.AppendVarint(b, getU(e, "lane"))
			if ph {
				b = wire.AppendVarintN(b, pos, 4)
			} else {
				b = wire.AppendVarint(b, pos)
			}
			b = wire.AppendVarint(b, getU(e, "delay"))
		}
		return b
	case "PIPE_LANE":
		return wire.AppendPipeLane(b, hexb(f["lane_key"]), getU(f, "pipe"), getU(f, "lane"))
	case "PIPE_PING":
		return wire.AppendPipePing(b)
	case "PIPE_ABANDON":
		return wire.AppendPipeAbandon(b, getU(f, "pipe"), getU(f, "code"))
	case "PIPE_STATUS":
		st := map[string]uint64{"available": 0, "standby": 1}[f["status"].(string)]
		return wire.AppendPipeStatus(b, getU(f, "pipe"), getU(f, "seq"), st, getU(f, "priority"))
	}
	panic("unknown frame type " + f["type"].(string))
}

// encodeRecords encodes a list of frame lists as byte-framed records.
func (c *encodeCtx) encodeRecords(recs []any) []byte {
	var out []byte
	for _, r := range recs {
		body := c.encodeBody(r)
		out = wire.AppendVarint(out, uint64(len(body)))
		out = append(out, body...)
	}
	return out
}

func (c *encodeCtx) encodeBody(r any) []byte {
	var body []byte
	for _, fv := range r.([]any) {
		body = c.encodeFrame(body, fv.(map[string]any))
	}
	return body
}

// ---- decoded frames as JSON-like objects ----

var paramNames = func() map[uint64]string {
	m := map[uint64]string{}
	for k, v := range paramIDs {
		m[v] = k
	}
	return m
}()

// frameObj converts a decoded frame to the field map of TRACES.md 6.
func frameObj(f *wire.Frame) map[string]any {
	o := map[string]any{"type": f.Kind.String()}
	switch f.Kind {
	case wire.KindPadding:
		o["count"] = uint64(f.Count)
	case wire.KindStream:
		o["id"], o["off"], o["data"], o["fin"] = f.StreamID, f.Offset, hex.EncodeToString(f.Data), f.Fin
		o["has_off"], o["has_len"] = f.HasOff, f.HasLen
	case wire.KindResetStream:
		o["id"], o["code"], o["final_size"] = f.StreamID, f.Code, f.FinalSize
	case wire.KindResetStreamAt:
		o["id"], o["code"], o["final_size"], o["reliable_size"] = f.StreamID, f.Code, f.FinalSize, f.Reliable
	case wire.KindStopSending:
		o["id"], o["code"] = f.StreamID, f.Code
	case wire.KindMaxData:
		o["max"] = f.Value
	case wire.KindMaxStreamData:
		o["id"], o["max"] = f.StreamID, f.Value
	case wire.KindMaxStreams:
		o["bidi"], o["max"] = f.Bidi, f.Value
	case wire.KindDataBlocked:
		o["limit"] = f.Value
	case wire.KindStreamDataBlocked:
		o["id"], o["limit"] = f.StreamID, f.Value
	case wire.KindStreamsBlocked:
		o["bidi"], o["limit"] = f.Bidi, f.Value
	case wire.KindClose:
		o["app"], o["code"], o["reason"] = f.App, f.Code, hex.EncodeToString(f.Data)
		if !f.App {
			o["frame_type"] = f.ErrFrame
		}
	case wire.KindDatagram:
		o["data"], o["has_len"] = hex.EncodeToString(f.Data), f.HasLen
	case wire.KindTransportParams:
		o["block"] = f.Data
	case wire.KindPing:
		o["response"], o["seq"] = f.Response, f.Value
	case wire.KindGoAway:
		o["max_bidi"], o["max_uni"], o["code"] = f.MaxBidi, f.MaxUni, f.Code
	case wire.KindPriorityUpdate:
		o["id"], o["seq"], o["urgency"], o["incremental"] = f.StreamID, f.Seq, uint64(f.Urgency), f.Incr
	case wire.KindPipeJoin:
		j := &f.Join
		o["locator"], o["pipe"] = hex.EncodeToString(j.Locator), j.PipeID
		o["direction"] = []string{"up", "down", "bidi"}[j.Dir]
		o["carrier_bound"], o["standby"], o["fate_group"], o["priority"] = j.CarrierBound, j.Standby, j.FateGroup, j.Priority
		o["token"] = hex.EncodeToString(j.Token)
	case wire.KindPipeAck:
		var ents []any
		it := f.Iter()
		for e, ok := it.Next(); ok; e, ok = it.Next() {
			ents = append(ents, map[string]any{"pipe": e.PipeID, "lane": e.Lane, "position": e.Position, "delay": e.Delay})
		}
		o["entries"] = ents
	case wire.KindPipeLane:
		o["lane_key"], o["pipe"], o["lane"] = hex.EncodeToString(f.LaneKey), f.PipeID, f.Lane
	case wire.KindPipeAbandon:
		o["pipe"], o["code"] = f.PipeID, f.Code
	case wire.KindPipeStatus:
		o["pipe"], o["seq"], o["priority"] = f.PipeID, f.Seq, f.Priority
		o["status"] = []string{"available", "standby"}[f.Status]
	}
	return o
}

// matchValue matches one pattern field (TRACES.md 5.6).
func matchValue(pat, got any) bool {
	if s, ok := pat.(string); ok && s == "*" {
		return true
	}
	switch p := pat.(type) {
	case map[string]any:
		if l, ok := p["len"]; ok {
			gs, _ := got.(string)
			return uint64(len(gs)/2) == num(l)
		}
		if pr, ok := p["prefix"]; ok {
			gs, _ := got.(string)
			ps := pr.(string)
			return len(gs) >= len(ps) && gs[:len(ps)] == ps
		}
		if _, ok := p["min"]; ok || p["max"] != nil {
			g := num(got)
			if mn, ok := p["min"]; ok && g < num(mn) {
				return false
			}
			if mx, ok := p["max"]; ok && g > num(mx) {
				return false
			}
			return true
		}
		return false
	case string:
		gs, ok := got.(string)
		return ok && gs == p
	case bool:
		gb, ok := got.(bool)
		return ok && gb == p
	case float64, json.Number:
		switch g := got.(type) {
		case uint64:
			return g == num(p)
		case bool:
			return false
		}
		return false
	case nil:
		return got == nil
	case []any:
		ga, ok := got.([]any)
		if !ok || len(ga) != len(p) {
			return false
		}
		for i := range p {
			pm, ok1 := p[i].(map[string]any)
			gm, ok2 := ga[i].(map[string]any)
			if ok1 && ok2 {
				for k, v := range pm {
					if !matchValue(v, gm[k]) {
						return false
					}
				}
				continue
			}
			if !matchValue(p[i], ga[i]) {
				return false
			}
		}
		return true
	}
	return false
}

func typeMatches(pat any, got string) bool {
	switch p := pat.(type) {
	case string:
		return p == "*" || p == got
	case map[string]any:
		for _, x := range p["oneof"].([]any) {
			if x.(string) == got {
				return true
			}
		}
	}
	return false
}

// matchFrame matches a frame pattern against a decoded frame object.
func matchFrame(pat map[string]any, got map[string]any) bool {
	if !typeMatches(pat["type"], got["type"].(string)) {
		return false
	}
	for k, v := range pat {
		switch k {
		case "type", "count":
			continue
		case "params_include", "params_exclude", "grease", "params":
			if !matchParams(pat, got) {
				return false
			}
			continue
		}
		if k == "data" && got["type"] == "STREAM" {
			if !matchValue(v, got["data"]) {
				return false
			}
			continue
		}
		if !matchValue(v, got[k]) {
			return false
		}
	}
	return true
}

func matchParams(pat, got map[string]any) bool {
	block, _ := got["block"].([]byte)
	type pv struct {
		id uint64
		v  []byte
	}
	var ps []pv
	b := block
	for len(b) > 0 {
		id, n := wire.ReadVarint(b)
		l, m := wire.ReadVarint(b[n:])
		ps = append(ps, pv{id, b[n+m : n+m+int(l)]})
		b = b[n+m+int(l):]
	}
	find := func(id uint64) ([]byte, bool) {
		for _, p := range ps {
			if p.id == id {
				return p.v, true
			}
		}
		return nil, false
	}
	if inc, ok := pat["params_include"].(map[string]any); ok {
		for name, v := range inc {
			id, ok := paramIDs[name]
			if !ok {
				id = num(json.Number(name))
			}
			val, ok := find(id)
			if !ok {
				return false
			}
			if s, ok := v.(string); ok && s == "*" {
				continue
			}
			want := encodeParamValue(name, v)
			if string(want) != string(val) {
				return false
			}
		}
	}
	if exc, ok := pat["params_exclude"].([]any); ok {
		for _, n := range exc {
			if _, ok := find(paramIDs[n.(string)]); ok {
				return false
			}
		}
	}
	if g, ok := pat["grease"].(bool); ok && g {
		found := false
		for _, p := range ps {
			if wire.IsGreaseParam(p.id) {
				found = true
			}
		}
		if !found {
			return false
		}
	}
	return true
}

var _ = math.MaxInt64
