package conformance

import (
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/zmuxio/zmux-go/internal/core"
	"github.com/zmuxio/zmux-go/internal/wire"
)

func errResult(err error) map[string]any {
	var se *core.StreamError
	var sess *core.SessionError
	var tl *core.DatagramTooLargeError
	switch {
	case errors.As(err, &se):
		k := map[core.StreamErrKind]string{core.StreamReset: "stream_reset", core.StreamStopped: "stream_stopped",
			core.StreamClosedLocally: "closed_locally", core.StreamRejected: "stream_rejected",
			core.StreamGoingAway: "going_away"}[se.Kind]
		return map[string]any{"error": map[string]any{"kind": k, "code": se.Code, "remote": se.Remote}}
	case errors.As(err, &sess):
		o := errorObj(sess)
		o["kind"] = "session"
		return map[string]any{"error": o}
	case errors.As(err, &tl):
		return map[string]any{"error": map[string]any{"kind": "too_large"}}
	case errors.Is(err, core.ErrNotNegotiated), errors.Is(err, core.ErrInvalid):
		return map[string]any{"error": map[string]any{"kind": "unsupported"}}
	}
	return map[string]any{"error": map[string]any{"kind": err.Error()}}
}

func (r *runner) stream(args map[string]any) (*core.Stream, error) {
	var st *core.Stream
	switch n := args["stream"].(type) {
	case string:
		st = r.streams[n]
	default:
		st = r.s.StreamByID(num(n))
	}
	if st == nil {
		return nil, fmt.Errorf("unknown stream %v", args["stream"])
	}
	return st, nil
}

func (r *runner) api(st map[string]any) error {
	call := st["call"].(string)
	args, _ := st["args"].(map[string]any)
	if args == nil {
		args = map[string]any{}
	}
	s := r.s
	s.SetNow(r.now)
	res := map[string]any{"ok": true}
	var err error
	switch call {
	case "open_bidi", "open_uni":
		var h *core.Stream
		h, err = s.OpenStream(call == "open_bidi")
		if err == nil {
			if oi, ok := args["open_info"].(string); ok {
				err = s.SetOpenInfo(h, hexb(oi))
			}
			if p, ok := args["priority"].(map[string]any); ok && err == nil {
				err = s.SetPeerPriority(h, uint8(num(p["urgency"])), getB(p, "incremental"))
			}
			if b, ok := st["bind"].(string); ok {
				r.streams[b] = h
			}
		}
	case "accept_bidi", "accept_uni":
		h, ok := s.AcceptStream(call == "accept_bidi")
		if !ok {
			res = map[string]any{"pending": true}
			break
		}
		if b, ok := st["bind"].(string); ok {
			r.streams[b] = h
		}
		id, _ := h.StreamID()
		res = map[string]any{"stream": id}
		if pp, ok := h.PeerPreamble(); ok {
			if pp.HasOpenInfo {
				res["open_info"] = hex.EncodeToString(pp.OpenInfo)
			}
			if pp.HasPriority {
				res["priority"] = map[string]any{"urgency": uint64(pp.Urgency), "incremental": pp.Incremental}
			}
			res["pipe_carrier"] = pp.PipeCarrier
			tl := make([]any, 0, len(pp.App))
			for _, t := range pp.App {
				tl = append(tl, map[string]any{"type": t.Type, "value": hex.EncodeToString(t.Value)})
			}
			res["app_tlvs"] = tl
		} else {
			res["pipe_carrier"] = false
			res["app_tlvs"] = []any{}
		}
	case "write":
		var h *core.Stream
		if h, err = r.stream(args); err != nil {
			return err
		}
		var data []byte
		if d, ok := args["data"].(string); ok {
			data = hexb(d)
		} else if f, ok := args["fill"].(map[string]any); ok {
			data = make([]byte, num(f["len"]))
			for i := range data {
				data[i] = byte(num(f["byte"]))
			}
		}
		var n int
		n, err = s.Write(h, data, getB(args, "fin"))
		if err == nil && getB(args, "fin") && n == len(data) && len(data) == 0 {
			err = s.Finish(h)
		}
		res = map[string]any{"accepted": uint64(n)}
	case "stream_id":
		h, e := r.stream(args)
		if e != nil {
			return e
		}
		var id uint64
		id, err = s.PinID(h)
		res = map[string]any{"stream": id}
	case "set_reliable_boundary":
		h, e := r.stream(args)
		if e != nil {
			return e
		}
		s.SetReliableBoundary(h)
	case "finish":
		h, e := r.stream(args)
		if e != nil {
			return e
		}
		err = s.Finish(h)
	case "reset":
		h, e := r.stream(args)
		if e != nil {
			return e
		}
		s.Reset(h, getU(args, "code"))
	case "stop":
		h, e := r.stream(args)
		if e != nil {
			return e
		}
		s.Stop(h, getU(args, "code"))
	case "read":
		h, e := r.stream(args)
		if e != nil {
			return e
		}
		buf := make([]byte, getU(args, "max"))
		var n int
		var fin bool
		n, fin, err = s.Read(h, buf)
		if err == nil {
			if n == 0 && !fin {
				res = map[string]any{"pending": true}
			} else {
				res = map[string]any{"data": hex.EncodeToString(buf[:n]), "fin": fin}
			}
		}
	case "release":
		h, e := r.stream(args)
		if e != nil {
			return e
		}
		s.Release(h)
	case "reject":
		h, e := r.stream(args)
		if e != nil {
			return e
		}
		s.Reject(h)
	case "set_priority":
		h, e := r.stream(args)
		if e != nil {
			return e
		}
		s.SetPriority(h, uint8(getU(args, "urgency")), getB(args, "incremental"))
	case "set_peer_priority":
		h, e := r.stream(args)
		if e != nil {
			return e
		}
		err = s.SetPeerPriority(h, uint8(getU(args, "urgency")), getB(args, "incremental"))
	case "send_datagram":
		var h *core.Stream
		if _, ok := args["stream"]; ok {
			if h, err = r.stream(args); err != nil {
				return err
			}
		}
		err = s.SendDatagram(h, getU(args, "context"), hexb(args["data"]))
	case "recv_datagram":
		if _, ok := args["stream"]; ok {
			h, e := r.stream(args)
			if e != nil {
				return e
			}
			ctx, d, ok := s.RecvStreamDatagram(h)
			if ok {
				res = map[string]any{"data": hex.EncodeToString(d), "context": ctx}
			} else {
				res = map[string]any{"pending": true}
			}
			break
		}
		d, ok := s.RecvDatagram()
		if ok {
			res = map[string]any{"data": hex.EncodeToString(d)}
		} else {
			res = map[string]any{"pending": true}
		}
	case "ping":
		_, err = s.Ping(nil)
	case "shutdown":
		s.Shutdown(getU(args, "code"))
	case "close":
		var reason []byte
		if rs, ok := args["reason"].(string); ok {
			reason = hexb(rs)
		}
		s.Close(getU(args, "code"), reason)
	case "add_pipe":
		res, err = r.addPipe(args)
	case "set_pipe_status":
		var o core.PipeSettings
		u := func(k string) *uint64 {
			if v, ok := args[k]; ok && v != nil {
				x := num(v)
				return &x
			}
			return nil
		}
		b := func(k string) *bool {
			if v, ok := args[k]; ok && v != nil {
				x := v.(bool)
				return &x
			}
			return nil
		}
		o.SendPriority, o.RecvPriority, o.Weight = u("send_priority"), u("receive_priority"), u("weight")
		o.SendStandby, o.RecvStandby = b("send_standby"), b("receive_standby")
		if v, ok := args["probation"]; ok && v != nil {
			d := ms(v)
			o.Probation = &d
		}
		err = s.SetPipe(getU(args, "pipe"), o)
	case "set_scheduler":
		pol := policyByName[args["policy"].(string)]
		if sn, ok := args["stream"].(string); ok {
			s.SetStreamPolicy(r.streams[sn], pol, false)
		} else {
			s.SetPolicy(pol)
		}
	case "set_redundant":
		var on *bool
		if v, ok := args["redundant"]; ok && v != nil {
			x := v.(bool)
			on = &x
		}
		s.SetRedundant(r.streams[args["stream"].(string)], on)
	case "revalidate_pipes":
		if v, ok := args["pipe"]; ok && v != nil {
			err = s.RevalidatePipes(num(v), false, r.now)
		} else {
			err = s.RevalidatePipes(0, true, r.now)
		}
	case "remove_pipe":
		err = s.RemovePipe(getU(args, "pipe"), getB(args, "graceful"))
	default:
		r.t.Skip("api " + call)
	}
	if err != nil {
		res = errResult(err)
	}
	r.drainEvents()
	if exp, ok := st["expect"].(map[string]any); ok {
		if !matchObj(exp, res) {
			return fmt.Errorf("result %v, want %v", res, exp)
		}
	}
	return nil
}

var _ = wire.MaxVarint
