package conformance

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"sort"
	"strings"
	"testing"

	"github.com/zmuxio/zmux-go/internal/core"
	"github.com/zmuxio/zmux-go/internal/wire"
)

// specDir holds the specification's fixtures (testdata/spec/SOURCE names
// their commit); localDir, when present, traces of local behaviour that
// the specification leaves to implementations.
const (
	specDir  = "../../testdata/spec/fixtures"
	localDir = "../../testdata/local/traces"

	specTraces  = 321
	localTraces = 49
)

type profile struct {
	TP     map[string]any `json:"tp"`
	Config map[string]any `json:"config"`
	Test   map[string]any `json:"test"`
}

type header struct {
	ID        string   `json:"id"`
	Profile   string   `json:"profile"`
	LocalRole string   `json:"local_role"`
	Overrides profile  `json:"overrides"`
	Groups    []string `json:"groups"`
}

func decodeJSON(b []byte, v any) error {
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	return d.Decode(v)
}

func loadProfiles(t *testing.T) map[string]profile {
	b, err := os.ReadFile(filepath.Join(specDir, "profiles.json"))
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]profile
	if err := decodeJSON(b, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

// enabled lists the trace id prefixes the implementation passes so far.
var enabled = []string{"core.", "ext.", "compat.", "findings.", "multipath.", "resume."}

var skipped = map[string]string{}

var policyByName = map[string]core.Policy{"affinity": core.PolicyAffinity, "session-affinity": core.PolicySessionAffinity,
	"spread-eta": core.PolicySpreadETA, "spread-round-robin": core.PolicySpreadRoundRobin,
	"spread-rate-weighted": core.PolicySpreadRateWeighted}

// traceFiles lists the traces under dir, sorted.
func traceFiles(dir string) []string {
	var files []string
	_ = filepath.WalkDir(dir, func(p string, _ os.DirEntry, _ error) error {
		if strings.HasSuffix(p, ".ndjson") {
			files = append(files, p)
		}
		return nil
	})
	sort.Strings(files)
	return files
}

func TestTraces(t *testing.T) {
	profiles := loadProfiles(t)
	files := traceFiles(filepath.Join(specDir, "traces"))
	if len(files) != specTraces {
		t.Errorf("found %d specification traces, want %d", len(files), specTraces)
	}
	if _, err := os.Stat(localDir); err == nil {
		local := traceFiles(localDir)
		if len(local) != localTraces {
			t.Errorf("found %d local traces, want %d", len(local), localTraces)
		}
		files = append(files, local...)
	}
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		lines := bytes.Split(bytes.TrimSpace(raw), []byte("\n"))
		var h header
		if err := decodeJSON(lines[0], &h); err != nil {
			t.Fatal(err)
		}
		t.Run(h.ID, func(t *testing.T) {
			on := false
			for _, p := range enabled {
				if strings.HasPrefix(h.ID, p) {
					on = true
				}
			}
			if why, ok := skipped[h.ID]; ok || !on {
				t.Skip("not enabled: " + why)
			}
			defer func() {
				if p := recover(); p != nil {
					t.Fatalf("panic: %v\n%s", p, debug.Stack())
				}
			}()
			r := newRunner(t, h, profiles)
			for i, l := range lines[1:] {
				var st map[string]any
				if err := decodeJSON(l, &st); err != nil {
					t.Fatal(err)
				}
				if err := r.step(st); err != nil {
					t.Fatalf("step %d (%s): %v\n%s", i+1, st["op"], err, l)
				}
			}
			if e := r.s.Err(); e != nil && !r.closeCovered {
				t.Fatalf("session closed without expect_close: %v", e)
			}
		})
	}
}

type rlane struct {
	in      []byte // unconsumed inbound bytes
	frames  []map[string]any
	first   []map[string]any
	recIdx  []int
	recs    int
	written uint64
	closed  string // "", "graceful", "abort"
}

type rpipe struct {
	id       uint64
	lanes    map[uint64]*rlane
	framing  core.Framing
	in, out  bool
	writable bool
}

func (p *rpipe) lane(l uint64) *rlane {
	if p.lanes[l] == nil {
		p.lanes[l] = &rlane{}
	}
	return p.lanes[l]
}

type rcarrier struct {
	name       string
	via        string
	dir        string
	binding    []byte
	pendJoin   *wire.Join
	pendMac    []byte
	conn       string
	buf        []byte
	pipe       uint64
	lane       uint64
	source     string // listener slot held until the first record
	deadline   core.Time
	framing    core.Framing
	held       bool
	pendInband bool
	attached   bool
	rejected   bool
}

type runner struct {
	pending      core.PendingCarriers
	pairing      core.SplitPairing
	carriers     map[string]*rcarrier
	held         []*rcarrier
	t            *testing.T
	h            header
	s            *core.Session
	now          core.Time
	streams      map[string]*core.Stream
	pipes        map[uint64]*rpipe
	events       []core.Event
	evSeen       map[string]int
	closeCovered bool
	closesSent   []map[string]any
	b            core.Batch
	enc          encodeCtx
	stepNo       any
}

func merge(dst, src map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range dst {
		out[k] = v
	}
	for k, v := range src {
		out[k] = v
	}
	return out
}

func ms(v any) core.Duration { return core.Duration(num(v)) * core.Millisecond }

func newRunner(t *testing.T, h header, profiles map[string]profile) *runner {
	p := profiles[h.Profile]
	tp := merge(p.TP, h.Overrides.TP)
	cf := merge(p.Config, h.Overrides.Config)
	// The test-mode sender (TRACES.md 3.4): the ETA choice, no probation,
	// no redundant copies, standby pipes probed at 15 s.
	switch cf["sender"] {
	case nil:
	case "test-mode":
		for k, v := range map[string]any{"scheduler": "spread-eta", "probation": json.Number("0"),
			"redundancy": "off", "standby_probe": json.Number("0")} {
			if _, ok := cf[k]; !ok {
				cf[k] = v
			}
		}
	default:
		t.Fatalf("unknown sender %v", cf["sender"])
	}
	th := merge(p.Test, h.Overrides.Test)
	cfg := core.DefaultConfig()
	cfg.Local = paramsFromMap(tp)
	cfg.Test.Enabled = true
	if g, ok := th["grease_tp"].(map[string]any); ok {
		cfg.Local.GreaseID = num(g["id"])
		cfg.Local.GreaseValue = hexb(g["value"])
	}
	if g, ok := th["grease_tlv"].(map[string]any); ok {
		cfg.Test.GreaseTLV = &wire.TLV{Type: num(g["type"]), Value: hexb(g["value"])}
	}
	if v, ok := th["ping_seq_start"]; ok {
		cfg.Test.PingSeqStart = num(v)
	}
	if v, ok := th["rtt_ms"]; ok {
		cfg.Test.RTT = ms(v)
	}
	if m, ok := th["pipe_rtt_ms"].(map[string]any); ok {
		cfg.Test.PipeRTT = map[uint64]core.Duration{}
		for k, v := range m {
			cfg.Test.PipeRTT[num(json.Number(k))] = ms(v)
		}
	}
	if v, ok := th["pipe_cap"]; ok {
		cfg.Test.PipeCap = int(num(v))
	}
	if v, ok := th["role_nonce"]; ok {
		cfg.Local.RoleNonce = num(v)
	}
	if v, ok := th["session_nonce"]; ok {
		copy(cfg.Local.SessionNonce[:], hexb(v))
	}
	if v, ok := th["lane_key"]; ok && cfg.Local.Has&wire.HasLanes != 0 {
		copy(cfg.Local.LaneKey[:], hexb(v))
	}
	if h.LocalRole == "responder" {
		cfg.Role, cfg.TransportRole = core.RoleResponder, core.RoleResponder
	} else {
		cfg.Role, cfg.TransportRole = core.RoleInitiator, core.RoleInitiator
	}
	for k, v := range cf {
		switch k {
		case "keep_alive_period":
			cfg.KeepAlive = ms(v)
		case "handshake_timeout":
			cfg.HandshakeTimeout = ms(v)
		case "max_incoming_uni_streams":
			cfg.StagedUni = v == nil
		case "enable_datagrams":
			cfg.Datagrams = v.(bool)
		case "role":
			cfg.Role = map[string]core.Role{"auto": core.RoleAuto, "initiator": core.RoleInitiator,
				"responder": core.RoleResponder}[v.(string)]
		case "max_pipes":
			cfg.MaxPipes = int(num(v))
		case "resume_timeout":
			cfg.Resume = ms(v)
		case "probation":
			if v != nil {
				cfg.Probation = ms(v)
			}
		case "suspect_floor":
			if v != nil {
				cfg.SuspectFloor = ms(v)
			}
		case "dead_timeout":
			if v != nil {
				cfg.DeadTimeout = ms(v)
			}
		case "overflow_delay":
			if v != nil {
				cfg.OverflowDelay = ms(v)
			}
		case "standby_probe":
			if v != nil {
				cfg.StandbyProbe = ms(v)
			}
		case "redundancy":
			cfg.Redundancy = v.(string) == "urgent"
		case "scheduler":
			cfg.Policy = policyByName[v.(string)]
		case "pipe_auth":
			m := v.(map[string]any)
			if ps, ok := m["psk"]; ok {
				cfg.PSK = hexb(ps)
			}
		case "early_data":
			m := v.(map[string]any)
			cfg.EarlyZmuxPeer, _ = m["zmux_peer"].(bool)
			cfg.EarlyReplayable, _ = m["replayable"].(bool)
		}
	}
	r := &runner{t: t, h: h, streams: map[string]*core.Stream{}, pipes: map[uint64]*rpipe{}, evSeen: map[string]int{},
		carriers: map[string]*rcarrier{}}
	r.s = core.New(cfg, 0)
	r.enc.sent = func(pipe, lane uint64) uint64 {
		if p := r.pipes[pipe]; p != nil {
			return p.lane(lane).written
		}
		return 0
	}
	return r
}

func (r *runner) step(st map[string]any) error {
	op := st["op"].(string)
	r.stepNo = st["step"]
	switch op {
	case "note":
		return nil
	case "pipe_up":
		return r.pipeUp(st)
	case "pipe_down":
		return r.pipeDown(st)
	case "set_writable":
		p := r.pipes[num(st["pipe"])]
		p.writable = st["writable"].(bool)
		for ln := range p.lanes {
			r.s.SetWritable(p.id, ln, p.writable, r.now)
		}
		return nil
	case "peer_send":
		return r.peerSend(st)
	case "api":
		return r.api(st)
	case "advance":
		return r.advance(ms(st["ms"]))
	}
	r.flush()
	switch op {
	case "expect_frames":
		return r.expectFrames(st)
	case "expect_no_frames":
		return r.expectNoFrames(st)
	case "expect_close":
		return r.expectClose(st)
	case "expect_open":
		if r.s.State() >= core.StateClosing {
			return fmt.Errorf("session is %v: %v", r.s.State(), r.s.Err())
		}
		return nil
	case "expect_state":
		return r.expectState(st)
	case "expect_event":
		return r.expectEvent(st, true)
	case "expect_no_event":
		return r.expectEvent(st, false)
	case "expect_pipe_closed":
		return r.expectPipeClosed(st)
	case "expect_reject_carrier":
		c := r.carriers[st["carrier"].(string)]
		if c == nil || !c.rejected {
			return fmt.Errorf("carrier not rejected")
		}
		return nil
	}
	return fmt.Errorf("unknown op %s", op)
}

func (r *runner) pipeUp(st map[string]any) error {
	via, _ := st["via"].(string)
	conn, _ := st["conn"].(string)
	if via == "lane" {
		name := st["carrier"].(string)
		r.carriers[name] = &rcarrier{name: name, conn: conn, via: via}
		return nil
	}
	if via == "listener" {
		return r.listenerUp(st)
	}
	if via != "" && via != "bootstrap" {
		r.t.Skip("pipe_up via " + via)
	}
	id := num(st["pipe"])
	p := &rpipe{id: id, lanes: map[uint64]*rlane{}, writable: true}
	if f, _ := st["framing"].(string); f == "records" {
		p.framing = core.FramingRecords
	}
	dir := st["dir"].(string)
	p.in, p.out = dir != "out", dir != "in"
	r.pipes[id] = p
	ci := core.CarrierInfo{QueueBound: true, Framing: p.framing, MinRTT: true, Lanes: conn != ""}
	r.s.AttachBootstrap(ci, ci, nil)
	if conn != "" {
		r.s.SetCarrierConn(id, conn)
	}
	r.drainEvents()
	return nil
}

func (r *runner) pipeDown(st map[string]any) error {
	if cn, ok := st["carrier"].(string); ok {
		c := r.carriers[cn]
		if c == nil || !c.attached {
			return nil
		}
		r.s.PipeEnded(c.pipe, c.lane, st["how"].(string) == "eof", len(c.buf) > 0, r.now)
		r.drainEvents()
		return nil
	}
	id := num(st["pipe"])
	lane := getU(st, "lane")
	dir := st["dir"].(string)
	how := st["how"].(string)
	if dir == "in" || dir == "bidi" {
		l := r.pipes[id].lane(lane)
		partial := len(l.in) > 0
		r.s.PipeEnded(id, lane, how == "eof", partial, r.now)
	}
	if dir == "out" || dir == "bidi" {
		r.s.WriteDone(&core.Batch{Pipe: id, Lane: lane}, errors.New("carrier failed"), r.now)
	}
	r.drainEvents()
	return nil
}

func (r *runner) peerSend(st map[string]any) error {
	if cn, ok := st["carrier"].(string); ok {
		c := r.carriers[cn]
		if c != nil && c.via == "listener" {
			return r.listenerSend(c, st)
		}
		return r.carrierSend(c, st)
	}
	id := num(st["pipe"])
	p := r.pipes[id]
	if p == nil {
		return fmt.Errorf("no pipe %d", id)
	}
	if msgs, ok := st["messages"].([]any); ok || st["messages_hex"] != nil {
		var list [][]byte
		if ok {
			for _, m := range msgs {
				list = append(list, r.enc.encodeBody(m))
			}
		} else {
			for _, m := range st["messages_hex"].([]any) {
				list = append(list, hexb(m))
			}
		}
		for _, m := range list {
			b := &core.Buf{B: m}
			r.s.ReceiveMessage(id, 0, b, m, r.now)
		}
		r.drainEvents()
		return nil
	}
	defer r.retryHeld()
	var data []byte
	if hx, ok := st["hex"].(string); ok {
		data = hexb(hx)
	} else {
		data = r.enc.encodeRecords(st["records"].([]any))
	}
	chunks := []int{len(data)}
	if c, ok := st["chunks"].([]any); ok {
		chunks = nil
		for _, x := range c {
			chunks = append(chunks, int(num(x)))
		}
	}
	l := p.lane(0)
	for _, c := range chunks {
		if c > len(data) {
			c = len(data)
		}
		l.in = append(l.in, data[:c]...)
		data = data[c:]
		buf := &core.Buf{B: l.in}
		buf.Ref()
		n := r.s.Receive(id, 0, buf, l.in, r.now)
		l.in = append([]byte(nil), l.in[n:]...)
		r.drainEvents()
	}
	if len(data) > 0 {
		l.in = append(l.in, data...)
		buf := &core.Buf{B: l.in}
		n := r.s.Receive(id, 0, buf, l.in, r.now)
		l.in = append([]byte(nil), l.in[n:]...)
		r.drainEvents()
	}
	return nil
}

func (r *runner) advance(d core.Duration) error {
	target := r.now.Add(d)
	for {
		r.flush()
		nt := r.s.NextTimeout()
		if nt > target {
			break
		}
		if nt > r.now {
			r.now = nt
		}
		r.s.HandleTimeout(r.now)
		r.drainEvents()
		if r.s.NextTimeout() <= r.now {
			// A timer that does not move would loop; let one flush happen.
			r.flush()
			if r.s.NextTimeout() <= r.now {
				break
			}
		}
	}
	r.now = target
	r.s.HandleTimeout(r.now)
	r.drainEvents()
	for _, c := range r.carriers {
		if c.source != "" && !c.rejected && r.now >= c.deadline {
			r.pending.Release(c.source)
			c.source = ""
			c.rejected = true
		}
	}
	return nil
}

func (r *runner) drainEvents() {
	for {
		e, ok := r.s.PollEvent()
		if !ok {
			break
		}
		r.events = append(r.events, e)
	}
	r.s.TakeWakes(nil)
	for _, a := range r.s.TakeActions(nil) {
		p := r.pipes[a.Pipe]
		if p == nil {
			continue
		}
		switch a.Kind {
		case core.ActShutdown:
			if l := p.lane(a.Lane); l.closed == "" {
				l.closed = "graceful"
			}
		case core.ActAbort:
			for _, l := range p.lanes {
				if l.closed == "" {
					l.closed = "abort"
				}
			}
			if len(p.lanes) == 0 {
				p.lane(0).closed = "abort"
			}
		case core.ActOpenLane:
			r.s.LaneOpened(a.Pipe, core.CarrierInfo{QueueBound: true, MinRTT: true, Lanes: true}, nil)
		case core.ActResetLane:
			if l := p.lane(a.Lane); l.closed == "" {
				l.closed = "abort"
			}
		case core.ActClose:
			for _, l := range p.lanes {
				if l.closed == "" {
					l.closed = "graceful"
				}
			}
		}
	}
}

// flush polls every writable outbound lane until nothing comes out.
func (r *runner) flush() {
	for i := 0; i < 64; i++ {
		var lanes []uint64
		matched := false
		ids := make([]uint64, 0, len(r.pipes))
		for id := range r.pipes {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(a, b int) bool { return ids[a] < ids[b] })
		for _, id := range ids {
			p := r.pipes[id]
			if !p.out || !p.writable {
				continue
			}
			lanes = r.s.OutLanes(id, lanes[:0])
			for _, ln := range lanes {
				for r.s.PollTransmit(id, ln, &r.b, core.DefaultBatch, r.now) {
					matched = true
					l := p.lane(ln)
					if l.closed != "" {
						r.t.Errorf("pipe %d lane %d: a batch after the lane was closed (%s)", id, ln, l.closed)
					}
					shut := r.b.Shutdown
					r.capture(p, ln)
					if shut && l.closed == "" {
						l.closed = "graceful"
					}
					r.s.WriteDone(&r.b, nil, r.now)
					r.drainEvents()
				}
			}
		}
		r.drainEvents()
		if !matched {
			return
		}
	}
	r.t.Fatalf("flush does not converge")
}

func (r *runner) capture(p *rpipe, lane uint64) {
	l := p.lane(lane)
	var recs [][]byte
	if p.framing == core.FramingRecords {
		recs = r.b.Messages()
		for _, m := range recs {
			l.written += uint64(wire.VarintLen(uint64(len(m))) + len(m))
		}
	} else {
		data := r.b.Flat()
		l.written += uint64(len(data))
		for len(data) > 0 {
			size, n := wire.ReadVarint(data)
			if n == 0 || uint64(len(data)-n) < size {
				r.t.Fatalf("local endpoint wrote a partial record")
			}
			recs = append(recs, data[n:n+int(size)])
			data = data[n+int(size):]
		}
	}
	for _, rec := range recs {
		l.recs++
		var f wire.Frame
		for len(rec) > 0 {
			n, err := wire.DecodeFrame(rec, &f)
			if err != nil {
				r.t.Fatalf("local endpoint wrote an undecodable frame: %v", err)
			}
			o := frameObj(&f)
			if os.Getenv("ZTRACE") != "" {
				j, _ := json.Marshal(o)
				if d, ok := o["data"].(string); ok && len(d) > 40 {
					o2 := map[string]any{}
					for k, v := range o {
						o2[k] = v
					}
					o2["data"] = fmt.Sprintf("<%d bytes>", len(d)/2)
					j, _ = json.Marshal(o2)
				}
				if len(j) > 200 {
					j = j[:200]
				}
				r.t.Logf("step %v pipe %d lane %d t=%d: %s", r.stepNo, p.id, lane, r.now/1e6, j)
			}
			if f.Kind == wire.KindClose {
				r.closesSent = append(r.closesSent, o)
			}
			l.frames = append(l.frames, o)
			l.recIdx = append(l.recIdx, l.recs)
			if len(l.first) < 64 {
				l.first = append(l.first, o)
			}
			rec = rec[n:]
		}
	}
}

func (r *runner) lanesFor(st map[string]any) []*rlane {
	lane := getU(st, "lane")
	if s, ok := st["pipe"].(string); ok && s == "*" {
		var out []*rlane
		ids := make([]uint64, 0)
		for id := range r.pipes {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(a, b int) bool { return ids[a] < ids[b] })
		for _, id := range ids {
			for _, l := range r.pipes[id].lanes {
				out = append(out, l)
			}
		}
		return out
	}
	p := r.pipes[num(st["pipe"])]
	if p == nil {
		return nil
	}
	return []*rlane{p.lane(lane)}
}

func dumpFrames(fs []map[string]any) string {
	var b strings.Builder
	for _, f := range fs {
		j, _ := json.Marshal(f)
		b.Write(j)
		b.WriteByte('\n')
	}
	return b.String()
}

func (r *runner) expectFrames(st map[string]any) error {
	lanes := r.lanesFor(st)
	var frames []map[string]any
	var recIdx []int
	for _, l := range lanes {
		frames = append(frames, l.frames...)
		recIdx = append(recIdx, l.recIdx...)
	}
	mode := st["match"].(string)
	if mode == "first" {
		frames = nil
		for _, l := range lanes {
			frames = append(frames, l.first...)
		}
	}
	var pats []map[string]any
	for _, p := range st["frames"].([]any) {
		pats = append(pats, p.(map[string]any))
	}
	err := matchFrames(mode, pats, nonPadding(frames, pats))
	if err == nil && getB(st, "one_credit_per_record") {
		err = oneCreditPerRecord(frames, recIdx)
	}
	for _, l := range lanes {
		l.frames, l.recIdx = nil, nil
	}
	if err != nil {
		return fmt.Errorf("%v\nframes:\n%s", err, dumpFrames(frames))
	}
	return nil
}

func oneCreditPerRecord(frames []map[string]any, recIdx []int) error {
	seen := map[string]bool{}
	last := -1
	for i, f := range frames {
		if recIdx[i] != last {
			seen = map[string]bool{}
			last = recIdx[i]
		}
		var k string
		switch f["type"] {
		case "MAX_DATA":
			k = "md"
		case "MAX_STREAM_DATA":
			k = fmt.Sprint("msd", f["id"])
		case "MAX_STREAMS":
			k = fmt.Sprint("ms", f["bidi"])
		default:
			continue
		}
		if seen[k] {
			return fmt.Errorf("two credit frames of one scope in a record")
		}
		seen[k] = true
	}
	return nil
}

func nonPadding(fs []map[string]any, pats []map[string]any) []map[string]any {
	for _, p := range pats {
		if p["type"] == "PADDING" {
			return fs
		}
	}
	out := fs[:0:0]
	for _, f := range fs {
		if f["type"] != "PADDING" {
			out = append(out, f)
		}
	}
	return out
}

func matchFrames(mode string, pats []map[string]any, frames []map[string]any) error {
	switch mode {
	case "exact", "first":
		i := 0
		for pi, p := range pats {
			if p["type"] == "STREAM_DATA" {
				j, ok := matchStreamData(p, frames, i, mode == "exact" || mode == "first")
				if !ok {
					return fmt.Errorf("pattern %d (STREAM_DATA) does not match", pi)
				}
				i = j
				continue
			}
			if i >= len(frames) {
				return fmt.Errorf("pattern %d: no frame left", pi)
			}
			if !matchFrame(p, frames[i]) {
				return fmt.Errorf("pattern %d does not match frame %d", pi, i)
			}
			i++
		}
		if mode == "exact" && i != len(frames) {
			return fmt.Errorf("%d extra frames", len(frames)-i)
		}
		return nil
	case "contains":
		i := 0
		for pi, p := range pats {
			if p["type"] == "STREAM_DATA" {
				j, ok := matchStreamData(p, frames, i, false)
				if !ok {
					return fmt.Errorf("pattern %d (STREAM_DATA) not found", pi)
				}
				i = j
				continue
			}
			for i < len(frames) && !matchFrame(p, frames[i]) {
				i++
			}
			if i >= len(frames) {
				return fmt.Errorf("pattern %d not found", pi)
			}
			i++
		}
		return nil
	case "unordered":
		for pi, p := range pats {
			if p["type"] == "STREAM_DATA" {
				if _, ok := matchStreamData(p, frames, 0, false); !ok {
					return fmt.Errorf("pattern %d (STREAM_DATA) not found", pi)
				}
				continue
			}
			c := 0
			for _, f := range frames {
				if matchFrame(p, f) {
					c++
				}
			}
			if cnt, ok := p["count"]; ok {
				if m, ok := cnt.(map[string]any); ok {
					if mn, ok := m["min"]; ok && uint64(c) < num(mn) {
						return fmt.Errorf("pattern %d: %d matches", pi, c)
					}
					if mx, ok := m["max"]; ok && uint64(c) > num(mx) {
						return fmt.Errorf("pattern %d: %d matches", pi, c)
					}
				} else if uint64(c) != num(cnt) {
					return fmt.Errorf("pattern %d: %d matches, want %v", pi, c, cnt)
				}
			} else if c == 0 {
				return fmt.Errorf("pattern %d not found", pi)
			}
		}
		return nil
	}
	return fmt.Errorf("unknown match %s", mode)
}

// matchStreamData matches the STREAM_DATA pseudo-frame starting the search
// at index from. strict: the STREAM frames must be consecutive at from.
// It returns the index after the last STREAM frame covering the range.
func matchStreamData(p map[string]any, frames []map[string]any, from int, strict bool) (int, bool) {
	id := num(p["id"])
	off := getU(p, "off")
	var want []byte
	wantLen := -1
	switch d := p["data"].(type) {
	case string:
		want = hexb(d)
		wantLen = len(want)
	case map[string]any:
		wantLen = int(num(d["len"]))
	}
	end := off + uint64(wantLen)
	got := make([]byte, wantLen)
	have := make([]bool, wantLen)
	finSeen := false
	last := -1
	i := from
	for ; i < len(frames); i++ {
		f := frames[i]
		if f["type"] != "STREAM" || f["id"].(uint64) != id {
			if strict {
				if last >= 0 {
					break
				}
				return 0, false
			}
			continue
		}
		fo := f["off"].(uint64)
		fd, _ := hex.DecodeString(f["data"].(string))
		fe := fo + uint64(len(fd))
		if fo < off || fe > end {
			if last >= 0 && strict {
				break
			}
			if fe <= off || fo >= end {
				continue
			}
			return 0, false
		}
		copy(got[fo-off:], fd)
		for k := fo; k < fe; k++ {
			have[k-off] = true
		}
		if f["fin"].(bool) {
			finSeen = fe == end
		}
		last = i
		complete := true
		for _, h := range have {
			if !h {
				complete = false
				break
			}
		}
		if complete && (p["fin"] == nil || p["fin"].(bool) == finSeen || !finSeen && p["fin"].(bool) && i+1 < len(frames)) {
			if want != nil && !bytes.Equal(got, want) {
				return 0, false
			}
			if fin, ok := p["fin"].(bool); ok && fin != finSeen {
				// A FIN-only frame may follow.
				if fin && i+1 < len(frames) {
					n := frames[i+1]
					if n["type"] == "STREAM" && n["id"].(uint64) == id && n["fin"].(bool) &&
						n["off"].(uint64) == end && n["data"].(string) == "" {
						return i + 2, true
					}
				}
				continue
			}
			return i + 1, true
		}
	}
	if wantLen == 0 && last < 0 {
		return from, false
	}
	return 0, false
}

func (r *runner) expectNoFrames(st map[string]any) error {
	lanes := r.lanesFor(st)
	var types []string
	if ts, ok := st["types"].([]any); ok {
		for _, x := range ts {
			types = append(types, x.(string))
		}
	}
	for _, l := range lanes {
		for _, f := range l.frames {
			if f["type"] == "PADDING" {
				continue
			}
			if len(types) == 0 {
				return fmt.Errorf("unexpected frames:\n%s", dumpFrames(l.frames))
			}
			for _, t := range types {
				if f["type"] == t {
					return fmt.Errorf("unexpected %s:\n%s", t, dumpFrames(l.frames))
				}
			}
		}
		l.frames, l.recIdx = nil, nil
	}
	return nil
}

func errorObj(e *core.SessionError) map[string]any {
	o := map[string]any{"remote": e.Remote}
	switch e.Kind {
	case core.CloseApplication:
		o["space"], o["code"] = "application", e.Code
	case core.CloseTransport:
		o["space"], o["code"] = "transport", wire.TransportErrorName(e.Code)
	case core.CloseIdle:
		o["space"], o["code"] = "local", "IDLE_TIMEOUT"
	case core.CloseHandshakeTimeout:
		o["space"], o["code"] = "local", "HANDSHAKE_TIMEOUT"
	case core.CloseEarlyRejected:
		o["space"], o["code"] = "local", "EARLY_DATA_REJECTED"
	case core.ClosePipesLost:
		o["space"], o["code"] = "local", "PIPES_LOST"
	case core.CloseResumeTimeout:
		o["space"], o["code"] = "transport", "ZMUX_RESUME_TIMEOUT"
	}
	return o
}

func matchObj(pat map[string]any, got map[string]any) bool {
	for k, v := range pat {
		g, ok := got[k]
		if !ok {
			return false
		}
		if pm, ok := v.(map[string]any); ok {
			gm, ok := g.(map[string]any)
			if !ok || !matchObj(pm, gm) {
				return false
			}
			continue
		}
		if pa, ok := v.([]any); ok {
			ga, ok := g.([]any)
			if !ok || len(ga) != len(pa) {
				return false
			}
			for i := range pa {
				pm, _ := pa[i].(map[string]any)
				gm, _ := ga[i].(map[string]any)
				if pm != nil {
					if gm == nil || !matchObj(pm, gm) {
						return false
					}
				} else if !matchValue(pa[i], ga[i]) {
					return false
				}
			}
			continue
		}
		if !matchValue(v, g) {
			return false
		}
	}
	return true
}

func (r *runner) expectClose(st map[string]any) error {
	e := r.s.Err()
	if e == nil {
		return fmt.Errorf("session not closed (state %v)", r.s.State())
	}
	r.closeCovered = true
	pat := st["error"].(map[string]any)
	got := errorObj(e)
	if !matchObj(pat, got) {
		return fmt.Errorf("close error %v (%v), want %v", got, e, pat)
	}
	if w, ok := st["wire"].(bool); ok {
		code := pat["code"]
		found := false
		for _, c := range r.closesSent {
			if cs, ok := code.(string); ok {
				if wc, ok2 := wire.TransportErrorCode(cs); ok2 && c["code"].(uint64) == wc && !c["app"].(bool) {
					found = true
				}
			} else if c["code"].(uint64) == num(code) {
				found = true
			}
		}
		if w != found {
			return fmt.Errorf("CONNECTION_CLOSE on the wire: %v, want %v (sent %v)", found, w, r.closesSent)
		}
	}
	return nil
}

func (r *runner) expectState(st map[string]any) error {
	if s, ok := st["session"].(string); ok {
		if got := r.s.State().String(); got != s {
			return fmt.Errorf("session state %s, want %s", got, s)
		}
	}
	if pv, ok := st["pipe"]; ok {
		out, in, ok := r.s.PipeState(num(pv))
		if !ok {
			out, in = "RETIRED", "RETIRED"
		}
		got := out
		if st["dir"] == "in" {
			got = in
		}
		want := st["state"]
		if m, ok := want.(map[string]any); ok {
			for _, x := range m["oneof"].([]any) {
				if x.(string) == got {
					return nil
				}
			}
			return fmt.Errorf("pipe %v %v state %s, want %v", pv, st["dir"], got, want)
		}
		if got != want.(string) {
			return fmt.Errorf("pipe %v %v state %s, want %v", pv, st["dir"], got, want)
		}
	}
	return nil
}

var featureNames = []string{"role", "goaway", "preamble", "priority", "datagram_flows", "multipath", "pipe_ack", "resume"}

func eventObj(e core.Event) (string, map[string]any) {
	switch e.Kind {
	case core.EvEstablished:
		f := e.Features
		on := []bool{f.Role, f.GoAway, f.Preamble, f.Priority, f.Flows, f.Multipath, f.PipeAck, f.Resume}
		var fs []any
		for i, n := range featureNames {
			if on[i] {
				fs = append(fs, n)
			}
		}
		role := "initiator"
		if e.Role == core.RoleResponder {
			role = "responder"
		}
		return "established", map[string]any{"role": role, "features": fs}
	case core.EvClosed:
		return "closed", map[string]any{}
	case core.EvGoAwayReceived:
		return "goaway_received", map[string]any{"max_bidi": e.MaxBidi, "max_uni": e.MaxUni, "code": e.Code}
	case core.EvDatagramDropped:
		return "datagram_dropped", map[string]any{"count": e.Count}
	case core.EvSuspended:
		return "suspended", map[string]any{}
	case core.EvResumed:
		return "resumed", map[string]any{}
	case core.EvPipeState:
		return "pipe_state", map[string]any{"pipe": e.Pipe, "state": e.State}
	case core.EvJoinAuthFailed:
		return "join_auth_failed", map[string]any{"reason": e.Reason}
	case core.EvOOBJoin:
		return "oob_join", map[string]any{"pipe": e.Pipe, "frame": hex.EncodeToString(e.Frame)}
	case core.EvJoinSecurityUnavailable, core.EvTierSaturation, core.EvPipePriority, core.EvPipeQuality,
		core.EvCapacityBelow, core.EvCapacityRestored:
		// Not in the trace vocabulary.
	}
	return "other", nil
}

func (r *runner) expectEvent(st map[string]any, want bool) error {
	name := st["event"].(string)
	start := r.evSeen[name]
	for i := start; i < len(r.events); i++ {
		n, o := eventObj(r.events[i])
		if n != name {
			continue
		}
		if !want {
			return fmt.Errorf("event %s reported", name)
		}
		pat, _ := st["fields"].(map[string]any)
		if pat != nil && !matchEventFields(pat, o) {
			continue
		}
		r.evSeen[name] = i + 1
		return nil
	}
	if want {
		return fmt.Errorf("event %s not reported", name)
	}
	r.evSeen[name] = len(r.events)
	return nil
}

func matchEventFields(pat, got map[string]any) bool {
	for k, v := range pat {
		if k == "features" {
			ps, _ := v.([]any)
			gs, _ := got[k].([]any)
			if len(ps) != len(gs) {
				return false
			}
			for _, p := range ps {
				found := false
				for _, g := range gs {
					if p == g {
						found = true
					}
				}
				if !found {
					return false
				}
			}
			continue
		}
		if !matchValue(v, got[k]) {
			return false
		}
	}
	return true
}

func (r *runner) expectPipeClosed(st map[string]any) error {
	p := r.pipes[num(st["pipe"])]
	how := st["how"].(string)
	lanes := p.lanes
	if ln, ok := st["lane"]; ok {
		lanes = map[uint64]*rlane{num(ln): p.lane(num(ln))}
	}
	for _, l := range lanes {
		if l.closed == "" || how != "any" && l.closed != how {
			return fmt.Errorf("pipe closed %q, want %s", l.closed, how)
		}
	}
	return nil
}

var _ = bufio.NewReader

// carrierSend delivers bytes on an inbound lane carrier: the first record
// is routed by its PIPE_LANE (MULTIPATH.md 5.8), the rest is the lane's.
func (r *runner) carrierSend(c *rcarrier, st map[string]any) error {
	if c == nil {
		return fmt.Errorf("unknown carrier")
	}
	var data []byte
	if hx, ok := st["hex"].(string); ok {
		data = hexb(hx)
	} else {
		data = r.enc.encodeRecords(st["records"].([]any))
	}
	c.buf = append(c.buf, data...)
	if c.rejected {
		c.buf = nil
		return nil
	}
	if !c.attached {
		size, n := wire.ReadVarint(c.buf)
		if n == 0 || uint64(len(c.buf)-n) < size {
			return nil
		}
		var f wire.Frame
		if _, err := wire.DecodeFrame(c.buf[n:n+int(size)], &f); err != nil || f.Kind != wire.KindPipeLane ||
			!r.s.LaneKeyMatches(f.LaneKey) {
			c.rejected = true
			c.buf = nil
			return nil
		}
		if !r.s.AcceptLane(c.conn, f.PipeID, f.Lane, core.CarrierInfo{QueueBound: true, MinRTT: true, Lanes: true}) {
			c.rejected = r.s.State() < core.StateClosing
			c.buf = nil
			r.drainEvents()
			return nil
		}
		c.attached, c.pipe, c.lane = true, f.PipeID, f.Lane
	}
	buf := &core.Buf{B: c.buf}
	n := r.s.Receive(c.pipe, c.lane, buf, c.buf, r.now)
	c.buf = append([]byte(nil), c.buf[n:]...)
	r.drainEvents()
	return nil
}
