package zmux

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/djylb/timeshim"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
	"github.com/zmuxio/zmux-go/internal/wire"
)

// headerLen is the length of the test transform's header: a lead byte
// that ClassifyFirstBytes judges Other, a length byte and the rest.
const headerLen = 24

func testHeader() []byte {
	h := bytes.Repeat([]byte{'h'}, headerLen)
	h[0], h[1] = 0x01, headerLen
	return h
}

// classifyingAdmission tells zmux first records ("") from carriers that
// begin with a test header ("x"), which it reads with VerdictMore and
// skips; calls counts its calls.
func classifyingAdmission(calls *sync.Map) *Admission {
	return &Admission{Peek: 2, MaxPeek: 64, PeekTimeout: 5 * time.Second,
		Decide: func(_ context.Context, _ CarrierInfo, prefix []byte) Decision {
			n, _ := calls.LoadOrStore("n", new(int))
			*n.(*int)++
			c, need := wire.ClassifyFirstBytes(prefix, false)
			switch {
			case c == wire.FirstRecord:
				return Decision{Verdict: VerdictAccept}
			case c == wire.FirstNeedMore:
				return Decision{Verdict: VerdictMore, Need: need}
			case len(prefix) < 2 || prefix[0] != 0x01:
				return Decision{Verdict: VerdictClose, Variant: "unknown"}
			case len(prefix) < int(prefix[1]):
				return Decision{Verdict: VerdictMore, Need: int(prefix[1])}
			}
			return Decision{Verdict: VerdictAccept, Skip: int(prefix[1]), Variant: "x"}
		}}
}

// VerdictMore reads a transform's header in rounds; the accepted carrier
// carries a session past the skipped header, labelled with the variant
// that reaches WrapPipe; a plain first record is accepted as it is. The
// counts by variant add up to the totals.
func TestAdmissionMore(t *testing.T) {
	leakcheck.Check(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var calls sync.Map
	var mu sync.Mutex
	var variants []string
	wrap := func(_ context.Context, p PipeIO, _ PipeHint) (PipeIO, error) {
		mu.Lock()
		variants = append(variants, p.Variant)
		mu.Unlock()
		return PipeIO{R: p.R, W: p.W, Framing: p.Framing}, nil // the result keeps it
	}
	lst := mustNewListener(&ListenerConfig{Config: mpConfig(), Admission: classifyingAdmission(&calls), WrapPipe: WrapPipeFunc(wrap)})
	defer func() { _ = lst.Close() }()
	for i, header := range [][]byte{testHeader(), nil} {
		a, b := net.Pipe()
		go func() { _ = lst.HandlePipe(ctx, NetPipe(a), PipeHint{}) }()
		if header != nil {
			if _, err := b.Write(header); err != nil {
				t.Fatal(err)
			}
		}
		c, err := Start(ctx, NetPipe(b), RoleClient, mpConfig())
		if err != nil {
			t.Fatal(i, err)
		}
		s, err := lst.Accept(ctx)
		if err != nil {
			t.Fatal(err)
		}
		echoOnce(t, ctx, c, s)
		_ = c.Close()
		_ = s.Close()
		waitDone(t, c)
		waitDone(t, s)
	}
	n, _ := calls.Load("n")
	if *n.(*int) != 4 { // header: 2 bytes, then 24; record: 2 bytes, then 3
		t.Fatalf("Decide called %d times", *n.(*int))
	}
	mu.Lock()
	if len(variants) != 2 || variants[0] != "x" || variants[1] != "" {
		t.Fatalf("WrapPipe saw variants %q", variants)
	}
	mu.Unlock()
	st, vs := lst.Stats().Admission, lst.VariantStats()
	if st.Accepted != 2 || len(vs) != 1 || vs[0] != (VariantStat{Name: "x", Accepted: 1}) {
		t.Fatalf("stats %+v, variants %+v", st, vs)
	}
}

// admitOnce hands a carrier that delivers send (and then ends, when end
// is set) to a listener with adm, and returns its admission stats and the
// prefix lengths Decide saw, and its per-variant counts.
func admitOnce(t *testing.T, adm *Admission, p func() (PipeIO, func()), seen *[]int) (AdmissionStats, []VariantStat) {
	t.Helper()
	decide := adm.Decide
	var mu sync.Mutex
	adm.Decide = func(ctx context.Context, info CarrierInfo, prefix []byte) Decision {
		mu.Lock()
		*seen = append(*seen, len(prefix))
		mu.Unlock()
		return decide(ctx, info, prefix)
	}
	cfg := mpConfig()
	cfg.ClockDomain = NewClockDomain(timeshim.Real)
	lst := mustNewListener(&ListenerConfig{Config: cfg, Admission: adm})
	defer func() { _ = lst.Close() }()
	q, peer := p()
	defer peer()
	_ = lst.HandlePipe(context.Background(), q, PipeHint{})
	return lst.Stats().Admission, lst.VariantStats()
}

// variantNamed is the per-variant count of name (zero if none).
func variantNamed(vs []VariantStat, name string) VariantStat {
	for _, v := range vs {
		if !v.Overflow && v.Name == name {
			return v
		}
	}
	return VariantStat{}
}

// streamCarrier delivers send, then ends when end is set (else stays open).
func streamCarrier(send []byte, end bool) func() (PipeIO, func()) {
	return func() (PipeIO, func()) {
		a, b := net.Pipe()
		go func() {
			_, _ = b.Write(send)
			if end {
				_ = b.Close()
				return
			}
			_, _ = io.Copy(io.Discard, b)
		}()
		return NetPipe(a), func() { _ = b.Close() }
	}
}

func seq(ds ...Decision) func(context.Context, CarrierInfo, []byte) Decision {
	var mu sync.Mutex
	i := 0
	return func(context.Context, CarrierInfo, []byte) Decision {
		mu.Lock()
		defer mu.Unlock()
		d := ds[min(i, len(ds)-1)]
		i++
		return d
	}
}

// The bounds of VerdictMore: Need must grow and stay within MaxPeek, a
// carrier that ends short gets one more decision on what it sent, and the
// rounds share one PeekTimeout.
func TestAdmissionMoreBounds(t *testing.T) {
	leakcheck.Check(t)
	more := func(n int) Decision { return Decision{Verdict: VerdictMore, Need: n, Variant: "m"} }
	for _, x := range []struct {
		name    string
		adm     *Admission
		send    []byte
		seen    []int
		end     bool
		closed  uint64
		timeout uint64
	}{
		{name: "need not above prefix", adm: &Admission{Peek: 4, MaxPeek: 8, Decide: seq(more(4))}, send: make([]byte, 8), end: false, seen: []int{4}, closed: 1, timeout: 0},
		{name: "need not growing", adm: &Admission{Peek: 2, MaxPeek: 8, Decide: seq(more(4), more(4))}, send: make([]byte, 8), end: false, seen: []int{2, 4}, closed: 1, timeout: 0},
		{name: "above MaxPeek", adm: &Admission{Peek: 2, MaxPeek: 8, Decide: seq(more(9))}, send: make([]byte, 16), end: false, seen: []int{2}, closed: 1, timeout: 0},
		{name: "no MaxPeek", adm: &Admission{Peek: 2, Decide: seq(more(3))}, send: make([]byte, 16), end: false, seen: []int{2}, closed: 1, timeout: 0},
		{name: "ended short", adm: &Admission{Peek: 2, MaxPeek: 8, Decide: seq(more(8), more(9))}, send: make([]byte, 5), end: true, seen: []int{2, 5}, closed: 1, timeout: 0},
		{name: "rounds", adm: &Admission{Peek: 1, MaxPeek: 8, Decide: seq(more(3), more(8), Decision{Verdict: VerdictClose, Variant: "m"})},
			send: make([]byte, 8), end: false, seen: []int{1, 3, 8}, closed: 1, timeout: 0},
		{name: "timeout", adm: &Admission{Peek: 1, MaxPeek: 8, PeekTimeout: 300 * time.Millisecond, Decide: func(context.Context, CarrierInfo, []byte) Decision {
			synctest.Sleep(100 * time.Millisecond)
			return more(8)
		}},
			send: make([]byte, 2), end: false, seen: []int{1}, closed: 0, timeout: 1},
	} {
		t.Run(x.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var seen []int
				adm := x.adm
				start := time.Now()
				st, vs := admitOnce(t, adm, streamCarrier(x.send, x.end), &seen)
				if st.Closed != x.closed || st.PeekTimedOut != x.timeout || !equalInts(seen, x.seen) {
					t.Fatalf("%s: stats %+v, Decide saw %v", x.name, st, seen)
				}
				if x.closed > 0 && variantNamed(vs, "m").Closed != x.closed {
					t.Fatalf("%s: variants %+v", x.name, vs)
				}
				if x.timeout > 0 && time.Since(start) != adm.PeekTimeout {
					t.Fatalf("%s: rounds did not share the timeout", x.name)
				}
			})
		})
	}
}

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// firstMsgCarrier is a message-framed carrier delivering msgs, then blocking
// until closed.
type firstMsgCarrier struct {
	msgs   chan []byte
	closed chan struct{}
	once   sync.Once
}

func newFirstMsgCarrier(msgs ...[]byte) *firstMsgCarrier {
	m := &firstMsgCarrier{msgs: make(chan []byte, len(msgs)), closed: make(chan struct{})}
	for _, b := range msgs {
		m.msgs <- b
	}
	return m
}

func (m *firstMsgCarrier) ReadMessage(int) (ReceivedMessage, error) {
	select {
	case b := <-m.msgs:
		return OwnMessage(b), nil
	case <-m.closed:
		return ReceivedMessage{}, io.EOF
	}
}
func (m *firstMsgCarrier) WriteMessage([]byte) error {
	select {
	case <-m.closed:
		return io.ErrClosedPipe
	default:
		return nil
	}
}
func (m *firstMsgCarrier) Read([]byte) (int, error)    { return 0, errors.New("framed") }
func (m *firstMsgCarrier) Write(b []byte) (int, error) { return len(b), nil }
func (m *firstMsgCarrier) Close() error {
	m.once.Do(func() { close(m.closed) })
	return nil
}

// On a message-framed carrier the prefix is the first message, whole,
// within MaxMessage (MaxRecordSize by default; Peek and MaxPeek do not
// bound it); Skip is 0 or all of it; VerdictMore is VerdictClose.
func TestAdmissionFirstMessage(t *testing.T) {
	leakcheck.Check(t)
	msg := bytes.Repeat([]byte{1}, 40)
	for _, x := range []struct {
		name             string
		adm              *Admission
		seen             []int
		accepted, closed uint64
	}{
		{"whole", &Admission{Peek: 1, MaxPeek: 64, Decide: seq(Decision{Verdict: VerdictAccept, Skip: 40, Variant: "w"})}, []int{40}, 1, 0},
		{"replayed", &Admission{Peek: 1, MaxPeek: 64, Decide: seq(Decision{Verdict: VerdictAccept, Variant: "w"})}, []int{40}, 1, 0},
		{"partial skip", &Admission{Peek: 1, MaxPeek: 64, Decide: seq(Decision{Verdict: VerdictAccept, Skip: 3, Variant: "w"})}, []int{40}, 0, 1},
		{"default bound", &Admission{Peek: 1, Decide: seq(Decision{Verdict: VerdictAccept, Variant: "w"})}, []int{40}, 1, 0},
		{"too long", &Admission{Peek: 1, MaxMessage: 39, Decide: seq(Decision{Verdict: VerdictAccept})}, nil, 0, 1},
		{"more", &Admission{Peek: 1, MaxPeek: 64, Decide: seq(Decision{Verdict: VerdictMore, Need: 50, Variant: "w"})}, []int{40}, 0, 1},
	} {
		var seen []int
		adm := x.adm
		st, vs := admitOnce(t, adm, func() (PipeIO, func()) {
			m := newFirstMsgCarrier(msg)
			return PipeIO{R: m, W: m, Framing: FramingRecords}, func() { _ = m.Close() }
		}, &seen)
		if st.Accepted != x.accepted || st.Closed != x.closed || st.TooLarge != 0 || !equalInts(seen, x.seen) {
			t.Fatalf("%s: stats %+v, Decide saw %v", x.name, st, seen)
		}
		var sum VariantStat
		for _, v := range vs {
			sum.Accepted += v.Accepted
			sum.Closed += v.Closed
		}
		if sum.Accepted != st.Accepted || sum.Closed != st.Closed && x.name != "too long" {
			t.Fatalf("%s: variants %+v do not add up", x.name, vs)
		}
	}
}

func TestAdmissionMaxPeekConfig(t *testing.T) {
	for _, a := range []*Admission{
		{Peek: 4, MaxPeek: 3, Decide: seq(Decision{})},
		{MaxPeek: -1, Decide: seq(Decision{})},
		{MaxMessage: -1, Decide: seq(Decision{})},
		{MaxMessage: MaxRecordSize + 1, Decide: seq(Decision{})},
	} {
		if err := a.Validate(); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("%+v: %v", a, err)
		}
	}
}

// Past MaxAdmissionVariants distinct variants, the outcomes of further
// ones share the Overflow bucket, listed last; the names come sorted.
func TestAdmissionVariantBound(t *testing.T) {
	var c admissionCounters
	for i := MaxAdmissionVariants + 2; i > 0; i-- {
		c.count(Decision{Variant: fmt.Sprintf("v%03d", i), Verdict: VerdictAccept})
	}
	c.count(Decision{Variant: "v001", Verdict: VerdictClose})
	c.count(Decision{Verdict: VerdictClose})
	c.count(Decision{Variant: "v001", Verdict: VerdictFallback})
	c.count(Decision{Variant: "v003", Verdict: VerdictTakeover})
	c.stallOverflow.Add(1)
	c.variant("v999", VerdictStall)
	vs := c.variantStats()
	if len(vs) != MaxAdmissionVariants+1 {
		t.Fatalf("%d variants", len(vs))
	}
	for i := 1; i < MaxAdmissionVariants; i++ {
		if vs[i-1].Name >= vs[i].Name || vs[i].Overflow {
			t.Fatalf("variants %+v not sorted", vs)
		}
	}
	// The first MaxAdmissionVariants seen were v066 down to v003.
	if vs[0] != (VariantStat{Name: "v003", Accepted: 1, Takeover: 1}) {
		t.Fatalf("first %+v", vs[0])
	}
	if o := vs[MaxAdmissionVariants]; o != (VariantStat{Overflow: true, Accepted: 2, Fallback: 1, Stalled: 1, Closed: 1}) {
		t.Fatalf("overflow %+v", o)
	}
	var sum VariantStat
	for _, b := range vs {
		sum.Accepted += b.Accepted
		sum.Closed += b.Closed
		sum.Fallback += b.Fallback
		sum.Takeover += b.Takeover
		sum.Stalled += b.Stalled
	}
	st := c.snapshot()
	if st.Accepted != sum.Accepted || st.Closed != sum.Closed+1 || st.Fallback != sum.Fallback ||
		st.Takeover != sum.Takeover || st.Stalled+st.StallOverflow != sum.Stalled {
		t.Fatalf("totals %+v, labeled outcomes %+v", st, sum)
	}
	var none *Listener
	if none.VariantStats() != nil {
		t.Fatal("a nil listener has variants")
	}
}

// limitMsgCarrier is a firstMsgCarrier that records the limits it is
// asked for and enforces them.
type limitMsgCarrier struct {
	*firstMsgCarrier
	limits []int
}

func (m *limitMsgCarrier) ReadMessage(limit int) (ReceivedMessage, error) {
	m.limits = append(m.limits, limit)
	b, err := m.firstMsgCarrier.ReadMessage(limit)
	if b.Len() > limit {
		b.Release()
		return ReceivedMessage{}, ErrMessageTooLarge
	}
	return b, err
}

// The first message is read with the admission's bound, MaxMessage
// (MaxRecordSize for 0), where the carrier can bound the read: a longer
// one is refused before Decide, counted Closed.
func TestAdmissionFirstMessageLimit(t *testing.T) {
	leakcheck.Check(t)
	msg := bytes.Repeat([]byte{1}, 40)
	for _, x := range []struct {
		adm              *Admission
		accepted, closed uint64
	}{
		{&Admission{Peek: 1, MaxMessage: 39, Decide: seq(Decision{Verdict: VerdictAccept})}, 0, 1},
		{&Admission{Peek: 1, MaxMessage: 40, Decide: seq(Decision{Verdict: VerdictAccept})}, 1, 0},
		{&Admission{Peek: 1, Decide: seq(Decision{Verdict: VerdictAccept})}, 1, 0},
	} {
		var seen []int
		var m *limitMsgCarrier
		st, _ := admitOnce(t, x.adm, func() (PipeIO, func()) {
			m = &limitMsgCarrier{firstMsgCarrier: newFirstMsgCarrier(msg)}
			return PipeIO{R: m, W: m, Framing: FramingRecords}, func() { _ = m.Close() }
		}, &seen)
		want := x.adm.MaxMessage
		if want == 0 {
			want = MaxRecordSize
		}
		if st.Accepted != x.accepted || st.Closed != x.closed || st.TooLarge != 0 {
			t.Fatalf("MaxMessage %d: stats %+v", x.adm.MaxMessage, st)
		}
		if len(m.limits) == 0 || m.limits[0] != want {
			t.Fatalf("MaxMessage %d: limits %v", x.adm.MaxMessage, m.limits)
		}
	}
}

func TestAdmissionOversizedFirstMessageRefusal(t *testing.T) {
	for _, tc := range []struct {
		name   string
		size   int
		refuse AdmissionVerdict
		want   AdmissionStats
	}{
		{"fallback", 40, VerdictFallback, AdmissionStats{Fallback: 1}},
		{"stall", 40, VerdictStall, AdmissionStats{Stalled: 1}},
		{"close", 40, VerdictClose, AdmissionStats{Closed: 1}},
		{"unreplayable", MaxRecordSize + 1, VerdictFallback, AdmissionStats{Closed: 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				msg := bytes.Repeat([]byte{1}, tc.size)
				next := []byte("next message")
				m := &limitMsgCarrier{firstMsgCarrier: newFirstMsgCarrier(msg, next)}
				defer func() { _ = m.Close() }()
				l := mustNewListener(&ListenerConfig{
					Admission: &Admission{
						Peek: 1, MaxMessage: 39, Refuse: tc.refuse,
						StallMin: time.Second, StallMax: time.Second,
						Decide: func(context.Context, CarrierInfo, []byte) Decision {
							t.Error("Decide was called on an oversized message")
							return Decision{Verdict: VerdictClose}
						},
					},
					Fallback: func(_ context.Context, take *Takeover) {
						r, _, ok := take.Messages()
						if !ok {
							t.Error("fallback did not receive a message carrier")
							return
						}
						for _, want := range [][]byte{msg, next} {
							got, err := r.ReadMessage(MaxRecordSize)
							if err != nil || !bytes.Equal(got.Bytes(), want) {
								t.Errorf("fallback read %d bytes, err %v, want %d bytes", got.Len(), err, len(want))
							}
							got.Release()
						}
					},
				})
				defer func() { _ = l.Close() }()
				_ = l.HandlePipe(context.Background(), PipeIO{R: m, W: m, Framing: FramingRecords}, PipeHint{})
				synctest.Wait()
				if got := l.Stats().Admission; got != tc.want {
					t.Fatalf("admission stats %+v, want %+v", got, tc.want)
				}
			})
		})
	}
}

// A partial Skip of a first message is counted once, as a close: a
// concurrent snapshot never sees it accepted.
func TestAdmissionPartialSkipCountedOnce(t *testing.T) {
	leakcheck.Check(t)
	adm := &Admission{Peek: 1, MaxPeek: 64, Decide: func(context.Context, CarrierInfo, []byte) Decision {
		return Decision{Verdict: VerdictAccept, Skip: 3, Variant: "w"}
	}}
	lst := mustNewListener(&ListenerConfig{Config: mpConfig(), Admission: adm})
	defer func() { _ = lst.Close() }()
	stop := make(chan struct{})
	bad := make(chan string, 1)
	go func() {
		for {
			select {
			case <-stop:
				close(bad)
				return
			default:
			}
			if st := lst.Stats().Admission; st.Accepted != 0 {
				bad <- "accepted"
				return
			}
			if v := variantNamed(lst.VariantStats(), "w"); v.Accepted != 0 {
				bad <- "variant accepted"
				return
			}
		}
	}()
	for range 20 {
		m := newFirstMsgCarrier(bytes.Repeat([]byte{1}, 40))
		_ = lst.HandlePipe(context.Background(), PipeIO{R: m, W: m, Framing: FramingRecords}, PipeHint{})
		_ = m.Close()
	}
	close(stop)
	if s, ok := <-bad; ok {
		t.Fatal(s)
	}
	if st := lst.Stats().Admission; st.Closed != 20 {
		t.Fatalf("stats %+v", st)
	}
}
