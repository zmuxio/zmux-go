package zmux

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/zmuxio/zmux-go/internal/leakcheck"
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
		Decide: func(_ context.Context, info *CarrierInfo, prefix []byte) Decision {
			n, _ := calls.LoadOrStore("n", new(int))
			*n.(*int)++
			c, need := ClassifyFirstBytes(prefix, FramingByteStream)
			switch {
			case c == FirstBytesRecord:
				return Decision{Verdict: VerdictAccept}
			case c == FirstBytesNeedMore:
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
	if st.Accepted != 2 || len(vs) != 2 || vs[0] != (VariantStat{Accepted: 1}) || vs[1] != (VariantStat{Name: "x", Accepted: 1}) {
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
	adm.Decide = func(ctx context.Context, info *CarrierInfo, prefix []byte) Decision {
		mu.Lock()
		*seen = append(*seen, len(prefix))
		mu.Unlock()
		return decide(ctx, info, prefix)
	}
	lst := mustNewListener(&ListenerConfig{Config: mpConfig(), Admission: adm})
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

func seq(ds ...Decision) func(context.Context, *CarrierInfo, []byte) Decision {
	var mu sync.Mutex
	i := 0
	return func(context.Context, *CarrierInfo, []byte) Decision {
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
		end     bool
		seen    []int
		closed  uint64
		timeout uint64
	}{
		{"need not above prefix", &Admission{Peek: 4, MaxPeek: 8, Decide: seq(more(4))}, make([]byte, 8), false, []int{4}, 1, 0},
		{"need not growing", &Admission{Peek: 2, MaxPeek: 8, Decide: seq(more(4), more(4))}, make([]byte, 8), false, []int{2, 4}, 1, 0},
		{"above MaxPeek", &Admission{Peek: 2, MaxPeek: 8, Decide: seq(more(9))}, make([]byte, 16), false, []int{2}, 1, 0},
		{"no MaxPeek", &Admission{Peek: 2, Decide: seq(more(3))}, make([]byte, 16), false, []int{2}, 1, 0},
		{"ended short", &Admission{Peek: 2, MaxPeek: 8, Decide: seq(more(8), more(9))}, make([]byte, 5), true, []int{2, 5}, 1, 0},
		{"rounds", &Admission{Peek: 1, MaxPeek: 8, Decide: seq(more(3), more(8), Decision{Verdict: VerdictClose, Variant: "m"})},
			make([]byte, 8), false, []int{1, 3, 8}, 1, 0},
		{"timeout", &Admission{Peek: 1, MaxPeek: 8, PeekTimeout: 300 * time.Millisecond, Decide: seq(more(8))},
			make([]byte, 2), false, []int{1}, 0, 1},
	} {
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
		if x.timeout > 0 && time.Since(start) > 3*time.Second {
			t.Fatalf("%s: rounds did not share the timeout", x.name)
		}
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

func (m *firstMsgCarrier) ReadMessage() ([]byte, error) {
	select {
	case b := <-m.msgs:
		return b, nil
	case <-m.closed:
		return nil, io.EOF
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
// within max(Peek, MaxPeek); Skip is 0 or all of it; VerdictMore is
// VerdictClose.
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
		{"Peek bounds without MaxPeek", &Admission{Peek: 40, Decide: seq(Decision{Verdict: VerdictAccept, Variant: "w"})}, []int{40}, 1, 0},
		{"too long", &Admission{Peek: 1, MaxPeek: 39, Decide: seq(Decision{Verdict: VerdictAccept})}, nil, 0, 1},
		{"more", &Admission{Peek: 1, MaxPeek: 64, Decide: seq(Decision{Verdict: VerdictMore, Need: 50, Variant: "w"})}, []int{40}, 0, 1},
	} {
		var seen []int
		adm := x.adm
		st, vs := admitOnce(t, adm, func() (PipeIO, func()) {
			m := newFirstMsgCarrier(msg)
			return PipeIO{R: m, W: m, Framing: FramingRecords}, func() { _ = m.Close() }
		}, &seen)
		if st.Accepted != x.accepted || st.Closed != x.closed || !equalInts(seen, x.seen) {
			t.Fatalf("%s: stats %+v, Decide saw %v", x.name, st, seen)
		}
		var sum VariantStat
		for _, v := range vs {
			sum.Accepted += v.Accepted
			sum.Closed += v.Closed
		}
		if sum.Accepted != st.Accepted || sum.Closed != st.Closed {
			t.Fatalf("%s: variants %+v do not add up", x.name, vs)
		}
	}
}

// An HTTP request carrier reads more of its body on VerdictMore.
func TestAdmissionMoreHTTP(t *testing.T) {
	leakcheck.Check(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var calls sync.Map
	lst := mustNewListener(&ListenerConfig{Config: mpConfig(), Admission: classifyingAdmission(&calls)})
	defer func() { _ = lst.Close() }()
	hh, err := NewHTTPHandler(lst, nil)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewUnstartedServer(hh)
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()
	p, err := StreamingCandidate(srv.Client(), srv.URL, nil).Dial(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.W.Write(testHeader()); err != nil {
		t.Fatal(err)
	}
	c, err := Start(ctx, p, RoleClient, mpConfig())
	if err != nil {
		t.Fatal(err)
	}
	s, err := lst.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close(); _ = s.Close(); waitDone(t, c); waitDone(t, s) }()
	echoOnce(t, ctx, c, s)
	if vs := lst.VariantStats(); variantNamed(vs, "x").Accepted != 1 {
		t.Fatalf("variants %+v", vs)
	}
}

func TestAdmissionMaxPeekConfig(t *testing.T) {
	for _, a := range []*Admission{
		{Peek: 4, MaxPeek: 3, Decide: seq(Decision{})},
		{MaxPeek: -1, Decide: seq(Decision{})},
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
		c.variant(fmt.Sprintf("v%03d", i), VerdictAccept)
	}
	c.variant("v001", VerdictClose)
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
	if vs[0] != (VariantStat{Name: "v003", Accepted: 1}) {
		t.Fatalf("first %+v", vs[0])
	}
	if o := vs[MaxAdmissionVariants]; o != (VariantStat{Overflow: true, Accepted: 2, Stalled: 1, Closed: 1}) {
		t.Fatalf("overflow %+v", o)
	}
	var none *Listener
	if none.VariantStats() != nil {
		t.Fatal("a nil listener has variants")
	}
}

// limitMsgCarrier is a firstMsgCarrier with ReadMessageLimit; ReadMessage
// must not be used.
type limitMsgCarrier struct {
	*firstMsgCarrier
	limits []int
	whole  bool
}

func (m *limitMsgCarrier) ReadMessage() ([]byte, error) {
	m.whole = true
	return m.firstMsgCarrier.ReadMessage()
}

func (m *limitMsgCarrier) ReadMessageLimit(limit int) ([]byte, error) {
	m.limits = append(m.limits, limit)
	b, err := m.firstMsgCarrier.ReadMessage()
	if len(b) > limit {
		return nil, ErrMessageTooLarge
	}
	return b, err
}

// The first message is read with the admission's bound,
// max(Peek, MaxPeek), where the carrier can bound the read: a longer one
// is refused before Decide.
func TestAdmissionFirstMessageLimit(t *testing.T) {
	leakcheck.Check(t)
	msg := bytes.Repeat([]byte{1}, 40)
	for _, x := range []struct {
		adm              *Admission
		accepted, closed uint64
	}{
		{&Admission{Peek: 1, MaxPeek: 39, Decide: seq(Decision{Verdict: VerdictAccept})}, 0, 1},
		{&Admission{Peek: 1, MaxPeek: 40, Decide: seq(Decision{Verdict: VerdictAccept})}, 1, 0},
	} {
		var seen []int
		var m *limitMsgCarrier
		st, _ := admitOnce(t, x.adm, func() (PipeIO, func()) {
			m = &limitMsgCarrier{firstMsgCarrier: newFirstMsgCarrier(msg)}
			return PipeIO{R: m, W: m, Framing: FramingRecords}, func() { _ = m.Close() }
		}, &seen)
		if st.Accepted != x.accepted || st.Closed != x.closed {
			t.Fatalf("MaxPeek %d: stats %+v", x.adm.MaxPeek, st)
		}
		if len(m.limits) == 0 || m.limits[0] != x.adm.MaxPeek {
			t.Fatalf("MaxPeek %d: limits %v, whole read %v", x.adm.MaxPeek, m.limits, m.whole)
		}
	}
}

// A partial Skip of a first message is counted once, as a close: a
// concurrent snapshot never sees it accepted.
func TestAdmissionPartialSkipCountedOnce(t *testing.T) {
	leakcheck.Check(t)
	adm := &Admission{Peek: 1, MaxPeek: 64, Decide: func(context.Context, *CarrierInfo, []byte) Decision {
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
