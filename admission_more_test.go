package zmux

import (
	"bytes"
	"context"
	"errors"
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
	wrap := func(_ context.Context, p PipeIO, _ JoinHint) (PipeIO, error) {
		mu.Lock()
		variants = append(variants, p.Variant)
		mu.Unlock()
		return PipeIO{R: p.R, W: p.W, Framing: p.Framing}, nil // the result keeps it
	}
	lst := NewListener(&ListenerConfig{Config: mpConfig(), Admission: classifyingAdmission(&calls), WrapPipe: wrap})
	defer func() { _ = lst.Close() }()
	for i, header := range [][]byte{testHeader(), nil} {
		a, b := net.Pipe()
		go func() { _ = lst.HandlePipe(ctx, NetPipe(a), JoinHint{}) }()
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
	st := lst.Stats().Admission
	if st.Accepted != 2 || st.Variants["x"].Accepted != 1 || st.Variants[""].Accepted != 1 {
		t.Fatalf("stats %+v", st)
	}
}

// admitOnce hands a carrier that delivers send (and then ends, when end
// is set) to a listener with adm, and returns its admission stats and the
// prefix lengths Decide saw.
func admitOnce(t *testing.T, adm *Admission, p func() (PipeIO, func()), seen *[]int) AdmissionStats {
	t.Helper()
	decide := adm.Decide
	var mu sync.Mutex
	adm.Decide = func(ctx context.Context, info *CarrierInfo, prefix []byte) Decision {
		mu.Lock()
		*seen = append(*seen, len(prefix))
		mu.Unlock()
		return decide(ctx, info, prefix)
	}
	lst := NewListener(&ListenerConfig{Config: mpConfig(), Admission: adm})
	defer func() { _ = lst.Close() }()
	q, peer := p()
	defer peer()
	_ = lst.HandlePipe(context.Background(), q, JoinHint{})
	return lst.Stats().Admission
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
		st := admitOnce(t, adm, streamCarrier(x.send, x.end), &seen)
		if st.Closed != x.closed || st.PeekTimedOut != x.timeout || !equalInts(seen, x.seen) {
			t.Fatalf("%s: stats %+v, Decide saw %v", x.name, st, seen)
		}
		if x.closed > 0 && st.Variants["m"].Closed != x.closed {
			t.Fatalf("%s: variants %+v", x.name, st.Variants)
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
		st := admitOnce(t, adm, func() (PipeIO, func()) {
			m := newFirstMsgCarrier(msg)
			return PipeIO{R: m, W: m, Framing: FramingRecords}, func() { _ = m.Close() }
		}, &seen)
		if st.Accepted != x.accepted || st.Closed != x.closed || !equalInts(seen, x.seen) {
			t.Fatalf("%s: stats %+v, Decide saw %v", x.name, st, seen)
		}
		var sum VariantStats
		for _, v := range st.Variants {
			sum.Accepted += v.Accepted
			sum.Closed += v.Closed
		}
		if sum.Accepted != st.Accepted || sum.Closed != st.Closed {
			t.Fatalf("%s: variants %+v do not add up", x.name, st.Variants)
		}
	}
}

// An HTTP request carrier reads more of its body on VerdictMore.
func TestAdmissionMoreHTTP(t *testing.T) {
	leakcheck.Check(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var calls sync.Map
	lst := NewListener(&ListenerConfig{Config: mpConfig(), Admission: classifyingAdmission(&calls)})
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
	if st := lst.Stats().Admission; st.Variants["x"].Accepted != 1 {
		t.Fatalf("stats %+v", st)
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
