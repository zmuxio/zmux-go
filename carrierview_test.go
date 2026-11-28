package zmux

import (
	"context"
	"crypto/tls"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"net"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// The classification of every exported interface of the package against
// the views of carrierview.go. TestCarrierViewCapabilities parses the
// package's own non-test files: an exported interface declared here that
// is not classified fails it, and each view must implement the sets
// below. It does not see interfaces of subpackages or other modules, nor
// anonymous interface assertions (the package names every capability it
// asserts, NetConner included), nor whether a forward is right in
// meaning: TestCarrierViewForwarding checks that for each one.
var (
	// viewCaps are forwarded by every view (halfCaps).
	viewCaps = []reflect.Type{
		reflect.TypeFor[ConnectionStater](),
		reflect.TypeFor[Handshaker](),
		reflect.TypeFor[KeyingMaterialExporter](),
		reflect.TypeFor[CarrierSecure](),
		reflect.TypeFor[FailureReporter](),
		reflect.TypeFor[CarrierConner](),
		reflect.TypeFor[PhysicalCarrier](),
		reflect.TypeFor[CloseCodeRecorder](),
		reflect.TypeFor[CarrierRecordLimit](),
		reflect.TypeFor[CarrierPeer](),
		reflect.TypeFor[CarrierTaker](),
	}
	// sendCaps are forwarded, beside viewCaps, by the views of a sending
	// half (Takeover's, and its Conn).
	sendOnlyCaps = []reflect.Type{
		reflect.TypeFor[CloseWriter](),
		reflect.TypeFor[Aborter](),
		reflect.TypeFor[BuffersWriter](),
		reflect.TypeFor[UnsentReporter](),
		reflect.TypeFor[LaneSource](),
		reflect.TypeFor[TLSRecordCarrier](),
	}
	// connOnlyCaps are the net.Conn view's (Takeover.Conn) besides.
	connOnlyCaps = []reflect.Type{
		reflect.TypeFor[NetConner](),
		reflect.TypeFor[io.WriterTo](),
		reflect.TypeFor[io.ReaderFrom](),
	}
	// framingCaps are a view's own, by the carrier's framing.
	framingCaps = []string{"MessageReader", "MessageWriter", "MessageBatchWriter"}
	// notForwarded run zmux sessions on a carrier and have no neutral
	// absence: found on a kept carrier's own sending half, reached
	// through Unwrap on a taken-over one.
	notForwarded = []string{"LaneOpener", "DatagramChannel", "DatagramDeadlineSender", "DatagramCapacityNotifier", "EarlyClientCarrier", "EarlyServerCarrier",
		"CarrierRecordMode", "RecordSealer", "RecordOpener"}
	// notCapabilities are interfaces of the package that no carrier half
	// implements.
	notCapabilities = []string{"AccountingTicket", "Admitted", "CarrierCredential", "CarrierTarget", "Limiter", "Pick",
		"CooldownEvent", "PipeWrapper", "Scheduler", "SpliceLimiter"}
)

func TestCarrierViewCapabilities(t *testing.T) {
	known := map[string]bool{}
	for _, c := range slices.Concat(viewCaps, sendOnlyCaps, connOnlyCaps) {
		known[c.Name()] = true
	}
	for _, n := range slices.Concat(framingCaps, notForwarded, notCapabilities) {
		known[n] = true
	}
	fset := token.NewFileSet()
	names, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range names {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range f.Decls {
			g, ok := d.(*ast.GenDecl)
			if !ok || g.Tok != token.TYPE {
				continue
			}
			for _, s := range g.Specs {
				ts := s.(*ast.TypeSpec)
				if _, ok := ts.Type.(*ast.InterfaceType); ok && ts.Name.IsExported() && !known[ts.Name.Name] {
					t.Errorf("interface %s is not classified for the carrier views (carrierview.go)", ts.Name.Name)
				}
			}
		}
	}
	want := func(v reflect.Type, sets ...[]reflect.Type) {
		for _, c := range slices.Concat(sets...) {
			if !v.Implements(c) {
				t.Errorf("%v does not forward %v", v, c)
			}
		}
	}
	mr, mw, mb := reflect.TypeFor[MessageReader](), reflect.TypeFor[MessageWriter](), reflect.TypeFor[MessageBatchWriter]()
	for _, v := range []reflect.Type{reflect.TypeFor[*prefixReader](), reflect.TypeFor[readOnly](), reflect.TypeFor[*ownedReader]()} {
		want(v, viewCaps)
	}
	for _, v := range []reflect.Type{reflect.TypeFor[*prefixMessages](), reflect.TypeFor[*ownedMessages]()} {
		want(v, viewCaps, []reflect.Type{mr})
	}
	want(reflect.TypeFor[*ownedWriter](), viewCaps, sendOnlyCaps)
	want(reflect.TypeFor[*ownedMessageWriter](), viewCaps, sendOnlyCaps, []reflect.Type{mw, mb})
	want(reflect.TypeFor[*prefixedConn](), viewCaps, sendOnlyCaps, connOnlyCaps)
}

// capHalf implements every forwarded capability with recognisable
// results.
type capHalf struct {
	net.Conn
	calls []string
}

func (c *capHalf) rec(s string) { c.calls = append(c.calls, s) }
func (c *capHalf) ConnectionState() tls.ConnectionState {
	return tls.ConnectionState{HandshakeComplete: true, ServerName: "cap"}
}
func (c *capHalf) HandshakeContext(context.Context) error                   { c.rec("hs"); return nil }
func (c *capHalf) ExportKeyingMaterial(string, []byte, int) ([]byte, error) { return []byte("km"), nil }
func (c *capHalf) CarrierSecure() bool                                      { return true }
func (c *capHalf) ReportsFailure() bool                                     { return true }
func (c *capHalf) CarrierConn() any                                         { return "cc" }
func (c *capHalf) PhysicalCarrier() any                                     { return "pc" }
func (c *capHalf) RecordCloseCode(ApplicationErrorCode, string)             { c.rec("code") }
func (c *capHalf) MaxRecord() int                                           { return 4096 }
func (c *capHalf) CarrierPeer() any                                         { return "peer" }
func (c *capHalf) CarrierTaken()                                            { c.rec("taken") }
func (c *capHalf) CloseWrite() error                                        { c.rec("cw"); return nil }
func (c *capHalf) Abort(error)                                              { c.rec("abort") }
func (c *capHalf) WriteBuffers([][]byte) (int, error)                       { c.rec("wb"); return 7, nil }
func (c *capHalf) Unsent() (uint64, bool)                                   { return 11, true }
func (c *capHalf) TLSRecords() bool                                         { return true }
func (c *capHalf) NetConn() net.Conn                                        { return c.Conn }

// Each view forwards each capability to the half it views, and reports a
// missing one as absent.
func TestCarrierViewForwarding(t *testing.T) {
	a, b := net.Pipe()
	defer func() { _ = a.Close(); _ = b.Close() }()
	h := &capHalf{Conn: a}
	byteView := withPrefix(PipeIO{R: h, W: h}, []byte("x")).R // over readOnly
	splitView := withPrefix(PipeIO{R: h}, []byte("x")).R
	msgView := withPrefix(PipeIO{R: h, W: h, Framing: FramingRecords}, []byte("x")).R
	var conn net.Conn = prefixConn(h, []byte("x"), newHalfOwner(NetPipe(h)))
	ownedW := takeoverOwnedWriter(h)
	for _, v := range []any{byteView, splitView, msgView, conn, ownedW} {
		if unprefixed(v) != h {
			t.Fatalf("%T does not view the half", v)
		}
		if v.(ConnectionStater).ConnectionState().ServerName != "cap" || !v.(CarrierSecure).CarrierSecure() ||
			!v.(FailureReporter).ReportsFailure() || v.(CarrierConner).CarrierConn() != "cc" ||
			v.(PhysicalCarrier).PhysicalCarrier() != "pc" ||
			v.(CarrierRecordLimit).MaxRecord() != 4096 || v.(CarrierPeer).CarrierPeer() != "peer" {
			t.Fatalf("%T: a value capability is not forwarded", v)
		}
		if k, err := v.(KeyingMaterialExporter).ExportKeyingMaterial("l", nil, 2); err != nil || string(k) != "km" {
			t.Fatalf("%T: exporter %q %v", v, k, err)
		}
		h.calls = nil
		_ = v.(Handshaker).HandshakeContext(context.Background())
		v.(CloseCodeRecorder).RecordCloseCode(1, "")
		v.(CarrierTaker).CarrierTaken()
		if strings.Join(h.calls, ",") != "hs,code,taken" {
			t.Fatalf("%T: calls %v", v, h.calls)
		}
	}
	h.calls = nil
	_ = ownedW.(CloseWriter).CloseWrite()
	if strings.Join(h.calls, ",") != "cw" {
		t.Fatalf("calls %v", h.calls)
	}
	h.calls = nil
	_ = conn.(CloseWriter).CloseWrite()
	conn.(Aborter).Abort(nil)
	n, _ := conn.(BuffersWriter).WriteBuffers(nil)
	if strings.Join(h.calls, ",") != "cw,abort,wb" || n != 7 {
		t.Fatalf("calls %v", h.calls)
	}
	if u, ok := conn.(UnsentReporter).Unsent(); u != 11 || !ok {
		t.Fatal("Unsent")
	}
	if !conn.(TLSRecordCarrier).TLSRecords() || conn.(NetConner).NetConn() != a {
		t.Fatal("TLSRecords or NetConn")
	}

	// Absent capabilities.
	var plain net.Conn = prefixConn(a, []byte("x"), newHalfOwner(NetPipe(a)))
	if plain.(ConnectionStater).ConnectionState().HandshakeComplete || plain.(CarrierSecure).CarrierSecure() ||
		plain.(FailureReporter).ReportsFailure() || plain.(CarrierConner).CarrierConn() != nil ||
		plain.(PhysicalCarrier).PhysicalCarrier() != nil ||
		plain.(TLSRecordCarrier).TLSRecords() || plain.(NetConner).NetConn() != a {
		t.Fatal("absent capabilities reported present")
	}
	if _, err := plain.(KeyingMaterialExporter).ExportKeyingMaterial("l", nil, 2); !errors.Is(err, errors.ErrUnsupported) {
		t.Fatal("exporter without one", err)
	}
	if err := plain.(CloseWriter).CloseWrite(); !errors.Is(err, errors.ErrUnsupported) {
		t.Fatal("CloseWrite without one", err)
	}
	if _, ok := plain.(UnsentReporter).Unsent(); ok {
		t.Fatal("Unsent without one")
	}
	if _, ok := TerminatedTLS(PipeIO{R: plain, W: plain}); ok {
		t.Fatal("TLS reported for a plain connection")
	}
}

// takeoverOwnedWriter is the sending half Takeover.Pipe gives for a split
// carrier whose sending half is w.
func takeoverOwnedWriter(w io.WriteCloser) any {
	p := PipeIO{W: w}
	q, _ := takeoverPipe(p, nil, newHalfOwner(p))
	return q.W
}

// carrierViewReadGate lets the peer finish writing before the server
// consumes its first inbound bytes.
type carrierViewReadGate struct {
	net.Conn
	afterRead func()
}

func (g *carrierViewReadGate) Read(p []byte) (int, error) {
	n, err := g.Conn.Read(p)
	if n > 0 {
		g.afterRead()
	}
	return n, err
}

func TestCarrierViewStatsForWriterOnly(t *testing.T) {
	a, b := net.Pipe()
	defer func() { _ = a.Close(); _ = b.Close() }()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	p := NetPipe(a)
	p.W = &ownedWriter{
		sendCaps: sendCaps{viewOf(struct{ io.Writer }{a})},
		o:        newHalfOwner(p),
	}
	read, release := make(chan struct{}), make(chan struct{})
	openRead := sync.OnceFunc(func() { close(release) })
	serverPipe := &carrierViewReadGate{Conn: b, afterRead: sync.OnceFunc(func() {
		close(read)
		<-release
	})}
	serve := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		c, err := Server(ctx, serverPipe, nil)
		serve <- err
		if err == nil {
			<-ctx.Done()
			_ = c.Close()
		}
	}()
	defer func() { openRead(); cancel(); <-done }()
	c, err := Start(ctx, p, RoleClient, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	// Force the client to finish Start while Server is still waiting for
	// the client's first record, then let Server report before cleanup.
	select {
	case <-read:
	case <-ctx.Done():
		t.Fatal("server did not reach the read gate")
	}
	openRead()
	err = <-serve
	if err != nil {
		t.Fatal(err)
	}
	if stats := c.PipeStats(); len(stats) != 1 || stats[0].CarrierSample != nil {
		t.Fatalf("write-only view stats = %+v", stats)
	}
	pipe, ok := c.Pipe(0)
	if !ok {
		t.Fatal("bootstrap pipe absent")
	}
	if stats, ok := pipe.Stats(); !ok || stats.CarrierSample != nil {
		t.Fatalf("pipe with no carrier identity: ok = %t, sample = %+v", ok, stats.CarrierSample)
	}
}
