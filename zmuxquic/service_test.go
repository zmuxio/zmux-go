package zmuxquic

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/quicvarint"
	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/internal/wire"
	"github.com/zmuxio/zmux-go/varint"
)

func TestNextBatchTransportAdmissionAndLanes(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 40*time.Second)
	defer cancel()
	serverTLS, clientTLS := tlsConfigs(t)
	ql, err := quic.ListenAddr(net.JoinHostPort(clientTLS.ServerName, "0"), serverTLS, testQUIC(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ql.Close() }()
	qc, err := quic.DialAddr(ctx, ql.Addr().String(), clientTLS, testQUIC(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = qc.CloseWithError(0, "") }()
	peer, err := ql.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = peer.CloseWithError(0, "") }()

	var block, tap atomic.Bool
	header := make(chan []byte, 1)
	resumeLane := make(chan struct{})
	releaseLane := sync.OnceFunc(func() { close(resumeLane) })
	defer releaseLane()
	entered := make(chan struct{})
	canceled := make(chan error, 1)
	l := mustNewListener(&zmux.ListenerConfig{
		Config: mpConfig(),
		WrapPipe: zmux.WrapPipeFunc(func(ctx context.Context, p zmux.PipeIO, _ zmux.PipeHint) (zmux.PipeIO, error) {
			if p.W != nil && block.CompareAndSwap(true, false) {
				close(entered)
				<-ctx.Done()
				canceled <- ctx.Err()
				return zmux.PipeIO{}, ctx.Err()
			}
			if lane, ok := p.R.(*laneReader); ok && tap.CompareAndSwap(true, false) {
				n, err := quicvarint.Read(quicvarint.NewReader(lane))
				if err != nil || n > 32 {
					return zmux.PipeIO{}, errors.New("invalid lane header in test")
				}
				body := make([]byte, n)
				if _, err := io.ReadFull(lane, body); err != nil {
					return zmux.PipeIO{}, err
				}
				prefix := append(varint.AppendVarint(nil, n), body...)
				header <- prefix
				select {
				case <-resumeLane:
					p.R = &replayLane{laneReader: lane, r: io.MultiReader(bytes.NewReader(prefix), lane)}
				case <-ctx.Done():
					return zmux.PipeIO{}, ctx.Err()
				}
			}
			return p, nil
		}),
	})
	defer func() { _ = l.Close() }()
	svc, err := AdoptConn(ctx, l, peer)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := AdoptConn(ctx, l, peer); err == nil {
		t.Fatal("registered two receivers for one connection")
	}
	c, err := Client(ctx, qc, mpConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Abort(0, "")
	s, err := l.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Abort(0, "")
	block.Store(true)
	pending, err := qc.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer pending.CancelWrite(0)
	if _, err := pending.Write([]byte{1}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	svc.StopAdmission()
	svc.StopAdmission()
	select {
	case err := <-canceled:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	rejectLane := func(conn *quic.Conn, hdr []byte) {
		t.Helper()
		lane, err := conn.OpenUniStreamSync(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer lane.CancelWrite(0)
		if _, err := lane.Write(hdr); err != nil {
			t.Fatal(err)
		}
		select {
		case <-lane.Context().Done():
			if err, ok := errors.AsType[*quic.StreamError](context.Cause(lane.Context())); !ok || !err.Remote {
				t.Fatalf("lane stop: %v", context.Cause(lane.Context()))
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	unknown := wire.AppendPipeLane(nil, make([]byte, 16), 0, 1)
	rejectLane(qc, append(varint.AppendVarint(nil, uint64(len(unknown))), unknown...))

	other, err := quic.DialAddr(ctx, ql.Addr().String(), clientTLS, testQUIC(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = other.CloseWithError(0, "") }()
	otherPeer, err := ql.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = otherPeer.CloseWithError(0, "") }()
	if _, err := AdoptConn(ctx, l, otherPeer); err != nil {
		t.Fatal(err)
	}
	otherClient, err := Client(ctx, other, mpConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer otherClient.Abort(0, "")
	otherSession, err := l.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer otherSession.Abort(0, "")

	tap.Store(true)
	var lanes sync.WaitGroup
	for range 4 {
		lanes.Go(func() { echo(t, c, s, 1<<20) })
	}
	select {
	case valid := <-header:
		// The legitimate lane cannot attach until this check completes.
		// A matching Lane Key and lane number are insufficient on other.
		rejectLane(other, valid)
		releaseLane()
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	lanes.Wait()
	if stats := s.PipeStats(); len(stats) != 1 || stats[0].LanesIn < 2 {
		t.Fatalf("server did not receive authenticated lanes after admission stopped: %+v", stats)
	}
	_ = c.Close()
	_ = s.Close()
	if err := svc.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if cause := context.Cause(peer.Context()); cause != nil {
		t.Fatalf("service closed the host's connection: %v", cause)
	}
}

type replayLane struct {
	*laneReader
	r io.Reader
}

func (l *replayLane) Read(p []byte) (int, error) { return l.r.Read(p) }
