package zmuxquic

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"sync"

	"github.com/quic-go/quic-go"
	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/internal/labels"
)

// ConnService accepts pipe streams on one QUIC connection and keeps its lane
// receiver active while any of its attachments is still draining.
type ConnService struct {
	stopAdmission context.CancelFunc
	stopLanes     context.CancelFunc
	done          chan struct{}
}

// StopAdmission cancels stream acceptance and setups that have not handed off
// their pipes. Existing attachments continue to receive authenticated lanes.
func (s *ConnService) StopAdmission() {
	if s != nil && s.stopAdmission != nil {
		s.stopAdmission()
	}
}

// Stop also stops accepting lanes. The connection and handed-off streams
// remain the responsibility of their owners.
func (s *ConnService) Stop() {
	s.StopAdmission()
	if s != nil && s.stopLanes != nil {
		s.stopLanes()
	}
}

// Wait waits for the service's accept, setup and attachment-drain tasks.
// A successful Wait says nothing about whether the QUIC connection is open.
func (s *ConnService) Wait(ctx context.Context) error {
	if s == nil || s.done == nil {
		return net.ErrClosed
	}
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

var errConnectionEnded = errors.New("zmuxquic: connection ended before adoption")

// ServeConn starts a service on qc. See AdoptConn for ownership and cancellation.
func ServeConn(ctx context.Context, l *zmux.Listener, qc *quic.Conn) error {
	_, err := AdoptConn(ctx, l, qc)
	return err
}

// laneConns registers the lane receiver once for the lifetime of a physical
// connection. A completed service does not make the connection adoptable again.
var laneConns sync.Map // *quic.Conn -> struct{}

func registerReceiver(qc *quic.Conn) bool {
	if qc.Context().Err() != nil {
		return false
	}
	if _, loaded := laneConns.LoadOrStore(qc, struct{}{}); loaded {
		return false
	}
	context.AfterFunc(qc.Context(), func() { laneConns.Delete(qc) })
	return true
}

// AdoptConn starts accepting pipe streams from qc. admitCtx bounds admission,
// including the first carrier and pending reservations. The lifetime of lanes
// follows the established attachments and the connection. A detached fallback
// retains qc and its streams.
func AdoptConn(admitCtx context.Context, l *zmux.Listener, qc *quic.Conn) (*ConnService, error) {
	if l == nil || qc == nil {
		return nil, zmux.ErrInvalidConfig
	}
	if err := admitCtx.Err(); err != nil {
		return nil, err
	}
	if !registerReceiver(qc) {
		if qc.Context().Err() != nil {
			return nil, errConnectionEnded
		}
		return nil, errors.New("zmuxquic: connection already has a lane receiver")
	}
	a, err := l.Adapter().Arrive(netip.AddrPort{}, addrPort(qc.RemoteAddr()))
	if err != nil {
		_ = qc.CloseWithError(0, "")
		if errors.Is(err, net.ErrClosed) {
			return nil, err
		}
		done := make(chan struct{})
		close(done)
		return &ConnService{stopAdmission: func() {}, stopLanes: func() {}, done: done}, nil
	}
	setup, stopAdmission := a.SetupContext(admitCtx)
	lanes, stopLanes := context.WithCancel(context.WithoutCancel(admitCtx))
	s := &ConnService{stopAdmission: stopAdmission, stopLanes: stopLanes, done: make(chan struct{})}
	stopConn := context.AfterFunc(qc.Context(), stopLanes)
	labels.Go(setup, "quic-accept-pipes", func() {
		defer close(s.done)
		defer stopConn()
		defer s.Stop()
		serveArrival(setup, lanes, stopAdmission, stopLanes, qc, a)
	})
	return s, nil
}

func serveArrival(setup, lanes context.Context, stopAdmission, stopLanes context.CancelFunc, qc *quic.Conn, a *zmux.Arrival) {
	defer a.Release()
	needsAdmission := !a.Admitted()
	p, ok := a.AdmitFirst(setup, func(first context.Context) (zmux.PipeIO, error) {
		if needsAdmission {
			if err := handshakeReady(first, qc); err != nil {
				return zmux.PipeIO{}, err
			}
			if qc.ConnectionState().Used0RTT {
				return zmux.PipeIO{}, errors.New("zmuxquic: admission requires a completed handshake")
			}
		}
		stream, err := qc.AcceptStream(first)
		if err != nil {
			return zmux.PipeIO{}, err
		}
		return streamPipe(qc, stream), nil
	})
	if !ok {
		if needsAdmission && setup.Err() == nil && !a.Detached() {
			_ = qc.CloseWithError(0, "")
		}
		return
	}
	var pipes sync.WaitGroup
	lifetime := laneLifetime{ctx: lanes, ready: make(chan struct{})}
	dispatch := func(p zmux.PipeIO) {
		run, err := a.Take(setup, p, zmux.PipeHint{})
		if err != nil {
			return
		}
		pipes.Add(1)
		lifetime.retain()
		labels.Go(setup, "quic-pipe", func() {
			defer pipes.Done()
			defer lifetime.release()
			_ = run(setup)
		})
	}
	dispatch(p)
	laneDone := make(chan struct{})
	labels.Go(lanes, "quic-accept-lanes", func() {
		defer close(laneDone)
		lifetime.accept(qc, a)
	})
	for setup.Err() == nil {
		stream, err := qc.AcceptStream(setup)
		if err != nil {
			break
		}
		p := streamPipe(qc, stream)
		if setup.Err() != nil {
			zmux.ClosePipeIO(p)
			break
		}
		dispatch(p)
	}
	stopAdmission()
	a.Release()
	pipes.Wait()
	stopLanes()
	<-laneDone
}

// laneLifetime keeps one receiver, suspending its accept when there is no
// pending or attached pipe on the connection. A pending pipe permits bounded
// header setup while the core publishes its attachment.
type laneLifetime struct {
	ctx    context.Context
	window context.Context
	cancel context.CancelFunc
	ready  chan struct{}
	refs   int
	mu     sync.Mutex
}

func (l *laneLifetime) retain() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.refs == 0 {
		l.window, l.cancel = context.WithCancel(l.ctx)
		close(l.ready)
	}
	l.refs++
}

func (l *laneLifetime) release() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.refs--
	if l.refs == 0 {
		l.cancel()
		l.window, l.cancel = nil, nil
		l.ready = make(chan struct{})
	}
}

func (l *laneLifetime) accept(qc *quic.Conn, a *zmux.Arrival) {
	for l.ctx.Err() == nil {
		l.mu.Lock()
		window, ready := l.window, l.ready
		l.mu.Unlock()
		if window != nil {
			acceptLanes(window, qc, a)
			if window.Err() == nil {
				return
			}
			continue
		}
		select {
		case <-ready:
		case <-l.ctx.Done():
		}
	}
}

func acceptLanes(ctx context.Context, qc *quic.Conn, a *zmux.Arrival) {
	var pending sync.WaitGroup
	defer pending.Wait()
	for ctx.Err() == nil {
		stream, err := qc.AcceptUniStream(ctx)
		if err != nil {
			return
		}
		if ctx.Err() != nil {
			stream.CancelRead(carrierFailed)
			return
		}
		pending.Add(1)
		labels.Go(ctx, "quic-lane", func() {
			defer pending.Done()
			p := zmux.PipeIO{R: &laneReader{s: stream, qc: qc}, Carrier: zmux.CarrierQUIC}
			if a != nil {
				_ = a.HandlePipe(ctx, p, zmux.PipeHint{})
			} else {
				_ = zmux.HandleLane(ctx, p)
			}
		})
	}
}

func clientLanes(qc *quic.Conn) {
	if registerReceiver(qc) {
		labels.Go(qc.Context(), "quic-client-lanes", func() { acceptLanes(qc.Context(), qc, nil) })
	}
}
