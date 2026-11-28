package zmuxh3

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/quic-go/quicvarint"
	"github.com/zmuxio/zmux-go/internal/labels"
)

const (
	defaultWTReorderingTimeout = 5 * time.Second
	defaultWTPendingStreams    = 128
)

type wtConnKey struct{}

type wtConn struct {
	qc         *quic.Conn
	timeout    time.Duration
	maxPending int
	server     bool

	mu      sync.Mutex
	ready   chan struct{} // closed when the CONNECT stream is registered
	session *Session
	waiting int
}

func newWTConn(qc *quic.Conn, server bool, timeout time.Duration, maxPending int) *wtConn {
	if timeout <= 0 {
		timeout = defaultWTReorderingTimeout
	}
	if maxPending <= 0 {
		maxPending = defaultWTPendingStreams
	}
	return &wtConn{qc: qc, server: server, timeout: timeout, maxPending: maxPending, ready: make(chan struct{})}
}

func (c *wtConn) waitSettings(ctx context.Context, h3 http3.Settingser, legacy bool) error {
	select {
	case <-h3.ReceivedSettings():
		if err := context.Cause(ctx); err != nil {
			return err
		}
		err := checkWTSettings(h3.Settings(), c.qc.ConnectionState(), !c.server, legacy)
		if errors.Is(err, errWTSettingsValue) {
			_ = c.qc.CloseWithError(quic.ApplicationErrorCode(http3.ErrCodeSettingsError), err.Error())
		}
		return err
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-c.qc.Context().Done():
		return context.Cause(c.qc.Context())
	}
}

func (c *wtConn) newSession(control wtControl) (*Session, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.session != nil {
		return nil, errWTSessionLimit
	}
	ctx, cancel := context.WithCancelCause(QUICConnContext(c.qc.Context(), c.qc))
	state := c.qc.ConnectionState()
	s := &Session{
		conn: c, control: control, id: control.StreamID(), ctx: ctx, cancel: cancel,
		draining: make(chan struct{}), changed: make(chan struct{}),
		controlDone: make(chan struct{}), streams: make(map[*Stream]struct{}),
		state: state, datagramOverhead: quicvarint.Len(uint64(control.StreamID() / 4)),
	}
	c.session = s
	close(c.ready)
	return s, nil
}

// run owns the QUIC accept loops. The raw HTTP/3 connection still handles
// SETTINGS, request framing, QPACK and RFC 9297 datagrams.
func (c *wtConn) run(bidi func(*quic.Stream), uni func(*quic.ReceiveStream)) {
	ctx := c.qc.Context()
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			r, err := c.qc.AcceptUniStream(ctx)
			if err != nil {
				return
			}
			wg.Go(func() {
				deadline := time.Now().Add(c.timeout)
				_ = r.SetReadDeadline(deadline)
				kind, err := quicvarint.Peek(r)
				switch {
				case err != nil:
					r.CancelRead(quic.StreamErrorCode(http3.ErrCodeRequestIncomplete))
					r.ReleaseStreamCredit()
				case kind == wtUni:
					c.handleData(deadline, r, nil)
				default:
					_ = r.SetReadDeadline(time.Time{})
					uni(r)
				}
			})
		}
	})
	for {
		s, err := c.qc.AcceptStream(ctx)
		if err != nil {
			break
		}
		wg.Go(func() {
			deadline := time.Now().Add(c.timeout)
			_ = s.SetReadDeadline(deadline)
			kind, err := quicvarint.Peek(s)
			switch {
			case err != nil:
				code := quic.StreamErrorCode(http3.ErrCodeRequestIncomplete)
				s.CancelRead(code)
				s.CancelWrite(code)
				s.ReleaseStreamCredit()
			case kind == wtBidi:
				c.handleData(deadline, s, s)
			default:
				_ = s.SetReadDeadline(time.Time{})
				bidi(s)
			}
		})
	}
	wg.Wait()
}

func (c *wtConn) handleData(deadline time.Time, r wtReceive, w wtSend) {
	if !c.waitSession(deadline) {
		rejectWTStream(r, w, wtBufferedStreamRejected)
		return
	}
	// Consume only the type and full CONNECT stream ID. Payload remains
	// in the QUIC stream, including while waiting for CONNECT.
	if _, err := wtReadVarint(r); err != nil {
		rejectWTStream(r, w, wtBufferedStreamRejected)
		return
	}
	id, err := wtReadVarint(r)
	if err != nil {
		rejectWTStream(r, w, wtBufferedStreamRejected)
		return
	}
	if id%4 != 0 {
		_ = c.qc.CloseWithError(quic.ApplicationErrorCode(http3.ErrCodeIDError), "invalid WebTransport session ID")
		return
	}
	_ = r.SetReadDeadline(time.Time{})
	if code := c.queueStream(quic.StreamID(id), r, w); code != 0 {
		rejectWTStream(r, w, code)
	}
}

func rejectWTStream(r wtReceive, w wtSend, code quic.StreamErrorCode) {
	r.CancelRead(code)
	r.ReleaseStreamCredit()
	if w != nil {
		w.CancelWrite(code)
	}
}

// Wait to parse the association until CONNECT has verified the peer's
// SETTINGS. Incoming streams can arrive before the CONNECT HEADERS.
func (c *wtConn) waitSession(deadline time.Time) bool {
	c.mu.Lock()
	if c.session != nil {
		c.mu.Unlock()
		return true
	}
	if c.waiting >= c.maxPending {
		c.mu.Unlock()
		return false
	}
	c.waiting++
	c.mu.Unlock()

	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	ready := false
	select {
	case <-c.ready:
		ready = true
	case <-timer.C:
	case <-c.qc.Context().Done():
	}
	c.mu.Lock()
	c.waiting--
	c.mu.Unlock()
	return ready
}

func (c *wtConn) queueStream(id quic.StreamID, r wtReceive, w wtSend) quic.StreamErrorCode {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.session
	switch {
	case s.id != id:
		return wtBufferedStreamRejected
	case s.ctx.Err() != nil:
		return wtSessionGone
	case c.waiting+len(s.incomingBidi)+len(s.incomingUni) >= c.maxPending:
		return wtBufferedStreamRejected
	}
	stream := s.trackStream(r, w)
	if w == nil {
		s.incomingUni = append(s.incomingUni, stream)
	} else {
		s.incomingBidi = append(s.incomingBidi, stream)
	}
	s.wakeAccept()
	return 0
}

func (c *wtConn) runClient(h3 *http3.RawClientConn) {
	labels.Go(c.qc.Context(), "wt-connection", func() {
		c.run(h3.HandleBidirectionalStream, h3.HandleUnidirectionalStream)
	})
}

func wtQUICConfig(config *quic.Config) *quic.Config {
	if config == nil {
		config = &quic.Config{EnableStreamResetPartialDelivery: true}
	} else {
		config = config.Clone()
	}
	config.EnableDatagrams = true
	return config
}

func wtConnError(qc *quic.Conn) error {
	if qc == nil || qc.Context() == nil {
		return net.ErrClosed
	}
	return context.Cause(qc.Context())
}

func wtCancelControl(s wtControl) {
	code := quic.StreamErrorCode(http3.ErrCodeRequestCanceled)
	s.CancelRead(code)
	s.CancelWrite(code)
}

func wtSetupError(ctx context.Context, err error) error {
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	if err == nil {
		return errors.New("zmuxh3: WebTransport CONNECT could not be established")
	}
	return err
}
