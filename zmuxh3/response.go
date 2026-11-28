package zmuxh3

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/zmuxio/zmux-go/internal/poolhost"
)

type upgradeState struct {
	taken     *http3.Stream
	started   bool
	committed bool
}

// upgradeWithin reports whether this handler owns the CONNECT stream,
// so admission can fall back only while the response is still untouched.
func upgradeWithin(ctx context.Context, s *Server, w http.ResponseWriter, r *http.Request) (*Session, upgradeState, error) {
	var state upgradeState
	c, _ := r.Context().Value(wtConnKey{}).(*wtConn)
	settings, settingOK := w.(http3.Settingser)
	streamer, streamOK := w.(http3.HTTPStreamer)
	if c == nil || !settingOK || !streamOK || r.Method != http.MethodConnect || !wtProtocolAccepted(r.Proto) {
		return nil, state, errors.New("zmuxh3: WebTransport requires an HTTP/3 Extended CONNECT")
	}
	early := r.Header.Get("Early-Data") == "1"
	select {
	case <-c.qc.HandshakeComplete():
	default:
		early = true
	}
	if early {
		w.WriteHeader(http.StatusTooEarly)
		state.started = true
		return nil, state, errors.New("zmuxh3: WebTransport CONNECT in early data")
	}
	if !s.originAllowed(r) {
		w.WriteHeader(http.StatusForbidden)
		state.started = true
		return nil, state, errors.New("zmuxh3: WebTransport origin rejected")
	}
	settingsCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	if err := c.waitSettings(settingsCtx, settings, r.Proto != wtProtocol); err != nil {
		if errors.Is(err, errWTSettings) && ctx.Err() == nil {
			state.taken = streamer.HTTPStream()
			if state.taken != nil {
				code := quic.StreamErrorCode(http3.ErrCodeMessageError)
				state.taken.CancelRead(code)
				state.taken.CancelWrite(code)
			}
		}
		return nil, state, err
	}
	if err := context.Cause(ctx); err != nil {
		return nil, state, err
	}

	controller := http.NewResponseController(w)
	finished := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		defer close(finished)
		poolhost.Safe(nil, "WebTransport response interrupt", func() {
			_ = controller.SetWriteDeadline(time.Now())
		})
	})
	var sess *Session
	var err error
	if !poolhost.Safe(nil, "WebTransport response commit", func() {
		state.taken = streamer.HTTPStream()
		if state.taken == nil {
			err = errors.New("zmuxh3: HTTP/3 response did not provide CONNECT")
			return
		}
		sess, err = c.newSession(state.taken)
		if err != nil {
			code := quic.StreamErrorCode(http3.ErrCodeRequestRejected)
			state.taken.CancelRead(code)
			state.taken.CancelWrite(code)
			return
		}
		state.started = true
		w.WriteHeader(http.StatusOK)
		err = controller.Flush()
		state.committed = err == nil
	}) {
		err = errors.New("zmuxh3: WebTransport response commit panicked")
	}
	if !stop() {
		<-finished
	}
	if err == nil {
		err = context.Cause(ctx)
	}
	if err != nil {
		if sess != nil {
			sess.finish(err)
		}
		if state.taken != nil && !errors.Is(err, errWTSessionLimit) {
			wtCancelControl(state.taken)
		}
		return nil, state, err
	}
	sess.start()
	return sess, state, nil
}
