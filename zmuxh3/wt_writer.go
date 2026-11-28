package zmuxh3

import (
	"io"
	"sync"
	"time"

	"github.com/zmuxio/zmux-go/carrier"
	"github.com/zmuxio/zmux-go/internal/carrierio"
	"github.com/zmuxio/zmux-go/internal/carrierstats"
	"github.com/zmuxio/zmux-go/zmuxquic/streamio"
)

type wtSendStream interface {
	io.WriteCloser
	CancelWrite(StreamErrorCode)
	SetWriteDeadline(time.Time) error
}

// wtWriter adapts one WebTransport sending half to the shared stream writer.
type wtWriter[S wtSendStream] struct {
	s     S
	state quicState
	lease *carrierio.HalfLease
	send  streamio.Writer
	end   sync.Once
}

func (w *wtWriter[S]) Write(p []byte) (int, error) { return w.send.Write(w.s, p) }

func (w *wtWriter[S]) WriteBuffers(bufs [][]byte) (int, error) {
	return carrier.WriteFlat(w, bufs)
}

func (w *wtWriter[S]) Close() error {
	err := w.send.CloseInterrupting(w.s, w.interrupt)
	w.end.Do(w.lease.HalfEnded)
	return err
}

func (w *wtWriter[S]) Abort(_ error) {
	w.send.AbortOpen(func() {
		w.s.CancelWrite(carrierFailed)
	})
	w.end.Do(w.lease.HalfEnded)
}

func (w *wtWriter[S]) interrupt() {
	_ = w.s.SetWriteDeadline(time.Unix(1, 0))
}

// CarrierSampler is for internal use by zmux modules.
func (w *wtWriter[S]) CarrierSampler() *carrierstats.Sampler { return w.state.CarrierSampler() }
