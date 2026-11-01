// Package xorpipe is an example of a carrier transform built on the public
// zmux API alone: a header that admission tells apart from a zmux first
// record, a first flight that carries the client's first streams in the
// carrier's first write, a server verdict on that flight (a replayed
// header is refused, and the session sends its streams again), and a byte
// transform (XOR with a key byte) on everything after the header.
//
// XOR is no protection: the package shows the hooks a transform uses
// (EarlyClientCarrier, Admission with VerdictMore and Variant,
// ListenerConfig.WrapPipe, ReplayFilter, EarlyData.HoldFirstFlight), not a
// transform to deploy.
//
// Wire format, client to server: a 20-byte header (0x01, 20, the first
// flight's length as a big-endian uint16, the Unix time in milliseconds
// as a big-endian uint64, an 8-byte nonce), then the XORed byte stream,
// whose first flight bytes are those of the client's first write. Server
// to client: one verdict byte (1: the flight is accepted, 0: dropped),
// then the XORed byte stream.
package xorpipe

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"github.com/zmuxio/zmux-go"
)

// Variant labels the transform's carriers (zmux.PipeIO.Variant).
const Variant = "xor"

// HeaderLen is the length of the client's header.
const HeaderLen = 20

const lead = 0x01 // ClassifyFirstBytes judges a byte stream starting so Other

// Client returns the client pipe of the transform over c, with key and
// nonce (draw it at random for each carrier; a repeated nonce is
// refused as a replay). Its first write becomes the header and the first
// flight: use it with zmux.EarlyData{ZmuxPeer, Replayable, HoldFirstFlight}.
func Client(c net.Conn, key byte, nonce [8]byte) zmux.PipeIO {
	w := &clientWriter{c: c, key: key, nonce: nonce, verdict: make(chan bool, 1)}
	w.r = &clientReader{c: c, key: key, w: w}
	return zmux.PipeIO{R: w.r, W: w, Variant: Variant}
}

// clientWriter states.
const (
	stFirst   = iota // the next write is the first flight
	stHolding        // writes wait for the verdict
	stPassing
	stDropping // the flight was refused: writes are dropped until the restart
)

type clientWriter struct {
	c       net.Conn
	key     byte
	nonce   [8]byte
	verdict chan bool
	r       *clientReader

	mu    sync.Mutex
	state int
	held  [][]byte
}

func xor(b []byte, k byte) []byte {
	out := make([]byte, len(b))
	for i, x := range b {
		out[i] = x ^ k
	}
	return out
}

func (w *clientWriter) Write(b []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	switch w.state {
	case stFirst:
		if len(b) > 0xffff {
			return 0, errors.New("xorpipe: first flight over 64 KiB")
		}
		h := make([]byte, HeaderLen, HeaderLen+len(b))
		h[0], h[1] = lead, HeaderLen
		binary.BigEndian.PutUint16(h[2:], uint16(len(b)))
		binary.BigEndian.PutUint64(h[4:], uint64(time.Now().UnixMilli()))
		copy(h[12:], w.nonce[:])
		w.state = stHolding
		if _, err := w.c.Write(append(h, xor(b, w.key)...)); err != nil {
			return 0, err
		}
		return len(b), nil
	case stHolding:
		w.held = append(w.held, xor(b, w.key))
		return len(b), nil
	case stDropping:
		return len(b), nil
	case stPassing:
	}
	return w.c.Write(xor(b, w.key))
}

// decided applies the server's verdict.
func (w *clientWriter) decided(ok bool) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	held := w.held
	w.held = nil
	if !ok {
		w.state = stDropping
		return nil
	}
	w.state = stPassing
	for _, b := range held {
		if _, err := w.c.Write(b); err != nil {
			return err
		}
	}
	return nil
}

func (w *clientWriter) Close() error { return w.c.Close() }

// SendsEarlyData reports the first flight, which the server may refuse
// (zmux.EarlyClientCarrier).
func (w *clientWriter) SendsEarlyData() bool { return true }

// EarlyAccepted reports the server's verdict on the first flight
// (zmux.EarlyClientCarrier).
func (w *clientWriter) EarlyAccepted(ctx context.Context) (bool, error) {
	select {
	case ok := <-w.verdict:
		return ok, nil
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

// RestartEarly returns the same halves, the writer now passing writes
// (zmux.EarlyClientCarrier): the session sends its parameters and
// streams again on them.
func (w *clientWriter) RestartEarly(context.Context) (zmux.PipeIO, error) {
	w.mu.Lock()
	w.state = stPassing
	w.mu.Unlock()
	return zmux.PipeIO{R: w.r, W: w}, nil
}

type clientReader struct {
	c    net.Conn
	w    *clientWriter
	key  byte
	once bool
}

func (r *clientReader) Read(b []byte) (int, error) {
	if !r.once {
		r.once = true
		var v [1]byte
		if _, err := io.ReadFull(r.c, v[:]); err != nil {
			return 0, err
		}
		ok := v[0] == 1
		if err := r.w.decided(ok); err != nil {
			return 0, err
		}
		r.w.verdict <- ok
	}
	n, err := r.c.Read(b)
	for i := range b[:n] {
		b[i] ^= r.key
	}
	return n, err
}

func (r *clientReader) Close() error { return r.c.Close() }

// Admission tells zmux first records ("" variant, accepted as they are)
// from carriers of the transform (Variant, accepted past their header,
// read with VerdictMore); anything else is closed.
func Admission() *zmux.Admission {
	return &zmux.Admission{Peek: 2, MaxPeek: HeaderLen,
		Decide: func(_ context.Context, _ *zmux.CarrierInfo, prefix []byte) zmux.Decision {
			c, need := zmux.ClassifyFirstBytes(prefix, zmux.FramingByteStream)
			switch {
			case c == zmux.FirstBytesRecord:
				return zmux.Decision{Verdict: zmux.VerdictAccept}
			case c == zmux.FirstBytesNeedMore:
				return zmux.Decision{Verdict: zmux.VerdictMore, Need: need}
			case prefix[0] != lead || prefix[1] != HeaderLen:
				return zmux.Decision{Verdict: zmux.VerdictClose}
			case len(prefix) < HeaderLen:
				return zmux.Decision{Verdict: zmux.VerdictMore, Need: HeaderLen, Variant: Variant}
			}
			h := [HeaderLen]byte(prefix)
			return zmux.Decision{Verdict: zmux.VerdictAccept, Skip: HeaderLen, Variant: Variant, Value: h}
		}}
}

// WrapPipe is the server side (zmux.ListenerConfig.WrapPipe): for a carrier
// of the transform it checks the header against filter, answers the
// verdict, drops a refused flight and returns the XOR pipe; other carriers
// pass as they are.
func WrapPipe(filter *zmux.ReplayFilter, key byte) func(context.Context, zmux.PipeIO, zmux.JoinHint) (zmux.PipeIO, error) {
	return func(_ context.Context, p zmux.PipeIO, hint zmux.JoinHint) (zmux.PipeIO, error) {
		h, ok := hint.Admitted.([HeaderLen]byte)
		if !ok {
			return p, nil
		}
		flight := int64(binary.BigEndian.Uint16(h[2:]))
		t := int64(binary.BigEndian.Uint64(h[4:]))
		fresh := filter.Check(h[12:], t) == zmux.ReplayFresh
		v := []byte{0}
		if fresh {
			v[0] = 1
		}
		if _, err := p.W.Write(v); err != nil {
			return zmux.PipeIO{}, err
		}
		if !fresh {
			if _, err := io.CopyN(io.Discard, p.R, flight); err != nil {
				return zmux.PipeIO{}, err
			}
		}
		return zmux.PipeIO{R: &xorReader{r: p.R, key: key}, W: &xorWriter{w: p.W, key: key}}, nil
	}
}

type xorReader struct {
	r   io.ReadCloser
	key byte
}

func (x *xorReader) Read(b []byte) (int, error) {
	n, err := x.r.Read(b)
	for i := range b[:n] {
		b[i] ^= x.key
	}
	return n, err
}

func (x *xorReader) Close() error { return x.r.Close() }

type xorWriter struct {
	w   io.WriteCloser
	key byte
}

func (x *xorWriter) Write(b []byte) (int, error) { return x.w.Write(xor(b, x.key)) }
func (x *xorWriter) Close() error                { return x.w.Close() }
