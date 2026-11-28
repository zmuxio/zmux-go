// Package xorpipe demonstrates a carrier transform built on the public
// zmux API: distinguish a carrier header in Admission, wrap the byte stream
// in ListenerConfig.WrapPipe, and release a held first write.
//
// XOR is no protection. The example illustrates carrier composition only.
//
// The client sends a header with a lead byte, header length, timestamp in
// milliseconds (big endian uint64) and an eight-byte nonce, followed by
// the transformed zmux byte stream. The server rejects a repeated nonce.
package xorpipe

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"time"

	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/admit"
)

// Variant labels the transform's carriers (zmux.PipeIO.Variant).
const Variant = "xor"

// HeaderLen is the length of the client's header.
const HeaderLen = 18

const lead = 0x01 // admit.ClassifyFirstBytes judges a byte stream starting so Other

// Client returns the client pipe over c. Its first write prefixes the
// carrier header. Draw a fresh nonce for each carrier.
func Client(c net.Conn, key byte, nonce [8]byte) zmux.PipeIO {
	return zmux.PipeIO{
		R:       &xorReader{r: c, key: key},
		W:       &clientWriter{w: c, key: key, nonce: nonce},
		Variant: Variant,
	}
}

type clientWriter struct {
	xorWriter
	nonce [8]byte
	sent  bool
}

func (w *clientWriter) Write(b []byte) (int, error) {
	if w.sent {
		return w.xorWriter.Write(b)
	}
	w.sent = true
	frame := make([]byte, HeaderLen+len(b))
	frame[0], frame[1] = lead, HeaderLen
	binary.BigEndian.PutUint64(frame[2:], uint64(time.Now().UnixMilli()))
	copy(frame[10:HeaderLen], w.nonce[:])
	xorInto(frame[HeaderLen:], b, w.key)
	n, err := w.w.Write(frame)
	if err == nil && n != len(frame) {
		err = io.ErrShortWrite
	}
	return max(0, n-HeaderLen), err
}

// Admission tells zmux first records ("" variant, accepted as they are)
// from carriers of the transform (Variant, accepted past their header,
// read with VerdictMore); anything else is closed.
func Admission() *zmux.Admission {
	return &zmux.Admission{Peek: 2, MaxPeek: HeaderLen,
		Decide: func(_ context.Context, _ zmux.CarrierInfo, prefix []byte) zmux.Decision {
			c, need := admit.ClassifyFirstBytes(prefix, zmux.FramingByteStream)
			switch {
			case c == admit.FirstBytesRecord:
				return zmux.Decision{Verdict: zmux.VerdictAccept}
			case c == admit.FirstBytesNeedMore:
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

// WrapPipe checks the carrier header with filter and installs the byte
// transform. Carriers admitted without such a header pass through.
func WrapPipe(filter *admit.ReplayFilter, key byte) zmux.WrapPipeFunc {
	return func(_ context.Context, p zmux.PipeIO, hint zmux.PipeHint) (zmux.PipeIO, error) {
		h, ok := hint.Admitted.([HeaderLen]byte)
		if !ok {
			return p, nil
		}
		t := int64(binary.BigEndian.Uint64(h[2:]))
		if filter.Check(h[10:], t) != admit.ReplayFresh {
			return zmux.PipeIO{}, errors.New("xorpipe: stale or repeated carrier header")
		}
		p.R, p.W = &xorReader{r: p.R, key: key}, &xorWriter{w: p.W, key: key}
		return p, nil
	}
}

type xorReader struct {
	r   io.ReadCloser
	key byte
}

func (x *xorReader) Read(b []byte) (int, error) {
	n, err := x.r.Read(b)
	xorInto(b[:n], b[:n], x.key)
	return n, err
}

func (x *xorReader) Close() error { return x.r.Close() }

type xorWriter struct {
	w   io.WriteCloser
	key byte
}

func (x *xorWriter) Write(b []byte) (int, error) {
	encoded := make([]byte, len(b))
	xorInto(encoded, b, x.key)
	return x.w.Write(encoded)
}
func (x *xorWriter) Close() error { return x.w.Close() }

func xorInto(dst, src []byte, key byte) {
	for i, v := range src {
		dst[i] = v ^ key
	}
}
