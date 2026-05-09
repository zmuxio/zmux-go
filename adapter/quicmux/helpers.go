package quicmux

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/zmuxio/zmux-go"
	"github.com/zmuxio/zmux-go/internal/errutil"
	"github.com/zmuxio/zmux-go/internal/wire"
)

const quicmuxStreamPreludeMaxPayload = 16 << 10
const metadataPayloadPoolInitCap = 128
const metadataPayloadPoolMaxRetainedCap = 2 << 10
const quicWritevCoalesceMaxBytes = 64 << 10

const quicmuxOpenCaps = wire.CapabilityOpenMetadata | wire.CapabilityPriorityHints | wire.CapabilityStreamGroups

var metadataPayloadPool = sync.Pool{
	New: func() any {
		buf := make([]byte, 0, metadataPayloadPoolInitCap)
		return &buf
	},
}

var emptyStreamPrelude = []byte{0}
var errWritevPayloadTooLarge = errors.New("quicmux: writev payload length exceeds int")

type acceptedStreamMetadata struct {
	priority     uint64
	prioritySet  bool
	group        uint64
	groupEncoded bool
	openInfo     []byte
}

func readAcceptedStreamMetadata(reader io.Reader) (acceptedStreamMetadata, error) {
	if reader == nil {
		return acceptedStreamMetadata{}, protocolPreludeErr("missing stream prelude reader", io.ErrUnexpectedEOF)
	}

	var (
		metadataLen uint64
		prefixLen   int
		err         error
	)
	if byteReader, ok := reader.(io.ByteReader); ok {
		metadataLen, prefixLen, err = wire.ReadVarint(byteReader)
	} else {
		byteReader := singleByteReader{reader: reader}
		metadataLen, prefixLen, err = wire.ReadVarint(&byteReader)
	}
	if err != nil {
		return acceptedStreamMetadata{}, protocolPreludeErr("read stream prelude length", err)
	}
	if metadataLen == 0 {
		return acceptedStreamMetadata{}, nil
	}
	if metadataLen+uint64(prefixLen) > quicmuxStreamPreludeMaxPayload {
		return acceptedStreamMetadata{}, zmux.ErrOpenMetadataTooLarge
	}

	ptr := metadataPayloadPool.Get().(*[]byte)
	payload := *ptr
	if uint64(cap(payload)) < metadataLen {
		payload = make([]byte, int(metadataLen))
	} else {
		payload = payload[:int(metadataLen)]
	}
	defer func() {
		if cap(payload) > metadataPayloadPoolMaxRetainedCap {
			*ptr = make([]byte, 0, metadataPayloadPoolInitCap)
		} else {
			*ptr = payload[:0]
		}
		metadataPayloadPool.Put(ptr)
	}()
	if _, err := io.ReadFull(reader, payload); err != nil {
		return acceptedStreamMetadata{}, protocolPreludeErr("read stream metadata", err)
	}

	parsed, ok, err := wire.ParseStreamMetadataBytesView(payload)
	if err != nil {
		return acceptedStreamMetadata{}, protocolPreludeErr("parse stream metadata", err)
	}
	if !ok {
		return acceptedStreamMetadata{}, nil
	}

	return acceptedStreamMetadata{
		priority:     parsed.Priority,
		prioritySet:  parsed.HasPriority,
		group:        parsed.Group,
		groupEncoded: parsed.HasGroup,
		openInfo:     cloneBytes(parsed.OpenInfo),
	}, nil
}

func buildStreamPrelude(openPrefix []byte) ([]byte, error) {
	if len(openPrefix) != 0 {
		return openPrefix, nil
	}
	return emptyStreamPrelude, nil
}

func quicWritevTotalLen(parts [][]byte) (int, bool) {
	return quicWritevTotalLenWithin(parts, int(^uint(0)>>1))
}

func quicWritevTotalLenWithin(parts [][]byte, limit int) (int, bool) {
	if limit < 0 {
		return 0, false
	}
	total := 0
	for _, part := range parts {
		if len(part) > limit-total {
			return 0, false
		}
		total += len(part)
	}
	return total, true
}

func coalesceQuicWritevParts(parts [][]byte, total int) []byte {
	if total <= 0 {
		return nil
	}
	out := make([]byte, 0, total)
	for _, part := range parts {
		out = append(out, part...)
	}
	return out
}

func cloneBytes(src []byte) []byte {
	if len(src) == 0 {
		return nil
	}
	return append([]byte(nil), src...)
}

func uint64PtrIf(ok bool, v uint64) *uint64 {
	if !ok {
		return nil
	}
	value := v
	return &value
}

func protocolPreludeErr(op string, err error) error {
	if err == nil {
		return &zmux.ApplicationError{Code: uint64(zmux.CodeProtocol), Reason: "quicmux: " + op}
	}
	return &zmux.ApplicationError{Code: uint64(zmux.CodeProtocol), Reason: fmt.Sprintf("quicmux: %s: %v", op, err)}
}

func translateReadError(err error) error {
	if errors.Is(err, io.EOF) {
		return io.EOF
	}
	return translateError(err)
}

func quicAdapterTerminalError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrClosedPipe) || errors.Is(err, net.ErrClosed) {
		return true
	}
	if errors.Is(err, zmux.ErrSessionClosed) || errors.Is(err, zmux.ErrReadClosed) || errors.Is(err, zmux.ErrWriteClosed) {
		return true
	}
	_, ok := findError[*zmux.ApplicationError](err)
	return ok
}

func translateWaitError(err error) error {
	if err == nil {
		return nil
	}
	if appErr, ok := findError[*quic.ApplicationError](err); ok && appErr.ErrorCode == 0 && appErr.ErrorMessage == "" {
		return nil
	}
	return translateError(err)
}

func translateError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}

	if appErr, ok := findError[*quic.ApplicationError](err); ok {
		if appErr.ErrorCode == 0 && appErr.ErrorMessage == "" {
			return errors.Join(zmux.ErrSessionClosed, err)
		}
		return &zmux.ApplicationError{Code: uint64(appErr.ErrorCode), Reason: appErr.ErrorMessage}
	}

	if streamErr, ok := findError[*quic.StreamError](err); ok {
		return &zmux.ApplicationError{Code: uint64(streamErr.ErrorCode)}
	}

	if _, ok := findError[quic.StreamLimitReachedError](err); ok {
		return errors.Join(zmux.ErrOpenLimited, err)
	}

	if _, ok := findError[*quic.IdleTimeoutError](err); ok {
		return errors.Join(zmux.ErrSessionClosed, err)
	}

	if _, ok := findError[*quic.HandshakeTimeoutError](err); ok {
		return errors.Join(zmux.ErrSessionClosed, err)
	}

	if _, ok := findError[*quic.StatelessResetError](err); ok {
		return errors.Join(zmux.ErrSessionClosed, err)
	}

	if _, ok := findError[*quic.VersionNegotiationError](err); ok {
		return errors.Join(zmux.ErrSessionClosed, err)
	}

	if errors.Is(err, quic.Err0RTTRejected) || errors.Is(err, net.ErrClosed) {
		return errors.Join(zmux.ErrSessionClosed, err)
	}

	return err
}

func mappedApplicationError(err error, defaultCode uint64) (uint64, string) {
	if err == nil {
		return defaultCode, ""
	}
	if appErr, ok := findError[*zmux.ApplicationError](err); ok {
		return appErr.Code, appErr.Reason
	}
	return defaultCode, err.Error()
}

func findError[T any](err error) (T, bool) {
	return errutil.Find[T](err)
}

func defaultContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func installContextWriteDeadline(ctx context.Context, deadlineSetter writeDeadlineSetter) func() {
	if ctx == nil || deadlineSetter == nil {
		return func() {}
	}
	if ctx.Done() == nil {
		return func() {}
	}
	if ctx.Err() != nil {
		_ = deadlineSetter.SetWriteDeadline(time.Now())
		return func() {
			_ = deadlineSetter.SetWriteDeadline(time.Time{})
		}
	}

	clearOnReturn := false
	if deadline, ok := ctx.Deadline(); ok {
		_ = deadlineSetter.SetWriteDeadline(deadline)
		clearOnReturn = true
	}
	var (
		mu     sync.Mutex
		cond   = sync.NewCond(&mu)
		done   bool
		firing bool
		fired  bool
	)
	stop := context.AfterFunc(ctx, func() {
		mu.Lock()
		if done {
			mu.Unlock()
			return
		}
		firing = true
		mu.Unlock()
		_ = deadlineSetter.SetWriteDeadline(time.Now())
		mu.Lock()
		firing = false
		fired = true
		cond.Broadcast()
		mu.Unlock()
	})
	return func() {
		stopped := stop()
		mu.Lock()
		done = true
		for firing && !fired {
			cond.Wait()
		}
		mu.Unlock()
		if stopped {
			if clearOnReturn {
				_ = deadlineSetter.SetWriteDeadline(time.Time{})
			}
			return
		}
		_ = deadlineSetter.SetWriteDeadline(time.Time{})
	}
}

var (
	_ zmux.Session    = (*quicSession)(nil)
	_ zmux.Stream     = (*quicStream)(nil)
	_ zmux.SendStream = (*quicSendStream)(nil)
	_ zmux.RecvStream = (*quicRecvStream)(nil)
)
