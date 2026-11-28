package zmux

import (
	"errors"
	"time"

	"github.com/zmuxio/zmux-go/internal/core"
)

// CarrierFaultError is an adapter's evidence of a physical connection failure.
// Use the same nonzero identity and generation on every I/O error caused by
// that failure. Advance the generation for a distinct failure on the same
// connection. Stream errors and uncorrelated observations use ordinary errors.
type CarrierFaultError struct {
	Cause      error
	CarrierID  CarrierID
	Generation uint64
}

func (e *CarrierFaultError) Error() string {
	if e.Cause != nil {
		return e.Cause.Error()
	}
	return "zmux: physical carrier failed"
}

func (e *CarrierFaultError) Unwrap() error { return e.Cause }

// Inspect the adapter's error before giving the evidence to the session owner.
func carrierFaultEvidence(err error) *core.PhysicalFault {
	if fault, ok := errors.AsType[*CarrierFaultError](err); ok && fault != nil &&
		fault.CarrierID != (CarrierID{}) && fault.Generation != 0 {
		return &core.PhysicalFault{CarrierID: fault.CarrierID, Generation: fault.Generation}
	}
	return nil
}

// CarrierRetryError is an opener's authenticated peer request to defer attempts
// sharing the configured cache key, kind and variant. Use it only if the
// authenticated scope covers that key. RetryAfter must be positive.
type CarrierRetryError struct {
	RetryAfter time.Duration
}

func (*CarrierRetryError) Error() string { return "zmux: peer requested a carrier retry delay" }

func (*CarrierRetryError) Unwrap() error { return ErrPeerRetry }

func peerRetryDelay(err error) time.Duration {
	if retry, ok := errors.AsType[*CarrierRetryError](err); ok && retry != nil {
		return max(retry.RetryAfter, 0)
	}
	return 0
}
