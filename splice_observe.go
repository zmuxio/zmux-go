package zmux

import (
	"context"
	"errors"
)

// SpliceDropReason identifies the local disposition of a source group.
type SpliceDropReason string

// Local drop reasons use the contract names.
const (
	SpliceDropHook              SpliceDropReason = "dropped_hook"
	SpliceDropExpired           SpliceDropReason = "dropped_expired"
	SpliceDropQueueFull         SpliceDropReason = "dropped_queue_full"
	SpliceDropLimiter           SpliceDropReason = "dropped_limiter"
	SpliceDropBlocked           SpliceDropReason = "dropped_blocked"
	SpliceDropTooLarge          SpliceDropReason = "dropped_too_large"
	SpliceDropNoPath            SpliceDropReason = "dropped_no_path"
	SpliceDropSequenceExhausted SpliceDropReason = "dropped_sequence_exhausted"
	SpliceDropFlowClosed        SpliceDropReason = "dropped_flow_closed"
	SpliceDropCoDel             SpliceDropReason = "dropped_codel"
)

func spliceDropFor(err error) SpliceDropReason {
	if e, ok := errors.AsType[DatagramDroppedError](err); ok {
		switch e.Reason {
		case DatagramDropDeadline:
			return SpliceDropExpired
		case DatagramDropNoNativePath:
			return SpliceDropNoPath
		case DatagramDropLimiter:
			return SpliceDropLimiter
		case DatagramDropQueueOverflow:
			return SpliceDropQueueFull
		case DatagramDropSequenceExhausted:
			return SpliceDropSequenceExhausted
		}
	}
	if _, ok := errors.AsType[*DatagramTooLargeError](err); ok {
		return SpliceDropTooLarge
	}
	if _, ok := errors.AsType[*StreamError](err); ok {
		return SpliceDropFlowClosed
	}
	if errors.Is(err, ErrNotNegotiated) {
		return SpliceDropNoPath
	}
	return SpliceDropBlocked
}

func (d *SpliceDirection) countDrop(reason SpliceDropReason) {
	if d.Drops == nil {
		d.Drops = make(map[SpliceDropReason]uint64)
	}
	d.Drops[reason]++
}

func (d *spliceRelay) dropGroup(g *spliceGroup, reason SpliceDropReason) {
	g.disposition.Reason = reason
	d.discard(g, nil)
}

// SpliceReceiptStatus describes local admission or the final disposition.
type SpliceReceiptStatus string

// Receipt statuses cover admission and final disposition.
const (
	SpliceRejected       SpliceReceiptStatus = "rejected"
	SpliceAccepted       SpliceReceiptStatus = "accepted"
	SpliceSubmitted      SpliceReceiptStatus = "submitted"
	SpliceDropped        SpliceReceiptStatus = "dropped"
	SpliceDirectionEnded SpliceReceiptStatus = "direction_ended"
)

// SpliceEvent is emitted in direction-owner order. DirectionFinished carries
// the committed result. Token identifies a pause within that direction.
type SpliceEvent struct {
	Name              SpliceEventName
	Generation, Token uint64
	End               SpliceEnd
}

// SpliceEventName identifies a direction control or termination event.
type SpliceEventName string

// Event names identify the owner action that committed.
const (
	SpliceHooksReplaced     SpliceEventName = "HooksReplaced"
	SplicePauseAcknowledged SpliceEventName = "PauseAcknowledged"
	SpliceDirectionFinished SpliceEventName = "DirectionFinished"
)

func (d *spliceRelay) event(e SpliceEvent) {
	if d.onEvent == nil {
		return
	}
	defer func() {
		if recover() != nil {
			d.onEvent = nil
		}
	}()
	d.onEvent(e)
}

func (d *spliceRelay) acknowledgePause() {
	if d.pause != nil && !d.pause.confirmed {
		d.pause.finish(nil)
		d.event(SpliceEvent{Name: SplicePauseAcknowledged, Token: d.pause.Generation})
	}
}

// SpliceVerdict names the operation selected by a hook.
type SpliceVerdict string

// Verdict names describe the resulting group operation.
const (
	SpliceVerdictPass    SpliceVerdict = "pass"
	SpliceVerdictReplace SpliceVerdict = "replace"
	SpliceVerdictDrop    SpliceVerdict = "drop"
	SpliceVerdictReset   SpliceVerdict = "reset"
)

// Verdict returns the contract name of a hook decision.
func (d SpliceDecision) Verdict() SpliceVerdict {
	switch d.Action {
	case SpliceDrop:
		return SpliceVerdictDrop
	case SpliceResetByHook:
		return SpliceVerdictReset
	case SpliceForward:
		if d.Replacement != nil {
			return SpliceVerdictReplace
		}
		return SpliceVerdictPass
	default:
		return ""
	}
}

// SpliceRejection is reported on a receipt rejected before acceptance.
type SpliceRejection string

// Rejection reasons apply before injection acceptance.
const (
	SpliceInvalidParameter SpliceRejection = "invalid_parameter"
	SpliceBudgetExceeded   SpliceRejection = "splice_budget"
	SplicePaused           SpliceRejection = "paused"
	SpliceClosed           SpliceRejection = "closed"
	SpliceCallCancelled    SpliceRejection = "cancelled"
)

// SpliceErrorName returns the contract name for a local splice error,
// including an error wrapped with additional context. Other errors have no
// local splice name and remain available through the direction's Cause.
func SpliceErrorName(err error) string {
	switch {
	case errors.Is(err, ErrInvalidConfig):
		return string(SpliceInvalidParameter)
	case errors.Is(err, ErrSpliceBudget):
		return string(SpliceBudgetExceeded)
	case errors.Is(err, ErrSplicePaused):
		return string(SplicePaused)
	case errors.Is(err, ErrSpliceClosed):
		return string(SpliceClosed)
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return string(SpliceCallCancelled)
	case errors.Is(err, ErrReceiveLoanLimit):
		return "borrow_limit"
	default:
		return ""
	}
}

func rejectedSplice(err error) (SpliceReceipt, error) {
	return SpliceReceipt{Status: SpliceRejected, Reason: SpliceRejection(SpliceErrorName(err))}, err
}

// Meters returns the direction counters, including settlement as of the
// call. A carrier may still settle earlier submissions after direction exit.
func (d *SpliceDirection) Meters() map[string]uint64 {
	h := d.Handoffs.Stats()
	m := map[string]uint64{
		"input_units": d.InputUnits, "input_bytes": d.InputBytes, "replacements": d.HookReplaced,
		"enqueued_items": d.OutputUnits, "wait_us": uint64(d.WaitTime.Microseconds()), "mirror_misses": d.MirrorDropped,
		"capsule_count": d.DatagramsAsCapsules, "native_count": h.NativeDatagrams, "record_count": h.RecordDatagrams,
		"accepted_bytes": h.Accepted, "first_carrier_bytes": h.FirstSent, "unsent_bytes": h.Unsent, "pending_bytes": h.Pending,
		"unsent_refunded_bytes": h.UnsentRefunded, "pre_admission_refunded_bytes": h.PreAdmissionRefunded,
		"payload_copy_bytes": d.PayloadCopyBytes, "encode_bytes": d.EncodeBytes, "mirror_copy_bytes": d.MirrorCopyBytes,
	}
	for _, reason := range [...]SpliceDropReason{SpliceDropHook, SpliceDropExpired, SpliceDropQueueFull, SpliceDropLimiter, SpliceDropBlocked, SpliceDropTooLarge, SpliceDropNoPath, SpliceDropSequenceExhausted, SpliceDropFlowClosed, SpliceDropCoDel} {
		m[string(reason)] = d.Drops[reason]
	}
	return m
}
