package zmux

import (
	"context"
	"errors"
	"net"
	"os"
	"testing"
)

func TestRecoveryFailureAttribution(t *testing.T) {
	carrierTimeout := &net.OpError{Op: "read", Net: "pipe", Err: os.ErrDeadlineExceeded}
	for _, tc := range []struct {
		outcome, stage AttemptReason
		err            error
		want           Attribution
	}{
		{ReasonDeadline, ReasonDeadline, context.DeadlineExceeded, AttributionNone},
		{ReasonDeadline, ReasonConfirmationFailed, carrierTimeout, AttributionCarrier},
		{ReasonDeadline, ReasonDialFailed, context.DeadlineExceeded, AttributionNone},
		{ReasonJoinRefused, ReasonJoinRefused, ErrJoinRefused, AttributionNone},
		{ReasonJoinRefused, ReasonJoinRefused, errors.Join(ErrJoinRefused, carrierTimeout), AttributionCarrier},
		{ReasonConfirmationFailed, ReasonConfirmationFailed, ErrJoinAuthorizationChanged, AttributionNone},
		{ReasonCancelled, ReasonConfirmationFailed, carrierTimeout, AttributionNone},
		{ReasonDialFailed, ReasonDialFailed, context.Canceled, AttributionNone},
	} {
		if got := failureAttribution(tc.outcome, tc.stage, tc.err); got != tc.want {
			t.Errorf("%s after %s with %v: got %v, want %v", tc.outcome, tc.stage, tc.err, got, tc.want)
		}
	}
}
