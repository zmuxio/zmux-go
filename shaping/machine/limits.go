package machine

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/zmuxio/zmux-go/shaping"
)

// ParseLimits reads JSON session limits: max_padding_bytes and
// max_injected, integers in [0, 2^53]; max_padding_ratio in [0, 1];
// max_pause_total and max_delay, seconds in [0, 1e9]. An absent or null
// member sets no limit; 0 allows none, which maps to NoPadding (padding
// bytes), NoInjection or NoPause (pause total or delay), except that a
// max_padding_ratio of 0 maps to NoPaddingRatio, which keeps the
// shaping.StartupAllowance. Parsing
// is strict as for Parse.
func ParseLimits(b []byte) (shaping.Limits, error) {
	var j struct {
		MaxPaddingBytes *json.Number `json:"max_padding_bytes"`
		MaxPaddingRatio *float64     `json:"max_padding_ratio"`
		MaxInjected     *json.Number `json:"max_injected"`
		MaxPauseTotal   *float64     `json:"max_pause_total"`
		MaxDelay        *float64     `json:"max_delay"`
	}
	var l shaping.Limits
	if err := decodeChecked(b, limitsShape, &j); err != nil {
		return l, fmt.Errorf("machine: limits: %w", err)
	}
	count := func(name string, n *json.Number, to *uint64, none *bool) error {
		if n == nil {
			return nil
		}
		x, err := strconv.ParseInt(string(*n), 10, 64)
		switch {
		case err != nil || x > maxCount:
			return fmt.Errorf("machine: limits: %s %s above the maximum 2^53", name, *n)
		case x < 0:
			return fmt.Errorf("machine: limits: %s %d negative", name, x)
		case x == 0:
			*none = true
		}
		*to = uint64(x)
		return nil
	}
	secs := func(name string, p *float64, to *time.Duration, none *bool) error {
		if p == nil {
			return nil
		}
		switch x := *p; {
		case x < 0:
			return fmt.Errorf("machine: limits: %s %v negative", name, x)
		case x > maxSeconds:
			return fmt.Errorf("machine: limits: %s %v above the maximum %v", name, x, maxSeconds)
		case x == 0:
			*none = true
		default:
			*to = max(time.Duration(math.Round(x*1e9)), 1) // a positive limit stays one
		}
		return nil
	}
	if err := count("max_padding_bytes", j.MaxPaddingBytes, &l.MaxPaddingBytes, &l.NoPadding); err != nil {
		return l, err
	}
	if err := count("max_injected", j.MaxInjected, &l.MaxInjected, &l.NoInjection); err != nil {
		return l, err
	}
	if p := j.MaxPaddingRatio; p != nil {
		if !(*p >= 0 && *p <= 1) {
			return l, fmt.Errorf("machine: limits: max_padding_ratio %v outside [0, 1]", *p)
		}
		l.MaxPaddingRatio = *p
		l.NoPaddingRatio = *p == 0
	}
	if err := secs("max_pause_total", j.MaxPauseTotal, &l.MaxPauseTotal, &l.NoPause); err != nil {
		return l, err
	}
	if err := secs("max_delay", j.MaxDelay, &l.MaxDelay, &l.NoPause); err != nil {
		return l, err
	}
	return l, nil
}
