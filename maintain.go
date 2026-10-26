package zmux

import (
	"context"
	"math/rand/v2"
	"time"
)

// PipePool keeps pipes of one path up (API.md Appendix A).
type PipePool struct {
	// Dial opens a carrier of the path.
	Dial func(ctx context.Context) (PipeIO, error)
	// Count is the number of usable pipes to keep (default 1).
	Count int
	// Options of the pipes the pool joins.
	Options PipeOptions
	// Rotate replaces pipes that carried MaxBytes or are MaxAge old.
	Rotate Rotation
	// Backoff bounds the redial backoff (from 1 s, at most 60 s).
	MinBackoff, MaxBackoff time.Duration
}

// Rotation replaces pipes before a lifetime limit: when a pipe has carried
// MaxBytes in either direction or is MaxAge old (each reduced by a random
// fraction of up to Jitter), a replacement is dialled and joined, and the
// old pipe is closed gracefully once the replacement is usable (make
// before break).
type Rotation struct {
	MaxBytes uint64
	MaxAge   time.Duration
	Jitter   float64 // 0 to 1
}

// MaintainPipes keeps pool.Count usable pipes dialled by pool.Dial: it
// redials failed pipes with exponential backoff and rotates pipes by
// pool.Rotate, until ctx ends or the session closes. It dials outside
// every lock and returns ctx's error or the session's.
func (c *Conn) MaintainPipes(ctx context.Context, pool PipePool) error {
	if pool.Count <= 0 {
		pool.Count = 1
	}
	if pool.MinBackoff <= 0 {
		pool.MinBackoff = time.Second
	}
	if pool.MaxBackoff <= 0 {
		pool.MaxBackoff = 60 * time.Second
	}
	type member struct {
		p      *Pipe
		born   time.Time
		limitB uint64
		limitT time.Duration
		repl   *Pipe // its replacement while rotating
	}
	var members []*member
	backoff := pool.MinBackoff
	jitter := func(v float64) float64 {
		if pool.Rotate.Jitter <= 0 {
			return v
		}
		return v * (1 - rand.Float64()*min(pool.Rotate.Jitter, 1))
	}
	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()
	join := func() (*Pipe, error) {
		dctx, cancel := context.WithTimeout(ctx, c.handshakeTimeout())
		defer cancel()
		p, err := pool.Dial(dctx)
		if err != nil {
			return nil, err
		}
		pp, err := c.AddPipe(dctx, p, pool.Options)
		if err != nil {
			closePipeIO(p)
			return nil, err
		}
		return pp, nil
	}
	newMember := func(p *Pipe) *member {
		return &member{p: p, born: time.Now(), limitB: uint64(jitter(float64(pool.Rotate.MaxBytes))),
			limitT: time.Duration(jitter(float64(pool.Rotate.MaxAge)))}
	}
	for {
		// Drop members whose pipe is gone or failed.
		live := members[:0]
		for _, m := range members {
			st, err := m.p.Stats()
			if err == nil && st.State != "failed" && st.State != "closed" {
				live = append(live, m)
			} else if m.repl != nil {
				live = append(live, newMember(m.repl))
			}
		}
		members = live
		out, in := false, false
		c.lock()
		if c.s.Features().Multipath {
			out, in = c.s.LiveDirections()
		}
		c.unlock()
		lacking := !out || !in
		// Redial the missing ones.
		if len(members) < pool.Count {
			p, err := join()
			if err != nil {
				select {
				case <-time.After(time.Duration(jitter(float64(backoff)))):
				case <-ctx.Done():
					return ctx.Err()
				case <-c.done:
					return c.closedErr()
				}
				backoff = min(2*backoff, pool.MaxBackoff)
				continue
			}
			backoff = pool.MinBackoff
			members = append(members, newMember(p))
			continue
		}
		// Rotate: only usable pipes, never while a direction is lacking.
		for _, m := range members {
			if lacking || m.repl != nil || pool.Rotate.MaxBytes == 0 && pool.Rotate.MaxAge == 0 {
				continue
			}
			st, err := m.p.Stats()
			if err != nil || st.State != "active" {
				continue
			}
			due := pool.Rotate.MaxBytes > 0 && max(st.BytesSent, st.BytesReceived) >= m.limitB ||
				pool.Rotate.MaxAge > 0 && time.Since(m.born) >= m.limitT
			if due {
				if r, err := join(); err == nil {
					m.repl = r
				}
			}
		}
		// Close an old pipe once its replacement is usable.
		for _, m := range members {
			if m.repl == nil {
				continue
			}
			if st, err := m.repl.Stats(); err == nil && st.State == "active" && st.ProbationUntil.IsZero() {
				_ = m.p.Close()
				*m = *newMember(m.repl)
			}
		}
		select {
		case <-tick.C:
		case <-ctx.Done():
			return ctx.Err()
		case <-c.done:
			return c.closedErr()
		}
	}
}
