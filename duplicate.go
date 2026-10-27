package zmux

import (
	"bytes"
	"context"
	"errors"

	"github.com/zmuxio/zmux-go/internal/wire"
)

// DuplicateOptions are the inputs of ResolveDuplicate.
type DuplicateOptions struct {
	// LocalID and PeerID are the application identities of the two
	// endpoints (the same byte strings at both ends), when it has them.
	LocalID, PeerID []byte
	// Rejoin: when this endpoint dialed the losing session and the
	// survivor uses multipath, Dial the route again and join it to the
	// survivor (in the background; a hint).
	Rejoin bool
	Dial   func(ctx context.Context) (PipeIO, error)
}

// DuplicateResult reports a resolution.
type DuplicateResult struct {
	Survivor, Loser *Conn
	RejoinStarted   bool
}

// ResolveDuplicate applies the rule of EXTENSIONS.md 11 to two established
// sessions that connect the same two endpoints and were dialed by
// different endpoints: it returns the survivor and starts draining the
// other session (GOAWAY, no new streams, close with NO_ERROR once its
// streams have ended). It changes nothing when the sessions have the same
// dialer or no input decides.
func ResolveDuplicate(s1, s2 *Conn, opts *DuplicateOptions) (DuplicateResult, error) {
	var o DuplicateOptions
	if opts != nil {
		o = *opts
	}
	if s1 == s2 || s1.dialed == s2.dialed || s1.dialed == dialUnknown || s2.dialed == dialUnknown {
		return DuplicateResult{}, ErrNoDecision
	}
	v1, v2 := dialerValue(s1, &o), dialerValue(s2, &o)
	if v1 == nil || v2 == nil {
		return DuplicateResult{}, ErrNoDecision
	}
	cmp := bytes.Compare(v1, v2)
	if cmp == 0 {
		return DuplicateResult{}, errors.New("zmux: duplicate sessions to this endpoint itself")
	}
	win, lose := s1, s2
	if cmp > 0 {
		win, lose = s2, s1
	}
	go func() { _ = lose.Shutdown(context.Background(), 0, "") }()
	res := DuplicateResult{Survivor: win, Loser: lose}
	if o.Rejoin && o.Dial != nil && lose.dialed == dialLocal && win.ConnectionState().Multipath {
		res.RejoinStarted = true
		go func() {
			ctx, cancel := context.WithTimeout(win.ctx, win.handshakeTimeout())
			defer cancel()
			if p, err := o.Dial(ctx); err == nil {
				if _, err := win.AddPipe(ctx, p, PipeOptions{}); err != nil {
					closePipeIO(p)
				}
			}
		}()
	}
	return res, nil
}

// dialerValue is the value of a session's dialer the rule compares:
// its identity, else its Session Nonce (both multipath), else its role
// nonce (both auto).
func dialerValue(c *Conn, o *DuplicateOptions) []byte {
	local := c.dialed == dialLocal
	if len(o.LocalID) > 0 && len(o.PeerID) > 0 {
		if local {
			return o.LocalID
		}
		return o.PeerID
	}
	c.lock()
	defer c.unlock()
	lp := c.s.LocalParams()
	pp, ok := c.s.PeerParams()
	if !ok {
		return nil
	}
	if c.s.Features().Multipath {
		if local {
			return append([]byte(nil), lp.SessionNonce[:]...)
		}
		return append([]byte(nil), pp.SessionNonce[:]...)
	}
	if lp.Has&wire.HasRole != 0 && pp.Has&wire.HasRole != 0 && lp.Role == wire.RoleAuto && pp.Role == wire.RoleAuto {
		n := lp.RoleNonce
		if !local {
			n = pp.RoleNonce
		}
		b := make([]byte, 8)
		for i := range b {
			b[7-i] = byte(n >> (8 * i))
		}
		return b
	}
	return nil
}

// dialer of a session: who established its bootstrap carrier.
type dialer uint8

const (
	dialUnknown dialer = iota
	dialLocal
	dialPeer
)

// DesignateDialer records which endpoint dialed a session whose bootstrap
// carrier is symmetric (role auto), for ResolveDuplicate.
func (c *Conn) DesignateDialer(local bool) {
	c.lock()
	defer c.unlock()
	if local {
		c.dialed = dialLocal
	} else {
		c.dialed = dialPeer
	}
}
