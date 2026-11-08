package zmux

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"

	"github.com/djylb/timeshim"

	"github.com/zmuxio/zmux-go/internal/core"
	"github.com/zmuxio/zmux-go/internal/labels"
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

// ResolveDuplicate applies the duplicate-session rule to two established
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
	if s1 == nil || s2 == nil || s1 == s2 {
		return DuplicateResult{}, ErrNoDecision
	}
	a, b := duplicateSnapshot(s1), duplicateSnapshot(s2)
	if !a.established || !b.established || a.dialer == b.dialer || a.dialer == 0 || b.dialer == 0 {
		return DuplicateResult{}, ErrNoDecision
	}
	var v1, v2 []byte
	switch {
	case len(o.LocalID) > 0 && len(o.PeerID) > 0:
		v1, v2 = o.LocalID, o.PeerID
		if a.dialer != EndpointLocal {
			v1, v2 = v2, v1
		}
	case a.multipath && b.multipath:
		v1, v2 = a.sessionNonce[:], b.sessionNonce[:]
	case a.autoRole && b.autoRole:
		v1, v2 = make([]byte, 8), make([]byte, 8)
		binary.BigEndian.PutUint64(v1, a.roleNonce)
		binary.BigEndian.PutUint64(v2, b.roleNonce)
	default:
		return DuplicateResult{}, ErrNoDecision
	}
	cmp := bytes.Compare(v1, v2)
	if cmp == 0 {
		return DuplicateResult{}, errors.New("zmux: duplicate sessions to this endpoint itself")
	}
	win, lose := s1, s2
	loserDialer := b.dialer
	if cmp > 0 {
		win, lose = s2, s1
		loserDialer = a.dialer
	}
	// Commit the existing drain state before returning; no separate waiter is needed.
	lose.lock()
	lose.s.SetNow(lose.clk.now())
	lose.s.Shutdown(0, nil)
	lose.unlock()
	res := DuplicateResult{Survivor: win, Loser: lose}
	if o.Rejoin && o.Dial != nil && loserDialer == EndpointLocal && win.ConnectionState().Multipath && win.JoinReady() {
		res.RejoinStarted = true
		labels.Go(win.lctx, "duplicate-rejoin", func() {
			clk := win.clk.source()
			ctx, cancel := timeshim.WithDeadline(win.ctx, clk, clk.Now().Add(win.cfg.joinTimeout()))
			defer cancel()
			if p, err := o.Dial(ctx); err == nil {
				_, _ = win.AddPipe(ctx, p, PipeOptions{})
			}
		})
	}
	return res, nil
}

// duplicateInput is a value snapshot, not another authority for session state.
// Capture the dialer under the same lock as its designation and transport parameters.
type duplicateInput struct {
	dialer                           Endpoint // 0: unknown
	established, multipath, autoRole bool
	sessionNonce                     [16]byte
	roleNonce                        uint64
}

func duplicateSnapshot(c *Conn) duplicateInput {
	c.lock()
	defer c.unlock()
	local := c.s.LocalParams()
	peer, ok := c.s.PeerParams()
	result := duplicateInput{dialer: c.dialed, established: ok && !c.closing && c.s.State() < core.StateClosing}
	if !result.established {
		return result
	}
	result.multipath = c.s.Features().Multipath
	result.autoRole = local.Has&wire.HasRole != 0 && peer.Has&wire.HasRole != 0 && local.Role == wire.RoleAuto && peer.Role == wire.RoleAuto
	params := local
	if result.dialer != EndpointLocal {
		params = peer
	}
	result.sessionNonce, result.roleNonce = params.SessionNonce, params.RoleNonce
	return result
}

// Endpoint names one end of a session, seen from this end.
type Endpoint uint8

const (
	EndpointLocal Endpoint = iota + 1
	EndpointPeer
)

func (e Endpoint) String() string {
	switch e {
	case EndpointLocal:
		return "local"
	case EndpointPeer:
		return "peer"
	}
	return "Endpoint(?)"
}

// DesignateDialer records which endpoint dialed a session whose bootstrap
// carrier is symmetric (role auto), for ResolveDuplicate. A value other
// than EndpointLocal or EndpointPeer forgets the designation.
func (c *Conn) DesignateDialer(e Endpoint) {
	c.lock()
	defer c.unlock()
	switch e {
	case EndpointLocal, EndpointPeer:
		c.dialed = e
	default:
		c.dialed = 0
	}
}
