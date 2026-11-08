package zmux

import (
	"context"
	"runtime/pprof"
	"strconv"
	"sync/atomic"

	"github.com/zmuxio/zmux-go/internal/core"
	"github.com/zmuxio/zmux-go/internal/labels"
)

// Goroutine labels (runtime/pprof) on the goroutines zmux starts, for
// profiles, execution traces and tracebacks (which show labels by
// default): "zmux" is the task ("read", "write", "lane", ...), with
// "zmux.role", "zmux.session" (a process-local sequence number, from 1)
// and "zmux.pipe" where they apply. A label never holds a key, secret,
// handle or address: tracebacks may be shared.
//
// A goroutine that works for one call (a dial, the setup of a carrier, a
// carrier fed to a Listener, an admission decision, a first-record read)
// adds the zmux labels to the labels of that call's context, so the
// application's own labels reach it. A goroutine of a session or a pipe
// (read, write, lanes, hooks, redial, pool) outlives the call that started
// it and serves the whole session: it carries the zmux labels only, as do
// the goroutines of a Listener itself.
//
// internal/labels starts these goroutines, here and in the adapter
// modules (zmuxquic, zmuxh3), by the same rules.

// sessionSeq numbers the sessions of the process for their labels.
var sessionSeq atomic.Uint64

// sessionLabels is the label set of a new session in role r.
func sessionLabels(r core.Role) context.Context {
	role := "auto"
	switch r {
	case core.RoleInitiator:
		role = "initiator"
	case core.RoleResponder:
		role = "responder"
	case core.RoleAuto:
		// Not resolved yet when the session starts: the label stays "auto".
	}
	return pprof.WithLabels(context.Background(), pprof.Labels(
		"zmux.role", role, "zmux.session", strconv.FormatUint(sessionSeq.Add(1), 10)))
}

// pipeLabels is the label set of pipe id of session c.
func (c *Conn) pipeLabels(id uint64) context.Context {
	return labels.With(c.lctx, "zmux.pipe", strconv.FormatUint(id, 10))
}
