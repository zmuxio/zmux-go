// Package labels starts the goroutines of zmux and its adapter modules
// with their runtime/pprof labels, by the rules of the root package's
// labels.go: "zmux" is the task; a goroutine that works for one call keeps
// the labels of that call's context, one that outlives the call starts
// from context.Background() and carries zmux labels only. A label never
// holds a key, secret, handle or address.
package labels

import (
	"context"
	"runtime/pprof"
)

// Go runs f on a new goroutine labelled "zmux"=task on top of the labels
// of base, which it keeps (context.Background() for none; a nil one has
// none either). Setting the labels in the new goroutine costs no
// allocation beyond the label set itself, made once per task.
func Go(base context.Context, task string, f func()) {
	ctx := With(base, "zmux", task)
	go func() {
		pprof.SetGoroutineLabels(ctx)
		f()
	}()
}

// With adds key=value to the labels of base (nil: none).
func With(base context.Context, key, value string) context.Context {
	if base == nil {
		base = context.Background()
	}
	return pprof.WithLabels(base, pprof.Labels(key, value))
}
