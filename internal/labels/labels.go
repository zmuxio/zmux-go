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
	"sync"
)

// taskContexts holds one label set for each task name used by Go.
var taskContexts sync.Map // map[string]context.Context

// Go runs f on a new goroutine labelled "zmux"=task on top of the labels
// of base, which it keeps (context.Background() or nil for none). Without
// base labels, goroutines share the task's label set.
func Go(base context.Context, task string, f func()) {
	ctx := taskContext(base, task)
	go func() {
		pprof.SetGoroutineLabels(ctx)
		f()
	}()
}

func taskContext(base context.Context, task string) context.Context {
	if base != nil {
		hasLabels := false
		pprof.ForLabels(base, func(_, _ string) bool {
			hasLabels = true
			return false
		})
		if hasLabels {
			return With(base, "zmux", task)
		}
	}
	if ctx, ok := taskContexts.Load(task); ok {
		return ctx.(context.Context)
	}
	// The goroutine only needs the labels. Avoid retaining the caller's context.
	ctx, _ := taskContexts.LoadOrStore(task, With(context.Background(), "zmux", task))
	return ctx.(context.Context)
}

// With adds key=value to the labels of base (nil: none).
func With(base context.Context, key, value string) context.Context {
	if base == nil {
		base = context.Background()
	}
	return pprof.WithLabels(base, pprof.Labels(key, value))
}
