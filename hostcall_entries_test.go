package zmux

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// TestHostCallbacksContained: every call of a host callback field of the
// configuration types runs inside the one containment entry (hostCall or
// safeLog), so that a host panic never escapes into the session (RB-S-r4).
func TestHostCallbacksContained(t *testing.T) {
	fields := map[string]bool{}
	for _, v := range []any{Config{}, ListenerConfig{}, Admission{}, PipeOptions{}, DuplicateOptions{}, Tracer{}, Candidate{}, PipePool{}} {
		for f := range reflect.TypeOf(v).Fields() {
			if f.IsExported() && f.Type.Kind() == reflect.Func {
				fields[f.Name] = true
			}
		}
	}
	entries := map[string]bool{"hostCall": true, "safeLog": true}
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Skip("no sources: a test binary run outside its package directory")
	}
	fset := token.NewFileSet()
	checked := 0
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		src, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		f, err := parser.ParseFile(fset, name, src, 0)
		if err != nil {
			t.Fatal(err)
		}
		var stack []ast.Node
		ast.Inspect(f, func(n ast.Node) bool {
			if n == nil {
				stack = stack[:len(stack)-1]
				return false
			}
			stack = append(stack, n)
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || !fields[sel.Sel.Name] || isCore(sel.X) {
				return true
			}
			checked++
			if !contained(stack, entries) {
				t.Errorf("%s: host callback %s called outside hostCall/safeLog", fset.Position(call.Pos()), sel.Sel.Name)
			}
			return true
		})
	}
	if checked < 10 {
		t.Fatalf("only %d host callback calls found: the check sees nothing", checked)
	}
}

// contained reports whether the innermost function literal around the top
// of stack is an argument of a call to one of entries.
func contained(stack []ast.Node, entries map[string]bool) bool {
	for i := len(stack) - 1; i > 0; i-- {
		lit, ok := stack[i].(*ast.FuncLit)
		if !ok {
			continue
		}
		call, ok := stack[i-1].(*ast.CallExpr)
		if !ok {
			return false
		}
		id, ok := call.Fun.(*ast.Ident)
		if !ok || !entries[id.Name] {
			return false
		}
		for _, a := range call.Args {
			if a == lit {
				return true
			}
		}
		return false
	}
	return false
}

// isCore reports whether x is the session's core (c.s), whose methods share
// some names with host callbacks.
func isCore(x ast.Expr) bool {
	sel, ok := x.(*ast.SelectorExpr)
	return ok && sel.Sel.Name == "s"
}
