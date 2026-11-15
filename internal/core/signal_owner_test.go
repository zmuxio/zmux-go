package core

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestStreamSignalsOneEntry: the core wakes a stream's waiter slots only
// through their change entries (sendChanged, recvChanged, dgramChanged),
// so no path wakes one by hand and forgets the others.
func TestStreamSignalsOneEntry(t *testing.T) {
	slots := map[string]bool{"rsig": true, "wsig": true, "dsig": true}
	allowed := map[string]bool{"SetSignals": true, "sendChanged": true, "recvChanged": true, "dgramChanged": true}
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Skip("no sources: a test binary run outside its package directory")
	}
	fset := token.NewFileSet()
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
		for _, d := range f.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if !ok || fn.Body == nil || allowed[fn.Name.Name] {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok || len(call.Args) != 1 {
					return true
				}
				if f, ok := call.Fun.(*ast.SelectorExpr); !ok || f.Sel.Name != "wake" {
					return true
				}
				if sel, ok := call.Args[0].(*ast.SelectorExpr); ok && slots[sel.Sel.Name] {
					t.Errorf("%s: %s wakes .%s outside the change entries", fset.Position(sel.Pos()), fn.Name.Name, sel.Sel.Name)
				}
				return true
			})
		}
	}
}
