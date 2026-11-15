package zmux

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestSessionLockEntries: the session lock is taken only through lock and
// peek and released only by their epilogues, so that every critical
// section converts a panic into the session's end (RA-A2).
func TestSessionLockEntries(t *testing.T) {
	allowed := map[string]bool{"lock": true, "peek": true, "unlockPeek": true, "unlockWith": true, "salvage": true}
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
				if sel, ok := n.(*ast.SelectorExpr); ok && sel.Sel.Name == "sessMu" {
					t.Errorf("%s: %s uses the session lock directly", fset.Position(sel.Pos()), fn.Name.Name)
				}
				return true
			})
		}
	}
}
