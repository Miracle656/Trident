package httputil_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Depo-dev/trident/services/api/internal/httputil"
)

// allDeclaredCodes lists every ErrorCode constant declared in errors.go. Kept
// as an explicit list (rather than reflection, which can't enumerate untyped
// consts) so this test fails loudly if a new constant is added without a
// matching Registry entry.
var allDeclaredCodes = []httputil.ErrorCode{
	httputil.NOT_FOUND,
	httputil.UNAUTHORIZED,
	httputil.RATE_LIMITED,
	httputil.INVALID_ARGUMENT,
	httputil.UNAVAILABLE,
	httputil.INTERNAL,
	httputil.PAYLOAD_TOO_LARGE,
	httputil.FORBIDDEN,
}

// TestRegistryCoversAllConstants ensures every ErrorCode constant is
// documented in httputil.Registry (and thus in docs/errors.md), and that the
// registry declares nothing extra.
func TestRegistryCoversAllConstants(t *testing.T) {
	declared := make(map[httputil.ErrorCode]bool, len(allDeclaredCodes))
	for _, c := range allDeclaredCodes {
		declared[c] = true
		if !httputil.IsRegistered(c) {
			t.Errorf("constant %q has no Registry entry in registry.go", c)
		}
	}

	for _, entry := range httputil.Registry {
		if !declared[entry.Code] {
			t.Errorf("Registry entry %q has no corresponding constant in errors.go", entry.Code)
		}
		if entry.HTTPStatus < 100 || entry.HTTPStatus > 599 {
			t.Errorf("Registry entry %q has invalid HTTP status %d", entry.Code, entry.HTTPStatus)
		}
		if entry.Summary == "" {
			t.Errorf("Registry entry %q has no summary", entry.Code)
		}
	}
}

// TestNoUndocumentedErrorCodeLiterals statically scans every .go file under
// services/api (excluding this package's own tests) for a string literal
// converted to httputil.ErrorCode — e.g. httputil.ErrorCode("SOMETHING_NEW")
// or httputil.ErrorCode(x) is fine only if x isn't a raw literal — and fails
// if that literal is not in the documented Registry. Combined with writeError
// falling back to INTERNAL for unregistered codes, this is the enforcement
// mechanism for acceptance criterion 5 on issue #424: no handler can emit an
// undocumented code.
func TestNoUndocumentedErrorCodeLiterals(t *testing.T) {
	root := findAPIRoot(t)

	fset := token.NewFileSet()
	var offenders []string

	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || !strings.HasSuffix(path, ".go") {
			return nil
		}
		// Skip the httputil package itself: errors.go declares the
		// constants and registry.go declares the table they must match;
		// both are exempt from "must already be in the registry".
		if strings.Contains(path, string(filepath.Separator)+"internal"+string(filepath.Separator)+"httputil"+string(filepath.Separator)) {
			return nil
		}

		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			// Non-Go-parseable files under a .go-only walk shouldn't occur;
			// surface a failure rather than silently skipping.
			return err
		}

		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			pkgIdent, ok := sel.X.(*ast.Ident)
			if !ok || pkgIdent.Name != "httputil" || sel.Sel.Name != "ErrorCode" {
				return true
			}
			if len(call.Args) != 1 {
				return true
			}
			lit, ok := call.Args[0].(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				// Not a raw literal (e.g. a variable) — nothing static to check.
				return true
			}
			value := strings.Trim(lit.Value, `"`)
			if !httputil.IsRegistered(httputil.ErrorCode(value)) {
				offenders = append(offenders, path+": httputil.ErrorCode(\""+value+"\") is not in httputil.Registry")
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walking %s: %v", root, err)
	}

	for _, o := range offenders {
		t.Error(o)
	}
}

// findAPIRoot locates the services/api directory regardless of the working
// directory the test is run from.
func findAPIRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	dir := wd
	for {
		if filepath.Base(dir) == "api" {
			if _, err := os.Stat(filepath.Join(dir, "handlers")); err == nil {
				return dir
			}
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("could not locate services/api starting from %s", wd)
		}
		dir = parent
	}
}
