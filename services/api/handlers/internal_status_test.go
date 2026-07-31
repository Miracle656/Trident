package handlers

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// These tests exercise the X-Internal-Key check on GET /internal/status
// directly (package handlers, not handlers_test) since statusDeps is
// unexported. statusDeps is left nil throughout, which is fine: the auth
// check runs before any dependency is touched.

func TestInternalStatus_CorrectKey_Returns200(t *testing.T) {
	t.Setenv("INTERNAL_API_KEY", "correct-horse-battery-staple")

	req := httptest.NewRequest(http.MethodGet, "/internal/status", nil)
	req.Header.Set("X-Internal-Key", "correct-horse-battery-staple")
	rr := httptest.NewRecorder()

	InternalStatus()(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("want 200, got %d: %s", rr.Code, rr.Body.String())
	}
}

func TestInternalStatus_WrongKey_Returns401(t *testing.T) {
	t.Setenv("INTERNAL_API_KEY", "correct-horse-battery-staple")

	req := httptest.NewRequest(http.MethodGet, "/internal/status", nil)
	req.Header.Set("X-Internal-Key", "wrong-key")
	rr := httptest.NewRecorder()

	InternalStatus()(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("want 401, got %d: %s", rr.Code, rr.Body.String())
	}
}

func TestInternalStatus_MissingHeader_Returns401(t *testing.T) {
	t.Setenv("INTERNAL_API_KEY", "correct-horse-battery-staple")

	req := httptest.NewRequest(http.MethodGet, "/internal/status", nil)
	rr := httptest.NewRecorder()

	InternalStatus()(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("want 401, got %d: %s", rr.Code, rr.Body.String())
	}
}

func TestInternalStatus_UnsetKey_FailsClosed(t *testing.T) {
	// INTERNAL_API_KEY unset entirely -> every request must be rejected,
	// even one that (implausibly) sends an empty X-Internal-Key.
	t.Setenv("INTERNAL_API_KEY", "")

	req := httptest.NewRequest(http.MethodGet, "/internal/status", nil)
	// Deliberately do not set X-Internal-Key at all.
	rr := httptest.NewRecorder()

	InternalStatus()(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("want 401 (fail closed), got %d: %s", rr.Code, rr.Body.String())
	}
}

func TestInternalStatus_UnsetKey_EmptyProvidedKey_StillRejected(t *testing.T) {
	// Guards against a regression where an empty expected key and an empty
	// provided key would compare equal and incorrectly authenticate.
	t.Setenv("INTERNAL_API_KEY", "")

	req := httptest.NewRequest(http.MethodGet, "/internal/status", nil)
	req.Header.Set("X-Internal-Key", "")
	rr := httptest.NewRecorder()

	InternalStatus()(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("want 401 (fail closed), got %d: %s", rr.Code, rr.Body.String())
	}
}
