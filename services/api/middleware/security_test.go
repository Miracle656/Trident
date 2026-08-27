package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSecurityHeaders_Production(t *testing.T) {
	handler := SecurityHeaders(true)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("GET", "/test", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	h := rec.Header()
	tests := []struct {
		key  string
		want string
	}{
		{"Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; connect-src 'self' wss: ws:; img-src 'self' data:; font-src 'self'; object-src 'none'; frame-ancestors 'none'"},
		{"X-Content-Type-Options", "nosniff"},
		{"Referrer-Policy", "strict-origin-when-cross-origin"},
		{"X-Frame-Options", "DENY"},
		{"X-XSS-Protection", "0"},
		{"Strict-Transport-Security", "max-age=31536000; includeSubDomains; preload"},
	}
	for _, tt := range tests {
		if got := h.Get(tt.key); got != tt.want {
			t.Errorf("header %s = %q, want %q", tt.key, got, tt.want)
		}
	}
}

func TestSecurityHeaders_NonProduction(t *testing.T) {
	handler := SecurityHeaders(false)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("GET", "/test", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	h := rec.Header()
	if got := h.Get("Strict-Transport-Security"); got != "" {
		t.Errorf("HSTS should not be set in non-production, got %q", got)
	}
	if got := h.Get("Content-Security-Policy"); got == "" {
		t.Error("CSP should be set in all environments")
	}
}

func TestCORS_AllowedOrigin(t *testing.T) {
	handler := CORS([]string{"https://explorer.example.com"})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("Origin", "https://explorer.example.com")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "https://explorer.example.com" {
		t.Errorf("CORS header = %q, want %q", got, "https://explorer.example.com")
	}
}

func TestCORS_DisallowedOrigin(t *testing.T) {
	handler := CORS([]string{"https://explorer.example.com"})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("Origin", "https://evil.example.com")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Errorf("expected 403, got %d", rec.Code)
	}
}

func TestCORS_Preflight(t *testing.T) {
	handler := CORS([]string{"https://explorer.example.com"})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("preflight should not call next handler")
	}))

	req := httptest.NewRequest("OPTIONS", "/test", nil)
	req.Header.Set("Origin", "https://explorer.example.com")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Errorf("expected 204, got %d", rec.Code)
	}
}
