package middleware_test

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Depo-dev/trident/services/api/middleware"
)

func hashKey(salt, key string) string {
	mac := hmac.New(sha256.New, []byte(salt))
	_, _ = mac.Write([]byte(key))
	return hex.EncodeToString(mac.Sum(nil))
}

func TestAPIKey(t *testing.T) {
	const (
		salt = "test-salt"
		key  = "valid-key"
	)
	t.Setenv("API_KEY_SALT", salt)
	t.Setenv("API_KEY_HASHES", hashKey(salt, key))

	handler := middleware.APIKey(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	tests := []struct {
		name       string
		path       string
		key        string
		wantStatus int
	}{
		{name: "valid protected request", path: "/v1/events/stream", key: key, wantStatus: http.StatusNoContent},
		{name: "missing key", path: "/v1/events/stream", wantStatus: http.StatusUnauthorized},
		{name: "invalid key", path: "/v1/events/stream", key: "wrong", wantStatus: http.StatusUnauthorized},
		{name: "health is public", path: "/v1/health", wantStatus: http.StatusNoContent},
		{name: "indexer stats is public", path: "/v1/stats/indexer", wantStatus: http.StatusNoContent},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			if tt.key != "" {
				req.Header.Set("X-API-Key", tt.key)
			}
			rec := httptest.NewRecorder()

			handler.ServeHTTP(rec, req)

			if rec.Code != tt.wantStatus {
				t.Fatalf("status: got %d, want %d", rec.Code, tt.wantStatus)
			}
		})
	}
}
