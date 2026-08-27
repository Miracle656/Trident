package middleware

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newIdempotencyTestRedis(t *testing.T) *redis.Client {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	t.Cleanup(mr.Close)
	return redis.NewClient(&redis.Options{Addr: mr.Addr()})
}

func countingCreateHandler(calls *int, status int, body string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*calls++
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	})
}

func TestIdempotency_MissingHeaderPassesThrough(t *testing.T) {
	rc := newIdempotencyTestRedis(t)
	calls := 0
	handler := Idempotency(IdempotencyConfig{Redis: rc})(countingCreateHandler(&calls, http.StatusCreated, `{"id":"1"}`))

	for i := 0; i < 3; i++ {
		req := httptest.NewRequest(http.MethodPost, "/v1/api-keys", bytes.NewBufferString(`{"label":"x"}`))
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)
		if rr.Code != http.StatusCreated {
			t.Fatalf("expected 201, got %d", rr.Code)
		}
	}
	if calls != 3 {
		t.Fatalf("expected handler to run every time without a key, got %d calls", calls)
	}
}

func TestIdempotency_ReplayReturnsCachedResponse(t *testing.T) {
	rc := newIdempotencyTestRedis(t)
	calls := 0
	handler := Idempotency(IdempotencyConfig{Redis: rc})(countingCreateHandler(&calls, http.StatusCreated, `{"id":"abc123"}`))

	body := `{"label":"my-key"}`
	key := "retry-key-1"

	req1 := httptest.NewRequest(http.MethodPost, "/v1/api-keys", bytes.NewBufferString(body))
	req1.Header.Set(IdempotencyKeyHeader, key)
	rr1 := httptest.NewRecorder()
	handler.ServeHTTP(rr1, req1)
	if rr1.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", rr1.Code)
	}

	req2 := httptest.NewRequest(http.MethodPost, "/v1/api-keys", bytes.NewBufferString(body))
	req2.Header.Set(IdempotencyKeyHeader, key)
	rr2 := httptest.NewRecorder()
	handler.ServeHTTP(rr2, req2)

	if calls != 1 {
		t.Fatalf("expected handler to run exactly once, got %d calls", calls)
	}
	if rr2.Code != http.StatusCreated {
		t.Fatalf("expected replayed 201, got %d", rr2.Code)
	}
	if rr2.Body.String() != rr1.Body.String() {
		t.Fatalf("expected replayed body to match original: got %q want %q", rr2.Body.String(), rr1.Body.String())
	}
	if rr2.Header().Get("Idempotency-Replayed") != "true" {
		t.Fatalf("expected Idempotency-Replayed header on replay")
	}
}

func TestIdempotency_ConflictingBodyReturns409(t *testing.T) {
	rc := newIdempotencyTestRedis(t)
	calls := 0
	handler := Idempotency(IdempotencyConfig{Redis: rc})(countingCreateHandler(&calls, http.StatusCreated, `{"id":"abc123"}`))

	key := "retry-key-2"

	req1 := httptest.NewRequest(http.MethodPost, "/v1/api-keys", bytes.NewBufferString(`{"label":"a"}`))
	req1.Header.Set(IdempotencyKeyHeader, key)
	rr1 := httptest.NewRecorder()
	handler.ServeHTTP(rr1, req1)
	if rr1.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", rr1.Code)
	}

	req2 := httptest.NewRequest(http.MethodPost, "/v1/api-keys", bytes.NewBufferString(`{"label":"different-body"}`))
	req2.Header.Set(IdempotencyKeyHeader, key)
	rr2 := httptest.NewRecorder()
	handler.ServeHTTP(rr2, req2)

	if calls != 1 {
		t.Fatalf("expected handler NOT to run again for a conflicting body, got %d calls", calls)
	}
	if rr2.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d", rr2.Code)
	}
	var resp struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rr2.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode error response: %v", err)
	}
	if resp.Error.Code != "CONFLICT" {
		t.Fatalf("expected CONFLICT error code, got %q", resp.Error.Code)
	}
}

func TestIdempotency_ServerErrorNotCached(t *testing.T) {
	rc := newIdempotencyTestRedis(t)
	calls := 0
	handler := Idempotency(IdempotencyConfig{Redis: rc})(countingCreateHandler(&calls, http.StatusInternalServerError, `{"error":"boom"}`))

	key := "retry-key-3"
	body := `{"label":"x"}`

	for i := 0; i < 2; i++ {
		req := httptest.NewRequest(http.MethodPost, "/v1/api-keys", bytes.NewBufferString(body))
		req.Header.Set(IdempotencyKeyHeader, key)
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)
		if rr.Code != http.StatusInternalServerError {
			t.Fatalf("expected 500, got %d", rr.Code)
		}
	}
	if calls != 2 {
		t.Fatalf("expected handler to run again after a 5xx (not cached), got %d calls", calls)
	}
}

func TestIdempotency_KeyExpiresAfterTTL(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	defer mr.Close()
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	calls := 0
	handler := Idempotency(IdempotencyConfig{Redis: rc, TTL: 50 * time.Millisecond})(
		countingCreateHandler(&calls, http.StatusCreated, `{"id":"abc123"}`),
	)

	key := "retry-key-4"
	body := `{"label":"x"}`

	req1 := httptest.NewRequest(http.MethodPost, "/v1/api-keys", bytes.NewBufferString(body))
	req1.Header.Set(IdempotencyKeyHeader, key)
	handler.ServeHTTP(httptest.NewRecorder(), req1)

	// Fast-forward miniredis's clock past the TTL instead of sleeping.
	mr.FastForward(100 * time.Millisecond)

	req2 := httptest.NewRequest(http.MethodPost, "/v1/api-keys", bytes.NewBufferString(body))
	req2.Header.Set(IdempotencyKeyHeader, key)
	rr2 := httptest.NewRecorder()
	handler.ServeHTTP(rr2, req2)

	if calls != 2 {
		t.Fatalf("expected handler to run again once the key expired, got %d calls", calls)
	}
	if rr2.Code != http.StatusCreated {
		t.Fatalf("expected 201 after expiry, got %d", rr2.Code)
	}
}

func TestIdempotency_NoRedisFailsOpen(t *testing.T) {
	calls := 0
	handler := Idempotency(IdempotencyConfig{})(countingCreateHandler(&calls, http.StatusCreated, `{"id":"1"}`))

	req := httptest.NewRequest(http.MethodPost, "/v1/api-keys", bytes.NewBufferString(`{"label":"x"}`))
	req.Header.Set(IdempotencyKeyHeader, "some-key")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("expected 201 (fail-open passthrough), got %d", rr.Code)
	}
	if calls != 1 {
		t.Fatalf("expected handler to run once, got %d calls", calls)
	}
}

func TestIdempotency_RequestBodyStillReadableByHandler(t *testing.T) {
	rc := newIdempotencyTestRedis(t)
	var seenBody string
	handler := Idempotency(IdempotencyConfig{Redis: rc})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf := new(bytes.Buffer)
		_, _ = buf.ReadFrom(r.Body)
		seenBody = buf.String()
		w.WriteHeader(http.StatusCreated)
	}))

	req := httptest.NewRequest(http.MethodPost, "/v1/api-keys", strings.NewReader(`{"label":"x"}`))
	req.Header.Set(IdempotencyKeyHeader, "some-key")
	handler.ServeHTTP(httptest.NewRecorder(), req)

	if seenBody != `{"label":"x"}` {
		t.Fatalf("expected handler to still be able to read the request body, got %q", seenBody)
	}
}
