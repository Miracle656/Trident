package middleware

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/Depo-dev/trident/services/api/internal/httputil"
	"github.com/redis/go-redis/v9"
)

// IdempotencyKeyHeader is the header clients set to make a resource-creating
// POST safe to retry (issue #426 / #225). SDKs that retry a POST on a
// network blip should generate one key per logical operation and resend the
// same value on every retry of that operation.
const IdempotencyKeyHeader = "Idempotency-Key"

// idempotencyKeyTTL is how long a replayed response stays available for a
// given Idempotency-Key, matching the window during which a client's retry
// logic can reasonably still be retrying the original request. Stored in
// Redis with a native EXPIRE, so keys are pruned automatically by Redis
// itself — no separate cleanup job is needed (unlike the Postgres-backed
// retention jobs such as startWebhookCleanupJob / startRetentionJob, which
// exist because Postgres has no equivalent to TTL-on-write).
const idempotencyKeyTTL = 24 * time.Hour

// idempotencyKeyPrefix namespaces idempotency records in Redis, mirroring
// the "ratelimit:"/"apiauth:" prefix convention used elsewhere in this
// package/service.
const idempotencyKeyPrefix = "idempotency:"

// maxIdempotentBodyBytes bounds how much of the request/response body this
// middleware will buffer in memory and store in Redis per key. Requests to
// the routes this middleware wraps are already bounded well under this by
// BodySizeLimit; this is a defensive backstop against caching runaway
// response bodies.
const maxIdempotentBodyBytes = 1 << 20 // 1 MiB

// idempotencyRecord is what gets JSON-encoded and stored in Redis under
// idempotency:<key>. BodyHash lets a replay detect a caller reusing the same
// key with a different request body (a client bug, not a legitimate retry).
type idempotencyRecord struct {
	BodyHash    string `json:"body_hash"`
	StatusCode  int    `json:"status_code"`
	ContentType string `json:"content_type"`
	Body        []byte `json:"body"`
}

// IdempotencyConfig configures the Idempotency middleware.
type IdempotencyConfig struct {
	// Redis backs the idempotency store. When nil, the middleware is a
	// no-op passthrough (fail-open: idempotency is a safety net, not a
	// hard dependency — the same fail-open stance TieredRateLimit takes on
	// a Redis outage).
	Redis *redis.Client
	// TTL overrides idempotencyKeyTTL; zero uses the default.
	TTL time.Duration
}

// hashBody returns the hex-encoded SHA-256 of body, used to detect a client
// reusing an Idempotency-Key with a different request payload.
func hashBody(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

// idempotencyResponseRecorder buffers the handler's response so it can be
// stored in Redis after a successful run, instead of streaming straight to
// the client — a replay needs the exact status/body pair, not just a status
// code.
type idempotencyResponseRecorder struct {
	http.ResponseWriter
	statusCode  int
	body        bytes.Buffer
	wroteHeader bool
}

func (rec *idempotencyResponseRecorder) Unwrap() http.ResponseWriter { return rec.ResponseWriter }

func (rec *idempotencyResponseRecorder) WriteHeader(code int) {
	if rec.wroteHeader {
		return
	}
	rec.wroteHeader = true
	rec.statusCode = code
	rec.ResponseWriter.WriteHeader(code)
}

func (rec *idempotencyResponseRecorder) Write(b []byte) (int, error) {
	if !rec.wroteHeader {
		rec.WriteHeader(http.StatusOK)
	}
	if rec.body.Len() < maxIdempotentBodyBytes {
		remaining := maxIdempotentBodyBytes - rec.body.Len()
		if remaining > len(b) {
			remaining = len(b)
		}
		rec.body.Write(b[:remaining])
	}
	return rec.ResponseWriter.Write(b)
}

// Idempotency returns middleware implementing safe retries for
// resource-creating POST endpoints (issue #426, closes #225).
//
// Behavior:
//   - No Idempotency-Key header: passthrough. Idempotency is optional, not
//     required, so every existing caller keeps working unchanged.
//   - New key: the request runs normally; on completion (status < 500) the
//     response is cached in Redis under idempotency:<key> for TTL, keyed
//     also by a hash of the request body.
//   - Same key, same body (within TTL): the cached status + body is replayed
//     verbatim and the handler does not run again, so a client-side retry
//     never creates a second resource.
//   - Same key, different body: this is a client bug (key reuse across two
//     different logical requests), not a retry, so the request is rejected
//     with 409 CONFLICT rather than silently running or silently replaying
//     the wrong response.
//   - A 5xx response is not cached, so a genuine server error can still be
//     retried (and succeed) with the same key.
//   - Redis unavailable/erroring: fails open and runs the handler normally,
//     matching TieredRateLimit's fail-open stance — idempotency is a safety
//     net, not a hard dependency for availability.
func Idempotency(cfg IdempotencyConfig) func(http.Handler) http.Handler {
	ttl := cfg.TTL
	if ttl <= 0 {
		ttl = idempotencyKeyTTL
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := r.Header.Get(IdempotencyKeyHeader)
			if key == "" || cfg.Redis == nil {
				next.ServeHTTP(w, r)
				return
			}

			var body []byte
			if r.Body != nil {
				var err error
				body, err = io.ReadAll(io.LimitReader(r.Body, maxIdempotentBodyBytes+1))
				_ = r.Body.Close()
				if err != nil {
					httputil.WriteErrorCtx(r.Context(), w, http.StatusBadRequest, httputil.INVALID_ARGUMENT, "failed to read request body")
					return
				}
				r.Body = io.NopCloser(bytes.NewReader(body))
			}
			bodyHash := hashBody(body)

			redisKey := idempotencyKeyPrefix + key
			ctx, cancel := context.WithTimeout(r.Context(), 500*time.Millisecond)
			raw, err := cfg.Redis.Get(ctx, redisKey).Bytes()
			cancel()

			switch {
			case err == nil:
				var rec idempotencyRecord
				if jsonErr := json.Unmarshal(raw, &rec); jsonErr != nil {
					slog.Warn("idempotency: failed to decode cached record; failing open", "err", jsonErr)
					next.ServeHTTP(w, r)
					return
				}
				if rec.BodyHash != bodyHash {
					httputil.WriteErrorCtx(r.Context(), w, http.StatusConflict, httputil.CONFLICT,
						"Idempotency-Key was already used with a different request body")
					return
				}
				if rec.ContentType != "" {
					w.Header().Set("Content-Type", rec.ContentType)
				}
				w.Header().Set("Idempotency-Replayed", "true")
				w.WriteHeader(rec.StatusCode)
				_, _ = w.Write(rec.Body)
				return

			case err == redis.Nil:
				// No cached record yet — run the handler and cache the result.

			default:
				slog.Warn("idempotency: redis lookup failed; failing open", "err", err)
				next.ServeHTTP(w, r)
				return
			}

			rec := &idempotencyResponseRecorder{ResponseWriter: w, statusCode: http.StatusOK}
			next.ServeHTTP(rec, r)

			if rec.statusCode >= 500 {
				// Don't cache server errors: the client should be able to
				// retry the same key and actually succeed.
				return
			}

			record := idempotencyRecord{
				BodyHash:    bodyHash,
				StatusCode:  rec.statusCode,
				ContentType: rec.Header().Get("Content-Type"),
				Body:        rec.body.Bytes(),
			}
			encoded, err := json.Marshal(record)
			if err != nil {
				slog.Warn("idempotency: failed to encode record for caching", "err", err)
				return
			}
			storeCtx, storeCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer storeCancel()
			if err := cfg.Redis.Set(storeCtx, redisKey, encoded, ttl).Err(); err != nil {
				slog.Warn("idempotency: failed to store record", "err", err)
			}
		})
	}
}
