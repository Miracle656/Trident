package httputil

// CodeMeta describes one entry in the error code catalogue: the HTTP status
// it is paired with, whether a client may safely retry the request as-is,
// and a short human-readable summary of when the code is returned.
//
// This is the single source of truth for every machine-readable error code
// the API (REST and gRPC alike) can emit. docs/errors.md documents the same
// table for humans; registry_test.go asserts the two stay in sync and that
// no handler emits a code missing from this table (issue #424).
type CodeMeta struct {
	Code       ErrorCode
	HTTPStatus int
	Retryable  bool
	Summary    string
}

// Registry enumerates every documented error code. Ordering matches
// docs/errors.md.
var Registry = []CodeMeta{
	{
		Code:       INVALID_ARGUMENT,
		HTTPStatus: 400,
		Retryable:  false,
		Summary:    "The request is malformed or fails validation (bad parameter, unknown field, wrong type). Fix the request before retrying.",
	},
	{
		Code:       UNAUTHORIZED,
		HTTPStatus: 401,
		Retryable:  false,
		Summary:    "The request is missing a valid API key or credential.",
	},
	{
		Code:       FORBIDDEN,
		HTTPStatus: 403,
		Retryable:  false,
		Summary:    "The request was rejected outright by abuse-protection (e.g. the global concurrency cap shedding load), not a normal auth failure.",
	},
	{
		Code:       NOT_FOUND,
		HTTPStatus: 404,
		Retryable:  false,
		Summary:    "The requested resource does not exist.",
	},
	{
		Code:       CONFLICT,
		HTTPStatus: 409,
		Retryable:  false,
		Summary:    "The request conflicts with existing state — e.g. an Idempotency-Key replayed with a different request body. Do not retry as-is; either reuse the original body or use a fresh key.",
	},
	{
		Code:       PAYLOAD_TOO_LARGE,
		HTTPStatus: 413,
		Retryable:  false,
		Summary:    "The request body exceeds the configured size limit.",
	},
	{
		Code:       RATE_LIMITED,
		HTTPStatus: 429,
		Retryable:  true,
		Summary:    "The caller has exceeded its rate limit tier. Retry after the window resets (see Retry-After / X-RateLimit-Reset).",
	},
	{
		Code:       INTERNAL,
		HTTPStatus: 500,
		Retryable:  false,
		Summary:    "An unexpected server-side fault. Not guaranteed to succeed on retry; safe to retry idempotent (GET) requests with backoff.",
	},
	{
		Code:       UNAVAILABLE,
		HTTPStatus: 503,
		Retryable:  true,
		Summary:    "A transient upstream/backend outage or gateway timeout, not an internal fault. Safe to retry with backoff.",
	},
}

// registryByCode indexes Registry for O(1) lookups. Built once at init and
// panics on a duplicate code, since a duplicate would mean the catalogue
// itself is inconsistent.
var registryByCode = func() map[ErrorCode]CodeMeta {
	m := make(map[ErrorCode]CodeMeta, len(Registry))
	for _, entry := range Registry {
		if _, exists := m[entry.Code]; exists {
			panic("httputil: duplicate error code in Registry: " + string(entry.Code))
		}
		m[entry.Code] = entry
	}
	return m
}()

// Lookup returns the catalogue entry for code and whether it is documented.
func Lookup(code ErrorCode) (CodeMeta, bool) {
	meta, ok := registryByCode[code]
	return meta, ok
}

// IsRegistered reports whether code appears in the documented catalogue.
func IsRegistered(code ErrorCode) bool {
	_, ok := registryByCode[code]
	return ok
}
