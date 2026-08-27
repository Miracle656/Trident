# Error codes

Every error response from the Trident API — REST and gRPC alike — uses one
envelope shape and one catalogue of machine-readable error codes, so clients
can branch on `error.code` instead of parsing prose or HTTP status codes
alone.

This is documentation for the source of truth in
[`services/api/internal/httputil/registry.go`](../services/api/internal/httputil/registry.go).
`registry_test.go` in that package asserts every constant declared in
`errors.go` has an entry here, and statically scans every handler under
`services/api` to fail the build if a hardcoded `httputil.ErrorCode("...")`
literal ever names a code missing from the table below. In addition,
`WriteError`/`WriteErrorCtx` (the only functions that write the JSON error
body) fall back to `INTERNAL` if somehow asked to emit an unregistered code,
so an undocumented code can never actually reach a client.

## Envelope shape

Every non-2xx JSON response (REST) and every gRPC error mapped to an HTTP
response by the gateway share this envelope:

```json
{
  "error": {
    "code": "INVALID_ARGUMENT",
    "message": "limit must be an integer between 1 and 200",
    "details": { "field": "limit" },
    "request_id": "c7b1e6b0-1a2b-4c3d-9e8f-0a1b2c3d4e5f"
  }
}
```

| Field        | Type   | Required | Meaning |
|--------------|--------|----------|---------|
| `code`       | string | yes      | One of the codes in the catalogue below. Stable across releases — new codes may be added, existing codes are never repurposed or removed. |
| `message`    | string | yes      | Human-readable description. Not stable — do not pattern-match on it; branch on `code` instead. |
| `details`    | object | no       | Optional structured context specific to the code (e.g. which request field failed validation). Absent when there is nothing more specific than the message. |
| `request_id` | string | no       | Correlates the failure with server logs/traces. Present whenever the handler has access to a request-scoped context (i.e. everywhere `WriteErrorCtx` is used instead of `WriteError`). |

Raw gRPC clients (see `services/api/grpc` and `services/api/grpcclient`)
receive a standard `google.rpc.Status` with one of the `codes.Code` values;
`httputil.GRPCToHTTP` (used by the HTTP surface when it calls into a gRPC
backend) maps each gRPC code to the HTTP status and `ErrorCode` documented
below, so the table applies to both surfaces.

## GraphQL

This repo does not implement a GraphQL query/mutation API. The GraphQL
surface under `services/api/ws/graphql.go` and the TypeScript SDK's
`transports/graphql.ts` is a WebSocket **subscription** transport for
streaming events — it is a transport-level alternative to the REST
`/v1/events/stream` endpoint, not a general-purpose GraphQL server. Transport
errors on that path (dropped connections, malformed subscription messages,
parse failures) surface to SDK callers as `TridentError` (see
`sdk/typescript/src/errors.ts`), not as the REST error envelope, since there
is no HTTP response to attach it to. Non-transport failures returned by the
resolver are still expressed with the codes below.

## Catalogue

| Code | HTTP status | Retryable | Meaning |
|------|-------------|-----------|---------|
| `INVALID_ARGUMENT` | 400 | No | The request is malformed or fails validation (bad parameter, unknown field, wrong type). Fix the request before retrying — retrying unchanged will fail identically. |
| `UNAUTHORIZED` | 401 | No | The request is missing a valid API key or credential. |
| `FORBIDDEN` | 403 | No | The request was rejected outright by abuse-protection (e.g. the global concurrency cap shedding load under `#318`), not a normal auth failure. |
| `NOT_FOUND` | 404 | No | The requested resource does not exist. |
| `CONFLICT` | 409 | No | The request conflicts with existing state — currently, an `Idempotency-Key` replayed with a different request body (`#426`). Do not retry as-is: either resend the original body to get the cached response, or use a fresh key. |
| `PAYLOAD_TOO_LARGE` | 413 | No | The request body exceeds the configured `http.MaxBytesReader` limit (`#317`). |
| `RATE_LIMITED` | 429 | Yes | The caller has exceeded its rate-limit tier. Retry after the window resets — honor `Retry-After` and `X-RateLimit-Reset` rather than retrying immediately. |
| `INTERNAL` | 500 | Sometimes | An unexpected server-side fault. Not guaranteed to succeed on retry; safe to retry idempotent (GET) requests with backoff, but do not blindly retry writes. |
| `UNAVAILABLE` | 503 / 504 | Yes | A transient upstream/backend outage (503) or gateway timeout waiting on a backend (504) — not an internal fault. Safe to retry with backoff. |

"Retryable" means retrying the *same* request is expected to eventually
succeed once the underlying condition clears, for an idempotent request. It
is not a blanket recommendation to retry non-idempotent writes automatically.

## Adding a new code

1. Add the constant in `services/api/internal/httputil/errors.go`.
2. Add a matching entry to `Registry` in `registry.go` (HTTP status,
   retryability, summary).
3. Add the code to the `code` enum in `api/openapi.yaml`
   (`components.schemas.ErrorResponse`).
4. Add a row to the catalogue table above.
5. Run `go test ./services/api/internal/httputil/...` — `registry_test.go`
   fails the build until steps 1–2 are consistent, and fails any handler that
   references the new literal directly instead of via a named constant.

## SDK typed errors

The TypeScript SDK (`sdk/typescript/src/errors.ts`) exposes `TridentApiError`
with a `code: ApiErrorCode` field typed against this same catalogue, so a
TypeScript consumer gets a compile error for an unrecognized code rather than
discovering a typo at runtime. Other SDKs (Go, Python, Rust) currently only
expose the raw string `code` from the parsed envelope; adding equivalent
typed error codes to those SDKs is tracked as follow-up work and out of scope
for this change.
