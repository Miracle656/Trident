package httputil

import (
	"context"
	"encoding/json"
	"net/http"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ErrorCode represents the machine-readable error type
type ErrorCode string

const (
	NOT_FOUND        ErrorCode = "NOT_FOUND"
	UNAUTHORIZED     ErrorCode = "UNAUTHORIZED"
	RATE_LIMITED     ErrorCode = "RATE_LIMITED"
	INVALID_ARGUMENT ErrorCode = "INVALID_ARGUMENT"
	UNAVAILABLE      ErrorCode = "UNAVAILABLE"
	INTERNAL         ErrorCode = "INTERNAL"
	// PAYLOAD_TOO_LARGE is returned when a request body exceeds the
	// configured http.MaxBytesReader limit (issue #317).
	PAYLOAD_TOO_LARGE ErrorCode = "PAYLOAD_TOO_LARGE"
	// FORBIDDEN is returned when abuse-protection middleware rejects a
	// request outright (e.g. the global concurrency cap shedding load,
	// issue #318) rather than for a normal auth failure.
	FORBIDDEN ErrorCode = "FORBIDDEN"
)

// ErrorDetail is the nested error object required by the OpenAPI ErrorResponse
// schema and parsed by the client SDKs. Every field but Code and Message is
// optional: Details carries structured, code-specific context (e.g. which
// field failed validation) and RequestID lets a client correlate a failure
// with server logs and traces.
type ErrorDetail struct {
	Code      ErrorCode      `json:"code"`
	Message   string         `json:"message"`
	Details   map[string]any `json:"details,omitempty"`
	RequestID string         `json:"request_id,omitempty"`
}

// ErrorResponse is the standardized JSON error body: {"error":{"code","message"}}.
type ErrorResponse struct {
	Error ErrorDetail `json:"error"`
}

// WriteError writes a standardized JSON error response matching the OpenAPI
// ErrorResponse schema ({"error":{"code","message"}}). It carries no request
// id; prefer WriteErrorCtx from a request-scoped handler so the error envelope
// includes error.request_id.
//
// code must be one of the constants declared in this package (all of which
// are registered in Registry, see registry.go); every call site is covered
// by registry_test.go's static check, and this is the only path (along with
// WriteErrorCtx/WriteErrorDetails/WriteErrorCtxDetails) by which an error
// code reaches a client, so the registry is the complete catalogue of codes
// this API can emit.
func WriteError(w http.ResponseWriter, statusCode int, code ErrorCode, message string) {
	writeError(w, statusCode, code, message, nil, "")
}

// WriteErrorCtx writes a standardized JSON error response and populates
// error.request_id from the request id attached to ctx by the RequestID
// middleware, so a client failure can be correlated to server logs and traces.
func WriteErrorCtx(ctx context.Context, w http.ResponseWriter, statusCode int, code ErrorCode, message string) {
	writeError(w, statusCode, code, message, nil, RequestIDFromContext(ctx))
}

// WriteErrorDetails is WriteError plus a structured details payload (e.g.
// {"field": "contractId"}) for callers that need to carry machine-readable
// context beyond the human-readable message.
func WriteErrorDetails(w http.ResponseWriter, statusCode int, code ErrorCode, message string, details map[string]any) {
	writeError(w, statusCode, code, message, details, "")
}

// WriteErrorCtxDetails combines WriteErrorCtx and WriteErrorDetails.
func WriteErrorCtxDetails(ctx context.Context, w http.ResponseWriter, statusCode int, code ErrorCode, message string, details map[string]any) {
	writeError(w, statusCode, code, message, details, RequestIDFromContext(ctx))
}

func writeError(w http.ResponseWriter, statusCode int, code ErrorCode, message string, details map[string]any, requestID string) {
	// Defense in depth: every constant in this package is registered (see
	// registry_test.go), but guard against a future hardcoded literal
	// slipping past review by falling back to the documented INTERNAL code
	// rather than emitting an undocumented one to a client.
	if !IsRegistered(code) {
		code = INTERNAL
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(ErrorResponse{
		Error: ErrorDetail{Code: code, Message: message, Details: details, RequestID: requestID},
	})
}

// GRPCToHTTP maps a gRPC error to HTTP status code and error code
func GRPCToHTTP(err error) (int, ErrorCode) {
	if err == nil {
		return http.StatusOK, ""
	}

	st, ok := status.FromError(err)
	if !ok {
		return http.StatusInternalServerError, INTERNAL
	}

	switch st.Code() {
	case codes.NotFound:
		return http.StatusNotFound, NOT_FOUND
	case codes.InvalidArgument:
		return http.StatusBadRequest, INVALID_ARGUMENT
	case codes.Unauthenticated:
		return http.StatusUnauthorized, UNAUTHORIZED
	case codes.ResourceExhausted:
		return http.StatusTooManyRequests, RATE_LIMITED
	case codes.DeadlineExceeded:
		// The backend did not answer within the call deadline: a gateway
		// timeout, not an internal fault — clients may retry (issue #227).
		return http.StatusGatewayTimeout, UNAVAILABLE
	case codes.Unavailable:
		// The backend is unreachable or refusing connections: a transient
		// upstream outage, not an internal fault — clients may retry.
		return http.StatusServiceUnavailable, UNAVAILABLE
	default:
		return http.StatusInternalServerError, INTERNAL
	}
}
