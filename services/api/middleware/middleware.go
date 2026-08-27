package middleware

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/Depo-dev/trident/services/api/internal/httputil"
	"github.com/google/uuid"
)

const RequestIDHeader = "X-Request-ID"

// RequestID middleware accepts a valid incoming X-Request-Id (so a caller can
// correlate a request end-to-end) or generates a UUID when absent/invalid,
// attaches it to the request context, and echoes it on the response header.
func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(RequestIDHeader)
		if !httputil.ValidRequestID(id) {
			id = uuid.New().String()
		}
		w.Header().Set(RequestIDHeader, id)
		ctx := httputil.ContextWithRequestID(r.Context(), id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// LoggingResponseWriter wraps http.ResponseWriter to capture status code
type LoggingResponseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (w *LoggingResponseWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *LoggingResponseWriter) WriteHeader(code int) {
	w.statusCode = code
	w.ResponseWriter.WriteHeader(code)
}

// StructuredLogging middleware logs requests with structured JSON logging
func StructuredLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// Get request ID from context (set by the RequestID middleware).
		requestID := httputil.RequestIDFromContext(r.Context())

		// Wrap response writer to capture status code
		wrapped := &LoggingResponseWriter{ResponseWriter: w, statusCode: http.StatusOK}

		next.ServeHTTP(wrapped, r)

		duration := time.Since(start)

		slog.InfoContext(r.Context(),
			"http_request",
			slog.String("request_id", requestID),
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path),
			slog.Int("status", wrapped.statusCode),
			slog.Duration("latency", duration),
		)
	})
}

// Chain combines multiple middleware
func Chain(handler http.Handler, middlewares ...func(http.Handler) http.Handler) http.Handler {
	for i := len(middlewares) - 1; i >= 0; i-- {
		handler = middlewares[i](handler)
	}
	return handler
}
