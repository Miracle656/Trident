package middleware

import (
	"fmt"
	"io"
	"sync"
)

// grpcLatencyBuckets are the histogram bucket bounds (seconds) for
// trident_api_grpc_client_request_duration_seconds. gRPC calls to the
// internal events backend are in-cluster and DB-backed, so the buckets skew
// tighter than the public-facing HTTP ones.
var grpcLatencyBuckets = []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5}

// grpcMetricKey identifies one gRPC client call series: method is the full
// gRPC method name (e.g. "/trident.Events/ListEvents"), code is the gRPC
// status code name (e.g. "OK", "NotFound", "Unavailable").
type grpcMetricKey struct {
	method string
	code   string
}

type grpcMetricSeries struct {
	bucketCounts []uint64
	sum          float64
	count        uint64
}

var (
	grpcMetricsMu   sync.Mutex
	grpcMetricsData = map[grpcMetricKey]*grpcMetricSeries{}
)

// RecordGRPCClientCall records the latency and outcome of a single unary
// gRPC call the Go API made to the internal events backend. Called from a
// grpc.UnaryClientInterceptor (see services/api/grpc.metricsUnaryInterceptor).
func RecordGRPCClientCall(method, code string, seconds float64) {
	key := grpcMetricKey{method: method, code: code}

	grpcMetricsMu.Lock()
	defer grpcMetricsMu.Unlock()

	series, ok := grpcMetricsData[key]
	if !ok {
		series = &grpcMetricSeries{bucketCounts: make([]uint64, len(grpcLatencyBuckets))}
		grpcMetricsData[key] = series
	}
	for i, bound := range grpcLatencyBuckets {
		if seconds <= bound {
			series.bucketCounts[i]++
		}
	}
	series.sum += seconds
	series.count++
}

// WriteGRPCClientMetrics renders trident_api_grpc_client_requests_total and
// trident_api_grpc_client_request_duration_seconds in Prometheus text format.
func WriteGRPCClientMetrics(w io.Writer) {
	_, _ = fmt.Fprint(w, "# HELP trident_api_grpc_client_requests_total Total unary gRPC calls made to the internal events backend.\n")
	_, _ = fmt.Fprint(w, "# TYPE trident_api_grpc_client_requests_total counter\n")
	_, _ = fmt.Fprint(w, "# HELP trident_api_grpc_client_request_duration_seconds gRPC client call latency in seconds.\n")
	_, _ = fmt.Fprint(w, "# TYPE trident_api_grpc_client_request_duration_seconds histogram\n")

	grpcMetricsMu.Lock()
	defer grpcMetricsMu.Unlock()

	for key, series := range grpcMetricsData {
		for i, bound := range grpcLatencyBuckets {
			_, _ = fmt.Fprintf(w, "trident_api_grpc_client_request_duration_seconds_bucket{method=%q,code=%q,le=%q} %d\n",
				key.method, key.code, formatBucketBound(bound), series.bucketCounts[i])
		}
		_, _ = fmt.Fprintf(w, "trident_api_grpc_client_request_duration_seconds_bucket{method=%q,code=%q,le=\"+Inf\"} %d\n",
			key.method, key.code, series.count)
		_, _ = fmt.Fprintf(w, "trident_api_grpc_client_request_duration_seconds_sum{method=%q,code=%q} %g\n",
			key.method, key.code, series.sum)
		_, _ = fmt.Fprintf(w, "trident_api_grpc_client_request_duration_seconds_count{method=%q,code=%q} %d\n",
			key.method, key.code, series.count)
		_, _ = fmt.Fprintf(w, "trident_api_grpc_client_requests_total{method=%q,code=%q} %d\n",
			key.method, key.code, series.count)
	}
}
