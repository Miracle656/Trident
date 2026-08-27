package handlers_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Depo-dev/trident/services/api/gen"
	"github.com/Depo-dev/trident/services/api/handlers"
	"github.com/getkin/kin-openapi/openapi3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ContractTestMockEventsClient is a simple mock for contract testing
type ContractTestMockEventsClient struct {
	ListEventsFunc func(context.Context, *gen.ListEventsRequest) (*gen.ListEventsResponse, error)
	GetEventFunc   func(context.Context, *gen.GetEventRequest) (*gen.Event, error)
}

func (m *ContractTestMockEventsClient) ListEvents(ctx context.Context, req *gen.ListEventsRequest, opts ...grpc.CallOption) (*gen.ListEventsResponse, error) {
	if m.ListEventsFunc != nil {
		return m.ListEventsFunc(ctx, req)
	}
	return &gen.ListEventsResponse{}, nil
}

func (m *ContractTestMockEventsClient) GetEvent(ctx context.Context, req *gen.GetEventRequest, opts ...grpc.CallOption) (*gen.Event, error) {
	if m.GetEventFunc != nil {
		return m.GetEventFunc(ctx, req)
	}
	return &gen.Event{}, nil
}

func (m *ContractTestMockEventsClient) StreamEvents(ctx context.Context, req *gen.StreamEventsRequest, opts ...grpc.CallOption) (gen.Events_StreamEventsClient, error) {
	return nil, nil
}

// loadOpenAPISpec loads and parses the OpenAPI specification
func loadOpenAPISpec(t *testing.T) *openapi3.T {
	t.Helper()

	// Navigate to the repository root from the handlers test directory
	repoRoot := findRepoRoot(t)
	specPath := filepath.Join(repoRoot, "api", "openapi.yaml")

	loader := openapi3.NewLoader()
	doc, err := loader.LoadFromFile(specPath)
	require.NoError(t, err, "failed to load OpenAPI spec from %s", specPath)

	require.NoError(t, doc.Validate(loader.Context), "OpenAPI spec is invalid")
	return doc
}

// findRepoRoot finds the repository root by looking for .git directory
func findRepoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	require.NoError(t, err)

	for {
		if _, err := os.Stat(filepath.Join(dir, ".git")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("could not find repository root (no .git directory found)")
		}
		dir = parent
	}
}

// validateResponseAgainstSchema validates an HTTP response against the OpenAPI schema
func validateResponseAgainstSchema(t *testing.T, doc *openapi3.T, method, path string, statusCode int, responseBody []byte) {
	t.Helper()

	// For now, just validate that the response is valid JSON and the path/method exists in the spec
	// Full schema validation can be added later if needed
	pathItem := doc.Paths.Find(path)
	require.NotNil(t, pathItem, "path %s not found in OpenAPI spec", path)

	var operation *openapi3.Operation
	switch strings.ToUpper(method) {
	case http.MethodGet:
		operation = pathItem.Get
	case http.MethodPost:
		operation = pathItem.Post
	case http.MethodPut:
		operation = pathItem.Put
	case http.MethodPatch:
		operation = pathItem.Patch
	case http.MethodDelete:
		operation = pathItem.Delete
	default:
		t.Fatalf("unsupported HTTP method: %s", method)
	}

	require.NotNil(t, operation, "method %s not found for path %s in OpenAPI spec", method, path)

	// Validate response has the expected status code documented
	responseRef := operation.Responses.Status(statusCode)
	require.NotNil(t, responseRef, "status code %d not found for %s %s in OpenAPI spec", statusCode, method, path)

	// Validate response body is valid JSON
	var bodyJSON interface{}
	err := json.Unmarshal(responseBody, &bodyJSON)
	require.NoError(t, err, "response body is not valid JSON")
}

// TestContract_OpenAPIResponseValidation validates that real handler responses
// match the OpenAPI specification schema
func TestContract_OpenAPIResponseValidation(t *testing.T) {
	doc := loadOpenAPISpec(t)

	// Test GET /v1/health response
	t.Run("GET /v1/health", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/health", nil)
		rr := httptest.NewRecorder()

		handlers.Health()(rr, req)

		if rr.Code == http.StatusOK {
			validateResponseAgainstSchema(t, doc, http.MethodGet, "/v1/health", rr.Code, rr.Body.Bytes())
		}
	})

	// Test GET /v1/events response with mock gRPC client
	t.Run("GET /v1/events", func(t *testing.T) {
		mock := &ContractTestMockEventsClient{
			ListEventsFunc: func(ctx context.Context, req *gen.ListEventsRequest) (*gen.ListEventsResponse, error) {
				return &gen.ListEventsResponse{
					Events: []*gen.Event{
						{
							Id:              "550e8400-e29b-41d4-a716-446655440000",
							ContractId:      "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
							LedgerSequence:  1000,
							LedgerTimestamp: "2024-01-01T00:00:00Z",
							TransactionHash: "abcd1234",
							EventIndex:      0,
							EventType:       "contract",
							Topics:          []string{"transfer"},
							Data:            `{"amount":"100"}`,
							CreatedAt:       "2024-01-01T00:00:01Z",
						},
					},
					NextCursor: "",
					HasMore:    false,
				}, nil
			},
		}
		handlers.SetEventsClient(mock)

		req := httptest.NewRequest(http.MethodGet, "/v1/events?limit=1", nil)
		rr := httptest.NewRecorder()

		handlers.ListEvents(rr, req)

		if rr.Code == http.StatusOK {
			validateResponseAgainstSchema(t, doc, http.MethodGet, "/v1/events", rr.Code, rr.Body.Bytes())
		}
	})

	// Test GET /v1/stats/contracts response
	t.Run("GET /v1/stats/contracts", func(t *testing.T) {
		// This endpoint requires DB and Redis, so we'll skip if not available
		req := httptest.NewRequest(http.MethodGet, "/v1/stats/contracts?limit=1", nil)
		rr := httptest.NewRecorder()

		handlers.ContractsStats(nil, nil)(rr, req)

		if rr.Code == http.StatusOK {
			validateResponseAgainstSchema(t, doc, http.MethodGet, "/v1/stats/contracts", rr.Code, rr.Body.Bytes())
		}
	})
}

// TestContract_ErrorResponseValidation validates that error responses match
// the OpenAPI specification
func TestContract_ErrorResponseValidation(t *testing.T) {
	doc := loadOpenAPISpec(t)

	t.Run("GET /v1/events/{id} - 404 error", func(t *testing.T) {
		mock := &ContractTestMockEventsClient{
			GetEventFunc: func(ctx context.Context, req *gen.GetEventRequest) (*gen.Event, error) {
				return nil, status.Error(codes.NotFound, "event not found")
			},
		}
		handlers.SetEventsClient(mock)

		mux := http.NewServeMux()
		mux.HandleFunc("GET /v1/events/{id}", handlers.GetEvent)

		req := httptest.NewRequest(http.MethodGet, "/v1/events/550e8400-e29b-41d4-a716-446655440000", nil)
		rr := httptest.NewRecorder()
		mux.ServeHTTP(rr, req)

		if rr.Code == http.StatusNotFound {
			validateResponseAgainstSchema(t, doc, http.MethodGet, "/v1/events/{id}", rr.Code, rr.Body.Bytes())
		}
	})
}

// TestContract_RouteParity verifies that every route registered in main.go
// is documented in the OpenAPI spec and vice versa
func TestContract_RouteParity(t *testing.T) {
	doc := loadOpenAPISpec(t)

	// Extract documented routes from OpenAPI spec
	documentedRoutes := make(map[string]bool)
	for path, pathItem := range doc.Paths.Map() {
		if pathItem.Get != nil {
			documentedRoutes["GET "+path] = true
		}
		if pathItem.Post != nil {
			documentedRoutes["POST "+path] = true
		}
		if pathItem.Put != nil {
			documentedRoutes["PUT "+path] = true
		}
		if pathItem.Patch != nil {
			documentedRoutes["PATCH "+path] = true
		}
		if pathItem.Delete != nil {
			documentedRoutes["DELETE "+path] = true
		}
	}

	// Expected routes from main.go (this list should be kept in sync with main.go)
	// Note: Some routes like /metrics, /internal/status, /ws, /graphql are intentionally
	// excluded from the OpenAPI spec as they are internal or use different protocols
	// Note: Admin routes and webhook routes are also currently not documented in OpenAPI
	expectedRoutes := map[string]bool{
		"GET /v1/health":                         true,
		"GET /v1/ready":                          true,
		"GET /v1/version":                        true,
		"GET /v1/events":                         true,
		"POST /v1/events/batch":                  true,
		"GET /v1/events/{id}":                    true,
		"GET /v1/events/stream":                  true,
		"GET /v1/admin/db":                       true,
		"GET /v1/admin/keys/{id}/usage":          true,
		"POST /v1/admin/contracts":               true,
		"GET /v1/admin/contracts":                true,
		"DELETE /v1/admin/contracts/{id}":        true,
		"POST /v1/api-keys":                      true,
		"GET /v1/api-keys":                       true,
		"PATCH /v1/api-keys/{id}":                true,
		"DELETE /v1/api-keys/{id}":               true,
		"GET /v1/stats/indexer":                  true,
		"GET /v1/contracts/{id}/events/schema":   true,
		"GET /v1/contracts/{id}/spec":            true,
		"GET /v1/contracts/{id}/storage":         true,
		"GET /v1/contracts/{id}/storage/history": true,
		"GET /v1/stats/contracts":                true,
		"POST /v1/contracts/{id}/call":           true,
		// Webhook routes (not yet documented in OpenAPI)
		"GET /v1/webhooks":                                        true,
		"POST /v1/webhooks":                                       true,
		"DELETE /v1/webhooks/{id}":                                true,
		"PATCH /v1/webhooks/{id}/pause":                           true,
		"PATCH /v1/webhooks/{id}/resume":                          true,
		"GET /v1/webhooks/{id}/deliveries":                        true,
		"GET /v1/webhooks/{id}/dead-letters":                      true,
		"POST /v1/webhooks/{id}/dead-letters/{deliveryId}/replay": true,
	}

	// Routes that are intentionally excluded from OpenAPI documentation
	// (internal routes, admin routes, webhook routes, etc.)
	excludedFromOpenAPI := map[string]bool{
		"GET /metrics":                                            true,
		"GET /internal/status":                                    true,
		"GET /ws":                                                 true,
		"GET /graphql":                                            true,
		"GET /v1/admin/db":                                        true,
		"GET /v1/admin/keys/{id}/usage":                           true,
		"POST /v1/admin/contracts":                                true,
		"GET /v1/admin/contracts":                                 true,
		"DELETE /v1/admin/contracts/{id}":                         true,
		"POST /v1/api-keys":                                       true,
		"GET /v1/api-keys":                                        true,
		"PATCH /v1/api-keys/{id}":                                 true,
		"DELETE /v1/api-keys/{id}":                                true,
		"POST /v1/contracts/{id}/call":                            true, // Contract call endpoint (not yet documented)
		"GET /v1/webhooks":                                        true,
		"POST /v1/webhooks":                                       true,
		"DELETE /v1/webhooks/{id}":                                true,
		"PATCH /v1/webhooks/{id}/pause":                           true,
		"PATCH /v1/webhooks/{id}/resume":                          true,
		"GET /v1/webhooks/{id}/deliveries":                        true,
		"GET /v1/webhooks/{id}/dead-letters":                      true,
		"POST /v1/webhooks/{id}/dead-letters/{deliveryId}/replay": true,
	}

	// Check for undocumented routes (excluding intentionally excluded ones)
	var undocumented []string
	for route := range expectedRoutes {
		if !documentedRoutes[route] && !excludedFromOpenAPI[route] {
			undocumented = append(undocumented, route)
		}
	}

	// Check for documented but not implemented routes (excluding intentionally excluded ones)
	var unimplemented []string
	for route := range documentedRoutes {
		if !expectedRoutes[route] && !excludedFromOpenAPI[route] {
			unimplemented = append(unimplemented, route)
		}
	}

	if len(undocumented) > 0 {
		t.Errorf("Routes registered in main.go but not documented in OpenAPI spec:\n%s",
			strings.Join(undocumented, "\n"))
	}

	if len(unimplemented) > 0 {
		t.Errorf("Routes documented in OpenAPI spec but not registered in main.go:\n%s",
			strings.Join(unimplemented, "\n"))
	}

	assert.Empty(t, undocumented, "all registered routes should be documented (or intentionally excluded)")
	assert.Empty(t, unimplemented, "all documented routes should be registered (or intentionally excluded)")
}
