package trident

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestQueryEvents(t *testing.T) {
	mockResponse := PaginatedEvents{
		Events: []*SorobanEvent{
			{
				ID:             "550e8400-e29b-41d4-a716-446655440000",
				ContractID:     "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD2KM",
				LedgerSequence: 50000,
				EventType:      "contract",
				Data:           `{"amount":"100"}`,
			},
		},
		HasMore:    true,
		NextCursor: "next-page-cursor",
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/events" {
			t.Errorf("expected path /v1/events, got %s", r.URL.Path)
		}
		if r.Header.Get("X-API-Key") != "test-key" {
			t.Errorf("expected X-API-Key test-key, got %s", r.Header.Get("X-API-Key"))
		}
		if r.URL.Query().Get("contractId") != "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD2KM" {
			t.Errorf("expected contractId query param, got %s", r.URL.Query().Get("contractId"))
		}
		if r.URL.Query().Get("limit") != "50" {
			t.Errorf("expected limit query param 50, got %s", r.URL.Query().Get("limit"))
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(mockResponse)
	}))
	defer server.Close()

	client := NewClient(TridentClientConfig{
		BaseURL: server.URL,
		APIKey:  "test-key",
	})

	limit := 50
	res, err := client.QueryEvents(context.Background(), QueryEventsParams{
		ContractID: "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD2KM",
		Limit:      limit,
	})
	if err != nil {
		t.Fatalf("QueryEvents failed: %v", err)
	}

	if len(res.Events) != 1 || res.Events[0].ID != mockResponse.Events[0].ID {
		t.Errorf("expected event %s, got %v", mockResponse.Events[0].ID, res.Events)
	}
	if !res.HasMore || res.NextCursor != "next-page-cursor" {
		t.Errorf("unexpected pagination metadata: %+v", res)
	}
}

func TestGetEventByID(t *testing.T) {
	mockResponse := struct {
		Event *SorobanEvent `json:"event"`
	}{
		Event: &SorobanEvent{
			ID:             "550e8400-e29b-41d4-a716-446655440000",
			ContractID:     "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD2KM",
			LedgerSequence: 50000,
			EventType:      "contract",
			Data:           `{"amount":"100"}`,
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		expectedPath := "/v1/events/550e8400-e29b-41d4-a716-446655440000"
		if r.URL.Path != expectedPath {
			t.Errorf("expected path %s, got %s", expectedPath, r.URL.Path)
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(mockResponse)
	}))
	defer server.Close()

	client := NewClient(TridentClientConfig{
		BaseURL: server.URL,
		APIKey:  "test-key",
	})

	res, err := client.GetEventByID(context.Background(), "550e8400-e29b-41d4-a716-446655440000")
	if err != nil {
		t.Fatalf("GetEventByID failed: %v", err)
	}

	if res.ID != mockResponse.Event.ID || res.ContractID != mockResponse.Event.ContractID {
		t.Errorf("unexpected event returned: %+v", res)
	}
}

func TestAllEvents_FollowsCursorAcrossPages(t *testing.T) {
	pages := [][]*SorobanEvent{
		{{ID: "a"}, {ID: "b"}},
		{{ID: "c"}},
	}
	var requestedCursors []string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cursor := r.URL.Query().Get("cursor")
		requestedCursors = append(requestedCursors, cursor)

		pageIdx := len(requestedCursors) - 1
		if pageIdx >= len(pages) {
			t.Errorf("unexpected extra page request (cursor=%q)", cursor)
			return
		}

		resp := PaginatedEvents{
			Events:  pages[pageIdx],
			HasMore: pageIdx < len(pages)-1,
		}
		if resp.HasMore {
			resp.NextCursor = "cursor-1"
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewClient(TridentClientConfig{BaseURL: server.URL})

	var gotIDs []string
	for ev, err := range client.AllEvents(context.Background(), QueryEventsParams{}) {
		if err != nil {
			t.Fatalf("AllEvents failed: %v", err)
		}
		gotIDs = append(gotIDs, ev.ID)
	}

	want := []string{"a", "b", "c"}
	if len(gotIDs) != len(want) {
		t.Fatalf("expected %d events across pages, got %v", len(want), gotIDs)
	}
	for i, id := range want {
		if gotIDs[i] != id {
			t.Errorf("event %d: want %s, got %s", i, id, gotIDs[i])
		}
	}
	if len(requestedCursors) != 2 || requestedCursors[1] != "cursor-1" {
		t.Errorf("expected second page requested with cursor-1, got %v", requestedCursors)
	}
}

func TestAllEvents_StopsAndYieldsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":{"code":"INTERNAL","message":"boom"}}`))
	}))
	defer server.Close()

	client := NewClient(TridentClientConfig{BaseURL: server.URL, RetryDisabled: true})

	var sawErr error
	for _, err := range client.AllEvents(context.Background(), QueryEventsParams{}) {
		sawErr = err
		break
	}
	if sawErr == nil {
		t.Fatal("expected AllEvents to yield an error")
	}
}

func TestBatchGetEvents(t *testing.T) {
	tests := []struct {
		name       string
		ids        []string
		statusCode int
		respBody   string
		wantEvents int
		wantMiss   int
		wantErr    bool
	}{
		{
			name:       "mixed found and missing",
			ids:        []string{"id-1", "id-2"},
			statusCode: http.StatusOK,
			respBody:   `{"events":[{"id":"id-1","contract_id":"C1"}],"missing":["id-2"]}`,
			wantEvents: 1,
			wantMiss:   1,
		},
		{
			name:       "empty ids short-circuits without a request",
			ids:        nil,
			wantEvents: 0,
			wantMiss:   0,
		},
		{
			name:    "too many ids is a client-side error",
			ids:     make([]string, batchEventsMaxIDs+1),
			wantErr: true,
		},
		{
			name:       "server error surfaces as TridentApiError",
			ids:        []string{"id-1"},
			statusCode: http.StatusBadRequest,
			respBody:   `{"error":{"code":"INVALID_ARGUMENT","message":"bad id"}}`,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var requested bool
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requested = true
				if r.URL.Path != "/v1/events/batch" {
					t.Errorf("expected path /v1/events/batch, got %s", r.URL.Path)
				}
				if r.Method != http.MethodPost {
					t.Errorf("expected POST, got %s", r.Method)
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tt.statusCode)
				_, _ = w.Write([]byte(tt.respBody))
			}))
			defer server.Close()

			client := NewClient(TridentClientConfig{BaseURL: server.URL, RetryDisabled: true})

			res, err := client.BatchGetEvents(context.Background(), tt.ids)

			if tt.wantErr {
				if err == nil {
					t.Fatal("expected an error")
				}
				return
			}
			if err != nil {
				t.Fatalf("BatchGetEvents failed: %v", err)
			}
			if len(res.Events) != tt.wantEvents {
				t.Errorf("expected %d events, got %d", tt.wantEvents, len(res.Events))
			}
			if len(res.Missing) != tt.wantMiss {
				t.Errorf("expected %d missing, got %d", tt.wantMiss, len(res.Missing))
			}
			if len(tt.ids) == 0 && requested {
				t.Error("expected no HTTP request for an empty id list")
			}
		})
	}
}

func TestGetIndexerStats(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/stats/indexer" {
			t.Errorf("expected path /v1/stats/indexer, got %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"status": "healthy",
			"network": "testnet",
			"last_ledger_indexed": 1000,
			"chain_tip_ledger": 1002,
			"lag_ledgers": 2,
			"lag_seconds_estimated": 10.0,
			"events_indexed_total": 500
		}`))
	}))
	defer server.Close()

	client := NewClient(TridentClientConfig{BaseURL: server.URL})

	stats, err := client.GetIndexerStats(context.Background())
	if err != nil {
		t.Fatalf("GetIndexerStats failed: %v", err)
	}
	if stats.Status != "healthy" || stats.Network != "testnet" {
		t.Errorf("unexpected stats: %+v", stats)
	}
	if stats.LagLedgers == nil || *stats.LagLedgers != 2 {
		t.Errorf("expected lag_ledgers 2, got %v", stats.LagLedgers)
	}
	if stats.LagSecondsEstimated == nil || *stats.LagSecondsEstimated != 10.0 {
		t.Errorf("expected lag_seconds_estimated 10.0, got %v", stats.LagSecondsEstimated)
	}
}

// writeSSEEvent writes one id/data SSE frame for the flat stream wire format
// (crates/indexer/src/redis_stream/mod.rs), flushing immediately so the
// client observes it without buffering delay.
func writeSSEEvent(w http.ResponseWriter, flusher http.Flusher, id string, wire streamEventWire) {
	payload, _ := json.Marshal(wire)
	fmt.Fprintf(w, "id: %s\ndata: %s\n\n", id, payload)
	flusher.Flush()
}

func TestSubscribeToContract_ReceivesAndDecodesEvent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/events/stream" {
			t.Errorf("expected path /v1/events/stream, got %s", r.URL.Path)
		}
		if r.URL.Query().Get("contractId") != "C123" {
			t.Errorf("expected contractId C123, got %s", r.URL.Query().Get("contractId"))
		}

		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Error("ResponseWriter does not support flushing")
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)

		writeSSEEvent(w, flusher, "1-0", streamEventWire{
			ContractID:      "C123",
			LedgerSequence:  "42",
			LedgerTimestamp: "2026-01-01T00:00:00Z",
			TransactionHash: "deadbeef",
			EventIndex:      "0",
			EventType:       "contract",
			Topics:          `["topic-a"]`,
			Data:            `{"foo":"bar"}`,
			EventID:         "test-uuid",
		})

		// Keep the connection open briefly so the client has time to read
		// before the handler returns and closes the body.
		<-r.Context().Done()
	}))
	defer server.Close()

	client := NewClient(TridentClientConfig{
		BaseURL: server.URL,
		APIKey:  "test-key",
	})

	sub, err := client.SubscribeToContract(context.Background(), SubscribeToContractParams{
		ContractID: "C123",
	})
	if err != nil {
		t.Fatalf("SubscribeToContract failed: %v", err)
	}
	defer sub.Unsubscribe()

	select {
	case ev := <-sub.Events:
		if ev.ID != "test-uuid" || ev.ContractID != "C123" {
			t.Errorf("unexpected event: %+v", ev)
		}
		if ev.LedgerSequence != 42 || ev.EventIndex != 0 {
			t.Errorf("unexpected numeric fields: %+v", ev)
		}
		if len(ev.Topics) != 1 || ev.Topics[0] != "topic-a" {
			t.Errorf("expected decoded topics [topic-a], got %v", ev.Topics)
		}
	case err := <-sub.Errors:
		t.Errorf("received subscription error: %v", err)
	case <-time.After(2 * time.Second):
		t.Error("timeout waiting for live event")
	}
}

func TestSubscribeToContract_ResumesWithLastEventID(t *testing.T) {
	var (
		mu                   sync.Mutex
		receivedLastEventIDs []string
		connCount            int32
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The append must happen-before the atomic increment below so that a
		// reader who observes connCount via atomic.LoadInt32 is guaranteed
		// (per the Go memory model's transitivity across sync primitives) to
		// also see this entry once it takes the mutex afterward.
		mu.Lock()
		receivedLastEventIDs = append(receivedLastEventIDs, r.Header.Get("Last-Event-ID"))
		mu.Unlock()
		n := atomic.AddInt32(&connCount, 1)

		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Error("ResponseWriter does not support flushing")
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)

		if n == 1 {
			// First connection: emit one event carrying id "5-0", then close
			// the connection to force a reconnect.
			writeSSEEvent(w, flusher, "5-0", streamEventWire{
				ContractID: "C123", LedgerSequence: "1", EventIndex: "0",
				EventType: "contract", Topics: "[]", Data: "{}", EventID: "evt-1",
			})
			return
		}

		// Second connection: block until the test unsubscribes.
		<-r.Context().Done()
	}))
	defer server.Close()

	client := NewClient(TridentClientConfig{BaseURL: server.URL})

	sub, err := client.SubscribeToContract(context.Background(), SubscribeToContractParams{
		ContractID: "C123",
	})
	if err != nil {
		t.Fatalf("SubscribeToContract failed: %v", err)
	}
	defer sub.Unsubscribe()

	select {
	case ev := <-sub.Events:
		if ev.ID != "evt-1" {
			t.Errorf("unexpected event: %+v", ev)
		}
	case err := <-sub.Errors:
		t.Fatalf("received subscription error: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for first event")
	}

	// Wait for the reconnect to land.
	deadline := time.After(3 * time.Second)
	for atomic.LoadInt32(&connCount) < 2 {
		select {
		case <-deadline:
			t.Fatal("timeout waiting for reconnect")
		case <-time.After(20 * time.Millisecond):
		}
	}

	mu.Lock()
	ids := append([]string(nil), receivedLastEventIDs...)
	mu.Unlock()

	if len(ids) < 2 {
		t.Fatalf("expected at least 2 connections, got %d", len(ids))
	}
	if ids[0] != "" {
		t.Errorf("expected no Last-Event-ID on first connection, got %q", ids[0])
	}
	if ids[1] != "5-0" {
		t.Errorf("expected Last-Event-ID 5-0 on reconnect, got %q", ids[1])
	}
}
