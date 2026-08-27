package handlers_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/Depo-dev/trident/services/api/handlers"
)

// TestListContracts_ConcurrentInserts_NoDuplicatesNoSkips is the
// acceptance-criteria test for issue #423: it walks GET /v1/admin/contracts
// page by page via its opaque keyset cursor while a background goroutine
// concurrently inserts new rows, and asserts the collected set of ids has no
// duplicates and includes every row that existed before the walk began.
//
// Uses the same TEST_DATABASE_URL / REQUIRE_TEST_SERVICES convention as
// services/api/handlers/usage_rollup_integration_test.go.
func TestListContracts_ConcurrentInserts_NoDuplicatesNoSkips(t *testing.T) {
	pool := connectRealTestDB(t)
	ctx := context.Background()

	run := fmt.Sprintf("%d", time.Now().UnixNano())

	// Seed a known baseline of rows before pagination starts.
	const baselineCount = 25
	baselineIDs := make(map[string]bool, baselineCount)
	for i := 0; i < baselineCount; i++ {
		var id string
		err := pool.QueryRow(ctx,
			`INSERT INTO indexed_contracts (contract_id, network, label, index_from)
			 VALUES ($1, 'testnet', $2, 0)
			 RETURNING id`,
			fmt.Sprintf("pagination-test-contract-%s-baseline-%d", run, i),
			fmt.Sprintf("pagination-baseline-%s-%d", run, i),
		).Scan(&id)
		if err != nil {
			t.Fatalf("seed baseline row %d: %v", i, err)
		}
		baselineIDs[id] = true
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(),
			`DELETE FROM indexed_contracts WHERE label LIKE $1`,
			fmt.Sprintf("pagination-%s-%%", run),
		)
	})

	cfg := handlers.ContractConfig{AdminKey: "test-admin-key-" + run, DB: pool}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/admin/contracts", handlers.ListContracts(cfg))
	server := httptest.NewServer(mux)
	defer server.Close()

	// Background writer: inserts additional rows concurrently with the page
	// walk below, simulating writes landing while a client is mid-pagination.
	const concurrentInserts = 15
	var wg sync.WaitGroup
	wg.Add(1)
	insertedIDs := make(map[string]bool)
	var insertedMu sync.Mutex
	go func() {
		defer wg.Done()
		for i := 0; i < concurrentInserts; i++ {
			var id string
			err := pool.QueryRow(ctx,
				`INSERT INTO indexed_contracts (contract_id, network, label, index_from)
				 VALUES ($1, 'testnet', $2, 0)
				 RETURNING id`,
				fmt.Sprintf("pagination-test-contract-%s-concurrent-%d", run, i),
				fmt.Sprintf("pagination-concurrent-%s-%d", run, i),
			).Scan(&id)
			if err != nil {
				// Best-effort: record failure via t.Errorf from the main
				// goroutine is unsafe here, so just stop early.
				return
			}
			insertedMu.Lock()
			insertedIDs[id] = true
			insertedMu.Unlock()
			time.Sleep(2 * time.Millisecond)
		}
	}()

	// Walk every page with a small page size so the walk spans many requests
	// and overlaps with the concurrent inserts above.
	seen := map[string]int{}
	var order []string
	cursorParam := ""
	const pageSize = 4
	for pages := 0; pages < 500; pages++ {
		url := fmt.Sprintf("%s/v1/admin/contracts?limit=%d", server.URL, pageSize)
		if cursorParam != "" {
			url += "&cursor=" + cursorParam
		}
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			t.Fatalf("build request: %v", err)
		}
		req.Header.Set("X-Admin-Key", cfg.AdminKey)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("GET /v1/admin/contracts: %v", err)
		}
		var body handlers.ListContractsResponse
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			resp.Body.Close()
			t.Fatalf("decode response: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET /v1/admin/contracts: status %d", resp.StatusCode)
		}

		for _, c := range body.Contracts {
			seen[c.ID]++
			order = append(order, c.ID)
		}

		if !body.HasMore {
			break
		}
		if body.NextCursor == nil {
			t.Fatal("has_more is true but next_cursor is nil")
		}
		cursorParam = *body.NextCursor
	}

	wg.Wait()

	// No duplicates: forward keyset pagination on an immutable, strictly
	// increasing key must never return the same row twice across pages.
	for id, count := range seen {
		if count > 1 {
			t.Errorf("row %s was returned %d times across pages (order: %v)", id, count, order)
		}
	}

	// No skips relative to the baseline: every row that existed before the
	// walk began must appear exactly once, regardless of what was inserted
	// concurrently during the walk (see docs/pagination.md).
	for id := range baselineIDs {
		if seen[id] != 1 {
			t.Errorf("baseline row %s was not returned exactly once (got %d)", id, seen[id])
		}
	}
}
