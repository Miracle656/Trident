package handlers_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Depo-dev/trident/services/api/handlers"
	"github.com/Depo-dev/trident/services/api/middleware"
	"github.com/redis/go-redis/v9"
)

// This file covers issue #428: proving SSE resume via Last-Event-ID is
// correct across a mid-stream disconnect, a gap beyond the Redis retention
// window, a full API-process restart, and a reconnect storm of many
// concurrent clients. All tests here require a real Redis (Last-Event-ID
// resume depends on real XREAD/XREVRANGE semantics that miniredis does not
// faithfully reproduce for stream trimming), so they share the same
// TEST_REDIS_URL skip gate as stream_integration_test.go.

func mustTestRedis(t *testing.T) *redis.Client {
	t.Helper()

	redisURL := os.Getenv("TEST_REDIS_URL")
	if redisURL == "" {
		// A skip here is only legitimate on a developer machine with no
		// Redis. In CI the service is declared, so an unset URL means the
		// job is misconfigured -- fail loudly rather than reporting green
		// on tests that never ran (issue #428's coverage skipped this way).
		if os.Getenv("REQUIRE_TEST_SERVICES") != "" {
			t.Fatal("TEST_REDIS_URL must be set when REQUIRE_TEST_SERVICES is set")
		}
		t.Skip("TEST_REDIS_URL is not set")
	}
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		t.Fatalf("parse TEST_REDIS_URL: %v", err)
	}
	rdb := redis.NewClient(opts)
	t.Cleanup(func() { _ = rdb.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatalf("ping Redis: %v", err)
	}
	return rdb
}

// newStreamTestServer starts an httptest.Server serving Stream(rdb) behind
// the same API key middleware used in production, returning the server and
// the api key to authenticate with.
func newStreamTestServer(t *testing.T, rdb *redis.Client) (*httptest.Server, string) {
	t.Helper()

	const (
		salt   = "reconnect-salt"
		apiKey = "reconnect-key"
	)
	t.Setenv("API_KEY_SALT", salt)
	t.Setenv("API_KEY_HASHES", integrationHashKey(salt, apiKey))

	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/events/stream", handlers.Stream(rdb))
	server := httptest.NewServer(middleware.APIKey(mux))
	t.Cleanup(server.Close)
	return server, apiKey
}

// sseEvent is one parsed "id: ...\ndata: ...\n\n" frame (or a bare event:
// frame such as the `gap` signal, in which case ID and Data are empty and
// Event holds the event name).
type sseEvent struct {
	Event string
	ID    string
	Data  map[string]any
}

// readSSEFrame reads one full SSE frame (up to the blank line) and parses
// its id/event/data fields, tolerating fields arriving in any order.
func readSSEFrame(t *testing.T, scanner *bufio.Scanner) sseEvent {
	t.Helper()

	var out sseEvent
	sawAnyField := false
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			if sawAnyField {
				return out
			}
			continue
		}
		sawAnyField = true
		switch {
		case strings.HasPrefix(line, "id: "):
			out.ID = strings.TrimPrefix(line, "id: ")
		case strings.HasPrefix(line, "event: "):
			out.Event = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "data: "):
			raw := strings.TrimPrefix(line, "data: ")
			var event map[string]any
			if err := json.Unmarshal([]byte(raw), &event); err == nil {
				out.Data = event
			}
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("read SSE frame: %v", err)
	}
	if !sawAnyField {
		t.Fatalf("SSE stream ended without another frame")
	}
	return out
}

func TestStream_ReconnectAfterMidStreamDisconnect(t *testing.T) {
	rdb := mustTestRedis(t)
	server, apiKey := newStreamTestServer(t, rdb)

	contractID := newTestContractID("reconnect")

	// Phase 1: connect, receive one event, then disconnect (simulating a
	// dropped client) without reading further.
	resp1, stop1 := connectSSE(t, server.URL, apiKey, contractID, "")
	publishStreamEvent(t, rdb, contractID, "t0", "event-1")
	frame1 := readSSEFrame(t, bufio.NewScanner(resp1.Body))
	if got := stringField(frame1.Data, "data"); got != "event-1" {
		t.Fatalf("event-1: got data %q", got)
	}
	lastEventID := frame1.ID
	if lastEventID == "" {
		t.Fatalf("event-1: missing id: field")
	}
	stop1() // disconnect mid-stream

	// While disconnected, more events are published that the client must
	// not miss on reconnect.
	publishStreamEvent(t, rdb, contractID, "t1", "event-2")
	publishStreamEvent(t, rdb, contractID, "t2", "event-3")

	// Phase 2: reconnect with Last-Event-ID set to the last id we saw.
	resp2, stop2 := connectSSEWithLastEventID(t, server.URL, apiKey, contractID, "", lastEventID)
	defer stop2()

	scanner := bufio.NewScanner(resp2.Body)
	want := []string{"event-2", "event-3"}
	seen := map[string]bool{}
	for _, w := range want {
		frame := readSSEFrame(t, scanner)
		if frame.Event == "gap" {
			t.Fatalf("unexpected gap event on resume from a fresh id: %+v", frame)
		}
		data := stringField(frame.Data, "data")
		if seen[data] {
			t.Fatalf("duplicate event received: %q", data)
		}
		seen[data] = true
		if data != w {
			t.Fatalf("resumed event: got %q, want %q (no missed/duplicated events)", data, w)
		}
	}
	if len(seen) != len(want) {
		t.Fatalf("got %d distinct events, want %d", len(seen), len(want))
	}
}

func TestStream_ResumeBeyondRetentionWindowEmitsGap(t *testing.T) {
	rdb := mustTestRedis(t)
	server, apiKey := newStreamTestServer(t, rdb)

	contractID := newTestContractID("gap")

	// Publish an event and capture its id — this id will be pushed outside
	// the retention window by the MAXLEN trim below.
	staleID := publishStreamEventGetID(t, rdb, contractID, "t0", "stale-event")

	// Force retention to a single entry so the stale id above is evicted:
	// MAXLEN with Approx:false trims exactly, deterministically simulating
	// "disconnected longer than the retention window" without needing to
	// wait out a real TTL.
	survivorData := "survivor-event"
	if err := rdb.XAdd(context.Background(), &redis.XAddArgs{
		Stream:     "trident:events",
		MaxLen:     1,
		Approx:     false,
		NoMkStream: false,
		Values: map[string]any{
			"contract_id":      contractID,
			"ledger_sequence":  "1",
			"ledger_timestamp": "2026-06-25T00:00:00Z",
			"transaction_hash": "txhash",
			"event_index":      "0",
			"event_type":       "contract",
			"topics":           `["t1"]`,
			"data":             survivorData,
		},
	}).Err(); err != nil {
		t.Fatalf("trim stream via MAXLEN: %v", err)
	}

	// Sanity check: the stale id must actually be gone now.
	if msgs, err := rdb.XRevRangeN(context.Background(), "trident:events", staleID, staleID, 1).Result(); err != nil || len(msgs) != 0 {
		t.Fatalf("expected staleID %q to be trimmed, got msgs=%v err=%v", staleID, msgs, err)
	}

	resp, stop := connectSSEWithLastEventID(t, server.URL, apiKey, contractID, "", staleID)
	defer stop()

	// The SSE headers must be present even on the gap path — regression
	// coverage for the response headers only being set correctly when no
	// bytes had already been written ahead of them.
	if got := resp.Header.Get("Content-Type"); !strings.HasPrefix(got, "text/event-stream") {
		t.Fatalf("Content-Type on gap path: got %q, want text/event-stream", got)
	}

	scanner := bufio.NewScanner(resp.Body)
	gapFrame := readSSEFrame(t, scanner)
	if gapFrame.Event != "gap" {
		t.Fatalf("first frame after resuming from an evicted id: got event %q, want gap: %+v", gapFrame.Event, gapFrame)
	}

	// After the gap notice, the client must resume from the oldest
	// available entry going forward — the survivor event published above
	// should still be delivered if a matching subsequent read occurs, and
	// any newly published event must arrive without loss.
	publishStreamEvent(t, rdb, contractID, "t2", "after-gap-event")
	frame := readSSEFrame(t, scanner)
	if got := stringField(frame.Data, "data"); got != "after-gap-event" {
		t.Fatalf("event after gap: got %q, want after-gap-event", got)
	}
}

// TestStream_APIRestartMidStream simulates an API process restart: the
// httptest.Server (standing in for the API process) is torn down mid-stream
// and a brand new one is created against the same Redis backing store,
// exactly as a real restart would leave the durable stream data intact
// while dropping all in-memory connection state. The client must resume
// with no gap and no duplication using the Last-Event-ID it already holds.
func TestStream_APIRestartMidStream(t *testing.T) {
	rdb := mustTestRedis(t)
	contractID := newTestContractID("restart")

	server1, apiKey := newStreamTestServer(t, rdb)
	resp1, stop1 := connectSSE(t, server1.URL, apiKey, contractID, "")
	publishStreamEvent(t, rdb, contractID, "t0", "before-restart")
	frame1 := readSSEFrame(t, bufio.NewScanner(resp1.Body))
	if got := stringField(frame1.Data, "data"); got != "before-restart" {
		t.Fatalf("before-restart event: got %q", got)
	}
	lastEventID := frame1.ID
	stop1()

	// "Restart": tear down the old server (process gone) and publish while
	// nothing is listening, then bring up a brand new server/handler bound
	// to the same Redis — the durable side of the system.
	server1.Close()
	publishStreamEvent(t, rdb, contractID, "t1", "during-restart")

	server2, apiKey2 := newStreamTestServer(t, rdb)
	_ = server2

	resp2, stop2 := connectSSEWithLastEventID(t, server2.URL, apiKey2, contractID, "", lastEventID)
	defer stop2()

	frame2 := readSSEFrame(t, bufio.NewScanner(resp2.Body))
	if frame2.Event == "gap" {
		t.Fatalf("unexpected gap after a short restart-sized outage: %+v", frame2)
	}
	if got := stringField(frame2.Data, "data"); got != "during-restart" {
		t.Fatalf("event delivered after restart: got %q, want during-restart (no missed events across restart)", got)
	}

	// Confirm forward delivery keeps working post-restart too.
	publishStreamEvent(t, rdb, contractID, "t2", "after-restart")
	frame3 := readSSEFrame(t, bufio.NewScanner(resp2.Body))
	if got := stringField(frame3.Data, "data"); got != "after-restart" {
		t.Fatalf("event after restart reconnect: got %q, want after-restart", got)
	}
}

// TestStream_ReconnectStormNoEventLoss is the reconnect-storm load test
// required by issue #428: many simulated clients disconnect and reconnect
// with valid Last-Event-IDs at roughly the same time, and every one of them
// must observe its own full, gapless, duplicate-free sequence of events.
func TestStream_ReconnectStormNoEventLoss(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping reconnect storm under -short")
	}

	rdb := mustTestRedis(t)
	server, apiKey := newStreamTestServer(t, rdb)

	const (
		numClients        = 50
		eventsBeforeDrop  = 2
		eventsAfterResume = 3
	)

	type client struct {
		contractID  string
		lastEventID string
	}

	clients := make([]client, numClients)
	for i := range clients {
		clients[i].contractID = newTestContractID(fmt.Sprintf("storm-%d", i))
	}

	// Phase 1: every client connects and receives its first eventsBeforeDrop
	// events, then disconnects — all roughly concurrently.
	var wg sync.WaitGroup
	for i := range clients {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := &clients[i]
			resp, stop := connectSSE(t, server.URL, apiKey, c.contractID, "")
			defer stop()

			for j := 0; j < eventsBeforeDrop; j++ {
				publishStreamEvent(t, rdb, c.contractID, "t0", fmt.Sprintf("e%d", j))
			}
			scanner := bufio.NewScanner(resp.Body)
			for j := 0; j < eventsBeforeDrop; j++ {
				frame := readSSEFrame(t, scanner)
				want := fmt.Sprintf("e%d", j)
				if got := stringField(frame.Data, "data"); got != want {
					t.Errorf("client %s pre-drop event %d: got %q, want %q", c.contractID, j, got, want)
				}
				c.lastEventID = frame.ID
			}
		}()
	}
	wg.Wait()

	// While every client is disconnected, publish more events per client
	// that must all be delivered on reconnect.
	for i := range clients {
		c := &clients[i]
		for j := 0; j < eventsAfterResume; j++ {
			publishStreamEvent(t, rdb, c.contractID, "t1", fmt.Sprintf("resumed-%d", j))
		}
	}

	// Phase 2: reconnect storm — all clients reconnect with their
	// Last-Event-ID simultaneously and must each see exactly their
	// eventsAfterResume events, in order, no gaps, no duplicates.
	var mu sync.Mutex
	var failures []string
	for i := range clients {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := clients[i]
			resp, stop := connectSSEWithLastEventID(t, server.URL, apiKey, c.contractID, "", c.lastEventID)
			defer stop()

			scanner := bufio.NewScanner(resp.Body)
			seen := map[string]bool{}
			for j := 0; j < eventsAfterResume; j++ {
				frame := readSSEFrame(t, scanner)
				if frame.Event == "gap" {
					mu.Lock()
					failures = append(failures, fmt.Sprintf("client %s: unexpected gap event", c.contractID))
					mu.Unlock()
					return
				}
				data := stringField(frame.Data, "data")
				want := fmt.Sprintf("resumed-%d", j)
				if seen[data] {
					mu.Lock()
					failures = append(failures, fmt.Sprintf("client %s: duplicate event %q", c.contractID, data))
					mu.Unlock()
					return
				}
				seen[data] = true
				if data != want {
					mu.Lock()
					failures = append(failures, fmt.Sprintf("client %s: got %q, want %q (event loss or reorder)", c.contractID, data, want))
					mu.Unlock()
					return
				}
			}
		}()
	}
	wg.Wait()

	if len(failures) > 0 {
		t.Fatalf("reconnect storm produced %d/%d client failures:\n%s", len(failures), numClients, strings.Join(failures, "\n"))
	}
}

// connectSSEWithLastEventID is connectSSE plus a Last-Event-ID header, for
// exercising the resume path.
func connectSSEWithLastEventID(t *testing.T, baseURL, apiKey, contractID, topic0, lastEventID string) (*http.Response, context.CancelFunc) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	url := baseURL + "/v1/events/stream?contractId=" + contractID
	if topic0 != "" {
		url += "&topic0=" + topic0
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		cancel()
		t.Fatalf("create SSE request: %v", err)
	}
	req.Header.Set("X-API-Key", apiKey)
	if lastEventID != "" {
		req.Header.Set("Last-Event-ID", lastEventID)
	}

	response, err := http.DefaultClient.Do(req)
	if err != nil {
		cancel()
		t.Fatalf("connect SSE: %v", err)
	}
	if response.StatusCode != http.StatusOK {
		_ = response.Body.Close()
		cancel()
		t.Fatalf("SSE status: got %d, want %d", response.StatusCode, http.StatusOK)
	}

	return response, func() {
		cancel()
		_ = response.Body.Close()
	}
}

// publishStreamEventGetID is publishStreamEvent but returns the assigned
// stream entry id, needed by tests that must reference a specific id later
// (e.g. to prove it was trimmed).
func publishStreamEventGetID(t *testing.T, rdb *redis.Client, contractID, topic0, data string) string {
	t.Helper()

	id, err := rdb.XAdd(context.Background(), &redis.XAddArgs{
		Stream: "trident:events",
		Values: map[string]any{
			"contract_id":      contractID,
			"ledger_sequence":  "42",
			"ledger_timestamp": "2026-06-25T00:00:00Z",
			"transaction_hash": "txhash",
			"event_index":      "0",
			"event_type":       "contract",
			"topics":           fmt.Sprintf("[%q]", topic0),
			"data":             data,
		},
	}).Result()
	if err != nil {
		t.Fatalf("publish event: %v", err)
	}
	return id
}
