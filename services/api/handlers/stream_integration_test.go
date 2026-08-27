package handlers_test

import (
	"bufio"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base32"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Depo-dev/trident/services/api/handlers"
	"github.com/Depo-dev/trident/services/api/middleware"
	"github.com/redis/go-redis/v9"
)

func TestStreamIntegration(t *testing.T) {
	redisURL := os.Getenv("TEST_REDIS_URL")
	if redisURL == "" {
		t.Skip("TEST_REDIS_URL is not set")
	}

	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		t.Fatalf("parse TEST_REDIS_URL: %v", err)
	}
	rdb := redis.NewClient(opts)
	defer func() { _ = rdb.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatalf("ping Redis: %v", err)
	}

	const (
		salt   = "integration-salt"
		apiKey = "integration-key"
	)
	t.Setenv("API_KEY_SALT", salt)
	t.Setenv("API_KEY_HASHES", integrationHashKey(salt, apiKey))

	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/events/stream", handlers.Stream(rdb))
	server := httptest.NewServer(middleware.APIKey(mux))
	defer server.Close()

	t.Run("forwards three events with SSE headers", func(t *testing.T) {
		contractID := newTestContractID("integration")
		response, stop := connectSSE(t, server.URL, apiKey, contractID, "")
		defer stop()

		if got := response.Header.Get("Content-Type"); !strings.HasPrefix(got, "text/event-stream") {
			t.Fatalf("Content-Type: got %q", got)
		}
		if got := response.Header.Get("Cache-Control"); got != "no-cache" {
			t.Fatalf("Cache-Control: got %q", got)
		}
		if got := response.Header.Get("X-Accel-Buffering"); got != "no" {
			t.Fatalf("X-Accel-Buffering: got %q", got)
		}

		wantData := []string{"payload-1", "payload-2", "payload-3"}
		for i, data := range wantData {
			publishStreamEvent(t, rdb, contractID, fmt.Sprintf("topic-%d", i), data)
		}

		scanner := bufio.NewScanner(response.Body)
		for i, want := range wantData {
			event := readSSEEvent(t, scanner)
			if got := stringField(event, "contract_id"); got != contractID {
				t.Fatalf("event %d contract_id: got %q, want %q", i, got, contractID)
			}
			if got := stringField(event, "data"); got != want {
				t.Fatalf("event %d data: got %q, want %q", i, got, want)
			}
		}
	})

	t.Run("filters topic0", func(t *testing.T) {
		contractID := newTestContractID("filter")
		response, stop := connectSSE(t, server.URL, apiKey, contractID, "wanted")
		defer stop()

		publishStreamEvent(t, rdb, contractID, "ignored", "wrong")
		publishStreamEvent(t, rdb, contractID, "wanted", "right")

		event := readSSEEvent(t, bufio.NewScanner(response.Body))
		if got := stringField(event, "data"); got != "right" {
			t.Fatalf("filtered event data: got %q, want right", got)
		}
	})
}

func connectSSE(t *testing.T, baseURL, apiKey, contractID, topic0 string) (*http.Response, context.CancelFunc) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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

func publishStreamEvent(t *testing.T, rdb *redis.Client, contractID, topic0, data string) {
	t.Helper()

	err := rdb.XAdd(context.Background(), &redis.XAddArgs{
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
	}).Err()
	if err != nil {
		t.Fatalf("publish event: %v", err)
	}
}

func readSSEEvent(t *testing.T, scanner *bufio.Scanner) map[string]any {
	t.Helper()

	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}

		var event map[string]any
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &event); err != nil {
			t.Fatalf("decode SSE data: %v", err)
		}
		return event
	}

	t.Fatalf("SSE stream ended: %v", scanner.Err())
	return nil
}

func stringField(event map[string]any, field string) string {
	value, _ := event[field].(string)
	return value
}

// testContractID builds a unique value that satisfies validation's Stellar
// strkey shape check (`^C[A-Z2-7]{55}$`) — a plain "C-<label>-<nanos>" string
// used to fail that validation with a 400 before a real contract id was
// required here. label lets tests keep distinguishable ids in logs; the
// current timestamp keeps concurrent subtests/goroutines from colliding on
// the same stream key.
func newTestContractID(label string) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s-%d", label, time.Now().UnixNano())))
	body := strings.TrimRight(base32.StdEncoding.EncodeToString(sum[:]), "=")
	for len(body) < 55 {
		body += "A"
	}
	return "C" + body[:55]
}

func integrationHashKey(salt, key string) string {
	mac := hmac.New(sha256.New, []byte(salt))
	_, _ = mac.Write([]byte(key))
	return hex.EncodeToString(mac.Sum(nil))
}
