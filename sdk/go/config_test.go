package trident

import (
	"errors"
	"strings"
	"testing"
)

func TestConfigPrecedenceExplicitOverEnv(t *testing.T) {
	t.Setenv(EnvAPIKey, "env-key")
	t.Setenv(EnvBaseURL, "https://env.example.com")

	cfg := TridentClientConfig{
		APIKey:  "explicit-key",
		BaseURL: "https://explicit.example.com",
	}.resolve()

	if cfg.APIKey != "explicit-key" {
		t.Errorf("expected explicit APIKey to win, got %q", cfg.APIKey)
	}
	if cfg.BaseURL != "https://explicit.example.com" {
		t.Errorf("expected explicit BaseURL to win, got %q", cfg.BaseURL)
	}
}

func TestConfigPrecedenceFallsBackToEnv(t *testing.T) {
	t.Setenv(EnvAPIKey, "env-key")
	t.Setenv(EnvBaseURL, "https://env.example.com")

	cfg := TridentClientConfig{}.resolve()

	if cfg.APIKey != "env-key" {
		t.Errorf("expected APIKey from env, got %q", cfg.APIKey)
	}
	if cfg.BaseURL != "https://env.example.com" {
		t.Errorf("expected BaseURL from env, got %q", cfg.BaseURL)
	}
}

func TestConfigMissingAPIKeyReturnsClearError(t *testing.T) {
	t.Setenv(EnvAPIKey, "")
	t.Setenv(EnvBaseURL, "")

	// This asserts the guard itself, not that the request methods call it.
	// The SDK deliberately allows keyless requests — the retry and pagination
	// suites in retry_test.go and client_test.go all drive QueryEvents with no
	// key and expect success, because the server only requires a key on
	// protected routes. requireAPIKey currently has no callers: its one caller
	// was the WebSocket SubscribeToContract that was removed as a duplicate of
	// the SSE implementation in stream.go. Whether the client should refuse
	// keyless calls up front is a real policy question, but it is a behaviour
	// change across every request path, not a test fix.
	cfg := TridentClientConfig{BaseURL: "https://api.example.com"}.resolve()
	if err := cfg.requireAPIKey(); !errors.Is(err, ErrMissingAPIKey) {
		t.Fatalf("expected ErrMissingAPIKey, got %v", err)
	}

	withKey := TridentClientConfig{BaseURL: "https://api.example.com", APIKey: "k"}.resolve()
	if err := withKey.requireAPIKey(); err != nil {
		t.Fatalf("expected no error when a key is configured, got %v", err)
	}
}

func TestConfigRedactsAPIKeyInString(t *testing.T) {
	cfg := TridentClientConfig{
		BaseURL: "https://api.example.com",
		APIKey:  "super-secret-value",
	}

	repr := cfg.String()
	if strings.Contains(repr, "super-secret-value") {
		t.Fatalf("expected APIKey to be redacted, got %q", repr)
	}
	// redactKey keeps the last four characters, matching the TypeScript SDK's
	// redactKey and the <=4 -> "***" rule TestRedactKeyShortKeys asserts.
	if !strings.HasSuffix(strings.TrimSuffix(repr, "}"), "alue") {
		t.Fatalf("expected redacted suffix to be preserved for debugging, got %q", repr)
	}
}

func TestRedactKeyShortKeys(t *testing.T) {
	if got := redactKey(""); got != "<empty>" {
		t.Errorf("expected <empty>, got %q", got)
	}
	if got := redactKey("abc"); got != "***" {
		t.Errorf("expected ***, got %q", got)
	}
}
