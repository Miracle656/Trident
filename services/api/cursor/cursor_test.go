package cursor_test

import (
	"encoding/base64"
	"strings"
	"testing"

	"github.com/Depo-dev/trident/services/api/cursor"
)

func TestRoundTrip(t *testing.T) {
	tokens := []string{
		"0000000000000001",
		"9999999999999999",
		"some-arbitrary-string",
		"ledger:12345678:tx:0:event:3",
	}

	for _, tok := range tokens {
		opaque := cursor.Encode(tok)
		if opaque == "" {
			t.Fatalf("Encode(%q) returned empty string", tok)
		}

		// Opaque cursor must not directly expose the raw token in plain text.
		if strings.Contains(opaque, tok) {
			t.Errorf("Encode(%q) appears to contain raw token in output %q", tok, opaque)
		}

		got, err := cursor.Decode(opaque)
		if err != nil {
			t.Fatalf("Decode(Encode(%q)) returned unexpected error: %v", tok, err)
		}
		if got != tok {
			t.Errorf("round-trip failed: Encode(%q) → Decode → %q", tok, got)
		}
	}
}

func TestDecodeEmptyToken(t *testing.T) {
	// An empty pagingToken should encode and round-trip without error.
	opaque := cursor.Encode("")
	got, err := cursor.Decode(opaque)
	if err != nil {
		t.Fatalf("unexpected error decoding empty-token cursor: %v", err)
	}
	if got != "" {
		t.Errorf("expected empty string, got %q", got)
	}
}

func TestDecodeInvalidInputs(t *testing.T) {
	// Manually craft a v=99 envelope to test version rejection (signature
	// need not be valid; version is checked first).
	wrongVersion := base64.URLEncoding.WithPadding(base64.NoPadding).
		EncodeToString([]byte(`{"p":{"v":99,"t":"tok"},"s":"x"}`))

	cases := []struct {
		name  string
		input string
	}{
		{"empty string", ""},
		{"not base64", "!!!notbase64!!!"},
		// base64url("hello") — valid base64 but not a JSON cursor envelope
		{"valid base64 not JSON", base64.URLEncoding.WithPadding(base64.NoPadding).EncodeToString([]byte("hello"))},
		{"wrong version", wrongVersion},
		{"over the length cap", strings.Repeat("A", 513)},
	}

	for _, tc := range cases {
		_, err := cursor.Decode(tc.input)
		if err == nil {
			t.Errorf("case %q: expected error for input %q, got nil", tc.name, tc.input)
		}
	}
}

// TestTamperedCursorRejected asserts the integrity-check requirement of
// issue #423: flipping a byte in the encoded token (as an attacker forging a
// cursor to skip/replay pages would) must be detected and rejected, not
// silently decoded to a different token.
func TestTamperedCursorRejected(t *testing.T) {
	opaque := cursor.Encode("id:00000000-0000-0000-0000-000000000001")

	raw, err := base64.URLEncoding.WithPadding(base64.NoPadding).DecodeString(opaque)
	if err != nil {
		t.Fatalf("decode base64: %v", err)
	}

	tampered := string(raw)
	// Try to rewrite the embedded token to a different id while leaving the
	// (now invalid) signature bytes alone.
	tampered = strings.Replace(tampered, "000000000001", "000000000002", 1)
	if tampered == string(raw) {
		t.Fatal("test setup: substring to tamper not found in payload")
	}
	tamperedOpaque := base64.URLEncoding.WithPadding(base64.NoPadding).EncodeToString([]byte(tampered))

	if _, err := cursor.Decode(tamperedOpaque); err == nil {
		t.Error("expected tampered cursor to be rejected, but Decode succeeded")
	}
}

// TestDifferentSecretsRejected confirms a cursor signed under one secret is
// rejected once the signing secret changes — e.g. across a documented key
// rotation.
func TestDifferentSecretsRejected(t *testing.T) {
	if err := cursor.SetSecret([]byte("secret-one")); err != nil {
		t.Fatalf("SetSecret: %v", err)
	}
	opaque := cursor.Encode("some-token")

	if err := cursor.SetSecret([]byte("secret-two")); err != nil {
		t.Fatalf("SetSecret: %v", err)
	}
	t.Cleanup(func() {
		_ = cursor.SetSecret([]byte("secret-one"))
	})

	if _, err := cursor.Decode(opaque); err == nil {
		t.Error("expected cursor signed under a different secret to be rejected")
	}
}
