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
	// Manually craft a v=99 cursor to test version rejection.
	wrongVersion := base64.URLEncoding.WithPadding(base64.NoPadding).
		EncodeToString([]byte(`{"v":99,"t":"tok"}`))

	cases := []struct {
		name  string
		input string
	}{
		{"empty string", ""},
		{"not base64", "!!!notbase64!!!"},
		// base64url("hello") — valid base64 but not a JSON cursor payload
		{"valid base64 not JSON", base64.URLEncoding.WithPadding(base64.NoPadding).EncodeToString([]byte("hello"))},
		{"wrong version", wrongVersion},
		{"over the length cap", strings.Repeat("A", 257)},
	}

	for _, tc := range cases {
		_, err := cursor.Decode(tc.input)
		if err == nil {
			t.Errorf("case %q: expected error for input %q, got nil", tc.name, tc.input)
		}
	}
}
