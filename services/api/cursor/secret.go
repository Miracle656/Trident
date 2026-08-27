package cursor

import (
	"crypto/rand"
	"fmt"
)

// randomFallbackSecret generates a 32-byte random HMAC key for use when no
// CURSOR_SIGNING_SECRET is configured. It panics on failure to read entropy,
// since continuing without a usable secret would silently produce
// unsigned/forgeable cursors.
func randomFallbackSecret() []byte {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		panic(fmt.Sprintf("cursor: failed to generate fallback secret: %v", err))
	}
	return buf
}
