// Package cursor provides the single opaque pagination cursor implementation
// shared by every list endpoint in the API (issue #423, generalizing the
// cursor introduced for GET /v1/events by issue #220).
//
// A cursor wraps an internal keyset paging token (e.g. "id:<uuid>" or a
// gRPC-backend-defined token) inside a base64url-encoded, HMAC-SHA256-signed
// envelope. This gives every endpoint the same three guarantees:
//
//  1. Opaque: the paging token's internal shape (a UUID, a composite
//     ledger/tx/event key, ...) is never exposed to API consumers, so it can
//     change without breaking the public contract.
//  2. Integrity-checked: the HMAC tag is verified on every Decode, so a
//     tampered or hand-crafted cursor (e.g. someone incrementing an embedded
//     ID to enumerate rows across a permission boundary) is rejected with an
//     error rather than silently accepted.
//  3. Stable across page-size changes: the cursor encodes only "resume after
//     this key", never an offset or page number, so requesting the same
//     cursor with a different `limit` resumes at the same row.
//
// See docs/pagination.md for the full pagination contract every list
// endpoint documents against.
package cursor

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
)

// payload is the internal JSON structure embedded in every opaque cursor.
type payload struct {
	V int    `json:"v"`
	T string `json:"t"`
}

// signedEnvelope is what actually gets base64-encoded: the payload plus an
// HMAC-SHA256 tag over its JSON bytes, keyed by the process's cursor secret.
type signedEnvelope struct {
	P payload `json:"p"`
	S string  `json:"s"` // base64url(HMAC-SHA256(secret, json(P)))
}

const currentVersion = 2

var (
	secretMu   sync.RWMutex
	secretOnce sync.Once
	secret     []byte
)

// defaultSecretEnvVar is checked once at first use if SetSecret has not been
// called explicitly (e.g. from main.go wiring). Deployments SHOULD set this
// so cursors remain valid across process restarts and are unforgeable by
// anyone without server-side access; an ephemeral random fallback is used
// otherwise so the package is still safe (if not restart-stable) out of the
// box for tests and local development.
const defaultSecretEnvVar = "CURSOR_SIGNING_SECRET"

// SetSecret installs the HMAC key used to sign and verify cursors. Call this
// once at startup (e.g. from main.go) before serving any traffic. Passing an
// empty secret is rejected — callers should let init-time defaulting supply
// a random key instead of accidentally disabling integrity checks.
func SetSecret(key []byte) error {
	if len(key) == 0 {
		return errors.New("cursor: secret must not be empty")
	}
	secretMu.Lock()
	defer secretMu.Unlock()
	secret = append([]byte(nil), key...)
	return nil
}

func getSecret() []byte {
	secretMu.RLock()
	s := secret
	secretMu.RUnlock()
	if s != nil {
		return s
	}

	secretOnce.Do(func() {
		secretMu.Lock()
		defer secretMu.Unlock()
		if secret != nil {
			return
		}
		if env := os.Getenv(defaultSecretEnvVar); env != "" {
			secret = []byte(env)
			return
		}
		// No configured secret: fall back to a process-local random key.
		// Cursors remain integrity-checked (still unforgeable without this
		// key) but won't survive a process restart — acceptable for tests
		// and local dev; production deployments should set
		// CURSOR_SIGNING_SECRET so cursors handed to clients keep working
		// across restarts/rolling deploys.
		secret = randomFallbackSecret()
	})

	secretMu.RLock()
	defer secretMu.RUnlock()
	return secret
}

func sign(data []byte) string {
	mac := hmac.New(sha256.New, getSecret())
	mac.Write(data)
	return base64.URLEncoding.WithPadding(base64.NoPadding).EncodeToString(mac.Sum(nil))
}

// Encode takes a raw pagingToken string and returns an opaque, URL-safe,
// integrity-checked cursor string that can be passed back to API consumers.
func Encode(pagingToken string) string {
	p := payload{V: currentVersion, T: pagingToken}
	pJSON, err := json.Marshal(p)
	if err != nil {
		// json.Marshal of a static struct with string fields cannot fail.
		panic(fmt.Sprintf("cursor: marshal failed: %v", err))
	}

	env := signedEnvelope{P: p, S: sign(pJSON)}
	data, err := json.Marshal(env)
	if err != nil {
		panic(fmt.Sprintf("cursor: marshal failed: %v", err))
	}
	return base64.URLEncoding.WithPadding(base64.NoPadding).EncodeToString(data)
}

// Decode takes an opaque cursor string produced by Encode and returns the
// underlying pagingToken. It returns an error if the cursor is malformed,
// cannot be base64-decoded, carries an unrecognised version number, or fails
// its HMAC integrity check (including a cursor signed under a different
// secret, e.g. after a key rotation, or a cursor that was hand-tampered
// with).
func Decode(opaque string) (string, error) {
	if opaque == "" {
		return "", errors.New("cursor: empty cursor string")
	}
	if len(opaque) > 512 {
		return "", errors.New("cursor: payload too large")
	}

	data, err := base64.URLEncoding.WithPadding(base64.NoPadding).DecodeString(opaque)
	if err != nil {
		return "", fmt.Errorf("cursor: base64 decode: %w", err)
	}

	var env signedEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		return "", fmt.Errorf("cursor: json unmarshal: %w", err)
	}

	if env.P.V != currentVersion {
		return "", fmt.Errorf("cursor: unsupported version %d", env.P.V)
	}

	pJSON, err := json.Marshal(env.P)
	if err != nil {
		return "", fmt.Errorf("cursor: re-marshal payload: %w", err)
	}
	wantSig := sign(pJSON)
	if subtle.ConstantTimeCompare([]byte(wantSig), []byte(env.S)) != 1 {
		return "", errors.New("cursor: integrity check failed")
	}

	return env.P.T, nil
}
