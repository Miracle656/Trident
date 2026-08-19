# syntax=docker/dockerfile:1

# ── builder ──────────────────────────────────────────────────────────────────
FROM golang:1.25-alpine@sha256:1e0126852075c9c60731c8ba49088448b91f63e2aed97ca9d1a9791622a05946 AS builder

WORKDIR /build

# Cache module downloads before copying source.
COPY services/api/go.mod services/api/go.sum ./
RUN go mod download

# Copy source and build the binary.
COPY services/api/ ./
RUN CGO_ENABLED=0 GOOS=linux go build -o /api .

# ── runtime ──────────────────────────────────────────────────────────────────
# Was pinned to a stale alpine:3.19 digest, so the runtime image kept
# shipping musl (CVE-2025-26519) and libcrypto3 (CVE-2024-6119) versions
# that upstream had already patched. 3.22 clears both (issue #371).
FROM alpine:3.22@sha256:14358309a308569c32bdc37e2e0e9694be33a9d99e68afb0f5ff33cc1f695dce AS runtime

RUN adduser -S -u 1001 trident

COPY --from=builder /api /usr/local/bin/api

USER trident

EXPOSE 3000

ENTRYPOINT ["/usr/local/bin/api"]
