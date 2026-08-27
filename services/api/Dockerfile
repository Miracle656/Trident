# syntax=docker/dockerfile:1

# ── builder ──────────────────────────────────────────────────────────────────
# The pinned digest determines the Go toolchain compiled into the binary, so a
# stale pin ships stdlib CVEs that trivy flags against usr/local/bin/api. This
# digest is Go 1.25.14; the previous one was 1.25.12, which predates the
# 1.25.13 fixes for CVE-2026-33818, -39821, -56853, -56858, -56859, -56860
# and -56862.
FROM golang:1.25-alpine@sha256:1ae0735f00daffa3aaf1363a5184c0d2dc55c78e3db4ec70241cdac97bf84b59 AS builder

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
