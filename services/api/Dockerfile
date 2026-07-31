# syntax=docker/dockerfile:1

# ── builder ──────────────────────────────────────────────────────────────────
FROM golang:1.25-alpine@sha256:56961d79ea8129efddcc0b8643fd8a5416b4e6228cfd477e3fd61deb2672c587 AS builder

WORKDIR /build

# Cache module downloads before copying source.
COPY services/api/go.mod services/api/go.sum ./
RUN go mod download

# Copy source and build the binary.
COPY services/api/ ./
RUN CGO_ENABLED=0 GOOS=linux go build -o /api .

# ── runtime ──────────────────────────────────────────────────────────────────
FROM alpine:3.19@sha256:c5b1261d6d3e43071626931fc004f70149baeba2c8ec672bd4f27761f8e1ad6b AS runtime

RUN adduser -S -u 1001 trident

COPY --from=builder /api /usr/local/bin/api

USER trident

EXPOSE 3000

ENTRYPOINT ["/usr/local/bin/api"]
