# Trident Hardening Sweep — Findings Report

Branch: `hardening-sweep` (cut from `pr-189-fix` @ `b93316a`, which is 78 commits ahead of
`origin/main` and is the "green under fire" work; `main` itself is behind and has red history).

## Baseline (Phase 0)

- HEAD `b93316a` = **all 8 CI jobs green** (Rust, Rust integration tests, Go, TypeScript,
  OpenAPI, Docker build, E2E smoke, Helm). Verified via check-runs API.
- Repo map: `crates/{api,indexer,backfill,common}` (Rust), `services/api` (Go REST + gRPC
  client), `sdk/{typescript,rust,go,python,react}`, `database/{schema.sql,migrations/0001-0009}`,
  `docker/*compose*`, `helm/trident`, `.github/workflows/{ci,release}.yml`.

## How "green" hides gaps (Phase 1 — proven from CI logs)

- The env-gated Rust tests use `require_services!` which does `eprintln!("SKIP…"); return;` on
  missing `TEST_DATABASE_URL`/`TEST_REDIS_URL`. cargo **hides passing-test stderr**, so
  "no SKIP lines in the log" proves nothing. The real evidence is timing:
  the same 61-test suite ran in **0.20s in the `rust` job** (env absent → early-returned/skipped)
  vs **9.42s in `rust-integration`** (env present → real DB/Redis work). So the DB/Redis
  integration tests genuinely execute **only** in `rust-integration`, and that job is
  `if`-gated to push-to-main/dev or PR-with-base-main/dev.

---

## Findings (ranked by severity)

### HIGH

**H1 — `database/schema.sql` is not a canonical mirror of the migration chain; a DB
bootstrapped from it is materially broken.**
`database/schema.sql:1` claims "Canonical definition. Migrations mirror this file." It has drifted hard:
- Missing table **`audit_log`** (migration `0006`). → Go audit middleware
  (`services/api/middleware/audit.go:165`), admin analytics (`services/api/handlers/admin.go:151`),
  and the 90-day cleanup (`services/api/main.go:290`) all hit `relation "audit_log" does not exist`.
- Missing table **`parse_errors`** (migration `0008`). → indexer parse-error isolation write
  (`crates/indexer/src/db/mod.rs:246`) and the Go `/status` count
  (`services/api/handlers/status.go:110`) fail at runtime.
- Missing `system_state` columns **`last_poll_at`, `last_alert_at`** (+`last_ledger_indexed`,
  `events_indexed_total`, `events_in_last_poll`, `poll_duration_ms`, `alert_fired`) from
  migrations `0002`/`0003`. → indexer health write `UPDATE system_state SET last_poll_at…`
  (`crates/indexer/src/db/mod.rs:159`) and alert-state read/write
  (`crates/indexer/src/db/mod.rs:201`) fail.
- **soroban_events indexes fully diverged.** schema.sql defines 9 legacy indexes
  (`idx_soroban_events_contract_id`, `…_topic_0`, `…_topic_1`, `…_topics_gin`,
  `…_contract_network`, `…_ledger_sequence`, `…_contract_topic_0`) that **no migration creates**,
  and is missing the migration `0009` high-cardinality perf indexes actually deployed to
  production (`idx_soroban_events_contract_ledger`, `…_id_desc`, `…_contract_topic0` partial,
  `…_ledger_timestamp`) plus `0004`'s `…_network_contract`.
- **Extra constraint** `uq_soroban_events_tx_index UNIQUE(transaction_hash,event_index)`
  (`database/schema.sql:42`) exists in no migration. Insert path uses `ON CONFLICT (id)`
  (`crates/indexer/src/db/mod.rs:66`), so it is redundant divergence.

*Failure scenario:* the **`rust-integration` CI job builds `trident_test` from schema.sql**
(`.github/workflows/ci.yml:206`), so every code path touching `audit_log`, `parse_errors`, or the
`system_state` health/alert columns is **not integration-covered** — tests are green only because
they never exercise those paths against the DB. Anyone who runs `psql -f schema.sql` for local/dev
gets a schema on which audit logging, parse-error isolation, health/alert tracking, and the
`/stats` + `/status` endpoints crash on first use.

### MEDIUM

**M1 — Integration tests can silently skip even inside the integration job.**
`require_services!` (`crates/indexer/src/streamer/mod.rs:400`,
`crates/api/src/services/events.rs:367`, `crates/indexer/src/db/mod.rs:304`) `return`s instead of
failing when the env is absent. GitHub Actions sets `CI=true` in *every* job, so `CI` can't
distinguish the two jobs. *Failure scenario:* if `TEST_DATABASE_URL` is ever misconfigured in the
`rust-integration` job (typo, service rename, port change), all DB/Redis tests skip and the job
still reports **green** — a false pass with zero signal.

**M2 — Indexer resume passes a ledger sequence where the RPC expects a paging-token cursor.**
`crates/indexer/src/streamer/mod.rs:186-190`: on restart (`*cursor != 0`) it calls
`get_events(start_ledger=None, cursor=Some(cursor.to_string()))`, but the persisted cursor is a
**ledger sequence** (`db::set_cursor` stores `last.ledger` at `streamer/mod.rs:317`), while
in-loop pagination correctly uses the RPC `paging_token` (`streamer/mod.rs:223,349`). The Soroban
`getEvents` `cursor` field (`crates/indexer/src/rpc/mod.rs:93`) is a paging token, not a ledger
number. *Failure scenario:* after any indexer restart the first poll sends
`cursor:"12345"`; the RPC rejects it as an invalid cursor and the poll loop errors/retries,
stalling ingestion. **Flagged for review — changes runtime behavior of the resume feature; not
auto-fixed.** Likely correct fix: resume via `start_ledger = Some(cursor + 1)` instead of the
ledger-as-cursor.

**M3 — Auth Redis cache is never invalidated on key revocation.**
`services/api/middleware/auth.go:99-123` caches a successful lookup for 5 min
(`authCacheTTL`) keyed by `sha256(key)` and does not delete/expire it when a key is revoked.
*Failure scenario:* a revoked/compromised API key keeps authenticating for up to 5 minutes after
revocation. **Flagged for review** (acceptable if documented; fix = delete cache key on revoke).

**M4 — Floating Docker base image breaks build reproducibility.**
`crates/api/Dockerfile:4` and `crates/indexer/Dockerfile:4` use `FROM rust:1-slim` (floating
major). *Failure scenario:* a new upstream `rust:1` publish silently changes the toolchain,
producing non-reproducible builds and potential clippy/edition breakage unrelated to any commit.
Pin to an explicit patch (e.g. `rust:1.83-slim`). Go/alpine bases are already pinned.

### LOW

**L1 — `rust-integration` job is entirely skipped on feature-branch pushes.**
`.github/workflows/ci.yml:168-170` `if:` gate means integration tests only run on push to
main/dev or PR based on main/dev. Green on a feature branch does not mean the DB tests ran.
Acceptable by design; documented here so it isn't mistaken for coverage.

**L2 — Obsolete compose `version:` keys.** `docker/docker-compose.yml:1` (`version: "3.9"`) and
`docker/docker-compose.dev.yml:1` — Compose v2 ignores and warns on these.

**L3 — `pgbouncer` service still defined in `docker/docker-compose.yml` /
`docker-compose.ci.yml`** though a prior fix removed it from E2E `depends_on`. Dead scaffolding in
the CI stack.

**L4 — Generated gRPC "not implemented" stubs** in `services/api/gen/trident_grpc.pb.go:95-103`
are normal protoc `Unimplemented*Server` output (the Go service is a gRPC *client*), not a real
gap. Noted to pre-empt false alarms. No other real-code TODO/FIXME/unimplemented in the tree.

---

## Fix plan (Phase 7)

1. **H1** — rewrite `database/schema.sql` to faithfully mirror migrations `0001–0009`
   (add `audit_log`, `parse_errors`, the `system_state` health/alert columns; replace the
   soroban_events index set with the `0004`+`0009` canonical indexes; drop the non-migration
   `uq_soroban_events_tx_index`).
2. **H1/M1 (durable)** — switch the `rust-integration` job to build `trident_test` from the
   **migration chain** instead of `schema.sql`, so CI validates the real deploy artifact and can
   never again silently depend on a stale mirror; and make `require_services!` **panic** (fail
   loud) when a dedicated `REQUIRE_TEST_SERVICES` flag (set only in that job) is present but the
   URLs are missing.
3. **M4 / L2 / L3** — pin the Rust Docker base, drop obsolete `version:` keys and the dead
   pgbouncer service (verified via the Docker build + E2E CI jobs).
4. **M2 / M3** — reported only; flagged for maintainer review because they change the runtime
   behavior of existing features.
