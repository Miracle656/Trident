-- Trident PostgreSQL Schema
-- Canonical definition. Migrations in ./migrations/ mirror this file incrementally.

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ---------------------------------------------------------------------------
-- soroban_events
-- Primary store for every indexed Soroban contract event.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS soroban_events (
    id                  UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    contract_id         TEXT        NOT NULL,
    ledger_sequence     BIGINT      NOT NULL,
    ledger_timestamp    TIMESTAMPTZ NOT NULL,
    transaction_hash    TEXT        NOT NULL,
    event_index         INTEGER     NOT NULL,
    event_type          TEXT        NOT NULL CHECK (event_type IN ('contract', 'system', 'diagnostic')),
    network             TEXT        NOT NULL DEFAULT 'testnet',
    topics              JSONB       NOT NULL DEFAULT '[]',
    topic_0             TEXT        GENERATED ALWAYS AS (topics ->> 0) STORED,
    topic_1             TEXT        GENERATED ALWAYS AS (topics ->> 1) STORED,
    data                JSONB       NOT NULL DEFAULT '{}',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Single-column indexes
CREATE INDEX IF NOT EXISTS idx_soroban_events_contract_id       ON soroban_events (contract_id);
CREATE INDEX IF NOT EXISTS idx_soroban_events_ledger_sequence   ON soroban_events (ledger_sequence);
CREATE INDEX IF NOT EXISTS idx_soroban_events_ledger_timestamp  ON soroban_events (ledger_timestamp);
CREATE INDEX IF NOT EXISTS idx_soroban_events_network           ON soroban_events (network);
CREATE INDEX IF NOT EXISTS idx_soroban_events_topic_0           ON soroban_events (topic_0);
CREATE INDEX IF NOT EXISTS idx_soroban_events_topic_1           ON soroban_events (topic_1);

-- Composite indexes
CREATE INDEX IF NOT EXISTS idx_soroban_events_contract_topic_0  ON soroban_events (contract_id, topic_0);
CREATE INDEX IF NOT EXISTS idx_soroban_events_contract_network  ON soroban_events (contract_id, network);

-- GIN index for arbitrary topic containment queries
CREATE INDEX IF NOT EXISTS idx_soroban_events_topics_gin        ON soroban_events USING GIN (topics);

-- Unique constraint: a given event position within a transaction is immutable
ALTER TABLE soroban_events
    ADD CONSTRAINT uq_soroban_events_tx_index
    UNIQUE (transaction_hash, event_index);

-- ---------------------------------------------------------------------------
-- system_state
-- Persistent cursor tracking so the indexer can resume after restart without
-- re-scanning from genesis.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS system_state (
    key         TEXT PRIMARY KEY,
    value       TEXT NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Seed the cursor row so the indexer can always do an UPDATE rather than
-- an upsert on the hot path.
INSERT INTO system_state (key, value)
VALUES ('latest_ledger_cursor', '0')
ON CONFLICT (key) DO NOTHING;

-- ---------------------------------------------------------------------------
-- indexed_contracts
-- Registry of contracts whose events Trident is actively indexing.
-- A NULL network means "all networks".
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS indexed_contracts (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    contract_id     TEXT        NOT NULL,
    network         TEXT,
    label           TEXT,
    index_from      BIGINT      NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_indexed_contracts_id_network UNIQUE (contract_id, network)
);

CREATE INDEX IF NOT EXISTS idx_indexed_contracts_contract_id ON indexed_contracts (contract_id);

-- ---------------------------------------------------------------------------
-- ledger_metadata
-- Lightweight record of every processed ledger for gap detection and
-- provenance tracking.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ledger_metadata (
    ledger_sequence     BIGINT      PRIMARY KEY,
    ledger_hash         TEXT        NOT NULL,
    ledger_timestamp    TIMESTAMPTZ NOT NULL,
    event_count         INTEGER     NOT NULL DEFAULT 0,
    processed_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ledger_metadata_timestamp ON ledger_metadata (ledger_timestamp);

-- ---------------------------------------------------------------------------
-- api_keys
-- Canonical definition; mirrors migration 0005_api_keys.sql.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS api_keys (
    id               UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    key_hash         TEXT        NOT NULL UNIQUE,   -- SHA-256 hex of full key
    key_prefix       TEXT        NOT NULL,           -- first 16 chars of plaintext key (for display)
    label            TEXT        NOT NULL DEFAULT '',
    network          TEXT        NOT NULL DEFAULT 'mainnet',
    rate_limit_tier  TEXT        NOT NULL DEFAULT 'standard',
    created_by       TEXT,                           -- optional creator identifier
    last_used_at     TIMESTAMPTZ,
    request_count    BIGINT      NOT NULL DEFAULT 0,
    revoked_at       TIMESTAMPTZ,                    -- NULL means active; set to revoke
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_api_keys_key_hash ON api_keys (key_hash);
CREATE INDEX IF NOT EXISTS idx_api_keys_active ON api_keys (key_hash)
    WHERE revoked_at IS NULL;

-- ---------------------------------------------------------------------------
-- webhook_subscriptions
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS webhook_subscriptions (
    id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    api_key_id   UUID        NOT NULL REFERENCES api_keys(id) ON DELETE CASCADE,
    contract_id  TEXT        NOT NULL,
    topic0       TEXT,
    target_url   TEXT        NOT NULL,
    secret       TEXT        NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    paused_at    TIMESTAMPTZ,
    network      TEXT        NOT NULL DEFAULT 'testnet'
);

CREATE TABLE IF NOT EXISTS webhook_deliveries (
    id              BIGSERIAL   PRIMARY KEY,
    subscription_id UUID        NOT NULL REFERENCES webhook_subscriptions(id) ON DELETE CASCADE,
    event_id        UUID        NOT NULL REFERENCES soroban_events(id),
    attempt         INT         NOT NULL DEFAULT 1,
    status_code     INT,
    response_body   TEXT,
    delivered_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    success         BOOLEAN     NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_webhook_subscriptions_contract_id ON webhook_subscriptions (contract_id);
CREATE INDEX IF NOT EXISTS idx_webhook_subscriptions_paused_at ON webhook_subscriptions (paused_at);
CREATE INDEX IF NOT EXISTS idx_webhook_deliveries_subscription_id ON webhook_deliveries (subscription_id);
CREATE INDEX IF NOT EXISTS idx_webhook_deliveries_delivered_at ON webhook_deliveries (delivered_at);
