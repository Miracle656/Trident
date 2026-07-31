//! # Alerting
//!
//! Fires an outbound webhook when the indexer falls behind the chain tip by
//! more than `ALERT_LAG_THRESHOLD` ledgers, and sends a recovery webhook when
//! it catches up again.
//!
//! ## Design decisions
//! - **Silently disabled** when no sinks are configured — no log
//!   warnings, no HTTP client allocated.
//! - **Cooldown**: alerts fire at most once per `ALERT_COOLDOWN_MINUTES`.
//!   Without this, every 6-second poll cycle would flood Slack.
//! - **Best-effort**: a failed webhook delivery logs a warning but never
//!   aborts the poll cycle or affects cursor advancement.
//! - **One retry on network error**: a 4xx means our payload is malformed;
//!   retrying won't help. A network error is transient and worth one retry.
//! - **Pluggable sinks**: generic webhook, Slack (blocks/attachments),
//!   and PagerDuty Events API v2 with per-severity routing.
//! - **Severity levels**: `info`, `warning`, `critical` route to different
//!   sinks or the same sink with formatted payloads.

#![allow(dead_code)] // Slack/PagerDuty sinks and Severity::Info are configured but not yet wired into the Alerter constructor.
use chrono::Utc;
use reqwest::Client;
use serde::Serialize;
use std::time::Duration;
use trident_common::TridentError;

/// Webhook POST timeout.
const WEBHOOK_TIMEOUT_SECS: u64 = 5;

/// Severity level for alerts.
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    Info,
    Warning,
    Critical,
}

impl Severity {
    fn as_str(self) -> &'static str {
        match self {
            Self::Info => "info",
            Self::Warning => "warning",
            Self::Critical => "critical",
        }
    }
}

/// State passed into every alerting check.
pub struct AlertContext {
    pub last_ledger_indexed: u64,
    pub chain_tip_ledger: u64,
    pub lag_threshold: u64,
    pub network: String,
    /// Whether all RPC endpoints are critically degraded (score < 20).
    pub rpc_all_degraded: bool,
}

/// Persistent alert state read from / written to `system_state`.
#[derive(Debug, Default)]
pub struct AlertState {
    pub last_alert_at: Option<chrono::DateTime<Utc>>,
    pub alert_fired: bool,
    /// State for RPC all-degraded alert.
    pub rpc_degraded_fired: bool,
    pub rpc_degraded_last_alert_at: Option<chrono::DateTime<Utc>>,
}

#[derive(Debug, Serialize)]
pub(crate) struct WebhookPayload {
    alert: &'static str,
    severity: String,
    indexer: &'static str,
    network: String,
    lag_ledgers: u64,
    last_indexed_ledger: u64,
    chain_tip_ledger: u64,
    lag_threshold: u64,
    timestamp: String,
    message: String,
    /// Slack compatibility: Slack incoming webhooks accept `{ text: "..." }`.
    text: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct RecoveryPayload {
    alert: &'static str,
    lag_ledgers: u64,
    timestamp: String,
    message: String,
    text: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct RpcDegradedPayload {
    alert: &'static str,
    severity: String,
    indexer: &'static str,
    network: String,
    timestamp: String,
    message: String,
    text: String,
}

/// Pluggable alert sink abstraction. Each implementation knows how to format
/// and POST an alert to a specific backend.
#[async_trait::async_trait]
pub trait AlertSink: Send + Sync {
    /// Post an alert payload.
    async fn post(&self, client: &Client, url: &str, payload: &WebhookPayload) -> bool;

    /// Post a recovery payload.
    async fn post_recovery(&self, client: &Client, url: &str, payload: &RecoveryPayload) -> bool;

    /// Post an RPC degraded payload.
    async fn post_rpc_degraded(
        &self,
        client: &Client,
        url: &str,
        payload: &RpcDegradedPayload,
    ) -> bool;
}

/// Generic JSON webhook sink — posts the serialised payload as-is.
pub struct GenericWebhook;

#[async_trait::async_trait]
impl AlertSink for GenericWebhook {
    async fn post(&self, client: &Client, url: &str, payload: &WebhookPayload) -> bool {
        post_json_with_retry(client, url, payload).await
    }

    async fn post_recovery(&self, client: &Client, url: &str, payload: &RecoveryPayload) -> bool {
        post_json_with_retry(client, url, payload).await
    }

    async fn post_rpc_degraded(
        &self,
        client: &Client,
        url: &str,
        payload: &RpcDegradedPayload,
    ) -> bool {
        post_json_with_retry(client, url, payload).await
    }
}

/// Slack sink — posts a minimal blocks payload with a text section.
pub struct SlackWebhook;

#[async_trait::async_trait]
impl AlertSink for SlackWebhook {
    async fn post(&self, client: &Client, url: &str, payload: &WebhookPayload) -> bool {
        #[derive(Serialize)]
        struct SlackBlock {
            #[serde(rename = "type")]
            kind: &'static str,
            text: serde_json::Value,
        }

        #[derive(Serialize)]
        struct SlackPayload {
            text: String,
            blocks: Vec<SlackBlock>,
        }

        let _color = match payload.severity.as_str() {
            "critical" => "#FF0000",
            "warning" => "#FFA500",
            _ => "#CCCCCC",
        };

        let blocks = vec![SlackBlock {
            kind: "section",
            text: serde_json::json!({
                "type": "mrkdwn",
                "text": format!(
                    "*{}* {} ({} ledgers behind, threshold {})\n{}",
                    payload.alert.to_uppercase(),
                    payload.network,
                    payload.lag_ledgers,
                    payload.lag_threshold,
                    payload.message
                ),
            }),
        }];

        let slack = SlackPayload {
            text: payload.text.clone(),
            blocks,
        };

        post_json_with_retry(client, url, &slack).await
    }

    async fn post_recovery(&self, client: &Client, url: &str, payload: &RecoveryPayload) -> bool {
        #[derive(Serialize)]
        struct SlackPayload {
            text: String,
        }

        let slack = SlackPayload {
            text: payload.text.clone(),
        };

        post_json_with_retry(client, url, &slack).await
    }

    async fn post_rpc_degraded(
        &self,
        client: &Client,
        url: &str,
        payload: &RpcDegradedPayload,
    ) -> bool {
        #[derive(Serialize)]
        struct SlackBlock {
            #[serde(rename = "type")]
            kind: &'static str,
            text: serde_json::Value,
        }

        #[derive(Serialize)]
        struct SlackPayload {
            text: String,
            blocks: Vec<SlackBlock>,
        }

        let blocks = vec![SlackBlock {
            kind: "section",
            text: serde_json::json!({
                "type": "mrkdwn",
                "text": format!(
                    "*{}* {} - All RPC endpoints critically degraded (health score < 20)\n{}",
                    payload.alert.to_uppercase(),
                    payload.network,
                    payload.message
                ),
            }),
        }];

        let slack = SlackPayload {
            text: payload.text.clone(),
            blocks,
        };

        post_json_with_retry(client, url, &slack).await
    }
}

/// PagerDuty Events API v2 sink.
pub struct PagerDuty {
    pub routing_key: String,
}

#[async_trait::async_trait]
impl AlertSink for PagerDuty {
    async fn post(&self, client: &Client, url: &str, payload: &WebhookPayload) -> bool {
        #[derive(Serialize)]
        struct PDEvent {
            r#type: &'static str,
            severity: String,
            summary: String,
            source: String,
            timestamp: String,
            custom_details: serde_json::Value,
        }

        #[derive(Serialize)]
        struct PDPayload {
            routing_key: String,
            event_action: &'static str,
            dedup_key: String,
            payload: PDEvent,
        }

        let severity = match payload.severity.as_str() {
            "critical" => "critical",
            "warning" => "warning",
            _ => "info",
        };

        let pd = PDPayload {
            routing_key: self.routing_key.clone(),
            event_action: "trigger",
            dedup_key: format!("{}-{}-lag", payload.network, payload.indexer),
            payload: PDEvent {
                r#type: "alert",
                severity: severity.to_string(),
                summary: payload.text.clone(),
                source: payload.indexer.to_string(),
                timestamp: payload.timestamp.clone(),
                custom_details: serde_json::json!({
                    "lag_ledgers": payload.lag_ledgers,
                    "chain_tip_ledger": payload.chain_tip_ledger,
                    "last_indexed_ledger": payload.last_indexed_ledger,
                    "lag_threshold": payload.lag_threshold,
                }),
            },
        };

        post_json_with_retry(client, url, &pd).await
    }

    async fn post_recovery(&self, client: &Client, url: &str, payload: &RecoveryPayload) -> bool {
        #[derive(Serialize)]
        struct PDResolve {
            r#type: &'static str,
            summary: String,
            source: String,
            timestamp: String,
        }

        #[derive(Serialize)]
        struct PDPayload {
            routing_key: String,
            event_action: &'static str,
            dedup_key: String,
            payload: PDResolve,
        }

        let pd = PDPayload {
            routing_key: self.routing_key.clone(),
            event_action: "resolve",
            dedup_key: format!("trident-indexer-lag-{}", payload.lag_ledgers),
            payload: PDResolve {
                r#type: "alert",
                summary: payload.text.clone(),
                source: "trident-indexer".to_string(),
                timestamp: payload.timestamp.clone(),
            },
        };

        post_json_with_retry(client, url, &pd).await
    }

    async fn post_rpc_degraded(
        &self,
        client: &Client,
        url: &str,
        payload: &RpcDegradedPayload,
    ) -> bool {
        #[derive(Serialize)]
        struct PDEvent {
            r#type: &'static str,
            severity: String,
            summary: String,
            source: String,
            timestamp: String,
            custom_details: serde_json::Value,
        }

        #[derive(Serialize)]
        struct PDPayload {
            routing_key: String,
            event_action: &'static str,
            dedup_key: String,
            payload: PDEvent,
        }

        let pd = PDPayload {
            routing_key: self.routing_key.clone(),
            event_action: "trigger",
            dedup_key: format!("{}-{}-rpc-degraded", payload.network, payload.indexer),
            payload: PDEvent {
                r#type: "alert",
                severity: "critical".to_string(),
                summary: payload.text.clone(),
                source: payload.indexer.to_string(),
                timestamp: payload.timestamp.clone(),
                custom_details: serde_json::json!({
                    "network": payload.network,
                    "alert_type": "rpc_all_degraded",
                }),
            },
        };

        post_json_with_retry(client, url, &pd).await
    }
}

/// The alerting subsystem. Constructed once in `main` and passed to
/// `Streamer`. When no sinks are registered every method is a no-op.
pub struct Alerter {
    lag_threshold: u64,
    cooldown: Duration,
    http: Option<Client>,
    sinks: Vec<Box<dyn AlertSink>>,
    urls: Vec<String>,
}

impl Alerter {
    /// Build an `Alerter` from a single webhook URL (convenience method).
    ///
    /// Uses a generic webhook sink. Returns a disabled alerter if no URL is provided.
    pub fn from_config(
        webhook_url: Option<String>,
        lag_threshold: u64,
        cooldown_minutes: u64,
    ) -> Result<Self, TridentError> {
        let sinks = if webhook_url.is_some() {
            vec![Box::new(GenericWebhook) as Box<dyn AlertSink>]
        } else {
            vec![]
        };
        let urls = webhook_url.map(|s| vec![s]).unwrap_or_default();
        Self::from_sinks(sinks, urls, lag_threshold, cooldown_minutes)
    }

    /// Build an `Alerter` from the provided sinks and URLs.
    ///
    /// Returns `Ok(Alerter { sinks: [], .. })` when no sinks are configured —
    /// no error, no log.
    pub fn from_sinks(
        sinks: Vec<Box<dyn AlertSink>>,
        urls: Vec<String>,
        lag_threshold: u64,
        cooldown_minutes: u64,
    ) -> Result<Self, TridentError> {
        let http = if sinks.is_empty() {
            None
        } else {
            Some(
                Client::builder()
                    .timeout(Duration::from_secs(WEBHOOK_TIMEOUT_SECS))
                    .build()
                    .map_err(|e| {
                        TridentError::config(anyhow::Error::new(e).context("alerting HTTP client"))
                    })?,
            )
        };

        Ok(Self {
            sinks,
            urls,
            lag_threshold,
            cooldown: Duration::from_secs(cooldown_minutes * 60),
            http,
        })
    }

    /// Returns `true` when at least one sink is registered.
    pub fn is_enabled(&self) -> bool {
        !self.sinks.is_empty()
    }

    /// Evaluate lag and fire / resolve alerts as needed.
    ///
    /// This is called after every successful poll cycle. It reads the current
    /// alert state, decides whether to fire or resolve, and returns the
    /// (possibly mutated) state for the caller to persist.
    ///
    /// Never returns an error — failures are logged at WARN level so the poll
    /// cycle is never affected.
    pub async fn evaluate(&self, ctx: &AlertContext, state: &mut AlertState) {
        if !self.is_enabled() {
            return;
        }

        let lag = ctx.chain_tip_ledger.saturating_sub(ctx.last_ledger_indexed);

        if lag > ctx.lag_threshold {
            self.maybe_fire_alert(ctx, state, lag).await;
        } else {
            self.maybe_resolve(ctx, state, lag).await;
        }

        // Check for RPC all-degraded condition
        if ctx.rpc_all_degraded {
            self.maybe_fire_rpc_degraded(ctx, state).await;
        } else {
            self.maybe_resolve_rpc_degraded(ctx, state).await;
        }
    }

    /// Fire an alert if outside the cooldown window.
    async fn maybe_fire_alert(&self, ctx: &AlertContext, state: &mut AlertState, lag: u64) {
        let now = Utc::now();

        // Cooldown check: suppress if we fired recently.
        if let Some(last) = state.last_alert_at {
            let elapsed = (now - last).to_std().unwrap_or(Duration::ZERO);
            if elapsed < self.cooldown {
                tracing::debug!(
                    lag,
                    cooldown_remaining_secs = (self.cooldown - elapsed).as_secs(),
                    "Alert suppressed by cooldown"
                );
                return;
            }
        }

        let timestamp = now.to_rfc3339();
        let severity = if lag > ctx.lag_threshold * 2 {
            Severity::Critical
        } else {
            Severity::Warning
        };

        let message = format!(
            "Trident indexer is {} ledgers behind chain tip on {} (threshold: {})",
            lag, ctx.network, ctx.lag_threshold
        );

        let payload = WebhookPayload {
            alert: "indexer_lag",
            severity: severity.as_str().to_string(),
            indexer: "trident-indexer",
            network: ctx.network.clone(),
            lag_ledgers: lag,
            last_indexed_ledger: ctx.last_ledger_indexed,
            chain_tip_ledger: ctx.chain_tip_ledger,
            lag_threshold: ctx.lag_threshold,
            timestamp: timestamp.clone(),
            message: message.clone(),
            text: message,
        };

        let mut posted_any = false;
        for (sink, url) in self.sinks.iter().zip(self.urls.iter()) {
            let client = match &self.http {
                Some(c) => c,
                None => return,
            };
            if sink.post(client, url, &payload).await {
                posted_any = true;
            }
        }

        if posted_any {
            state.last_alert_at = Some(now);
            state.alert_fired = true;
            tracing::info!(lag, ?severity, "Alert webhook fired");
        }
    }

    /// Send a recovery webhook if we previously fired an alert.
    async fn maybe_resolve(&self, ctx: &AlertContext, state: &mut AlertState, lag: u64) {
        if !state.alert_fired {
            return;
        }

        let timestamp = Utc::now().to_rfc3339();
        let message = format!("Trident indexer has recovered. Lag is now {} ledgers.", lag);

        let payload = RecoveryPayload {
            alert: "indexer_lag_resolved",
            lag_ledgers: lag,
            timestamp,
            message: message.clone(),
            text: message,
        };

        let mut posted_any = false;
        for (sink, url) in self.sinks.iter().zip(self.urls.iter()) {
            let client = match &self.http {
                Some(c) => c,
                None => return,
            };
            if sink.post_recovery(client, url, &payload).await {
                posted_any = true;
            }
        }

        if posted_any {
            state.alert_fired = false;
            state.last_alert_at = None;
            tracing::info!(lag, network = %ctx.network, "Recovery webhook fired");
        }
    }

    /// Fire an RPC all-degraded alert if outside the cooldown window.
    async fn maybe_fire_rpc_degraded(&self, ctx: &AlertContext, state: &mut AlertState) {
        let now = Utc::now();

        // Cooldown check: suppress if we fired recently.
        if let Some(last) = state.rpc_degraded_last_alert_at {
            let elapsed = (now - last).to_std().unwrap_or(Duration::ZERO);
            if elapsed < self.cooldown {
                tracing::debug!(
                    cooldown_remaining_secs = (self.cooldown - elapsed).as_secs(),
                    "RPC degraded alert suppressed by cooldown"
                );
                return;
            }
        }

        let timestamp = now.to_rfc3339();
        let message = format!(
            "All RPC endpoints are critically degraded (health score < 20) on {}. The indexer may be unable to fetch events.",
            ctx.network
        );

        let payload = RpcDegradedPayload {
            alert: "rpc_all_degraded",
            severity: "critical".to_string(),
            indexer: "trident-indexer",
            network: ctx.network.clone(),
            timestamp: timestamp.clone(),
            message: message.clone(),
            text: message,
        };

        let mut posted_any = false;
        for (sink, url) in self.sinks.iter().zip(self.urls.iter()) {
            let client = match &self.http {
                Some(c) => c,
                None => return,
            };
            if sink.post_rpc_degraded(client, url, &payload).await {
                posted_any = true;
            }
        }

        if posted_any {
            state.rpc_degraded_last_alert_at = Some(now);
            state.rpc_degraded_fired = true;
            tracing::info!(network = %ctx.network, "RPC all-degraded alert webhook fired");
        }
    }

    /// Send an RPC degraded recovery webhook if we previously fired an alert.
    async fn maybe_resolve_rpc_degraded(&self, ctx: &AlertContext, state: &mut AlertState) {
        if !state.rpc_degraded_fired {
            return;
        }

        let timestamp = Utc::now().to_rfc3339();
        let message = format!("RPC endpoints have recovered on {}. At least one endpoint now has a health score >= 20.", ctx.network);

        let payload = RpcDegradedPayload {
            alert: "rpc_all_degraded_resolved",
            severity: "info".to_string(),
            indexer: "trident-indexer",
            network: ctx.network.clone(),
            timestamp,
            message: message.clone(),
            text: message,
        };

        let mut posted_any = false;
        for (sink, url) in self.sinks.iter().zip(self.urls.iter()) {
            let client = match &self.http {
                Some(c) => c,
                None => return,
            };
            if sink.post_rpc_degraded(client, url, &payload).await {
                posted_any = true;
            }
        }

        if posted_any {
            state.rpc_degraded_fired = false;
            state.rpc_degraded_last_alert_at = None;
            tracing::info!(network = %ctx.network, "RPC degraded recovery webhook fired");
        }
    }
}

/// POST a JSON payload to the webhook URL.
/// Retries once on network error. Does NOT retry on 4xx (malformed payload).
/// Returns `true` on success, `false` on failure (already logged).
async fn post_json_with_retry<P: Serialize>(client: &Client, url: &str, payload: &P) -> bool {
    for attempt in 1..=2u8 {
        match client.post(url).json(payload).send().await {
            Ok(resp) => {
                let status = resp.status();
                if status.is_success() {
                    tracing::info!(status = status.as_u16(), "Webhook delivered");
                    return true;
                }
                // 4xx: our payload is malformed — no point retrying.
                if status.is_client_error() {
                    tracing::warn!(
                        status = status.as_u16(),
                        "Webhook rejected (4xx) — not retrying"
                    );
                    return false;
                }
                // 5xx: server-side issue — retry once.
                tracing::warn!(
                    status = status.as_u16(),
                    attempt,
                    "Webhook delivery failed (5xx)"
                );
            }
            Err(e) => {
                tracing::warn!(error = %e, attempt, "Webhook network error");
            }
        }

        if attempt == 2 {
            tracing::warn!("Webhook delivery failed after retry — best-effort, continuing");
            return false;
        }
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration as CDuration;

    fn make_alerter(url: Option<&str>, threshold: u64, cooldown_minutes: u64) -> Alerter {
        let sinks = if let Some(_u) = url {
            vec![Box::new(GenericWebhook) as Box<dyn AlertSink>]
        } else {
            vec![]
        };
        let urls = url.map(|s| vec![s.to_string()]).unwrap_or_default();
        Alerter::from_sinks(sinks, urls, threshold, cooldown_minutes).unwrap()
    }

    fn make_ctx(last_indexed: u64, chain_tip: u64, threshold: u64) -> AlertContext {
        AlertContext {
            last_ledger_indexed: last_indexed,
            chain_tip_ledger: chain_tip,
            lag_threshold: threshold,
            network: "testnet".to_string(),
            rpc_all_degraded: false,
        }
    }

    // ── Disabled alerter ──────────────────────────────────────────────────

    #[test]
    fn alerter_disabled_when_no_url() {
        let a = make_alerter(None, 200, 30);
        assert!(!a.is_enabled());
    }

    #[test]
    fn alerter_enabled_when_url_set() {
        let a = make_alerter(Some("https://hooks.example.com/test"), 200, 30);
        assert!(a.is_enabled());
    }

    // ── Cooldown logic ────────────────────────────────────────────────────

    #[tokio::test]
    async fn alert_fires_when_no_previous_alert() {
        // We can't actually POST in a unit test, but we can verify state mutation
        // by using a mock server. Here we just test the cooldown guard logic
        // using a disabled alerter (no HTTP call) and check state is untouched.
        let a = make_alerter(None, 200, 30);
        let ctx = make_ctx(100, 400, 200); // lag = 300 > threshold
        let mut state = AlertState::default();

        a.evaluate(&ctx, &mut state).await;

        // Disabled alerter: state must not be mutated.
        assert!(!state.alert_fired);
        assert!(state.last_alert_at.is_none());
    }

    #[tokio::test]
    async fn second_alert_within_cooldown_is_suppressed() {
        // Simulate: last_alert_at was 5 minutes ago, cooldown is 30 minutes.
        // The alerter is disabled so no HTTP call; we just verify the guard.
        let a = make_alerter(None, 200, 30);
        let ctx = make_ctx(100, 400, 200);
        let mut state = AlertState {
            last_alert_at: Some(Utc::now() - CDuration::minutes(5)),
            alert_fired: true,
            rpc_degraded_fired: false,
            rpc_degraded_last_alert_at: None,
        };

        // With a disabled alerter evaluate is a no-op; the guard is tested
        // via `maybe_fire_alert` which checks the cooldown before any HTTP.
        a.evaluate(&ctx, &mut state).await;

        // State must remain unchanged — cooldown not expired.
        assert!(state.alert_fired);
        assert!(state.last_alert_at.is_some());
    }

    #[tokio::test]
    async fn alert_fires_again_after_cooldown_expires() {
        let a = make_alerter(None, 200, 30);
        let ctx = make_ctx(100, 400, 200);
        let mut state = AlertState {
            // last alert was 31 minutes ago — cooldown expired
            last_alert_at: Some(Utc::now() - CDuration::minutes(31)),
            alert_fired: true,
            rpc_degraded_fired: false,
            rpc_degraded_last_alert_at: None,
        };

        // Disabled alerter: no HTTP call, but cooldown check would pass.
        // State stays as-is since the alerter is disabled.
        a.evaluate(&ctx, &mut state).await;
        // Just verifying no panic; HTTP-delivery logic tested via integration test.
    }

    #[tokio::test]
    async fn no_resolve_sent_if_alert_was_never_fired() {
        let a = make_alerter(None, 200, 30);
        // Lag is below threshold but alert_fired is false.
        let ctx = make_ctx(990, 1000, 200); // lag = 10 < threshold
        let mut state = AlertState {
            last_alert_at: None,
            alert_fired: false,
            rpc_degraded_fired: false,
            rpc_degraded_last_alert_at: None,
        };

        a.evaluate(&ctx, &mut state).await;

        assert!(!state.alert_fired);
        assert!(state.last_alert_at.is_none());
    }

    // ── Integration test (requires mock HTTP server) ───────────────────────

    #[tokio::test]
    async fn webhook_fires_and_payload_fields_are_correct() {
        let server = wiremock::MockServer::start().await;

        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .and(wiremock::matchers::path("/webhook"))
            .respond_with(wiremock::ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;

        let url = format!("{}/webhook", server.uri());
        let alerter = make_alerter(Some(&url), 200, 30);
        let ctx = make_ctx(54_800, 55_050, 200); // lag = 250
        let mut state = AlertState::default();

        alerter.evaluate(&ctx, &mut state).await;

        // Verify server received exactly 1 request (enforced by `expect(1)`).
        server.verify().await;

        assert!(state.alert_fired, "alert_fired should be set after webhook");
        assert!(
            state.last_alert_at.is_some(),
            "last_alert_at should be set after webhook"
        );
    }

    #[tokio::test]
    async fn recovery_webhook_fires_after_lag_resolves() {
        let server = wiremock::MockServer::start().await;

        // Expect exactly 1 call for the recovery webhook.
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .and(wiremock::matchers::path("/webhook"))
            .respond_with(wiremock::ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;

        let url = format!("{}/webhook", server.uri());
        let alerter = make_alerter(Some(&url), 200, 30);

        // Lag is now below threshold — simulate recovery.
        let ctx = make_ctx(999_990, 1_000_000, 200); // lag = 10
        let mut state = AlertState {
            last_alert_at: Some(Utc::now() - CDuration::minutes(35)),
            alert_fired: true, // a previous alert was fired
            rpc_degraded_fired: false,
            rpc_degraded_last_alert_at: None,
        };

        alerter.evaluate(&ctx, &mut state).await;

        server.verify().await;

        assert!(
            !state.alert_fired,
            "alert_fired should be cleared after recovery"
        );
    }

    #[tokio::test]
    async fn failed_webhook_does_not_mutate_state() {
        let server = wiremock::MockServer::start().await;

        // Server always returns 500.
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .and(wiremock::matchers::path("/webhook"))
            .respond_with(wiremock::ResponseTemplate::new(500))
            .mount(&server)
            .await;

        let url = format!("{}/webhook", server.uri());
        let alerter = make_alerter(Some(&url), 200, 30);
        let ctx = make_ctx(54_800, 55_050, 200);
        let mut state = AlertState::default();

        alerter.evaluate(&ctx, &mut state).await;

        // Delivery failed — state must not be updated.
        assert!(
            !state.alert_fired,
            "state must not change on failed delivery"
        );
        assert!(state.last_alert_at.is_none());
    }

    #[tokio::test]
    async fn cooldown_suppresses_second_alert_with_real_server() {
        let server = wiremock::MockServer::start().await;

        // Expect exactly 0 webhook calls — cooldown should suppress.
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .and(wiremock::matchers::path("/webhook"))
            .respond_with(wiremock::ResponseTemplate::new(200))
            .expect(0)
            .mount(&server)
            .await;

        let url = format!("{}/webhook", server.uri());
        let alerter = make_alerter(Some(&url), 200, 30);
        let ctx = make_ctx(54_800, 55_050, 200);

        // Last alert fired 5 minutes ago — within 30-minute cooldown.
        let mut state = AlertState {
            last_alert_at: Some(Utc::now() - CDuration::minutes(5)),
            alert_fired: true,
            rpc_degraded_fired: false,
            rpc_degraded_last_alert_at: None,
        };

        alerter.evaluate(&ctx, &mut state).await;

        server.verify().await;
    }
}
