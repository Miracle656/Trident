//! # Parser
//!
//! Owns XDR decoding and event normalisation. Responsibilities:
//!
//! - Decoding raw base64-encoded XDR strings as returned by the Soroban RPC
//!   `getEvents` method into typed Rust values via the `stellar-xdr` crate.
//! - Normalising decoded `ScVal` topics into human-readable string representations
//!   and the event body into a `serde_json::Value` for storage and forwarding.
//! - Type coercion: Symbol/String → plain string, Address → strkey, I128/U128 →
//!   decimal string, Bool → "true"/"false", Bytes → hex, Map/Vec → JSON object/array.
//! - Returning `TridentError::ParseError` for any input that cannot be decoded so
//!   the caller (Streamer) can decide whether to skip or halt.

use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::Value as Json;
use stellar_strkey::{ed25519, Contract};
use stellar_xdr::curr::{
    AccountId, ContractId, Limited, Limits, PublicKey, ReadXdr, ScAddress, ScVal,
};
use trident_common::{EventType, SorobanEvent, TridentError};

use crate::rpc::RawEvent;

pub mod invocation_metrics;
pub mod nft_events;
pub mod sac;
pub mod token_events;

use nft_events::NftEvent;
use sac::SacRegistry;
use token_events::TokenEvent;

/// A normalised event together with its optional typed projections.
pub struct ParsedEvent {
    pub event: SorobanEvent,
    /// `Some` only for standard SEP-41 / SAC value-movement events whose payload
    /// matches the token interface layout.
    pub token: Option<TokenEvent>,
    /// `Some` only for NFT mint/transfer events (issue #275).
    #[allow(dead_code)]
    pub nft: Option<NftEvent>,
}

pub struct Parser {
    pub index_diagnostic: bool,
    /// Tracked SAC contract id -> asset context lookup (issue #262). Empty
    /// when no assets are configured; every contract then decodes with no
    /// asset context, same as before this feature existed.
    sac_registry: SacRegistry,
}

impl Parser {
    pub fn new(index_diagnostic: bool) -> Self {
        Self {
            index_diagnostic,
            sac_registry: SacRegistry::default(),
        }
    }

    /// Attach a pre-built SAC registry (issue #262). Kept separate from `new`
    /// so callers that don't track any assets pay no extra setup cost.
    pub fn with_sac_registry(mut self, sac_registry: SacRegistry) -> Self {
        self.sac_registry = sac_registry;
        self
    }

    /// Decode a raw RPC event into a normalised `SorobanEvent` plus, when the
    /// payload matches the standard token interface, a typed token projection
    /// (issue #211).
    ///
    /// The topic and body `ScVal`s are decoded once and reused for both outputs,
    /// so the projection costs no extra XDR work.
    pub fn parse_event_with_projection(
        &self,
        raw: &RawEvent,
    ) -> Result<Option<ParsedEvent>, TridentError> {
        let event_type = parse_event_type(&raw.event_type)?;

        if event_type == EventType::Diagnostic && !self.index_diagnostic {
            return Ok(None);
        }

        // Skip events emitted by failed contract calls — they have no observable effect.
        if !raw.in_successful_contract_call {
            return Ok(None);
        }

        let contract_id = raw.contract_id.clone().unwrap_or_default();

        let topic_vals: Vec<ScVal> = raw
            .topic
            .iter()
            .map(|xdr| decode_scval(xdr))
            .collect::<Result<_, _>>()?;
        let topics: Vec<String> = topic_vals.iter().map(scval_to_string).collect();

        let (data, data_val) = if raw.value.is_empty() {
            (Json::Null, ScVal::Void)
        } else {
            let val = decode_scval(&raw.value)?;
            (scval_to_json(&val), val)
        };

        // Attach asset context when the emitting contract is a recognised SAC
        // instance (issue #262). Any token-interface contract is eligible for
        // typed projection; only known SACs also carry asset_code/issuer.
        let token = token_events::decode_token_event(&topic_vals, &data_val).map(|mut event| {
            if let Some((code, issuer)) = self.sac_registry.lookup(&contract_id) {
                event.asset_code = Some(code.to_string());
                event.asset_issuer = Some(issuer.to_string());
            }
            event
        });
        let nft = nft_events::decode_nft_event(&topic_vals, &data_val);

        let ledger_sequence: u64 = raw
            .ledger
            .parse()
            .map_err(|_| TridentError::parse(anyhow::anyhow!("invalid ledger: {}", raw.ledger)))?;

        let event_index = raw_event_index(raw);

        Ok(Some(ParsedEvent {
            event: SorobanEvent {
                contract_id,
                topics,
                data,
                ledger_sequence,
                ledger_timestamp: raw.ledger_closed_at.clone(),
                transaction_hash: raw.tx_hash.clone(),
                event_index,
                event_type,
            },
            token,
            nft,
        }))
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Position of an event within its transaction.
///
/// `soroban_events` carries a natural-key constraint on
/// `(ledger_sequence, transaction_hash, event_index, network)` (migration
/// 0025), so this must distinguish every event sharing a transaction or the
/// batch insert aborts with a duplicate-key violation.
///
/// It used to be scraped off the tail of the opaque `id`, which was formatted
/// `"{encoded}-{index}"`. stellar-rpc#382 changed `id`, so the parse silently
/// fell through to `unwrap_or(0)` and stamped *every* event with 0, colliding
/// the whole batch (issue #388).
///
/// stellar-rpc#383 added an explicit `operationIndex`, which is the correct
/// source where available; servers predating it omit the field, so the legacy
/// `id` suffix stays as the fallback, and 0 is the last resort.
///
/// A single operation can emit several events, so this alone is not guaranteed
/// unique within a transaction — [`assign_unique_event_indexes`] resolves any
/// remaining ties before insert.
pub(crate) fn raw_event_index(raw: &RawEvent) -> u32 {
    raw.operation_index
        .or_else(|| {
            raw.id
                .split('-')
                .next_back()
                .and_then(|s| s.parse::<u32>().ok())
        })
        .unwrap_or(0)
}

/// Break ties so `(ledger_sequence, transaction_hash, event_index, network)`
/// is unique across a batch, as migration 0025's constraint requires.
///
/// `raw_event_index` derives a position per event, but a single operation can
/// emit several events, and older servers may not supply an index at all — in
/// both cases two events in the same transaction land on the same value and
/// the batch insert aborts with a duplicate-key violation (issue #388).
///
/// Walks the batch in order and, whenever a `(ledger, tx_hash, index)` triple
/// repeats, advances the index to the next free slot. Events that already have
/// distinct indices are left untouched, so behaviour against a server that
/// reports them correctly is unchanged.
pub(crate) fn assign_unique_event_indexes(events: &mut [SorobanEvent]) {
    let mut seen: std::collections::HashSet<(u64, String, u32)> = std::collections::HashSet::new();
    for event in events.iter_mut() {
        while !seen.insert((
            event.ledger_sequence,
            event.transaction_hash.clone(),
            event.event_index,
        )) {
            event.event_index = event.event_index.saturating_add(1);
        }
    }
}

fn parse_event_type(raw: &str) -> Result<EventType, TridentError> {
    match raw {
        "contract" => Ok(EventType::Contract),
        "system" => Ok(EventType::System),
        "diagnostic" => Ok(EventType::Diagnostic),
        other => Err(TridentError::parse(anyhow::anyhow!(
            "unknown event type: {other}"
        ))),
    }
}

pub(crate) fn decode_scval(b64: &str) -> Result<ScVal, TridentError> {
    let bytes = STANDARD
        .decode(b64)
        .map_err(|e| TridentError::parse(anyhow::Error::new(e).context("base64 decode")))?;
    let mut cursor = std::io::Cursor::new(bytes);
    ScVal::read_xdr(&mut Limited::new(&mut cursor, Limits::none()))
        .map_err(|e| TridentError::parse(anyhow::Error::new(e).context("XDR decode ScVal")))
}

/// Convert a topic `ScVal` to a compact string representation.
pub fn scval_to_string(val: &ScVal) -> String {
    match val {
        ScVal::Symbol(s) => s.to_utf8_string_lossy(),
        ScVal::String(s) => s.to_utf8_string_lossy(),
        ScVal::Bool(b) => b.to_string(),
        ScVal::Void => "void".into(),
        ScVal::U32(n) => n.to_string(),
        ScVal::I32(n) => n.to_string(),
        ScVal::U64(n) => n.to_string(),
        ScVal::I64(n) => n.to_string(),
        ScVal::U128(parts) => {
            let val = ((parts.hi as u128) << 64) | (parts.lo as u128);
            val.to_string()
        }
        ScVal::I128(parts) => {
            let val = ((parts.hi as i128) << 64) | (parts.lo as i128);
            val.to_string()
        }
        ScVal::U256(parts) => {
            let val = ((parts.hi_hi as u128) << 96)
                | ((parts.hi_lo as u128) << 64)
                | ((parts.lo_hi as u128) << 32)
                | (parts.lo_lo as u128);
            val.to_string()
        }
        ScVal::I256(parts) => {
            let val = ((parts.hi_hi as i128) << 96)
                | ((parts.hi_lo as i128) << 64)
                | ((parts.lo_hi as i128) << 32)
                | (parts.lo_lo as i128);
            val.to_string()
        }
        ScVal::Bytes(b) => hex::encode(b.as_slice()),
        ScVal::Address(addr) => scaddress_to_string(addr),
        // For complex types in topic position, fall back to debug representation
        other => format!("{other:?}"),
    }
}

/// Recursively convert a `ScVal` to a `serde_json::Value` for the event body.
pub fn scval_to_json(val: &ScVal) -> Json {
    match val {
        ScVal::Void => Json::Null,
        ScVal::Bool(b) => Json::Bool(*b),
        ScVal::Symbol(s) => Json::String(s.to_utf8_string_lossy()),
        ScVal::String(s) => Json::String(s.to_utf8_string_lossy()),
        ScVal::U32(n) => Json::from(*n),
        ScVal::I32(n) => Json::from(*n),
        ScVal::U64(n) => Json::from(*n),
        ScVal::I64(n) => Json::from(*n),
        ScVal::U128(parts) => {
            let v = ((parts.hi as u128) << 64) | (parts.lo as u128);
            // Use string for values that overflow JSON's safe integer range
            if v <= u64::MAX as u128 {
                Json::from(v as u64)
            } else {
                Json::String(v.to_string())
            }
        }
        ScVal::I128(parts) => {
            let v = ((parts.hi as i128) << 64) | (parts.lo as i128);
            if v >= i64::MIN as i128 && v <= i64::MAX as i128 {
                Json::from(v as i64)
            } else {
                Json::String(v.to_string())
            }
        }
        ScVal::U256(parts) => {
            let val = ((parts.hi_hi as u128) << 96)
                | ((parts.hi_lo as u128) << 64)
                | ((parts.lo_hi as u128) << 32)
                | (parts.lo_lo as u128);
            Json::String(val.to_string())
        }
        ScVal::I256(parts) => {
            let val = ((parts.hi_hi as i128) << 96)
                | ((parts.hi_lo as i128) << 64)
                | ((parts.lo_hi as i128) << 32)
                | (parts.lo_lo as i128);
            Json::String(val.to_string())
        }
        ScVal::Bytes(b) => Json::String(hex::encode(b.as_slice())),
        ScVal::Address(addr) => Json::String(scaddress_to_string(addr)),
        ScVal::Vec(Some(items)) => Json::Array(items.iter().map(scval_to_json).collect()),
        ScVal::Vec(None) => Json::Array(vec![]),
        ScVal::Map(Some(entries)) => {
            let obj: serde_json::Map<String, Json> = entries
                .iter()
                .map(|e| (scval_to_string(&e.key), scval_to_json(&e.val)))
                .collect();
            Json::Object(obj)
        }
        ScVal::Map(None) => Json::Object(serde_json::Map::new()),
        other => Json::String(format!("{other:?}")),
    }
}

pub(crate) fn scaddress_to_string(addr: &ScAddress) -> String {
    match addr {
        ScAddress::Account(AccountId(PublicKey::PublicKeyTypeEd25519(bytes))) => {
            // stellar-strkey 0.0.16+ returns heapless::String — convert to std::String
            ed25519::PublicKey(bytes.0).to_string().as_str().to_owned()
        }
        // stellar-xdr 26.x wraps the hash in ContractId; the inner Hash holds [u8; 32]
        ScAddress::Contract(ContractId(hash)) => Contract(hash.0).to_string().as_str().to_owned(),
        // stellar-xdr 26.x added MuxedAccount, ClaimableBalance, LiquidityPool variants;
        // these do not appear in Soroban contract events but the match must be exhaustive.
        other => format!("{other:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{engine::general_purpose::STANDARD, Engine};
    use stellar_xdr::curr::{
        AccountId, ContractId, Hash, Int128Parts, Limited, Limits, PublicKey, ScAddress, ScMap,
        ScMapEntry, ScSymbol, ScVal, Uint256, VecM, WriteXdr,
    };

    use crate::rpc::RawEvent;

    fn ev(ledger: u64, tx: &str, index: u32) -> SorobanEvent {
        SorobanEvent {
            contract_id: "C".to_string(),
            topics: Vec::new(),
            data: serde_json::json!(null),
            ledger_sequence: ledger,
            ledger_timestamp: "2026-08-09T20:00:00Z".to_string(),
            transaction_hash: tx.to_string(),
            event_index: index,
            event_type: EventType::Contract,
        }
    }

    #[test]
    fn duplicate_event_indexes_within_a_transaction_are_separated() {
        // Regression (#388): every event arrived with index 0, so the batch
        // violated the (ledger, tx_hash, event_index, network) constraint from
        // migration 0025 and no events were ever inserted.
        let mut events = vec![ev(7, "aa", 0), ev(7, "aa", 0), ev(7, "aa", 0)];
        assign_unique_event_indexes(&mut events);
        let indexes: Vec<u32> = events.iter().map(|e| e.event_index).collect();
        assert_eq!(indexes, vec![0, 1, 2]);
    }

    #[test]
    fn distinct_event_indexes_are_left_alone() {
        // A server that reports operationIndex correctly must not be perturbed.
        let mut events = vec![ev(7, "aa", 0), ev(7, "aa", 1), ev(7, "aa", 2)];
        assign_unique_event_indexes(&mut events);
        let indexes: Vec<u32> = events.iter().map(|e| e.event_index).collect();
        assert_eq!(indexes, vec![0, 1, 2]);
    }

    #[test]
    fn the_key_is_scoped_per_transaction_and_ledger() {
        // The constraint includes tx_hash and ledger, so the same index in a
        // different transaction or ledger is legitimate and must be preserved.
        let mut events = vec![ev(7, "aa", 0), ev(7, "bb", 0), ev(8, "aa", 0)];
        assign_unique_event_indexes(&mut events);
        let indexes: Vec<u32> = events.iter().map(|e| e.event_index).collect();
        assert_eq!(indexes, vec![0, 0, 0]);
    }

    #[test]
    fn event_index_prefers_operation_index_over_the_legacy_id_suffix() {
        let mut raw = raw_event_fixture();
        raw.id = "0000000000000000007-0000000009".to_string();
        raw.operation_index = Some(3);
        assert_eq!(raw_event_index(&raw), 3);
    }

    #[test]
    fn event_index_falls_back_to_the_id_suffix_on_older_servers() {
        let mut raw = raw_event_fixture();
        raw.id = "0000000000000000007-0000000009".to_string();
        raw.operation_index = None;
        assert_eq!(raw_event_index(&raw), 9);
    }

    fn xdr_b64(val: &ScVal) -> String {
        let mut buf = Vec::new();
        val.write_xdr(&mut Limited::new(&mut buf, Limits::none()))
            .expect("XDR encode failed");
        STANDARD.encode(buf)
    }

    fn sym(s: &str) -> ScVal {
        ScVal::Symbol(ScSymbol::try_from(s.to_string()).expect("symbol too long"))
    }

    fn make_event(
        event_type: &str,
        contract_id: Option<&str>,
        topics: Vec<ScVal>,
        value: ScVal,
        successful: bool,
    ) -> RawEvent {
        RawEvent {
            event_type: event_type.to_string(),
            ledger: "500".to_string(),
            ledger_closed_at: "2024-06-01T00:00:00Z".to_string(),
            contract_id: contract_id.map(str::to_string),
            id: "0000000000500000-0".to_string(),
            paging_token: Some("token1".to_string()),
            tx_hash: "deadbeefdeadbeef".to_string(),
            operation_index: None,
            topic: topics.iter().map(xdr_b64).collect(),
            value: xdr_b64(&value),
            in_successful_contract_call: successful,
        }
    }

    fn raw_event_fixture() -> RawEvent {
        make_event("contract", Some("CA"), vec![], ScVal::Void, true)
    }

    #[test]
    fn token_transfer_topics_and_amount_decoded() {
        // Standard SEP-41 transfer: topics=[Symbol("transfer"), Addr(from), Addr(to)], data=I128(amount)
        let from = ScVal::Address(ScAddress::Account(AccountId(
            PublicKey::PublicKeyTypeEd25519(Uint256([1u8; 32])),
        )));
        let to = ScVal::Address(ScAddress::Account(AccountId(
            PublicKey::PublicKeyTypeEd25519(Uint256([2u8; 32])),
        )));
        let amount = ScVal::I128(Int128Parts {
            hi: 0,
            lo: 1_000_000,
        });

        let contract_id = "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD2KM";
        let raw = make_event(
            "contract",
            Some(contract_id),
            vec![sym("transfer"), from, to],
            amount,
            true,
        );

        let parser = Parser::new(false);
        let event = parser
            .parse_event_with_projection(&raw)
            .unwrap()
            .unwrap()
            .event;

        assert_eq!(event.contract_id, contract_id);
        assert_eq!(event.topics[0], "transfer");
        assert_eq!(event.data, serde_json::json!(1_000_000u64));
        assert_eq!(event.ledger_sequence, 500);
        assert_eq!(event.transaction_hash, "deadbeefdeadbeef");
    }

    #[test]
    fn mint_event_symbol_and_address_topics() {
        let to = ScVal::Address(ScAddress::Account(AccountId(
            PublicKey::PublicKeyTypeEd25519(Uint256([3u8; 32])),
        )));
        let amount = ScVal::I128(Int128Parts { hi: 0, lo: 5_000 });

        let raw = make_event(
            "contract",
            Some("CONTRACT"),
            vec![sym("mint"), to],
            amount,
            true,
        );

        let parser = Parser::new(false);
        let event = parser
            .parse_event_with_projection(&raw)
            .unwrap()
            .unwrap()
            .event;

        assert_eq!(event.topics[0], "mint");
        assert_eq!(event.data, serde_json::json!(5_000u64));
    }

    #[test]
    fn transfer_event_yields_a_token_projection() {
        let from = ScVal::Address(ScAddress::Account(AccountId(
            PublicKey::PublicKeyTypeEd25519(Uint256([1u8; 32])),
        )));
        let to = ScVal::Address(ScAddress::Account(AccountId(
            PublicKey::PublicKeyTypeEd25519(Uint256([2u8; 32])),
        )));
        let amount = ScVal::I128(Int128Parts { hi: 0, lo: 7_500 });

        let raw = make_event(
            "contract",
            Some("CTOKEN"),
            vec![sym("transfer"), from, to],
            amount,
            true,
        );

        let parsed = Parser::new(false)
            .parse_event_with_projection(&raw)
            .unwrap()
            .unwrap();
        let token = parsed.token.expect("transfer must produce a projection");
        assert_eq!(token.amount.as_deref(), Some("7500"));
        assert_eq!(parsed.event.contract_id, "CTOKEN");
    }

    #[test]
    fn non_token_event_yields_no_projection() {
        let raw = make_event(
            "contract",
            Some("CAPP"),
            vec![sym("swap")],
            ScVal::Void,
            true,
        );
        let parsed = Parser::new(false)
            .parse_event_with_projection(&raw)
            .unwrap()
            .unwrap();
        assert!(
            parsed.token.is_none(),
            "a non-token event must not be projected"
        );
    }

    #[test]
    fn map_event_data_decodes_to_json_object() {
        let entries: Vec<ScMapEntry> = vec![
            ScMapEntry {
                key: ScVal::Symbol(ScSymbol::try_from("amount".to_string()).unwrap()),
                val: ScVal::I128(Int128Parts { hi: 0, lo: 100 }),
            },
            ScMapEntry {
                key: ScVal::Symbol(ScSymbol::try_from("fee".to_string()).unwrap()),
                val: ScVal::I128(Int128Parts { hi: 0, lo: 1 }),
            },
        ];
        let map_val = ScVal::Map(Some(ScMap(VecM::try_from(entries).unwrap())));
        let raw = make_event("contract", None, vec![sym("custom")], map_val, true);

        let parser = Parser::new(false);
        let event = parser
            .parse_event_with_projection(&raw)
            .unwrap()
            .unwrap()
            .event;

        let obj = event
            .data
            .as_object()
            .expect("data should be a JSON object");
        assert_eq!(obj["amount"], serde_json::json!(100u64));
        assert_eq!(obj["fee"], serde_json::json!(1u64));
    }

    #[test]
    fn diagnostic_event_skipped_when_index_diagnostic_false() {
        let raw = make_event("diagnostic", None, vec![sym("debug")], ScVal::Void, true);
        let parser = Parser::new(false);
        assert!(
            parser.parse_event_with_projection(&raw).unwrap().is_none(),
            "diagnostic events must be skipped when index_diagnostic=false"
        );
    }

    #[test]
    fn diagnostic_event_included_when_index_diagnostic_true() {
        let raw = make_event("diagnostic", None, vec![sym("debug")], ScVal::Void, true);
        let parser = Parser::new(true);
        assert!(
            parser.parse_event_with_projection(&raw).unwrap().is_some(),
            "diagnostic events must be indexed when index_diagnostic=true"
        );
    }

    #[test]
    fn failed_contract_call_filtered() {
        let raw = make_event("contract", None, vec![sym("transfer")], ScVal::Void, false);
        let parser = Parser::new(false);
        assert!(
            parser.parse_event_with_projection(&raw).unwrap().is_none(),
            "events from failed contract calls must be filtered out"
        );
    }

    #[test]
    fn large_i128_decoded_as_json_string() {
        // i128::MAX — the largest value the decoder can represent, and well
        // beyond the 2^53 range a JSON number can carry losslessly, so it must
        // come back as a decimal string rather than a number.
        let v = ScVal::I128(Int128Parts {
            hi: (i128::MAX >> 64) as i64,
            lo: u64::MAX,
        });

        let raw = make_event("contract", None, vec![], v, true);
        let event = Parser::new(false)
            .parse_event_with_projection(&raw)
            .unwrap()
            .unwrap()
            .event;

        assert!(
            event.data.is_string(),
            "large i128 must be a JSON string, got: {event:?}"
        );
        assert_eq!(
            event.data.as_str().unwrap(),
            "170141183460469231731687303715884105727",
            "exact decimal string must survive round-trip XDR -> JSON"
        );
    }

    #[test]
    fn u256_decoded_as_decimal_string() {
        let v = ScVal::U256(stellar_xdr::curr::UInt256Parts {
            hi_hi: 0,
            hi_lo: 0,
            lo_hi: 0,
            lo_lo: 1,
        });

        let raw = make_event("contract", None, vec![], v, true);
        let event = Parser::new(false)
            .parse_event_with_projection(&raw)
            .unwrap()
            .unwrap()
            .event;

        assert_eq!(
            event.data,
            serde_json::json!("1"),
            "u256(1) must encode as \"1\""
        );
    }

    #[test]
    fn i256_decoded_as_decimal_string() {
        // -1 as a 256-bit two's-complement integer: every limb is all-ones.
        // `hi_hi` is the signed high limb; the rest are unsigned.
        let v = ScVal::I256(stellar_xdr::curr::Int256Parts {
            hi_hi: -1,
            hi_lo: u64::MAX,
            lo_hi: u64::MAX,
            lo_lo: u64::MAX,
        });

        let raw = make_event("contract", None, vec![], v, true);
        let event = Parser::new(false)
            .parse_event_with_projection(&raw)
            .unwrap()
            .unwrap()
            .event;

        assert_eq!(
            event.data,
            serde_json::json!("-1"),
            "i256(-1) must encode as \"-1\""
        );
    }

    #[test]
    fn small_i128_remains_json_number() {
        let v = ScVal::I128(Int128Parts { hi: 0, lo: 123 });
        let raw = make_event("contract", None, vec![], v, true);
        let event = Parser::new(false)
            .parse_event_with_projection(&raw)
            .unwrap()
            .unwrap()
            .event;

        assert!(
            event.data.is_number(),
            "small i128 that fits in i64 should remain a JSON number: {event:?}",
        );
        assert_eq!(event.data, serde_json::json!(123));
    }

    #[test]
    fn contract_address_topic_decoded_to_strkey() {
        let contract_hash = [0xABu8; 32];
        let addr = ScVal::Address(ScAddress::Contract(ContractId(Hash(contract_hash))));
        let raw = make_event(
            "contract",
            None,
            vec![sym("event"), addr],
            ScVal::Void,
            true,
        );

        let parser = Parser::new(false);
        let event = parser
            .parse_event_with_projection(&raw)
            .unwrap()
            .unwrap()
            .event;

        assert!(
            event.topics[1].starts_with('C'),
            "contract strkey must start with C, got: {}",
            event.topics[1]
        );
        assert_eq!(
            event.topics[1].len(),
            56,
            "contract strkey must be 56 chars"
        );
    }

    // -----------------------------------------------------------------------
    // SAC asset context (issue #262)
    //
    // The fixture JSON carries topics/data/expected exactly like the plain
    // token_events fixtures; the tracked asset's SAC contract id is derived at
    // test time (rather than hardcoded) and used as the fixture event's
    // contract_id, so the test stays correct regardless of the exact strkey
    // the derivation produces.
    // -----------------------------------------------------------------------

    const TEST_PASSPHRASE: &str = "Test SDF Network ; September 2015";

    #[test]
    fn sac_transfer_fixture_gets_asset_context_attached() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../fixtures/token_events/sac_transfer.json"
        ))
        .expect("fixture JSON");

        let asset_code = fixture["asset_code"].as_str().unwrap();
        let asset_issuer = fixture["asset_issuer"].as_str().unwrap();
        let contract_id =
            sac::derive_sac_contract_id(asset_code, asset_issuer, TEST_PASSPHRASE).unwrap();

        let registry = SacRegistry::build(
            &[sac::TrackedAsset {
                code: asset_code.to_string(),
                issuer: asset_issuer.to_string(),
            }],
            TEST_PASSPHRASE,
        )
        .unwrap();

        let topics: Vec<ScVal> = fixture["topics"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| decode_scval(t.as_str().unwrap()).unwrap())
            .collect();
        let value = decode_scval(fixture["data"].as_str().unwrap()).unwrap();

        let raw = make_event("contract", Some(&contract_id), topics, value, true);

        let parser = Parser::new(false).with_sac_registry(registry);
        let parsed = parser.parse_event_with_projection(&raw).unwrap().unwrap();
        let token = parsed.token.expect("transfer must produce a projection");

        let expected = &fixture["expected"];
        assert_eq!(token.amount.as_deref(), expected["amount"].as_str());
        assert_eq!(token.asset_code.as_deref(), Some(asset_code));
        assert_eq!(token.asset_issuer.as_deref(), Some(asset_issuer));
    }

    #[test]
    fn sac_transfer_from_an_untracked_contract_gets_no_asset_context() {
        // Same event shape as sac_transfer.json, but the contract_id is an
        // arbitrary contract not present in the SAC registry — this must not
        // be misattributed to any tracked asset.
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../fixtures/token_events/sac_transfer_untracked.json"
        ))
        .expect("fixture JSON");

        let registry = SacRegistry::build(
            &[sac::TrackedAsset {
                code: "USDC".to_string(),
                issuer: "GAIRCEIRCEIRCEIRCEIRCEIRCEIRCEIRCEIRCEIRCEIRCEIRCEIRCF6M".to_string(),
            }],
            TEST_PASSPHRASE,
        )
        .unwrap();

        let topics: Vec<ScVal> = fixture["topics"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| decode_scval(t.as_str().unwrap()).unwrap())
            .collect();
        let value = decode_scval(fixture["data"].as_str().unwrap()).unwrap();

        let raw = make_event(
            "contract",
            Some("CARBITRARYUNRELATEDCONTRACTNOTINREGISTRY"),
            topics,
            value,
            true,
        );

        let parser = Parser::new(false).with_sac_registry(registry);
        let parsed = parser.parse_event_with_projection(&raw).unwrap().unwrap();
        let token = parsed
            .token
            .expect("transfer must still produce a projection");

        assert!(token.asset_code.is_none());
        assert!(token.asset_issuer.is_none());
    }

    #[test]
    fn parser_with_no_sac_registry_attaches_no_asset_context() {
        // Default Parser::new (no with_sac_registry call) must behave exactly
        // as before this feature existed.
        let from = ScVal::Address(ScAddress::Account(AccountId(
            PublicKey::PublicKeyTypeEd25519(Uint256([1u8; 32])),
        )));
        let to = ScVal::Address(ScAddress::Account(AccountId(
            PublicKey::PublicKeyTypeEd25519(Uint256([2u8; 32])),
        )));
        let amount = ScVal::I128(Int128Parts { hi: 0, lo: 42 });
        let raw = make_event(
            "contract",
            Some("CANYCONTRACT"),
            vec![sym("transfer"), from, to],
            amount,
            true,
        );

        let parsed = Parser::new(false)
            .parse_event_with_projection(&raw)
            .unwrap()
            .unwrap();
        let token = parsed.token.expect("transfer must produce a projection");
        assert!(token.asset_code.is_none());
        assert!(token.asset_issuer.is_none());
    }
}
