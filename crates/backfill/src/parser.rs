use base64::{engine::general_purpose::STANDARD, Engine};
use serde::Deserialize;
use serde_json::Value as Json;
use stellar_strkey::{ed25519, Contract};
use stellar_xdr::curr::{
    AccountId, ContractId, Limited, Limits, PublicKey, ReadXdr, ScAddress, ScVal,
};
use trident_common::{EventType, SorobanEvent, TridentError};

/// Accept a field the RPC sends as either a JSON string or a JSON number.
/// `ledger` is quoted on older servers and a bare integer on current ones;
/// see the matching helper in `trident-indexer`'s rpc module.
fn string_or_number<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum StringOrNumber {
        String(String),
        Number(u64),
    }

    Ok(match StringOrNumber::deserialize(deserializer)? {
        StringOrNumber::String(s) => s,
        StringOrNumber::Number(n) => n.to_string(),
    })
}

fn default_true() -> bool {
    true
}

#[derive(Deserialize, Debug, Clone)]
pub struct RawEvent {
    #[serde(rename = "type")]
    pub event_type: String,
    #[serde(deserialize_with = "string_or_number")]
    pub ledger: String,
    #[serde(rename = "ledgerClosedAt")]
    pub ledger_closed_at: String,
    #[serde(rename = "contractId")]
    pub contract_id: Option<String>,
    pub id: String,
    /// Removed in stellar-rpc v22 (stellar-rpc#382) in favour of `id`; kept
    /// optional so older servers still parse. Use [`RawEvent::page_cursor`].
    #[serde(rename = "pagingToken", default)]
    pub paging_token: Option<String>,
    #[serde(rename = "txHash")]
    pub tx_hash: String,
    /// Operation index within the transaction, added in stellar-rpc#383.
    /// Absent on older servers, where the index was encoded in `id`.
    #[serde(rename = "operationIndex", default)]
    pub operation_index: Option<u32>,
    pub topic: Vec<String>,
    pub value: String,
    /// Deprecated upstream (stellar-rpc#4590); absent means not filtered out.
    #[serde(rename = "inSuccessfulContractCall", default = "default_true")]
    pub in_successful_contract_call: bool,
}

impl RawEvent {
    /// Token to resume paging from, preferring `pagingToken` when present and
    /// falling back to `id`, its designated replacement.
    pub fn page_cursor(&self) -> String {
        self.paging_token.clone().unwrap_or_else(|| self.id.clone())
    }
}

pub struct EventsPage {
    pub events: Vec<RawEvent>,
    #[allow(dead_code)]
    pub latest_ledger: u64,
}

pub struct Parser {
    pub index_diagnostic: bool,
}

impl Parser {
    pub fn new(index_diagnostic: bool) -> Self {
        Self { index_diagnostic }
    }

    pub fn parse_event(&self, raw: &RawEvent) -> Result<Option<SorobanEvent>, TridentError> {
        let event_type = parse_event_type(&raw.event_type)?;

        if event_type == EventType::Diagnostic && !self.index_diagnostic {
            return Ok(None);
        }

        if !raw.in_successful_contract_call {
            return Ok(None);
        }

        let contract_id = raw.contract_id.clone().unwrap_or_default();

        let topics: Vec<String> = raw
            .topic
            .iter()
            .map(|xdr| decode_scval(xdr).map(|v| scval_to_string(&v)))
            .collect::<Result<_, _>>()?;

        let data = if raw.value.is_empty() {
            Json::Null
        } else {
            decode_scval(&raw.value).map(|v| scval_to_json(&v))?
        };

        let ledger_sequence: u64 = raw
            .ledger
            .parse()
            .map_err(|_| TridentError::parse(anyhow::anyhow!("invalid ledger: {}", raw.ledger)))?;

        // Prefer the explicit operationIndex (stellar-rpc#383); the legacy
        // `id` suffix is only correct on servers predating #382, which changed
        // the `id` format and made this parse fall through to 0 for every
        // event — colliding them all on the natural key (issue #388).
        let event_index: u32 = raw.operation_index.unwrap_or_else(|| {
            raw.id
                .split('-')
                .next_back()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0)
        });

        Ok(Some(SorobanEvent {
            contract_id,
            topics,
            data,
            ledger_sequence,
            ledger_timestamp: raw.ledger_closed_at.clone(),
            transaction_hash: raw.tx_hash.clone(),
            event_index,
            event_type,
        }))
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

fn decode_scval(b64: &str) -> Result<ScVal, TridentError> {
    let bytes = STANDARD
        .decode(b64)
        .map_err(|e| TridentError::parse(anyhow::Error::new(e).context("base64 decode")))?;
    let mut cursor = std::io::Cursor::new(bytes);
    ScVal::read_xdr(&mut Limited::new(&mut cursor, Limits::none()))
        .map_err(|e| TridentError::parse(anyhow::Error::new(e).context("XDR decode ScVal")))
}

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
        ScVal::U256(parts) => format!(
            "u256({:x}{:x}{:x}{:x})",
            parts.hi_hi, parts.hi_lo, parts.lo_hi, parts.lo_lo
        ),
        ScVal::I256(parts) => format!(
            "i256({:x}{:x}{:x}{:x})",
            parts.hi_hi, parts.hi_lo, parts.lo_hi, parts.lo_lo
        ),
        ScVal::Bytes(b) => hex::encode(b.as_slice()),
        ScVal::Address(addr) => scaddress_to_string(addr),
        other => format!("{other:?}"),
    }
}

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

fn scaddress_to_string(addr: &ScAddress) -> String {
    match addr {
        ScAddress::Account(AccountId(PublicKey::PublicKeyTypeEd25519(bytes))) => {
            ed25519::PublicKey(bytes.0).to_string().as_str().to_owned()
        }
        ScAddress::Contract(ContractId(hash)) => Contract(hash.0).to_string().as_str().to_owned(),
        other => format!("{other:?}"),
    }
}
