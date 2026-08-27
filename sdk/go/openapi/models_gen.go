// Code generated from JSON Schema using quicktype. DO NOT EDIT.
// To parse and unparse this JSON data, add this code to your project and do:
//
//    openAPIModels, err := UnmarshalOpenAPIModels(bytes)
//    bytes, err = openAPIModels.Marshal()

package openapi

import "time"

import "encoding/json"

func UnmarshalOpenAPIModels(data []byte) (OpenAPIModels, error) {
	var r OpenAPIModels
	err := json.Unmarshal(data, &r)
	return r, err
}

func (r *OpenAPIModels) Marshal() ([]byte, error) {
	return json.Marshal(r)
}

type OpenAPIModels struct {
	APIKeyResponse              *APIKeyResponse              `json:"APIKeyResponse,omitempty"`
	ContractEventFieldSchema    *ContractEventFieldSchema    `json:"ContractEventFieldSchema,omitempty"`
	ContractEventSchema         *ContractEventSchema         `json:"ContractEventSchema,omitempty"`
	ContractEventSchemaResponse *ContractEventSchemaResponse `json:"ContractEventSchemaResponse,omitempty"`
	ContractSpecFunction        *ContractSpecFunction        `json:"ContractSpecFunction,omitempty"`
	ContractSpecResponse        *ContractSpecResponse        `json:"ContractSpecResponse,omitempty"`
	ContractStats               *ContractStats               `json:"ContractStats,omitempty"`
	ContractStatsResponse       *ContractStatsResponse       `json:"ContractStatsResponse,omitempty"`
	ContractStorageResponse     *ContractStorageResponse     `json:"ContractStorageResponse,omitempty"`
	ContractStorageValue        *ContractStorageValue        `json:"ContractStorageValue,omitempty"`
	ErrorResponse               *ErrorResponse               `json:"ErrorResponse,omitempty"`
	EventListResponse           *EventListResponse           `json:"EventListResponse,omitempty"`
	IndexerStatsResponse        *IndexerStatsResponse        `json:"IndexerStatsResponse,omitempty"`
	LivenessResponse            *LivenessResponse            `json:"LivenessResponse,omitempty"`
	ReadyChecks                 *ReadyChecks                 `json:"ReadyChecks,omitempty"`
	ReadyResponse               *ReadyResponse               `json:"ReadyResponse,omitempty"`
	SorobanEvent                *SorobanEvent                `json:"SorobanEvent,omitempty"`
	TokenMetadataResponse       *TokenMetadataResponse       `json:"TokenMetadataResponse,omitempty"`
	VersionResponse             *VersionResponse             `json:"VersionResponse,omitempty"`
}

type APIKeyResponse struct {
	CreatedAt                                  time.Time  `json:"created_at"`
	CreatedBy                                  *string    `json:"created_by,omitempty"`
	ID                                         string     `json:"id"`
	// Raw key, returned only at creation time.           
	Key                                        *string    `json:"key,omitempty"`
	KeyPrefix                                  string     `json:"key_prefix"`
	Label                                      string     `json:"label"`
	LastUsedAt                                 time.Time  `json:"last_used_at"`
	Network                                    Network    `json:"network"`
	RateLimitTier                              string     `json:"rate_limit_tier"`
	RequestCount                               int64      `json:"request_count"`
	RevokedAt                                  *time.Time `json:"revoked_at,omitempty"`
}

type ContractEventFieldSchema struct {
	// Stable field name for this event payload position or property              
	Name                                                                   string `json:"name"`
	// Field type inferred from the contract interface or observed payloads       
	Type                                                                   string `json:"type"`
}

type ContractEventSchema struct {
	// Contract event name (topic_0)                                 
	EventName                             string                     `json:"event_name"`
	// Named fields for this event payload                           
	Fields                                []ContractEventFieldSchema `json:"fields"`
}

type ContractEventSchemaResponse struct {
	// Contract code hash for this schema version                              
	CodeHash                                             string                `json:"code_hash"`
	// Soroban contract address                                                
	ContractID                                           string                `json:"contract_id"`
	// Observed event names and their typed field schemas                      
	Events                                               []ContractEventSchema `json:"events"`
	// Network queried                                                         
	Network                                              Network               `json:"network"`
}

type ContractSpecFunction struct {
	// Exported function name       
	Name                     string `json:"name"`
}

type ContractSpecResponse struct {
	// Deployed WASM code hash this spec was parsed from                                                       
	CodeHash                                                                            string                 `json:"code_hash"`
	// Soroban contract address                                                                                
	ContractID                                                                          string                 `json:"contract_id"`
	// Primary classification derived from detected interfaces (e.g. token, nft, custom)                       
	ContractType                                                                        string                 `json:"contract_type"`
	// Functions captured from the contract's spec                                                             
	Functions                                                                           []ContractSpecFunction `json:"functions"`
	// Whether an embedded contractspecv0 section was found                                                    
	HasSpec                                                                             bool                   `json:"has_spec"`
	// Every standard interface detected from the contract's spec functions                                    
	Interfaces                                                                          []string               `json:"interfaces"`
	// Network queried                                                                                         
	Network                                                                             Network                `json:"network"`
}

type ContractStats struct {
	AvgCPUInstructions                          float64   `json:"avg_cpu_instructions"`
	AvgFeeCharged                               float64   `json:"avg_fee_charged"`
	AvgReadBytes                                float64   `json:"avg_read_bytes"`
	AvgWriteBytes                               float64   `json:"avg_write_bytes"`
	// Soroban contract address                           
	ContractID                                  string    `json:"contract_id"`
	// Total events for this contract in range            
	EventCount                                  int64     `json:"event_count"`
	InvocationCount                             int64     `json:"invocation_count"`
	// Timestamp of last event for this contract          
	LastSeenAt                                  time.Time `json:"last_seen_at"`
	// Latest ledger sequence for this contract           
	LastSeenLedger                              int64     `json:"last_seen_ledger"`
	TotalFeeCharged                             int64     `json:"total_fee_charged"`
}

type ContractStatsResponse struct {
	// Contracts sorted by event count (descending)                
	Contracts                                      []ContractStats `json:"contracts"`
	// Lower bound of queried ledger range                         
	FromLedger                                     int64           `json:"from_ledger"`
	// Timestamp when response was generated                       
	GeneratedAt                                    time.Time       `json:"generated_at"`
	// Network queried                                             
	Network                                        Network         `json:"network"`
	// Upper bound of queried ledger range                         
	ToLedger                                       int64           `json:"to_ledger"`
}

type ContractStorageResponse struct {
	// Soroban contract address                                                                                  
	ContractID                                                                            string                 `json:"contract_id"`
	// Network queried                                                                                           
	Network                                                                               Network                `json:"network"`
	// Storage snapshot values (latest, or full history when queried via /storage/history)                       
	Values                                                                                []ContractStorageValue `json:"values"`
}

type ContractStorageValue struct {
	// Human-readable decoded storage key                                          
	Key                                                                interface{} `json:"key"`
	// Ledger sequence at which this value was observed                            
	LedgerSequence                                                     int64       `json:"ledger_sequence"`
	// Timestamp this snapshot row was recorded                                    
	ObservedAt                                                         time.Time   `json:"observed_at"`
	// Base64-encoded XDR LedgerKey this value was read from                       
	StorageKey                                                         string      `json:"storage_key"`
	// Human-readable decoded value (absent when the entry was removed)            
	Value                                                              interface{} `json:"value"`
}

type ErrorResponse struct {
	Error Error `json:"error"`
}

type Error struct {
	// Error code (e.g., INVALID_ARGUMENT, INTERNAL, UNAVAILABLE)        
	Code                                                         string  `json:"code"`
	// Human-readable error message                                      
	Message                                                      string  `json:"message"`
	// Request ID for debugging                                          
	RequestID                                                    *string `json:"request_id,omitempty"`
}

type EventListResponse struct {
	// List of events                                                        
	Events                                                    []SorobanEvent `json:"events"`
	// Whether more results are available                                    
	HasMore                                                   bool           `json:"has_more"`
	// Opaque cursor for next page (null if has_more is false)               
	NextCursor                                                string         `json:"next_cursor"`
}

type SorobanEvent struct {
	// Soroban contract address                     
	ContractID                            string    `json:"contract_id"`
	// Timestamp when event was indexed             
	CreatedAt                             time.Time `json:"created_at"`
	// Event data (XDR-encoded)                     
	Data                                  string    `json:"data"`
	// Event index within transaction               
	EventIndex                            int64     `json:"event_index"`
	// Type of event                                
	EventType                             EventType `json:"event_type"`
	// Unique event identifier                      
	ID                                    string    `json:"id"`
	// Ledger sequence number                       
	LedgerSequence                        int64     `json:"ledger_sequence"`
	// Ledger timestamp in ISO 8601 format          
	LedgerTimestamp                       time.Time `json:"ledger_timestamp"`
	// Event topics (XDR-encoded)                   
	Topics                                []string  `json:"topics"`
	// Transaction hash (XDR-encoded)               
	TransactionHash                       string    `json:"transaction_hash"`
}

type IndexerStatsResponse struct {
	// Average poll duration in milliseconds                                                                             
	AvgPollDurationMS                                                                         int64                      `json:"avg_poll_duration_ms"`
	// Current chain tip ledger (from RPC)                                                                               
	ChainTipLedger                                                                            int64                      `json:"chain_tip_ledger"`
	// Cumulative events indexed                                                                                         
	EventsIndexedTotal                                                                        int64                      `json:"events_indexed_total"`
	// Events processed in last poll                                                                                     
	EventsLastPoll                                                                            int64                      `json:"events_last_poll"`
	// Number of ledgers behind chain tip                                                                                
	LagLedgers                                                                                int64                      `json:"lag_ledgers"`
	// Estimated wall-clock staleness in seconds: lag_ledgers times Stellar's protocol-target                            
	// ledger close time (~5s). Null whenever lag_ledgers is null. See                                                   
	// docs/observability/data-freshness.md for the full freshness contract this field is part                           
	// of.                                                                                                               
	LagSecondsEstimated                                                                       float64                    `json:"lag_seconds_estimated"`
	// Latest indexed ledger sequence                                                                                    
	LastLedgerIndexed                                                                         int64                      `json:"last_ledger_indexed"`
	// Timestamp of last successful poll                                                                                 
	LastPollAt                                                                                time.Time                  `json:"last_poll_at"`
	// Network name from NETWORK environment variable                                                                    
	Network                                                                                   string                     `json:"network"`
	// Indexer health status                                                                                             
	Status                                                                                    IndexerStatsResponseStatus `json:"status"`
}

type LivenessResponse struct {
	// Always "ok" while the process is up — no dependency checks.                       
	Status                                                        LivenessResponseStatus `json:"status"`
}

type ReadyChecks struct {
	// "ok" or "error: <message>"       
	GrpcAPI                      string `json:"grpc_api"`
	// "ok" or "error: <message>"       
	Postgres                     string `json:"postgres"`
	// "ok" or "error: <message>"       
	Redis                        string `json:"redis"`
}

type ReadyResponse struct {
	Checks                                                                                  ReadyChecks         `json:"checks"`
	// Ledgers behind chain tip, from system_state. Null when Postgres is unreachable or the                    
	// chain-tip cache hasn't been populated yet.                                                               
	IndexerLag                                                                              int64               `json:"indexer_lag"`
	// "degraded" when any dependency check in `checks` failed.                                                 
	Status                                                                                  ReadyResponseStatus `json:"status"`
}

type TokenMetadataResponse struct {
	// Soroban contract address                                                                          
	ContractID                                                                                string     `json:"contract_id"`
	// Token decimals, from decimals(). Null unless is_token is true.                                    
	Decimals                                                                                  *int64     `json:"decimals,omitempty"`
	// True when the contract was resolved and implements the SEP-41 read interface. False for           
	// both "not yet resolved" and "resolved, not a token".                                              
	IsToken                                                                                   bool       `json:"is_token"`
	// Token name, from name(). Null unless is_token is true.                                            
	Name                                                                                      *string    `json:"name,omitempty"`
	// Network queried                                                                                   
	Network                                                                                   Network    `json:"network"`
	// When this contract was last resolved. Null if never resolved.                                     
	ResolvedAt                                                                                *time.Time `json:"resolved_at,omitempty"`
	// Token symbol, from symbol(). Null unless is_token is true.                                        
	Symbol                                                                                    *string    `json:"symbol,omitempty"`
}

type VersionResponse struct {
	// RFC 3339 build time, or "unknown" when not injected at build time. Not typed as date-time       
	// because of that sentinel.                                                                       
	BuildTimestamp                                                                              string `json:"build_timestamp"`
	// Full git commit SHA the binary was built from, or "unknown" when not injected at build          
	// time.                                                                                           
	CommitSHA                                                                                   string `json:"commit_sha"`
	// Highest applied migration version from _sqlx_migrations, as a string. Null when no              
	// migrations have been applied yet or when Postgres is unreachable — the endpoint still           
	// returns 200 in that case so build metadata stays available during an outage.                    
	SchemaVersion                                                                               string `json:"schema_version"`
	// Semantic version tag of the running build, or "dev" for a binary built without release          
	// ldflags.                                                                                        
	Version                                                                                     string `json:"version"`
}

// Network queried
type Network string

const (
	Mainnet Network = "mainnet"
	Testnet Network = "testnet"
)

// Type of event
type EventType string

const (
	Contract   EventType = "contract"
	Diagnostic EventType = "diagnostic"
	System     EventType = "system"
)

// Indexer health status
type IndexerStatsResponseStatus string

const (
	Healthy IndexerStatsResponseStatus = "healthy"
	Lagging IndexerStatsResponseStatus = "lagging"
	Stalled IndexerStatsResponseStatus = "stalled"
)

// Always "ok" while the process is up — no dependency checks.
type LivenessResponseStatus string

const (
	PurpleOk LivenessResponseStatus = "ok"
)

// "degraded" when any dependency check in `checks` failed.
type ReadyResponseStatus string

const (
	Degraded ReadyResponseStatus = "degraded"
	FluffyOk ReadyResponseStatus = "ok"
)
