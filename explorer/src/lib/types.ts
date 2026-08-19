export interface SorobanEvent {
  id: string;
  contract_id: string;
  ledger_sequence: number;
  ledger_timestamp: string;
  transaction_hash: string;
  event_index: number;
  event_type: string;
  topics: string[];
  data: string;
  created_at: string;
}

export interface ListEventsResponse {
  events: SorobanEvent[];
  has_more: boolean;
  next_cursor: string | null;
}

export type Network = 'testnet' | 'mainnet';

export type IndexerStatus = 'healthy' | 'lagging' | 'stalled';

export interface IndexerStats {
  status: IndexerStatus;
  network: string;
  last_ledger_indexed: number | null;
  chain_tip_ledger: number | null;
  lag_ledgers: number | null;
  lag_seconds_estimated: number | null;
}
