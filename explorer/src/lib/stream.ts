import type { Network } from './types';

/**
 * One event from the live SSE stream. The upstream publishes the Redis stream
 * entry verbatim: a flat object whose values are all strings — numbers are
 * stringified and `topics`/`data` are JSON-encoded strings. This type is the
 * decoded form. `event_id` (the database row id, usable for detail links) is
 * only present when the publisher had it, so it is optional here.
 */
export interface StreamEvent {
  contract_id: string;
  ledger_sequence: number;
  ledger_timestamp: string;
  transaction_hash: string;
  event_index: number;
  event_type: string;
  topics: string[];
  data: string;
  event_id?: string;
}

export type StreamStatus = 'connecting' | 'live' | 'disconnected';

export interface StreamOptions {
  contractId: string;
  network: Network;
  topic0?: string;
  onEvent: (event: StreamEvent) => void;
  onStatus: (status: StreamStatus) => void;
}

export interface StreamHandle {
  close: () => void;
}

interface RawStreamPayload {
  contract_id?: string;
  ledger_sequence?: string;
  ledger_timestamp?: string;
  transaction_hash?: string;
  event_index?: string;
  event_type?: string;
  topics?: string;
  data?: string;
  event_id?: string;
}

function parseStreamEvent(raw: string): StreamEvent | null {
  let payload: RawStreamPayload;
  try {
    payload = JSON.parse(raw) as RawStreamPayload;
  } catch {
    return null;
  }
  if (!payload.contract_id || !payload.ledger_sequence) return null;

  let topics: string[] = [];
  try {
    const parsed: unknown = JSON.parse(payload.topics ?? '[]');
    if (Array.isArray(parsed)) topics = parsed.filter((t): t is string => typeof t === 'string');
  } catch {
    // leave topics empty on malformed payload
  }

  return {
    contract_id: payload.contract_id,
    ledger_sequence: Number(payload.ledger_sequence),
    ledger_timestamp: payload.ledger_timestamp ?? '',
    transaction_hash: payload.transaction_hash ?? '',
    event_index: Number(payload.event_index ?? '0'),
    event_type: payload.event_type ?? 'contract',
    topics,
    data: payload.data ?? '',
    event_id: payload.event_id,
  };
}

const BASE_DELAY_MS = 1_000;
const MAX_DELAY_MS = 30_000;
const MAX_ATTEMPTS = 8;

/**
 * Subscribe to a contract's live event stream through the /api/stream proxy.
 *
 * EventSource does reconnect on its own, but with no backoff and no ceiling —
 * a dead API would be hammered once a second forever. So reconnection is
 * managed here instead: close on error, retry with exponential backoff, and
 * give up (status "disconnected") after MAX_ATTEMPTS consecutive failures.
 */
export function subscribeToContract(options: StreamOptions): StreamHandle {
  let source: EventSource | null = null;
  let attempts = 0;
  let retryTimer: ReturnType<typeof setTimeout> | null = null;
  let closed = false;

  const connect = () => {
    if (closed) return;
    options.onStatus('connecting');

    const params = new URLSearchParams({
      contractId: options.contractId,
      network: options.network,
    });
    if (options.topic0) params.set('topic0', options.topic0);

    source = new EventSource(`/api/stream?${params.toString()}`);

    source.onopen = () => {
      attempts = 0;
      options.onStatus('live');
    };

    source.onmessage = (msg: MessageEvent<string>) => {
      const event = parseStreamEvent(msg.data);
      if (event) options.onEvent(event);
    };

    source.onerror = () => {
      source?.close();
      source = null;
      if (closed) return;
      attempts += 1;
      if (attempts >= MAX_ATTEMPTS) {
        options.onStatus('disconnected');
        return;
      }
      options.onStatus('connecting');
      const delay = Math.min(BASE_DELAY_MS * 2 ** (attempts - 1), MAX_DELAY_MS);
      retryTimer = setTimeout(connect, delay);
    };
  };

  connect();

  return {
    close: () => {
      closed = true;
      if (retryTimer !== null) clearTimeout(retryTimer);
      source?.close();
      source = null;
    },
  };
}
