import type { SorobanEvent, ListEventsResponse, Network, IndexerStats } from './types';

const TESTNET_URL = import.meta.env.TRIDENT_TESTNET_API_URL ?? 'https://api.testnet.trident.dev';
const MAINNET_URL = import.meta.env.TRIDENT_MAINNET_API_URL ?? 'https://api.mainnet.trident.dev';
const API_KEY: string = import.meta.env.EXPLORER_API_KEY ?? '';

function baseUrl(network: Network): string {
  return network === 'mainnet' ? MAINNET_URL : TESTNET_URL;
}

function authHeaders(): HeadersInit {
  const h: Record<string, string> = {};
  if (API_KEY) h['X-API-Key'] = API_KEY;
  return h;
}

export interface QueryEventsParams {
  contractId?: string;
  topic0?: string;
  ledgerFrom?: number;
  ledgerTo?: number;
  cursor?: string;
  limit?: number;
  network?: Network;
}

export async function listEvents(params: QueryEventsParams = {}): Promise<ListEventsResponse> {
  const network: Network = params.network ?? 'testnet';
  const url = new URL(`${baseUrl(network)}/v1/events`);
  if (params.contractId) url.searchParams.set('contractId', params.contractId);
  if (params.topic0) url.searchParams.set('topic0', params.topic0);
  if (params.ledgerFrom != null) url.searchParams.set('ledgerFrom', String(params.ledgerFrom));
  if (params.ledgerTo != null) url.searchParams.set('ledgerTo', String(params.ledgerTo));
  if (params.cursor) url.searchParams.set('cursor', params.cursor);
  url.searchParams.set('limit', String(params.limit ?? 25));

  const res = await fetch(url.toString(), { headers: authHeaders() });
  if (!res.ok) throw new Error(`API ${res.status}`);
  return (await res.json()) as ListEventsResponse;
}

export async function getEvent(id: string, network: Network = 'testnet'): Promise<SorobanEvent> {
  const res = await fetch(`${baseUrl(network)}/v1/events/${encodeURIComponent(id)}`, {
    headers: authHeaders(),
  });
  if (!res.ok) throw new Error(`API ${res.status}`);
  const body = (await res.json()) as { event: SorobanEvent };
  return body.event;
}

export async function getIndexerStats(network: Network = 'testnet'): Promise<IndexerStats> {
  // Public endpoint (security: [] in the OpenAPI spec) — no API key attached.
  const res = await fetch(`${baseUrl(network)}/v1/stats/indexer`);
  if (!res.ok) throw new Error(`API ${res.status}`);
  return (await res.json()) as IndexerStats;
}
