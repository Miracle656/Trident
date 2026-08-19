import type { APIRoute } from 'astro';
import type { Network } from '../../lib/types';

// EventSource cannot send custom headers, so the browser connects here and
// this endpoint attaches the API key while piping the upstream SSE body
// through untouched (same pattern as /api/events.json, but streaming).
const TESTNET_URL = import.meta.env.TRIDENT_TESTNET_API_URL ?? 'https://api.testnet.trident.dev';
const MAINNET_URL = import.meta.env.TRIDENT_MAINNET_API_URL ?? 'https://api.mainnet.trident.dev';
const API_KEY: string = import.meta.env.EXPLORER_API_KEY ?? '';

const CONTRACT_ID_RE = /^C[A-Z2-7]{55}$/;

export const GET: APIRoute = async ({ url, request }) => {
  const rawNetwork = url.searchParams.get('network');
  const network: Network = rawNetwork === 'mainnet' ? 'mainnet' : 'testnet';
  const contractId = url.searchParams.get('contractId') ?? '';
  const topic0 = url.searchParams.get('topic0') ?? '';

  if (!CONTRACT_ID_RE.test(contractId)) {
    return new Response(JSON.stringify({ error: 'invalid contractId' }), {
      status: 400,
      headers: { 'Content-Type': 'application/json' },
    });
  }

  const upstream = new URL(
    `${network === 'mainnet' ? MAINNET_URL : TESTNET_URL}/v1/events/stream`
  );
  upstream.searchParams.set('contract_id', contractId);
  if (topic0) upstream.searchParams.set('topic_0', topic0);

  const headers: Record<string, string> = { Accept: 'text/event-stream' };
  if (API_KEY) headers['X-API-Key'] = API_KEY;
  // Preserve resume semantics: the browser re-sends Last-Event-ID on
  // reconnect and the upstream uses it to continue from that stream entry.
  const lastEventId = request.headers.get('last-event-id');
  if (lastEventId) headers['Last-Event-ID'] = lastEventId;

  let res: Response;
  try {
    res = await fetch(upstream.toString(), {
      headers,
      signal: request.signal,
    });
  } catch {
    return new Response(JSON.stringify({ error: 'upstream unreachable' }), {
      status: 502,
      headers: { 'Content-Type': 'application/json' },
    });
  }

  if (!res.ok || !res.body) {
    return new Response(JSON.stringify({ error: `upstream ${res.status}` }), {
      status: res.status === 429 ? 429 : 502,
      headers: { 'Content-Type': 'application/json' },
    });
  }

  return new Response(res.body, {
    status: 200,
    headers: {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-store',
      Connection: 'keep-alive',
      'X-Accel-Buffering': 'no',
    },
  });
};
