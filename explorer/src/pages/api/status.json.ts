import type { APIRoute } from 'astro';
import { getIndexerStats } from '../../lib/api';
import type { Network } from '../../lib/types';

export const GET: APIRoute = async ({ url }) => {
  const rawNetwork = url.searchParams.get('network');
  const network: Network = rawNetwork === 'mainnet' ? 'mainnet' : 'testnet';
  try {
    const stats = await getIndexerStats(network);
    return new Response(JSON.stringify(stats), {
      headers: {
        'Content-Type': 'application/json',
        'Cache-Control': 'no-store',
      },
    });
  } catch {
    // Covers both the documented 503 and an unreachable API — the page shows
    // "indexer unreachable" either way.
    return new Response(JSON.stringify({ error: 'indexer unreachable' }), {
      status: 503,
      headers: { 'Content-Type': 'application/json' },
    });
  }
};
