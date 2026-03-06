import { json } from '@sveltejs/kit';

const BACKEND = 'http://127.0.0.1:8000';

export async function GET({ params }) {
	const graphId = String(params.graphId ?? '').trim();
	if (!graphId) {
		return new Response('graphId is required', { status: 400 });
	}
	const upstream = await fetch(`${BACKEND}/graphs/${encodeURIComponent(graphId)}/latest`);
	if (!upstream.ok) {
		return new Response(await upstream.text(), { status: upstream.status });
	}
	return json(await upstream.json());
}

