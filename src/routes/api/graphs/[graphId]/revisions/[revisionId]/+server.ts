import { json } from '@sveltejs/kit';

const BACKEND = 'http://127.0.0.1:8000';

export async function GET({ params }) {
	const graphId = String(params.graphId ?? '').trim();
	const revisionId = String(params.revisionId ?? '').trim();
	if (!graphId) return new Response('graphId is required', { status: 400 });
	if (!revisionId) return new Response('revisionId is required', { status: 400 });
	const upstream = await fetch(
		`${BACKEND}/graphs/${encodeURIComponent(graphId)}/revisions/${encodeURIComponent(revisionId)}`
	);
	if (!upstream.ok) {
		return new Response(await upstream.text(), { status: upstream.status });
	}
	return json(await upstream.json());
}

