import { json } from '@sveltejs/kit';

const BACKEND = 'http://127.0.0.1:8000';

export async function GET({ params }) {
	const componentId = String(params.componentId ?? '').trim();
	const revisionId = String(params.revisionId ?? '').trim();
	if (!componentId) return new Response('componentId is required', { status: 400 });
	if (!revisionId) return new Response('revisionId is required', { status: 400 });
	const upstream = await fetch(
		`${BACKEND}/components/${encodeURIComponent(componentId)}/revisions/${encodeURIComponent(revisionId)}`
	);
	if (!upstream.ok) {
		return new Response(await upstream.text(), { status: upstream.status });
	}
	return json(await upstream.json());
}
