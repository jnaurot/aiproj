import { json } from '@sveltejs/kit';

const BACKEND = 'http://127.0.0.1:8000';

export async function GET({ params, url }) {
	const componentId = String(params.componentId ?? '').trim();
	if (!componentId) return new Response('componentId is required', { status: 400 });
	const query = url.searchParams.toString();
	const suffix = query ? `?${query}` : '';
	const upstream = await fetch(
		`${BACKEND}/components/${encodeURIComponent(componentId)}/revisions${suffix}`
	);
	if (!upstream.ok) {
		return new Response(await upstream.text(), { status: upstream.status });
	}
	return json(await upstream.json());
}
