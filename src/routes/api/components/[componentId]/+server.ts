import { json } from '@sveltejs/kit';

const BACKEND = 'http://127.0.0.1:8000';

export async function PATCH({ params, request }) {
	const componentId = String(params.componentId ?? '').trim();
	if (!componentId) return new Response('componentId is required', { status: 400 });
	const body = await request.text();
	const upstream = await fetch(`${BACKEND}/components/${encodeURIComponent(componentId)}`, {
		method: 'PATCH',
		headers: { 'Content-Type': 'application/json' },
		body
	});
	if (!upstream.ok) {
		return new Response(await upstream.text(), { status: upstream.status });
	}
	return json(await upstream.json());
}

export async function DELETE({ params }) {
	const componentId = String(params.componentId ?? '').trim();
	if (!componentId) return new Response('componentId is required', { status: 400 });
	const upstream = await fetch(`${BACKEND}/components/${encodeURIComponent(componentId)}`, {
		method: 'DELETE'
	});
	if (!upstream.ok) {
		return new Response(await upstream.text(), { status: upstream.status });
	}
	return json(await upstream.json());
}
