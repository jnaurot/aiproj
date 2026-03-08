import { json } from '@sveltejs/kit';

const BACKEND = 'http://127.0.0.1:8000';

export async function POST({ request }) {
	const body = await request.text();
	const upstream = await fetch(`${BACKEND}/components/validate`, {
		method: 'POST',
		headers: { 'Content-Type': 'application/json' },
		body
	});
	if (!upstream.ok) {
		return new Response(await upstream.text(), { status: upstream.status });
	}
	return json(await upstream.json());
}
