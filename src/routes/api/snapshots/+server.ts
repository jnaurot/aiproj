import { json } from '@sveltejs/kit';

const BACKEND = 'http://127.0.0.1:8000';

export async function POST({ request }) {
	const form = await request.formData();
	const upstream = await fetch(`${BACKEND}/snapshots`, {
		method: 'POST',
		body: form
	});
	if (!upstream.ok) {
		return new Response(await upstream.text(), { status: upstream.status });
	}
	return json(await upstream.json());
}
