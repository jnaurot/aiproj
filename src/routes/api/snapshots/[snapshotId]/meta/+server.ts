import { json } from '@sveltejs/kit';

const BACKEND = 'http://127.0.0.1:8000';

export async function GET({ params }) {
	const snapshotId = params.snapshotId;
	const upstream = await fetch(`${BACKEND}/snapshots/${encodeURIComponent(snapshotId)}/meta`);
	if (!upstream.ok) {
		return new Response(await upstream.text(), { status: upstream.status });
	}
	return json(await upstream.json());
}
