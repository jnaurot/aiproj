import { json } from "@sveltejs/kit";

const BACKEND = "http://127.0.0.1:8000";

export async function POST({ request }) {
  const body = await request.text();

  const r = await fetch(`${BACKEND}/runs`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body
  });

  if (!r.ok) {
    return new Response(await r.text(), { status: r.status });
  }

  return json(await r.json());
}
