const BACKEND = "http://127.0.0.1:8000";

export async function GET({ params }) {
  const runId = params.runId;

  const upstream = await fetch(`${BACKEND}/runs/${runId}/events`, {
    headers: { Accept: "text/event-stream" }
  });

  if (!upstream.ok || !upstream.body) {
    return new Response(await upstream.text(), { status: upstream.status });
  }

  return new Response(upstream.body, {
    headers: {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache",
      "Connection": "keep-alive"
    }
  });
}
