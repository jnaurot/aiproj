<script lang="ts">
	export let artifactId: string;
	export let mimeType: string | undefined;
	export let preview: string | undefined;

	let loading = false;
	let error: string | null = null;
	let text: string | null = null;
	let jsonObj: any = null;

	$: if (artifactId) void loadArtifact(artifactId);

	async function loadArtifact(id: string) {
		loading = true;
		error = null;
		text = null;
		jsonObj = null;

		try {
			const res = await fetch(`/runs/artifacts/${encodeURIComponent(id)}`);
			if (!res.ok) {
				const body = await res.text().catch(() => '');
				throw new Error(`${res.status} ${res.statusText} ${body}`);
			}

			// Prefer server header over the SSE-provided prop (the prop can be stale/wrong).
			const headerCt = res.headers.get('content-type') ?? '';
			const propCt = mimeType ?? '';
			const ct = headerCt || propCt;

			if (ct.includes('application/json')) {
				// If mislabeled JSON, fall back to text so the viewer still works.
				const raw = await res.text();
				try {
					jsonObj = JSON.parse(raw);
				} catch {
					text = raw;
				}
			} else {
				text = await res.text();
			}
		} catch (e: any) {
			error = e?.message ?? String(e);
		} finally {
			loading = false;
		}
	}
</script>

{#if preview}
	<div class="block">
		<div class="label">Preview</div>
		<pre>{preview}</pre>
	</div>
{/if}

<div class="meta">
	<div><b>artifactId:</b> <code>{artifactId}</code></div>
	<div><b>mimeType:</b> <code>{mimeType}</code></div>
</div>

{#if loading}
	<div class="muted">Loading artifact…</div>
{:else if error}
	<div class="error">{error}</div>
{:else if jsonObj}
	<div class="block">
		<div class="label">JSON</div>
		<pre>{JSON.stringify(jsonObj, null, 2)}</pre>
	</div>
{:else if text !== null}
	<div class="block">
		<div class="label">Content</div>
		<pre>{text}</pre>
	</div>
{:else}
	<div class="muted">No content.</div>
{/if}

<style>
	.meta {
		margin: 8px 0 12px;
		font-size: 12px;
		opacity: 0.9;
	}
	.block {
		margin-top: 10px;
	}
	.label {
		font-weight: 700;
		margin-bottom: 6px;
	}
	pre {
		white-space: pre-wrap;
		word-break: break-word;
	}
	.muted {
		opacity: 0.7;
	}
	.error {
		color: #b00020;
		white-space: pre-wrap;
	}
</style>
