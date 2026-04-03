<script lang="ts">
	import { onMount } from 'svelte';
	import { page } from '$app/stores';
	import { getRun } from '$lib/flow/client/runs';
	import ArtifactViewer from '$lib/flow/components/ArtifactViewer.svelte';

	$: artifactId = $page.params.id ?? '';
	$: graphId = ($page.url.searchParams.get('graphId') ?? '').trim();
	$: runId = ($page.url.searchParams.get('runId') ?? '').trim();
	let resolvedGraphId = '';
	$: resolvedGraphId = graphId;
	$: source = ($page.url.searchParams.get('source') ?? '').trim();

	onMount(() => {
		if (!runId) return;
		void (async () => {
			try {
				const run = await getRun(runId);
				const runGraphId = String((run as any)?.graphId ?? '').trim();
				if (runGraphId) {
					resolvedGraphId = runGraphId;
				}
			} catch {
				// Keep URL-provided graphId when run lookup is unavailable.
			}
		})();
	});

	function handleReturnToCanvas(event: MouseEvent): void {
		event.preventDefault();
		const openedFromRunLog = source === 'run_log';
		if (openedFromRunLog) {
			window.close();
			setTimeout(() => {
				window.location.assign('/');
			}, 40);
			return;
		}
		window.location.assign('/');
	}
</script>

<main class="wrap">
	<header class="head">
		<a href="/" on:click={handleReturnToCanvas}>Back to Canvas</a>
		<div class="id">{artifactId}</div>
	</header>

	{#if artifactId}
		<ArtifactViewer artifactId={artifactId} graphId={resolvedGraphId} />
	{:else}
		<div class="muted">Missing artifact id.</div>
	{/if}
</main>

<style>
	:global(body) {
		background: var(--av-surface, #0b1220);
		color: var(--av-text, #e5e7eb);
	}
	.wrap {
		padding: 12px;
		max-width: 1100px;
		margin: 0 auto;
		min-height: 100vh;
		background: var(--av-surface, #0b1220);
		color: var(--av-text, #e5e7eb);
	}
	.head {
		display: flex;
		align-items: center;
		gap: 12px;
		margin-bottom: 8px;
	}
	.head a {
		color: var(--av-text, #e5e7eb);
		text-decoration: none;
		border: 1px solid var(--av-border, #374151);
		border-radius: 8px;
		padding: 4px 8px;
		background: var(--av-surface-alt, #111827);
	}
	.head a:hover {
		filter: brightness(1.08);
	}
	.id {
		font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New',
			monospace;
		font-size: 12px;
		opacity: 0.95;
		word-break: break-all;
	}
	.muted {
		opacity: 0.75;
	}
</style>
