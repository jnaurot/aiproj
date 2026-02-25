<script lang="ts">
	import { page } from '$app/stores';
	import ArtifactViewer from '$lib/flow/components/ArtifactViewer.svelte';

	$: artifactId = $page.params.id ?? '';
</script>

<main class="wrap">
	<header class="head">
		<a href="/">Back to Canvas</a>
		<div class="id">{artifactId}</div>
	</header>

	{#if artifactId}
		<ArtifactViewer artifactId={artifactId} />
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
