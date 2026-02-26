<script lang="ts">
	import { createEventDispatcher } from 'svelte';
	import { get } from 'svelte/store';
	import { graphStore } from '$lib/flow/store/graphStore';

	export let open = false;
	export let nodeId: string | null = null;

	const dispatch = createEventDispatcher();

	type Loaded =
		| { kind: 'empty' }
		| { kind: 'loading' }
		| { kind: 'json'; data: any }
		| { kind: 'text'; text: string }
		| { kind: 'error'; message: string };

	let loaded: Loaded = { kind: 'empty' };

	async function load() {
		if (!nodeId) {
			loaded = { kind: 'empty' };
			return;
		}

		const state = get(graphStore);
		const binding = state.nodeBindings?.[nodeId];
		const info = state.nodeOutputs?.[nodeId];
		const artifactId = binding?.currentArtifactId ?? binding?.lastArtifactId;
		const mimeType = info?.mimeType;
		if (!artifactId) {
			loaded = { kind: 'empty' };
			return;
		}

		loaded = { kind: 'loading' };
		try {
			const res = await fetch(`http://127.0.0.1:8000/runs/artifacts/${artifactId}`)
			if (!res.ok) throw new Error(`HTTP ${res.status}`);

			if (mimeType === 'application/json') {
				loaded = { kind: 'json', data: await res.json() };
			} else {
				loaded = { kind: 'text', text: await res.text() };
			}
		} catch (e) {
			loaded = { kind: 'error', message: String(e) };
		}
	}

	// load whenever opened or node changes
	$: if (open) load();

	function close() {
		open = false;
		dispatch('close');
	}
</script>

{#if open}
	<div class="backdrop" on:click={close} />
	<div class="modal" role="dialog" aria-modal="true">
		<div class="header">
			<div class="title">Node output</div>
			<button class="x" on:click={close}>✕</button>
		</div>

		<div class="meta">
			<div><b>nodeId:</b> {nodeId ?? '—'}</div>
		</div>

		<div class="body">
			{#if loaded.kind === 'empty'}
				<div class="hint">No output recorded for this node yet.</div>
			{:else if loaded.kind === 'loading'}
				<div class="hint">Loading…</div>
			{:else if loaded.kind === 'error'}
				<div class="err">{loaded.message}</div>
			{:else if loaded.kind === 'json'}
				<pre>{JSON.stringify(loaded.data, null, 2)}</pre>
			{:else if loaded.kind === 'text'}
				<pre>{loaded.text}</pre>
			{/if}
		</div>
	</div>
{/if}

<style>
	.backdrop {
		position: fixed;
		inset: 0;
		background: rgba(0, 0, 0, 0.45);
		z-index: 1000;
	}
	.modal {
		position: fixed;
		top: 8vh;
		left: 50%;
		transform: translateX(-50%);
		width: min(900px, 92vw);
		max-height: 84vh;
		background: #111;
		color: #eee;
		border: 1px solid rgba(255, 255, 255, 0.12);
		border-radius: 14px;
		z-index: 1001;
		display: flex;
		flex-direction: column;
		overflow: hidden;
	}
	.header {
		display: flex;
		align-items: center;
		justify-content: space-between;
		padding: 12px 14px;
		border-bottom: 1px solid rgba(255, 255, 255, 0.12);
	}
	.title {
		font-size: 16px;
		font-weight: 700;
	}
	.x {
		background: transparent;
		border: 1px solid rgba(255, 255, 255, 0.18);
		color: #eee;
		border-radius: 10px;
		padding: 6px 10px;
		cursor: pointer;
	}
	.meta {
		padding: 10px 14px;
		font-size: 12px;
		opacity: 0.85;
		border-bottom: 1px solid rgba(255, 255, 255, 0.08);
	}
	.body {
		padding: 12px 14px;
		overflow: auto;
		flex: 1;
	}
	pre {
		margin: 0;
		white-space: pre-wrap;
		word-break: break-word;
		font-family:
			ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New',
			monospace;
		font-size: 12px;
		line-height: 1.35;
	}
	.hint {
		opacity: 0.8;
	}
	.err {
		color: #ff8080;
	}
</style>
