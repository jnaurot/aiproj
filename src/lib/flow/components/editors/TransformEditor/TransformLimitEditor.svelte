<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { TransformNodeData } from '$lib/flow/types/transform';
	import type { TransformLimitParams } from '$lib/flow/schema/transform';

	// NOTE: ports/enabled/notes handled upstream. This editor only edits limit params.
	export let selectedNode: Node<TransformNodeData>;
	export let params: TransformLimitParams;
	export let onDraft: (patch: Partial<TransformLimitParams>) => void;
	export let onCommit: (patch: Partial<TransformLimitParams>) => void;

	const DEFAULTS: TransformLimitParams = { n: 100 };

	$: nVal = (params?.n ?? DEFAULTS.n) as number;

	function clampInt(v: number) {
		if (!Number.isFinite(v)) return DEFAULTS.n;
		const i = Math.trunc(v);
		return Math.max(1, i);
	}

	function setN(v: number) {
		onDraft({ n: clampInt(v) });
	}

	function commit() {
		onCommit({ n: clampInt(nVal) });
	}

	function resetDefaults() {
		onDraft({ ...DEFAULTS });
		onCommit({ ...DEFAULTS });
	}
</script>

<div class="section">
	<div class="sectionTitle">Limit</div>

	<div class="hint">Return only the first <code>n</code> rows.</div>

	<div class="row">
		<div class="field grow">
			<div class="k">n</div>
			<div class="v">
				<input
					type="number"
					min="1"
					step="1"
					value={nVal}
					on:input={(e) => setN(parseInt((e.currentTarget as HTMLInputElement).value || '0', 10))}
					on:blur={commit}
				/>
			</div>
		</div>

		<button class="small ghost" on:click={resetDefaults}>Reset</button>
	</div>

	<div class="preview">
		<div class="label">Preview</div>
		<pre>SELECT * FROM input LIMIT {clampInt(nVal)}</pre>
	</div>
</div>

<style>
	.section {
		margin-top: 8px;
	}
	.sectionTitle {
		font-weight: 600;
		margin-bottom: 6px;
	}
	.hint {
		font-size: 12px;
		opacity: 0.8;
		margin-bottom: 10px;
	}

	.row {
		display: flex;
		gap: 8px;
		align-items: center;
	}

	.field {
		display: flex;
		gap: 6px;
		align-items: center;
	}

	.grow {
		flex: 1;
	}

	.k {
		width: 44px;
		font-size: 12px;
		opacity: 0.8;
	}

	.v {
		width: 100%;
	}

	input {
		width: 100%;
		padding: 4px 6px;
		border-radius: 4px;
		border: 1px solid #ccc;
		box-sizing: border-box;
	}

	button.small {
		padding: 4px 8px;
		font-size: 12px;
	}

	button.ghost {
		background: transparent;
		border: 1px solid #ccc;
		border-radius: 4px;
		cursor: pointer;
	}

	.preview {
		margin-top: 10px;
	}

	.label {
		font-weight: 700;
		margin-bottom: 6px;
	}

	pre {
		white-space: pre-wrap;
		word-break: break-word;
		padding: 8px;
		border: 1px solid #ddd;
		border-radius: 6px;
		font-size: 12px;
		opacity: 0.95;
	}
</style>
