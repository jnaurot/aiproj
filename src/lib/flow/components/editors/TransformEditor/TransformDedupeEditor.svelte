<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { TransformNodeData } from '$lib/flow/types/transform';
	import type { TransformDedupeParams } from '$lib/flow/schema/transform';

	// NOTE: ports/enabled/notes handled upstream. This editor only edits dedupe params.
	export let selectedNode: Node<TransformNodeData>;
	export let params: TransformDedupeParams;
	export let onDraft: (patch: Partial<TransformDedupeParams>) => void;
	export let onCommit: (patch: Partial<TransformDedupeParams>) => void;

	const DEFAULTS: TransformDedupeParams = { by: ['text'] };

	// Canonicalize:
	// - trim
	// - remove empties
	// - unique
	function normalizeCols(cols: string[]) {
		const out: string[] = [];
		const seen = new Set<string>();
		for (const raw of cols) {
			const c = (raw ?? '').trim();
			if (!c) continue;
			if (seen.has(c)) continue;
			seen.add(c);
			out.push(c);
		}
		return out;
	}

	$: byCols = normalizeCols(params?.by ?? DEFAULTS.by);

	function setByCols(next: string[]) {
		onDraft({ by: normalizeCols(next) });
	}

	function commit() {
		onCommit({ by: normalizeCols(byCols) });
	}

	function resetDefaults() {
		onDraft({ ...DEFAULTS });
		onCommit({ ...DEFAULTS });
	}

	function addCol() {
		setByCols([...byCols, '']);
	}

	function removeCol(idx: number) {
		const next = [...byCols];
		next.splice(idx, 1);
		setByCols(next);
	}

	function updateCol(idx: number, value: string) {
		const next = [...byCols];
		next[idx] = value;
		setByCols(next);
	}

	// Toggle “dedupe on all columns”: represent as by=[]
	$: dedupeAll = byCols.length === 0;

	function toggleDedupeAll(checked: boolean) {
		if (checked) {
			onDraft({ by: [] });
			onCommit({ by: [] });
		} else {
			onDraft({ ...DEFAULTS });
			onCommit({ ...DEFAULTS });
		}
	}
</script>

<div class="section">
	<div class="sectionTitle">Deduplicate</div>

	<div class="hint">
		If <code>by</code> is empty, the backend will dedupe on the entire row (<code>SELECT DISTINCT *</code>).
	</div>

	<div class="row">
		<label class="toggle">
			<input type="checkbox" checked={dedupeAll} on:change={(e) => toggleDedupeAll((e.currentTarget as HTMLInputElement).checked)} />
			<span>Deduplicate on all columns</span>
		</label>

		<button class="small ghost" on:click={resetDefaults}>Reset</button>
	</div>

	{#if !dedupeAll}
		<div class="subTitle">by (columns)</div>

		{#each byCols as col, idx (idx)}
			<div class="line">
				<input
					class="colInput"
					placeholder="e.g. text"
					value={col}
					on:input={(e) => updateCol(idx, (e.currentTarget as HTMLInputElement).value)}
					on:blur={commit}
				/>
				<button class="small danger" on:click={() => removeCol(idx)} title="Remove column">Remove</button>
			</div>
		{/each}

		<div class="row">
			<button class="small" on:click={addCol}>+ Add column</button>
			<button class="small ghost" on:click={commit}>Commit</button>
		</div>
	{/if}

	<div class="preview">
		<div class="label">Preview</div>
		{#if dedupeAll}
			<pre>SELECT DISTINCT * FROM input</pre>
		{:else}
			<pre>{`SELECT *
FROM (
  SELECT *,
         row_number() OVER (PARTITION BY ${byCols.map((c) => `"${c}"`).join(', ')}) AS rn
  FROM input
)
WHERE rn = 1`}</pre>
		{/if}
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
	.subTitle {
		margin-top: 10px;
		font-weight: 600;
		font-size: 12px;
		opacity: 0.9;
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
		justify-content: space-between;
		margin-bottom: 8px;
	}

	.toggle {
		display: flex;
		gap: 8px;
		align-items: center;
		font-size: 12px;
		opacity: 0.95;
	}

	.line {
		display: flex;
		gap: 8px;
		align-items: center;
		margin-top: 6px;
	}

	.colInput {
		flex: 1;
		padding: 4px 6px;
		border-radius: 4px;
		border: 1px solid #ccc;
		box-sizing: border-box;
	}

	button.small {
		padding: 4px 8px;
		font-size: 12px;
		border-radius: 4px;
		border: 1px solid #ccc;
		background: #fff;
		cursor: pointer;
	}

	button.ghost {
		background: transparent;
	}

	button.danger {
		border-color: #d32f2f;
		color: #d32f2f;
		background: transparent;
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
