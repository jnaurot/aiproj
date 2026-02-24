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

	// Toggle â€œdedupe on all columnsâ€: represent as by=[]
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
		border: 1px solid rgba(255, 255, 255, 0.08);
		border-radius: 12px;
		padding: 12px;
		background: rgba(255, 255, 255, 0.03);
		margin-top: 8px;
	}

	.sectionTitle {
		font-weight: 650;
		font-size: 14px;
		margin-bottom: 10px;
		opacity: 0.9;
	}

	.subTitle {
		margin-top: 10px;
		font-weight: 600;
		font-size: 13px;
		opacity: 0.95;
	}

	.hint {
		font-size: 12px;
		opacity: 0.75;
		margin-bottom: 10px;
		line-height: 1.35;
	}

	.row {
		display: flex;
		gap: 8px;
		align-items: flex-start;
		margin-bottom: 8px;
	}

	.line {
		display: flex;
		gap: 8px;
		align-items: center;
		margin-bottom: 8px;
	}

	.field {
		display: grid;
		grid-template-columns: 100px minmax(0, 1fr);
		align-items: start;
		gap: 8px;
		margin-bottom: 10px;
	}

	.field.grow {
		flex: 1;
	}

	.field.dir {
		grid-template-columns: 70px minmax(0, 1fr);
	}

	.k,
	.label {
		font-size: 14px;
		opacity: 0.85;
		padding-top: 8px;
		font-weight: 400;
	}

	.v {
		min-width: 0;
		width: 100%;
	}

	.colInput {
		flex: 1;
	}

	.arrow {
		opacity: 0.75;
		padding-top: 8px;
	}

	.toggle {
		display: inline-flex;
		gap: 8px;
		align-items: center;
	}

	input,
	select,
	textarea,
	.readonly,
	.code {
		width: 100%;
		box-sizing: border-box;
		border-radius: 10px;
		border: 1px solid rgba(255, 255, 255, 0.1);
		background: rgba(0, 0, 0, 0.2);
		color: inherit;
		padding: 8px 10px;
		font-size: 14px;
		outline: none;
		min-height: 40px;
	}

	textarea,
	.code {
		resize: vertical;
		line-height: 1.35;
		min-height: 96px;
	}

	input[type='checkbox'] {
		width: auto;
		min-height: 0;
		padding: 0;
	}

	input:focus,
	select:focus,
	textarea:focus,
	.code:focus,
	.readonly:focus {
		border-color: rgba(255, 255, 255, 0.25);
	}

	.actions,
	.snips {
		margin-top: 8px;
		display: flex;
		gap: 8px;
		justify-content: flex-end;
		flex-wrap: wrap;
	}

	.snipsTitle {
		font-size: 12px;
		opacity: 0.8;
		align-self: center;
	}

	.snipRow {
		display: flex;
		gap: 8px;
		width: 100%;
	}

	button.small {
		padding: 6px 10px;
		font-size: 12px;
		border-radius: 10px;
		border: 1px solid rgba(255, 255, 255, 0.16);
		background: rgba(255, 255, 255, 0.06);
		color: inherit;
		cursor: pointer;
	}

	button.ghost {
		background: transparent;
	}

	button.danger {
		border-color: rgba(239, 68, 68, 0.5);
		background: rgba(239, 68, 68, 0.14);
		color: #fecaca;
	}

	.warn {
		margin-top: 8px;
		font-size: 12px;
		color: #fca5a5;
		white-space: pre-wrap;
	}

	.warn ul {
		margin: 6px 0 0 16px;
		padding: 0;
	}

	.preview {
		margin-top: 12px;
	}

	pre {
		white-space: pre-wrap;
		word-break: break-word;
		padding: 10px;
		border: 1px solid rgba(255, 255, 255, 0.1);
		border-radius: 10px;
		font-size: 12px;
		opacity: 0.95;
	}

	code {
		font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace;
		font-size: 12px;
	}
</style>

