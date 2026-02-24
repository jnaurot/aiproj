<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { TransformNodeData } from '$lib/flow/types/transform';
	import type { TransformSortParams } from '$lib/flow/schema/transform';

	// NOTE: ports/enabled/notes handled upstream. This editor only edits sort params.
	export let selectedNode: Node<TransformNodeData>;
	export let params: TransformSortParams;
	export let onDraft: (patch: Partial<TransformSortParams>) => void;
	export let onCommit: (patch: Partial<TransformSortParams>) => void;

	type SortDir = 'asc' | 'desc';
	type SortItem = { col: string; dir: SortDir };

	const DEFAULTS: TransformSortParams = {
		by: [{ col: 'text', dir: 'asc' }]
	};

	$: items = (params?.by?.length ? params.by : DEFAULTS.by) as SortItem[];

	function norm(list: SortItem[]) {
		const cleaned = list
			.map((x) => ({ col: (x.col ?? '').trim(), dir: (x.dir ?? 'asc') as SortDir }))
			.filter((x) => x.col.length > 0);
		return cleaned.length ? cleaned : [{ ...DEFAULTS.by[0] }];
	}

	function setCol(i: number, v: string) {
		const next = items.map((x, idx) => (idx === i ? { ...x, col: v } : x));
		onDraft({ by: norm(next) as any });
	}

	function setDir(i: number, v: SortDir) {
		const next = items.map((x, idx) => (idx === i ? { ...x, dir: v } : x));
		onDraft({ by: norm(next) as any });
	}

	function addRow() {
		const next = [...items, { col: '', dir: 'asc' as const }];
		onDraft({ by: norm(next) as any });
	}

	function removeRow(i: number) {
		const next = items.filter((_, idx) => idx !== i);
		onDraft({ by: norm(next) as any });
	}

	function resetDefaults() {
		onDraft({ ...DEFAULTS });
		onCommit({ ...DEFAULTS });
	}

	function commit() {
		onCommit({ by: norm(items) as any });
	}

	$: orderSql = norm(items)
		.map((x) => `${x.col} ${x.dir.toUpperCase()}`)
		.join(', ');
</script>

<div class="section">
	<div class="sectionTitle">Sort</div>

	<div class="hint">
		Sort rows by one or more columns. Order is applied left-to-right.
	</div>

	{#each items as it, i (i)}
		<div class="row">
			<div class="field grow">
				<div class="k">col</div>
				<div class="v">
					<input
						value={it.col}
						placeholder="e.g. text"
						on:input={(e) => setCol(i, (e.currentTarget as HTMLInputElement).value)}
						on:blur={commit}
					/>
				</div>
			</div>

			<div class="field dir">
				<div class="k">dir</div>
				<div class="v">
					<select
						value={it.dir}
						on:change={(e) => {
							setDir(i, (e.currentTarget as HTMLSelectElement).value as SortDir);
							commit();
						}}
					>
						<option value="asc">asc</option>
						<option value="desc">desc</option>
					</select>
				</div>
			</div>

			<button class="danger small" title="Remove sort key" on:click={() => removeRow(i)}>âœ•</button>
		</div>
	{/each}

	<div class="actions">
		<button class="small" on:click={addRow}>+ Add key</button>
		<button class="small ghost" on:click={resetDefaults}>Reset defaults</button>
	</div>

	<div class="preview">
		<div class="label">Preview</div>
		<pre>SELECT * FROM input ORDER BY {orderSql}</pre>
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

