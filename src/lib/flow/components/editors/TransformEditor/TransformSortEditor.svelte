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

			<button class="danger small" title="Remove sort key" on:click={() => removeRow(i)}>✕</button>
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
		margin-bottom: 8px;
	}

	.field {
		display: flex;
		gap: 6px;
		align-items: center;
	}

	.grow {
		flex: 1;
	}

	.dir {
		width: 180px;
	}

	.k {
		width: 44px;
		font-size: 12px;
		opacity: 0.8;
	}

	.v {
		width: 100%;
	}

	input,
	select {
		width: 100%;
		padding: 4px 6px;
		border-radius: 4px;
		border: 1px solid #ccc;
		box-sizing: border-box;
	}

	.actions {
		display: flex;
		gap: 8px;
		margin-top: 6px;
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

	button.danger {
		background: #f44336;
		color: white;
		border: none;
		border-radius: 4px;
		cursor: pointer;
		padding: 4px 8px;
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
