<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { TransformNodeData } from '$lib/flow/types/transform';
	import type { TransformDeriveParams } from '$lib/flow/schema/transform';

	export let selectedNode: Node<TransformNodeData>;
	export let params: TransformDeriveParams;
	export let onDraft: (patch: Partial<TransformDeriveParams>) => void;
	export let onCommit: (patch: Partial<TransformDeriveParams>) => void;

	type DeriveCol = { name: string; expr: string };

	const DEFAULTS: DeriveCol[] = [
		{ name: 'length_text', expr: 'length(text)' },
		{ name: 'is_long', expr: 'length(text) > 50' }
	];

	// local view-model (always show at least defaults)
	$: cols = (() => {
		const c = (params?.columns ?? []) as DeriveCol[];
		return c.length ? c : DEFAULTS;
	})();

	function normalize(next: DeriveCol[]) {
		// keep order; trim; drop empty rows
		const cleaned = next
			.map((c) => ({ name: (c.name ?? '').trim(), expr: (c.expr ?? '').trim() }))
			.filter((c) => c.name.length > 0 && c.expr.length > 0);

		// backend schema requires at least one column; allow draft empty, but commit should be valid
		return cleaned.length ? cleaned : [];
	}

	function draft(next: DeriveCol[]) {
		onDraft({ columns: normalize(next) as any });
	}

	function commit() {
		onCommit({ columns: normalize(cols) as any });
	}

	function updateName(i: number, value: string) {
		const next = cols.map((c, idx) => (idx === i ? { ...c, name: value } : c));
		draft(next);
	}

	function updateExpr(i: number, value: string) {
		const next = cols.map((c, idx) => (idx === i ? { ...c, expr: value } : c));
		draft(next);
	}

	function addRow() {
		const next = [...cols, { name: '', expr: '' }];
		draft(next);
	}

	function removeRow(i: number) {
		const next = cols.filter((_, idx) => idx !== i);
		draft(next);
	}

	function resetToDefaults() {
		draft(DEFAULTS);
		onCommit({ columns: DEFAULTS as any });
	}
</script>

<div class="section">
	<div class="sectionTitle">Derive Columns</div>

	<div class="hint">
		Add computed columns with DuckDB expressions. Each row is appended as
		<code>(expr) AS name</code>.
	</div>

	{#each cols as c, i}
		<div class="row">
			<div class="field">
				<div class="k">name</div>
				<div class="v">
					<input
						value={c.name}
						placeholder="e.g. length_text"
						on:input={(e) => updateName(i, (e.currentTarget as HTMLInputElement).value)}
						on:blur={commit}
					/>
				</div>
			</div>

			<div class="field grow">
				<div class="k">expr</div>
				<div class="v">
					<input
						value={c.expr}
						placeholder="e.g. length(text)"
						on:input={(e) => updateExpr(i, (e.currentTarget as HTMLInputElement).value)}
						on:blur={commit}
					/>
				</div>
			</div>

			<button class="danger small" title="Remove derived column" on:click={() => removeRow(i)}>
				✕
			</button>
		</div>
	{/each}

	<div class="actions">
		<button class="small" on:click={addRow}>+ Add derived column</button>
		<button class="small ghost" on:click={resetToDefaults}>Reset defaults</button>
	</div>

	{#if !params?.columns || params.columns.length === 0}
		<div class="warn">At least one derived column with non-empty name and expr is required.</div>
	{/if}
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

	.field.grow .v {
		width: 100%;
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
		width: 180px;
	}

	input {
		width: 100%;
		padding: 4px 6px;
		border-radius: 4px;
		border: 1px solid #ccc;
		box-sizing: border-box;
	}

	.actions {
		margin-top: 6px;
		display: flex;
		gap: 8px;
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

	.warn {
		margin-top: 8px;
		font-size: 12px;
		color: #b00020;
		white-space: pre-wrap;
	}
</style>
