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
				âœ•
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

