<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { TransformNodeData } from '$lib/flow/types/transform';
	import type { TransformRenameParams } from '$lib/flow/schema/transform';

	export let selectedNode: Node<TransformNodeData>;
	export let params: TransformRenameParams;
	export let onDraft: (patch: Partial<TransformRenameParams>) => void;
	export let onCommit: (patch: Partial<TransformRenameParams>) => void;

	type Pair = { from: string; to: string };

	function mapToPairs(map: Record<string, string> | undefined): Pair[] {
		if (!map) return [];
		// stable order for UI: sort by key
		return Object.keys(map)
			.sort((a, b) => a.localeCompare(b))
			.map((k) => ({ from: k, to: map[k] }));
	}

	function pairsToMap(pairs: Pair[]): Record<string, string> {
		const out: Record<string, string> = {};
		for (const p of pairs) {
			const from = (p.from ?? '').trim();
			const to = (p.to ?? '').trim();
			if (!from || !to) continue;
			out[from] = to;
		}
		return out;
	}

	// seed from params, or a sensible default row if empty
	$: pairs = (() => {
		const p = mapToPairs(params?.map);
		return p.length ? p : [{ from: 'text', to: 'description' }];
	})();

	function updateFrom(i: number, value: string) {
		const next = pairs.map((p, idx) => (idx === i ? { ...p, from: value } : p));
		onDraft({ map: pairsToMap(next) });
	}

	function updateTo(i: number, value: string) {
		const next = pairs.map((p, idx) => (idx === i ? { ...p, to: value } : p));
		onDraft({ map: pairsToMap(next) });
	}

	function addRow() {
		const next = [...pairs, { from: '', to: '' }];
		onDraft({ map: pairsToMap(next) });
	}

	function removeRow(i: number) {
		const next = pairs.filter((_, idx) => idx !== i);
		onDraft({ map: pairsToMap(next) });
	}

	function commit() {
		onCommit({ map: pairsToMap(pairs) });
	}
</script>

<div class="section">
	<div class="sectionTitle">Rename Columns</div>

	<div class="hint">
		Map <code>old_name</code> â†’ <code>new_name</code>. Empty rows are ignored.
	</div>

	{#each pairs as p, i}
		<div class="row">
			<div class="field">
				<div class="k">from</div>
				<div class="v">
					<input
						value={p.from}
						placeholder="old column"
						on:input={(e) => updateFrom(i, (e.currentTarget as HTMLInputElement).value)}
						on:blur={commit}
					/>
				</div>
			</div>

			<div class="arrow">â†’</div>

			<div class="field">
				<div class="k">to</div>
				<div class="v">
					<input
						value={p.to}
						placeholder="new column"
						on:input={(e) => updateTo(i, (e.currentTarget as HTMLInputElement).value)}
						on:blur={commit}
					/>
				</div>
			</div>

			<button class="danger small" title="Remove mapping" on:click={() => removeRow(i)}>
				âœ•
			</button>
		</div>
	{/each}

	<div class="actions">
		<button class="small" on:click={addRow}>+ Add mapping</button>
	</div>

	{#if !params?.map || Object.keys(params.map).length === 0}
		<div class="warn">At least one valid mapping is required (non-empty from/to).</div>
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

