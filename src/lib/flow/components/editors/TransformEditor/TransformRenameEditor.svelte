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
		Map <code>old_name</code> → <code>new_name</code>. Empty rows are ignored.
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

			<div class="arrow">→</div>

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
				✕
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

	.k {
		width: 40px;
		font-size: 12px;
		opacity: 0.8;
	}

	.v {
		width: 160px;
	}

	.arrow {
		opacity: 0.8;
		font-size: 12px;
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
	}

	button.small {
		padding: 4px 8px;
		font-size: 12px;
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
