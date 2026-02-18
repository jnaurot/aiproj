<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { TransformNodeData } from '$lib/flow/types/transform';
	import type { TransformSelectParams } from '$lib/flow/schema/transform';

	export let selectedNode: Node<TransformNodeData>;
	export let params: TransformSelectParams;
	export let onDraft: (patch: Partial<TransformSelectParams>) => void;
	export let onCommit: (patch: Partial<TransformSelectParams>) => void;

	// fallback if params missing (should rarely happen)
	$: columns = params?.columns?.length ? params.columns : ["text", "id"];

	function updateColumn(i: number, value: string) {
		const next = [...columns];
		next[i] = value;
		onDraft({ columns: next });
	}

	function addColumn() {
		onDraft({ columns: [...columns, ""] });
	}

	function removeColumn(i: number) {
		const next = columns.filter((_, idx) => idx !== i);
		if (next.length === 0) return; // must keep at least one
		onDraft({ columns: next });
	}

	function commit() {
		onCommit({ columns });
	}
</script>

<div class="section">
	<div class="sectionTitle">Select Columns</div>

	{#each columns as col, i}
		<div class="field">
			<div class="k">column {i + 1}</div>

			<div class="v row">
				<input
					value={col}
					placeholder="column name"
					on:input={(e) => updateColumn(i, (e.currentTarget as HTMLInputElement).value)}
					on:blur={commit}
				/>

				<button
					class="danger small"
					title="Remove column"
					on:click={() => removeColumn(i)}
					disabled={columns.length <= 1}
				>
					✕
				</button>
			</div>
		</div>
	{/each}

	<div class="actions">
		<button class="small" on:click={addColumn}>+ Add column</button>
	</div>
</div>

<style>
	.section {
		margin-top: 8px;
	}

	.sectionTitle {
		font-weight: 600;
		margin-bottom: 8px;
	}

	.field {
		display: flex;
		gap: 8px;
		align-items: center;
		margin-bottom: 6px;
	}

	.k {
		width: 80px;
		font-size: 12px;
		opacity: 0.8;
	}

	.v {
		flex: 1;
	}

	.row {
		display: flex;
		gap: 6px;
	}

	input {
		flex: 1;
		padding: 4px 6px;
		border-radius: 4px;
		border: 1px solid #ccc;
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
	}

	button.danger:disabled {
		opacity: 0.3;
		cursor: default;
	}

	.actions {
		margin-top: 6px;
	}
</style>
