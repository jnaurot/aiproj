<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import type { TransformSelectParams } from '$lib/flow/schema/transform';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';

	export let selectedNode: Node<PipelineNodeData>;
	export let params: Partial<TransformSelectParams>;
	export let onDraft: (patch: Partial<TransformSelectParams>) => void;
	export let onCommit: (patch: Partial<TransformSelectParams>) => void;

	$: void selectedNode?.id;
	$: columns = params?.columns?.length ? params.columns : ['text', 'id'];

	function updateColumn(index: number, value: string): void {
		const next = [...columns];
		next[index] = value;
		onDraft({ columns: next });
	}

	function removeColumn(index: number): void {
		if (columns.length <= 1) return;
		onDraft({ columns: columns.filter((_, current) => current !== index) });
	}
</script>

<Section title="Select Columns">
	{#each columns as column, index}
		<Field label={`column ${index + 1}`}>
			<div class="row">
				<Input
					value={column}
					placeholder="column name"
					onInput={(event) => updateColumn(index, (event.currentTarget as HTMLInputElement).value)}
					onBlur={() => onCommit({ columns })}
				/>
				<button
					class="small danger"
					type="button"
					title="Remove column"
					on:click={() => removeColumn(index)}
					disabled={columns.length <= 1}
				>
					x
				</button>
			</div>
		</Field>
	{/each}

	<div class="actions">
		<button class="small" type="button" on:click={() => onDraft({ columns: [...columns, ''] })}>+ Add column</button>
	</div>
</Section>

<style>
	.row {
		display: flex;
		gap: 8px;
		align-items: center;
	}

	.actions {
		display: flex;
		justify-content: flex-end;
		margin-top: 8px;
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

	button.danger {
		border-color: rgba(239, 68, 68, 0.5);
		background: rgba(239, 68, 68, 0.14);
	}
</style>
