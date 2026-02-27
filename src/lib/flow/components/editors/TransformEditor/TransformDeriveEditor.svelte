<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import type { TransformDeriveParams } from '$lib/flow/schema/transform';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';

	type DeriveColumn = { name: string; expr: string };

	export let selectedNode: Node<PipelineNodeData>;
	export let params: Partial<TransformDeriveParams>;
	export let onDraft: (patch: Partial<TransformDeriveParams>) => void;
	export let onCommit: (patch: Partial<TransformDeriveParams>) => void;

	const defaults: DeriveColumn[] = [
		{ name: 'length_text', expr: 'length(text)' },
		{ name: 'is_long', expr: 'length(text) > 50' }
	];

	$: void selectedNode?.id;
	$: columns = params?.columns?.length ? params.columns : defaults;

	function normalize(items: DeriveColumn[]): DeriveColumn[] {
		return items
			.map((item) => ({ name: item.name.trim(), expr: item.expr.trim() }))
			.filter((item) => item.name.length > 0 && item.expr.length > 0);
	}

	function updateColumn(index: number, key: keyof DeriveColumn, value: string): void {
		const next = columns.map((item, current) => (current === index ? { ...item, [key]: value } : item));
		onDraft({ columns: normalize(next) });
	}
</script>

<Section title="Derive Columns">
	<div class="hint">
		Add computed columns with DuckDB expressions. Each row becomes <code>(expr) AS name</code>.
	</div>

	{#each columns as column, index}
		<div class="row">
			<Input
				value={column.name}
				placeholder="name"
				onInput={(event) => updateColumn(index, 'name', (event.currentTarget as HTMLInputElement).value)}
				onBlur={() => onCommit({ columns: normalize(columns) })}
			/>
			<Input
				value={column.expr}
				placeholder="expression"
				onInput={(event) => updateColumn(index, 'expr', (event.currentTarget as HTMLInputElement).value)}
				onBlur={() => onCommit({ columns: normalize(columns) })}
			/>
			<button class="small danger" type="button" on:click={() => onDraft({ columns: normalize(columns.filter((_, current) => current !== index)) })}>
				x
			</button>
		</div>
	{/each}

	<div class="actions">
		<button class="small" type="button" on:click={() => onDraft({ columns: normalize([...columns, { name: '', expr: '' }]) })}>
			+ Add derived column
		</button>
		<button class="small ghost" type="button" on:click={() => { onDraft({ columns: defaults }); onCommit({ columns: defaults }); }}>
			Reset defaults
		</button>
	</div>

	{#if !params?.columns || params.columns.length === 0}
		<div class="warn">At least one derived column is required.</div>
	{/if}
</Section>

<style>
	.row {
		display: grid;
		grid-template-columns: minmax(160px, 0.7fr) minmax(0, 1.3fr) auto;
		gap: 8px;
		align-items: center;
		margin: 8px 0;
	}

	.hint,
	.warn {
		font-size: 12px;
		margin-top: 6px;
	}

	.hint {
		opacity: 0.75;
	}

	.warn {
		color: #fca5a5;
	}

	.actions {
		display: flex;
		gap: 8px;
		justify-content: flex-end;
		margin-top: 8px;
		flex-wrap: wrap;
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
	}

	code {
		font-family: ui-monospace, Menlo, Consolas, monospace;
		font-size: 12px;
	}
</style>
