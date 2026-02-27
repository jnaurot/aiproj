<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import type { TransformSortParams } from '$lib/flow/schema/transform';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';

	type SortItem = NonNullable<TransformSortParams['by']>[number];
	type SortDir = SortItem['dir'];

	export let selectedNode: Node<PipelineNodeData>;
	export let params: Partial<TransformSortParams>;
	export let onDraft: (patch: Partial<TransformSortParams>) => void;
	export let onCommit: (patch: Partial<TransformSortParams>) => void;

	const defaults: TransformSortParams = {
		by: [{ col: 'text', dir: 'asc' }]
	};

	$: void selectedNode?.id;
	$: items = params?.by?.length ? params.by : defaults.by;
	$: normalized = items
		.map((item) => ({ col: item.col.trim(), dir: item.dir }))
		.filter((item) => item.col.length > 0);
	$: orderSql = (normalized.length > 0 ? normalized : defaults.by)
		.map((item) => `${item.col} ${item.dir.toUpperCase()}`)
		.join(', ');
</script>

<Section title="Sort">
	<div class="hint">Sort rows by one or more columns. Order is applied left-to-right.</div>

	{#each items as item, index}
		<div class="row">
			<Input
				value={item.col}
				placeholder="column"
				onInput={(event) =>
					onDraft({
						by: items.map((entry, current) =>
							current === index ? { ...entry, col: (event.currentTarget as HTMLInputElement).value } : entry
						)
					})}
				onBlur={() => onCommit({ by: normalized.length > 0 ? normalized : defaults.by })}
			/>
			<select
				value={item.dir}
				on:change={(event) => {
					const value = (event.currentTarget as HTMLSelectElement).value as SortDir;
					const next = items.map((entry, current) => (current === index ? { ...entry, dir: value } : entry));
					onDraft({ by: next });
					onCommit({
						by: next
							.map((entry) => ({ col: entry.col.trim(), dir: entry.dir }))
							.filter((entry) => entry.col.length > 0)
					});
				}}
			>
				<option value="asc">asc</option>
				<option value="desc">desc</option>
			</select>
			<button class="small danger" type="button" on:click={() => onDraft({ by: items.filter((_, current) => current !== index) })}>
				x
			</button>
		</div>
	{/each}

	<div class="actions">
		<button class="small" type="button" on:click={() => onDraft({ by: [...items, { col: '', dir: 'asc' }] })}>+ Add key</button>
		<button
			class="small ghost"
			type="button"
			on:click={() => {
				onDraft(defaults);
				onCommit(defaults);
			}}
		>
			Reset defaults
		</button>
	</div>

	<div class="preview">
		<div class="subTitle">Preview</div>
		<pre>SELECT * FROM input ORDER BY {orderSql}</pre>
	</div>
</Section>

<style>
	.row {
		display: grid;
		grid-template-columns: minmax(0, 1fr) 110px auto;
		gap: 8px;
		align-items: center;
		margin: 8px 0;
	}

	.subTitle {
		margin-top: 10px;
		font-size: 13px;
		font-weight: 600;
	}

	.hint {
		font-size: 12px;
		opacity: 0.75;
		margin-top: 6px;
	}

	.actions {
		display: flex;
		gap: 8px;
		justify-content: flex-end;
		margin-top: 8px;
		flex-wrap: wrap;
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
</style>
