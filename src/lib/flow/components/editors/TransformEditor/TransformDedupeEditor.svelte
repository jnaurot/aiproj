<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import type { TransformDedupeParams } from '$lib/flow/schema/transform';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';
	import { uniqueStrings } from '$lib/flow/components/editors/shared';

	export let selectedNode: Node<PipelineNodeData>;
	export let params: Partial<TransformDedupeParams>;
	export let onDraft: (patch: Partial<TransformDedupeParams>) => void;
	export let onCommit: (patch: Partial<TransformDedupeParams>) => void;

	const defaults: TransformDedupeParams = { by: ['text'] };

	$: void selectedNode?.id;
	$: by = uniqueStrings(params?.by ?? defaults.by);
	$: dedupeAll = by.length === 0;
</script>

<Section title="Deduplicate">
	<div class="hint">
		If <code>by</code> is empty, the backend deduplicates on the entire row.
	</div>

	<div class="actions">
		<label class="toggle">
			<Input
				type="checkbox"
				checked={dedupeAll}
				onChange={(event) => {
					const checked = (event.currentTarget as HTMLInputElement).checked;
					const next = checked ? [] : defaults.by;
					onDraft({ by: next });
					onCommit({ by: next });
				}}
			/>
			<span>Deduplicate on all columns</span>
		</label>

		<button
			class="small ghost"
			type="button"
			on:click={() => {
				onDraft(defaults);
				onCommit(defaults);
			}}
		>
			Reset
		</button>
	</div>

	{#if !dedupeAll}
		{#each by as column, index}
			<div class="row">
				<Input
					value={column}
					placeholder="column"
					onInput={(event) => {
						const next = [...by];
						next[index] = (event.currentTarget as HTMLInputElement).value;
						onDraft({ by: uniqueStrings(next) });
					}}
					onBlur={() => onCommit({ by })}
				/>
				<button class="small danger" type="button" on:click={() => onDraft({ by: by.filter((_, current) => current !== index) })}>
					Remove
				</button>
			</div>
		{/each}

		<div class="actions">
			<button class="small" type="button" on:click={() => onDraft({ by: [...by, ''] })}>+ Add column</button>
			<button class="small ghost" type="button" on:click={() => onCommit({ by })}>Commit</button>
		</div>
	{/if}

	<div class="preview">
		<div class="subTitle">Preview</div>
		{#if dedupeAll}
			<pre>SELECT DISTINCT * FROM input</pre>
		{:else}
			<pre>{`SELECT *
FROM (
  SELECT *,
         row_number() OVER (PARTITION BY ${by.map((column) => `"${column}"`).join(', ')}) AS rn
  FROM input
)
WHERE rn = 1`}</pre>
		{/if}
	</div>
</Section>

<style>
	.row {
		display: flex;
		gap: 8px;
		align-items: center;
		margin: 8px 0;
	}

	.toggle {
		display: inline-flex;
		gap: 8px;
		align-items: center;
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
		justify-content: space-between;
		align-items: center;
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

	code {
		font-family: ui-monospace, Menlo, Consolas, monospace;
		font-size: 12px;
	}
</style>
