<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import type { TransformRenameParams } from '$lib/flow/schema/transform';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';

	type Pair = { from: string; to: string };

	export let selectedNode: Node<PipelineNodeData>;
	export let params: Partial<TransformRenameParams>;
	export let onDraft: (patch: Partial<TransformRenameParams>) => void;
	export let onCommit: (patch: Partial<TransformRenameParams>) => void;

	$: void selectedNode?.id;
	$: pairs = (() => {
		const map = params?.map ?? {};
		const entries = Object.entries(map).map(([from, to]) => ({ from, to }));
		return entries.length > 0 ? entries : [{ from: 'text', to: 'description' }];
	})();

	function pairsToMap(items: Pair[]): Record<string, string> {
		return Object.fromEntries(
			items
				.map(({ from, to }) => [from.trim(), to.trim()] as const)
				.filter(([from, to]) => from.length > 0 && to.length > 0)
		);
	}

	function updatePair(index: number, key: 'from' | 'to', value: string): void {
		const next = pairs.map((pair, current) => (current === index ? { ...pair, [key]: value } : pair));
		onDraft({ map: pairsToMap(next) });
	}
</script>

<Section title="Rename Columns">
	<div class="hint">Map old_name to new_name. Empty rows are ignored.</div>

	{#each pairs as pair, index}
		<div class="row">
			<Input
				value={pair.from}
				placeholder="old column"
				onInput={(event) => updatePair(index, 'from', (event.currentTarget as HTMLInputElement).value)}
				onBlur={() => onCommit({ map: pairsToMap(pairs) })}
			/>
			<div class="arrow">to</div>
			<Input
				value={pair.to}
				placeholder="new column"
				onInput={(event) => updatePair(index, 'to', (event.currentTarget as HTMLInputElement).value)}
				onBlur={() => onCommit({ map: pairsToMap(pairs) })}
			/>
			<button class="small danger" type="button" on:click={() => onDraft({ map: pairsToMap(pairs.filter((_, current) => current !== index)) })}>
				x
			</button>
		</div>
	{/each}

	<div class="actions">
		<button class="small" type="button" on:click={() => onDraft({ map: pairsToMap([...pairs, { from: '', to: '' }]) })}>
			+ Add mapping
		</button>
	</div>

	{#if !params?.map || Object.keys(params.map).length === 0}
		<div class="warn">At least one valid mapping is required.</div>
	{/if}
</Section>

<style>
	.row {
		display: grid;
		grid-template-columns: minmax(0, 1fr) auto minmax(0, 1fr) auto;
		gap: 8px;
		align-items: center;
		margin: 8px 0;
	}

	.arrow {
		font-size: 12px;
		opacity: 0.75;
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
