<script lang="ts">
	import type { TransformDatasetSplitParams } from '$lib/flow/schema/transform';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';

	export let params: Partial<TransformDatasetSplitParams> | Record<string, unknown>;
	export let onDraft: (patch: Partial<TransformDatasetSplitParams>) => void;
	export let onCommit: (patch: Partial<TransformDatasetSplitParams>) => void;

	const nested = (params as any)?.dataset_split ?? params ?? {};
	$: strategy = String((nested as any)?.strategy ?? 'random');
	$: trainRatio = Number((nested as any)?.trainRatio ?? 0.8);
	$: valRatio = Number((nested as any)?.valRatio ?? 0.1);
	$: testRatio = Number((nested as any)?.testRatio ?? 0.1);
	$: seed = Number((nested as any)?.seed ?? 42);
	$: shuffle = Boolean((nested as any)?.shuffle ?? true);
	$: stratifyColumn = String((nested as any)?.stratifyColumn ?? '');
	$: groupColumn = String((nested as any)?.groupColumn ?? '');
	$: timeColumn = String((nested as any)?.timeColumn ?? '');

	function commitPatch(next: Partial<TransformDatasetSplitParams>) {
		const merged = { strategy, trainRatio, valRatio, testRatio, seed, shuffle, stratifyColumn, groupColumn, timeColumn, ...next };
		onDraft({ op: 'dataset_split', dataset_split: merged } as unknown as Partial<TransformDatasetSplitParams>);
		onCommit({ op: 'dataset_split', dataset_split: merged } as unknown as Partial<TransformDatasetSplitParams>);
	}
</script>

<Section title="Dataset Split">
	<Field label="strategy">
		<select value={strategy} on:change={(e) => commitPatch({ strategy: (e.currentTarget as HTMLSelectElement).value as any })}>
			<option value="random">random</option>
			<option value="stratified">stratified</option>
			<option value="group">group</option>
			<option value="time">time</option>
		</select>
	</Field>
	<Field label="ratios train / val / test">
		<div class="triple">
			<Input value={String(trainRatio)} onInput={(e) => commitPatch({ trainRatio: Number((e.currentTarget as HTMLInputElement).value || 0) })} />
			<Input value={String(valRatio)} onInput={(e) => commitPatch({ valRatio: Number((e.currentTarget as HTMLInputElement).value || 0) })} />
			<Input value={String(testRatio)} onInput={(e) => commitPatch({ testRatio: Number((e.currentTarget as HTMLInputElement).value || 0) })} />
		</div>
	</Field>
	<Field label="seed">
		<Input value={String(seed)} onInput={(e) => commitPatch({ seed: Number((e.currentTarget as HTMLInputElement).value || 0) })} />
	</Field>
	<Field label="shuffle">
		<input type="checkbox" checked={shuffle} on:change={(e) => commitPatch({ shuffle: (e.currentTarget as HTMLInputElement).checked })} />
	</Field>
	{#if strategy === 'stratified'}
		<Field label="stratify column">
			<Input value={stratifyColumn} onInput={(e) => commitPatch({ stratifyColumn: (e.currentTarget as HTMLInputElement).value })} />
		</Field>
	{/if}
	{#if strategy === 'group'}
		<Field label="group column">
			<Input value={groupColumn} onInput={(e) => commitPatch({ groupColumn: (e.currentTarget as HTMLInputElement).value })} />
		</Field>
	{/if}
	{#if strategy === 'time'}
		<Field label="time column">
			<Input value={timeColumn} onInput={(e) => commitPatch({ timeColumn: (e.currentTarget as HTMLInputElement).value })} />
		</Field>
	{/if}
</Section>

<style>
	.triple {
		display: grid;
		grid-template-columns: 1fr 1fr 1fr;
		gap: 8px;
	}
</style>
