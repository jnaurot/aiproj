<script lang="ts">
	import type { TransformDatasetSplitParams } from '$lib/flow/schema/transform';
	import { getTransformMeta } from '$lib/flow/schema/transformMeta';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';
	import ConditionalHint from './ConditionalHint.svelte';

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
	$: ratioTotal = Number((trainRatio + valRatio + testRatio).toFixed(6));
	$: ratioOk = Math.abs(ratioTotal - 1) <= 1e-6;
	const meta = getTransformMeta('dataset_split');

	function commitPatch(next: Partial<TransformDatasetSplitParams>, immediate = false) {
		const merged = { strategy, trainRatio, valRatio, testRatio, seed, shuffle, stratifyColumn, groupColumn, timeColumn, ...next };
		onDraft({ op: 'dataset_split', dataset_split: merged } as unknown as Partial<TransformDatasetSplitParams>);
		if (immediate) {
			onCommit({ op: 'dataset_split', dataset_split: merged } as unknown as Partial<TransformDatasetSplitParams>);
		}
	}
</script>

<Section title={meta.label}>
	<ConditionalHint text={meta.description} />
	<Field label="strategy">
		<select value={strategy} on:change={(e) => commitPatch({ strategy: (e.currentTarget as HTMLSelectElement).value as any }, true)}>
			<option value="random">random</option>
			<option value="stratified">stratified</option>
			<option value="group">group</option>
			<option value="time">time</option>
		</select>
	</Field>
	{#if strategy === 'stratified'}
		<ConditionalHint text="Strategy stratified requires a stratify column below." />
	{:else if strategy === 'group'}
		<ConditionalHint text="Strategy group requires a group column below." />
	{:else if strategy === 'time'}
		<ConditionalHint text="Strategy time requires a time column below." />
	{/if}
	<Field label="ratios train / val / test">
		<div class="triple">
			<Input type="number" min="0" max="1" step="0.01" value={String(trainRatio)} onInput={(e) => commitPatch({ trainRatio: Number((e.currentTarget as HTMLInputElement).value || 0) })} />
			<Input type="number" min="0" max="1" step="0.01" value={String(valRatio)} onInput={(e) => commitPatch({ valRatio: Number((e.currentTarget as HTMLInputElement).value || 0) })} />
			<Input type="number" min="0" max="1" step="0.01" value={String(testRatio)} onInput={(e) => commitPatch({ testRatio: Number((e.currentTarget as HTMLInputElement).value || 0) })} />
		</div>
		<div class={`ratioHint ${ratioOk ? 'ok' : 'bad'}`}>
			total: {ratioTotal} {ratioOk ? 'OK' : 'must equal 1'}
		</div>
	</Field>
	<Field label="seed">
		<Input type="number" step="1" value={String(seed)} onInput={(e) => commitPatch({ seed: Number((e.currentTarget as HTMLInputElement).value || 0) })} />
	</Field>
	<Field label="shuffle">
		<input type="checkbox" checked={shuffle} on:change={(e) => commitPatch({ shuffle: (e.currentTarget as HTMLInputElement).checked }, true)} />
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

	.ratioHint {
		font-size: 12px;
		margin-top: 6px;
	}

	.ratioHint.ok {
		opacity: 0.78;
	}

	.ratioHint.bad {
		color: #fca5a5;
	}
</style>
