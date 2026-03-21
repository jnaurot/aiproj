<script lang="ts">
	import type { TransformClassImbalanceParams } from '$lib/flow/schema/transform';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';

	export let params: Partial<TransformClassImbalanceParams> | Record<string, unknown>;
	export let onDraft: (patch: Partial<TransformClassImbalanceParams>) => void;
	export let onCommit: (patch: Partial<TransformClassImbalanceParams>) => void;

	const nested = (params as any)?.class_imbalance ?? params ?? {};
	$: strategy = String((nested as any)?.strategy ?? 'report');
	$: labelColumn = String((nested as any)?.labelColumn ?? 'label');
	$: targetRatio = Number((nested as any)?.targetRatio ?? 1);
	$: seed = Number((nested as any)?.seed ?? 42);

	function commitPatch(next: Partial<TransformClassImbalanceParams>) {
		const merged = { strategy, labelColumn, targetRatio, seed, ...next };
		onDraft({ op: 'class_imbalance', class_imbalance: merged } as unknown as Partial<TransformClassImbalanceParams>);
		onCommit({ op: 'class_imbalance', class_imbalance: merged } as unknown as Partial<TransformClassImbalanceParams>);
	}
</script>

<Section title="Class Imbalance">
	<Field label="strategy">
		<select value={strategy} on:change={(e) => commitPatch({ strategy: (e.currentTarget as HTMLSelectElement).value as any })}>
			<option value="report">report</option>
			<option value="undersample">undersample</option>
			<option value="oversample">oversample</option>
			<option value="class_weight">class_weight</option>
		</select>
	</Field>
	<Field label="label column">
		<Input value={labelColumn} onInput={(e) => commitPatch({ labelColumn: (e.currentTarget as HTMLInputElement).value })} />
	</Field>
	<Field label="target ratio">
		<Input value={String(targetRatio)} onInput={(e) => commitPatch({ targetRatio: Number((e.currentTarget as HTMLInputElement).value || 0) })} />
	</Field>
	<Field label="seed">
		<Input value={String(seed)} onInput={(e) => commitPatch({ seed: Number((e.currentTarget as HTMLInputElement).value || 0) })} />
	</Field>
</Section>
