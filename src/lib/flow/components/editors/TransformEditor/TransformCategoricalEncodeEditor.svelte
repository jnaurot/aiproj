<script lang="ts">
	import type { TransformCategoricalEncodeParams } from '$lib/flow/schema/transform';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';

	export let params: Partial<TransformCategoricalEncodeParams> | Record<string, unknown>;
	export let onDraft: (patch: Partial<TransformCategoricalEncodeParams>) => void;
	export let onCommit: (patch: Partial<TransformCategoricalEncodeParams>) => void;

	const nested = (params as any)?.categorical_encode ?? params ?? {};
	$: encoding = String((nested as any)?.encoding ?? 'one_hot');
	$: unknownPolicy = String((nested as any)?.unknownPolicy ?? 'ignore');
	$: rareThreshold = Number((nested as any)?.rareThreshold ?? 0);
	$: dropFirst = Boolean((nested as any)?.dropFirst ?? false);
	$: columnsText = Array.isArray((nested as any)?.columns) ? (nested as any).columns.join(', ') : '';

	function parseColumns(raw: string): string[] {
		return raw.split(',').map((v) => v.trim()).filter((v) => v.length > 0);
	}

	function commitPatch(next: Partial<TransformCategoricalEncodeParams>, immediate = false) {
		const merged = { encoding, unknownPolicy, rareThreshold, dropFirst, columns: parseColumns(columnsText), ...next };
		onDraft({ op: 'categorical_encode', categorical_encode: merged } as unknown as Partial<TransformCategoricalEncodeParams>);
		if (immediate) {
			onCommit({ op: 'categorical_encode', categorical_encode: merged } as unknown as Partial<TransformCategoricalEncodeParams>);
		}
	}
</script>

<Section title="Categorical Encode">
	<Field label="columns (comma-separated)">
		<Input value={columnsText} onInput={(e) => commitPatch({ columns: parseColumns((e.currentTarget as HTMLInputElement).value) })} />
	</Field>
	<Field label="encoding">
		<select value={encoding} on:change={(e) => commitPatch({ encoding: (e.currentTarget as HTMLSelectElement).value as any }, true)}>
			<option value="one_hot">one_hot</option>
			<option value="ordinal">ordinal</option>
			<option value="frequency">frequency</option>
		</select>
	</Field>
	<Field label="unknown policy">
		<select value={unknownPolicy} on:change={(e) => commitPatch({ unknownPolicy: (e.currentTarget as HTMLSelectElement).value as any }, true)}>
			<option value="ignore">ignore</option>
			<option value="error">error</option>
			<option value="impute">impute</option>
		</select>
	</Field>
	<Field label="rare threshold">
		<Input value={String(rareThreshold)} onInput={(e) => commitPatch({ rareThreshold: Number((e.currentTarget as HTMLInputElement).value || 0) })} />
	</Field>
	<Field label="drop first">
		<input type="checkbox" checked={dropFirst} on:change={(e) => commitPatch({ dropFirst: (e.currentTarget as HTMLInputElement).checked }, true)} />
	</Field>
</Section>
