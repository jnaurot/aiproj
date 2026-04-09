<script lang="ts">
	import type { TransformCategoricalEncodeParams } from '$lib/flow/schema/transform';
	import { getTransformMeta } from '$lib/flow/schema/transformMeta';
	import { uniqueStrings } from '$lib/flow/components/editors/shared';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';
	import ConditionalHint from './ConditionalHint.svelte';
	import ColumnTokenInput from './ColumnTokenInput.svelte';

	export let params: Partial<TransformCategoricalEncodeParams> | Record<string, unknown>;
	export let onDraft: (patch: Partial<TransformCategoricalEncodeParams>) => void;
	export let onCommit: (patch: Partial<TransformCategoricalEncodeParams>) => void;
	export let inputColumns: string[] = [];
	export let inputSchemaColumns: Array<{ name: string; type?: string }> = [];

	const nested = (params as any)?.categorical_encode ?? params ?? {};
	$: encoding = String((nested as any)?.encoding ?? 'one_hot');
	$: unknownPolicy = String((nested as any)?.unknownPolicy ?? 'ignore');
	$: rareThreshold = Number((nested as any)?.rareThreshold ?? 0);
	$: dropFirst = Boolean((nested as any)?.dropFirst ?? false);
	$: columns = Array.isArray((nested as any)?.columns)
		? uniqueStrings((nested as any).columns.map((v: unknown) => String(v ?? '').trim()).filter(Boolean))
		: [];
	$: schemaColumns = uniqueStrings(
		inputSchemaColumns.map((c) => String(c?.name ?? '').trim()).filter(Boolean)
	).sort((a, b) => a.localeCompare(b));
	$: fallbackColumns = uniqueStrings(inputColumns.map((c) => String(c ?? '').trim()).filter(Boolean)).sort(
		(a, b) => a.localeCompare(b)
	);
	$: columnOptions = schemaColumns.length > 0 ? schemaColumns : fallbackColumns;
	const meta = getTransformMeta('categorical_encode');

	function commitPatch(next: Partial<TransformCategoricalEncodeParams>, immediate = false) {
		const merged = { encoding, unknownPolicy, rareThreshold, dropFirst, columns, ...next };
		onDraft({ op: 'categorical_encode', categorical_encode: merged } as unknown as Partial<TransformCategoricalEncodeParams>);
		if (immediate) {
			onCommit({ op: 'categorical_encode', categorical_encode: merged } as unknown as Partial<TransformCategoricalEncodeParams>);
		}
	}
</script>

<Section title={meta.label}>
	<ConditionalHint text={meta.description} />
	<Field label="columns">
		<ColumnTokenInput value={columns} schema={columnOptions} onChange={(next) => commitPatch({ columns: next })} placeholder="Add categorical column" />
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
		<Input type="number" min="0" max="1" step="0.01" value={String(rareThreshold)} onInput={(e) => commitPatch({ rareThreshold: Number((e.currentTarget as HTMLInputElement).value || 0) })} />
	</Field>
	<Field label="drop first">
		<input type="checkbox" checked={dropFirst} on:change={(e) => commitPatch({ dropFirst: (e.currentTarget as HTMLInputElement).checked }, true)} />
	</Field>
</Section>
