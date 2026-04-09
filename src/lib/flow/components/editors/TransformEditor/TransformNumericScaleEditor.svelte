<script lang="ts">
	import type { TransformNumericScaleParams } from '$lib/flow/schema/transform';
	import { getTransformMeta } from '$lib/flow/schema/transformMeta';
	import { uniqueStrings } from '$lib/flow/components/editors/shared';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';
	import ConditionalHint from './ConditionalHint.svelte';
	import ColumnTokenInput from './ColumnTokenInput.svelte';

	export let params: Partial<TransformNumericScaleParams> | Record<string, unknown>;
	export let onDraft: (patch: Partial<TransformNumericScaleParams>) => void;
	export let onCommit: (patch: Partial<TransformNumericScaleParams>) => void;
	export let inputColumns: string[] = [];
	export let inputSchemaColumns: Array<{ name: string; type?: string }> = [];

	const nested = (params as any)?.numeric_scale ?? params ?? {};
	$: method = String((nested as any)?.method ?? 'standard');
	$: clip = Boolean((nested as any)?.clip ?? false);
	$: columns = Array.isArray((nested as any)?.columns)
		? uniqueStrings((nested as any).columns.map((v: unknown) => String(v ?? '').trim()).filter(Boolean))
		: [];
	$: clipMin = (nested as any)?.clipMin;
	$: clipMax = (nested as any)?.clipMax;
	$: schemaColumns = uniqueStrings(
		inputSchemaColumns.map((c) => String(c?.name ?? '').trim()).filter(Boolean)
	).sort((a, b) => a.localeCompare(b));
	$: fallbackColumns = uniqueStrings(inputColumns.map((c) => String(c ?? '').trim()).filter(Boolean)).sort(
		(a, b) => a.localeCompare(b)
	);
	$: columnOptions = schemaColumns.length > 0 ? schemaColumns : fallbackColumns;
	const meta = getTransformMeta('numeric_scale');

	function commitPatch(next: Partial<TransformNumericScaleParams>, immediate = false) {
		const merged = { method, clip, columns, clipMin, clipMax, ...next };
		onDraft({ op: 'numeric_scale', numeric_scale: merged } as unknown as Partial<TransformNumericScaleParams>);
		if (immediate) {
			onCommit({ op: 'numeric_scale', numeric_scale: merged } as unknown as Partial<TransformNumericScaleParams>);
		}
	}
</script>

<Section title={meta.label}>
	<ConditionalHint text={meta.description} />
	<Field label="columns">
		<ColumnTokenInput value={columns} schema={columnOptions} onChange={(next) => commitPatch({ columns: next })} placeholder="Add numeric column" />
	</Field>
	<Field label="method">
		<select value={method} on:change={(e) => commitPatch({ method: (e.currentTarget as HTMLSelectElement).value as any }, true)}>
			<option value="standard">standard</option>
			<option value="minmax">minmax</option>
			<option value="robust">robust</option>
		</select>
	</Field>
	<Field label="clip">
		<input type="checkbox" checked={clip} on:change={(e) => commitPatch({ clip: (e.currentTarget as HTMLInputElement).checked }, true)} />
	</Field>
	{#if clip}
		<ConditionalHint text="Clip bounds are only applied when clip is enabled." />
		<Field label="clip min">
			<Input type="number" value={clipMin === undefined ? '' : String(clipMin)} onInput={(e) => commitPatch({ clipMin: Number((e.currentTarget as HTMLInputElement).value || 0) })} />
		</Field>
		<Field label="clip max">
			<Input type="number" value={clipMax === undefined ? '' : String(clipMax)} onInput={(e) => commitPatch({ clipMax: Number((e.currentTarget as HTMLInputElement).value || 0) })} />
		</Field>
	{/if}
</Section>
