<script lang="ts">
	import type { TransformFeatureSelectionParams } from '$lib/flow/schema/transform';
	import { getTransformMeta } from '$lib/flow/schema/transformMeta';
	import { uniqueStrings } from '$lib/flow/components/editors/shared';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';
	import ConditionalHint from './ConditionalHint.svelte';
	import ColumnTokenInput from './ColumnTokenInput.svelte';

	export let params: Partial<TransformFeatureSelectionParams> | Record<string, unknown>;
	export let onDraft: (patch: Partial<TransformFeatureSelectionParams>) => void;
	export let onCommit: (patch: Partial<TransformFeatureSelectionParams>) => void;
	export let inputColumns: string[] = [];
	export let inputSchemaColumns: Array<{ name: string; type?: string }> = [];

	const nested = (params as any)?.feature_selection ?? params ?? {};
	$: method = String((nested as any)?.method ?? 'variance');
	$: topK = Number((nested as any)?.topK ?? 50);
	$: varianceThreshold = Number((nested as any)?.varianceThreshold ?? 0);
	$: targetColumn = String((nested as any)?.targetColumn ?? 'label');
	$: columns = Array.isArray((nested as any)?.columns)
		? uniqueStrings((nested as any).columns.map((v: unknown) => String(v ?? '').trim()).filter(Boolean))
		: [];
	$: selectedColumns = Array.isArray((nested as any)?.selectedColumns)
		? uniqueStrings((nested as any).selectedColumns.map((v: unknown) => String(v ?? '').trim()).filter(Boolean))
		: [];
	$: schemaColumns = uniqueStrings(
		inputSchemaColumns.map((c) => String(c?.name ?? '').trim()).filter(Boolean)
	).sort((a, b) => a.localeCompare(b));
	$: fallbackColumns = uniqueStrings(inputColumns.map((c) => String(c ?? '').trim()).filter(Boolean)).sort(
		(a, b) => a.localeCompare(b)
	);
	$: columnOptions = schemaColumns.length > 0 ? schemaColumns : fallbackColumns;
	const meta = getTransformMeta('feature_selection');

	function commitPatch(next: Partial<TransformFeatureSelectionParams>, immediate = false) {
		const merged = { method, topK, varianceThreshold, targetColumn, columns, selectedColumns, ...next };
		onDraft({ op: 'feature_selection', feature_selection: merged } as unknown as Partial<TransformFeatureSelectionParams>);
		if (immediate) {
			onCommit({ op: 'feature_selection', feature_selection: merged } as unknown as Partial<TransformFeatureSelectionParams>);
		}
	}
</script>

<Section title={meta.label}>
	<ConditionalHint text={meta.description} />
	<Field label="method">
		<select value={method} on:change={(e) => commitPatch({ method: (e.currentTarget as HTMLSelectElement).value as any }, true)}>
			<option value="variance">variance</option>
			<option value="mutual_info">mutual_info</option>
			<option value="model_importance">model_importance</option>
			<option value="manual">manual</option>
		</select>
	</Field>
	<Field label="candidate columns">
		<ColumnTokenInput value={columns} schema={columnOptions} onChange={(next) => commitPatch({ columns: next })} placeholder="Add candidate column" />
	</Field>
	{#if method === 'manual'}
		<ConditionalHint text="Manual method requires selected columns below." />
		<Field label="selected columns">
			<ColumnTokenInput value={selectedColumns} schema={columnOptions} onChange={(next) => commitPatch({ selectedColumns: next })} placeholder="Add selected column" />
		</Field>
	{/if}
	<Field label="top K">
		<Input type="number" min="1" step="1" value={String(topK)} onInput={(e) => commitPatch({ topK: Number((e.currentTarget as HTMLInputElement).value || 1) })} />
	</Field>
	<Field label="variance threshold">
		<Input type="number" min="0" step="0.01" value={String(varianceThreshold)} onInput={(e) => commitPatch({ varianceThreshold: Number((e.currentTarget as HTMLInputElement).value || 0) })} />
	</Field>
	<Field label="target column">
		<Input value={targetColumn} onInput={(e) => commitPatch({ targetColumn: (e.currentTarget as HTMLInputElement).value })} />
	</Field>
</Section>
