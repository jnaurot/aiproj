<script lang="ts">
	import type { TransformFeatureSelectionParams } from '$lib/flow/schema/transform';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';

	export let params: Partial<TransformFeatureSelectionParams> | Record<string, unknown>;
	export let onDraft: (patch: Partial<TransformFeatureSelectionParams>) => void;
	export let onCommit: (patch: Partial<TransformFeatureSelectionParams>) => void;

	const nested = (params as any)?.feature_selection ?? params ?? {};
	$: method = String((nested as any)?.method ?? 'variance');
	$: topK = Number((nested as any)?.topK ?? 50);
	$: varianceThreshold = Number((nested as any)?.varianceThreshold ?? 0);
	$: targetColumn = String((nested as any)?.targetColumn ?? 'label');
	$: columnsText = Array.isArray((nested as any)?.columns) ? (nested as any).columns.join(', ') : '';
	$: selectedText = Array.isArray((nested as any)?.selectedColumns) ? (nested as any).selectedColumns.join(', ') : '';

	function parseColumns(raw: string): string[] {
		return raw.split(',').map((v) => v.trim()).filter((v) => v.length > 0);
	}

	function commitPatch(next: Partial<TransformFeatureSelectionParams>) {
		const merged = { method, topK, varianceThreshold, targetColumn, columns: parseColumns(columnsText), selectedColumns: parseColumns(selectedText), ...next };
		onDraft({ op: 'feature_selection', feature_selection: merged } as unknown as Partial<TransformFeatureSelectionParams>);
		onCommit({ op: 'feature_selection', feature_selection: merged } as unknown as Partial<TransformFeatureSelectionParams>);
	}
</script>

<Section title="Feature Selection">
	<Field label="method">
		<select value={method} on:change={(e) => commitPatch({ method: (e.currentTarget as HTMLSelectElement).value as any })}>
			<option value="variance">variance</option>
			<option value="mutual_info">mutual_info</option>
			<option value="model_importance">model_importance</option>
			<option value="manual">manual</option>
		</select>
	</Field>
	<Field label="candidate columns">
		<Input value={columnsText} onInput={(e) => commitPatch({ columns: parseColumns((e.currentTarget as HTMLInputElement).value) })} />
	</Field>
	{#if method === 'manual'}
		<Field label="selected columns">
			<Input value={selectedText} onInput={(e) => commitPatch({ selectedColumns: parseColumns((e.currentTarget as HTMLInputElement).value) })} />
		</Field>
	{/if}
	<Field label="top K">
		<Input value={String(topK)} onInput={(e) => commitPatch({ topK: Number((e.currentTarget as HTMLInputElement).value || 1) })} />
	</Field>
	<Field label="variance threshold">
		<Input value={String(varianceThreshold)} onInput={(e) => commitPatch({ varianceThreshold: Number((e.currentTarget as HTMLInputElement).value || 0) })} />
	</Field>
	<Field label="target column">
		<Input value={targetColumn} onInput={(e) => commitPatch({ targetColumn: (e.currentTarget as HTMLInputElement).value })} />
	</Field>
</Section>
