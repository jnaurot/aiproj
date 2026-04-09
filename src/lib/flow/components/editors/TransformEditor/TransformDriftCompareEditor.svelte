<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import type { TransformDriftCompareParams } from '$lib/flow/schema/transform';
	import { uniqueStrings } from '$lib/flow/components/editors/shared';
	import { getTransformMeta } from '$lib/flow/schema/transformMeta';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';
	import ConditionalHint from './ConditionalHint.svelte';
	import ColumnTokenInput from './ColumnTokenInput.svelte';

	export let selectedNode: Node<PipelineNodeData>;
	export let params: Partial<TransformDriftCompareParams> | Record<string, unknown>;
	export let onDraft: (patch: Partial<TransformDriftCompareParams>) => void;
	export let onCommit: (patch: Partial<TransformDriftCompareParams>) => void;
	export let inputColumns: string[] = [];
	export let inputSchemaColumns: Array<{ name: string; type?: string }> = [];

	$: void selectedNode?.id;
	const nested = (params as any)?.drift_compare ?? params ?? {};
	$: baselineRef = String((nested as any)?.baselineRef ?? '');
	$: metric = String((nested as any)?.metric ?? 'psi') as TransformDriftCompareParams['metric'];
	$: threshold = Number((nested as any)?.threshold ?? 0.2);
	$: failOnDrift = Boolean((nested as any)?.failOnDrift ?? false);
	$: compareColumns = Array.isArray((nested as any)?.compareColumns)
		? uniqueStrings((nested as any).compareColumns.map((v: unknown) => String(v ?? '').trim()).filter(Boolean))
		: [];
	$: schemaColumns = uniqueStrings(
		inputSchemaColumns.map((c) => String(c?.name ?? '').trim()).filter(Boolean)
	).sort((a, b) => a.localeCompare(b));
	$: fallbackColumns = uniqueStrings(inputColumns.map((c) => String(c ?? '').trim()).filter(Boolean)).sort(
		(a, b) => a.localeCompare(b)
	);
	$: columnOptions = schemaColumns.length > 0 ? schemaColumns : fallbackColumns;
	const meta = getTransformMeta('drift_compare');

	function commitPatch(next: Partial<TransformDriftCompareParams>, immediate = false): void {
		const merged = { baselineRef, compareColumns, metric, threshold, failOnDrift, ...next };
		onDraft({ op: 'drift_compare', drift_compare: merged } as unknown as Partial<TransformDriftCompareParams>);
		if (immediate) onCommit({ op: 'drift_compare', drift_compare: merged } as unknown as Partial<TransformDriftCompareParams>);
	}
</script>

<Section title={meta.label}>
	<ConditionalHint text={meta.description} />

	<Field label="baseline ref">
		<Input value={baselineRef} placeholder="baseline id or ref" onInput={(e) => commitPatch({ baselineRef: (e.currentTarget as HTMLInputElement).value })} />
	</Field>

	<Field label="compare columns">
		<ColumnTokenInput value={compareColumns} schema={columnOptions} onChange={(next) => commitPatch({ compareColumns: next })} placeholder="Add compare column" />
	</Field>

	<Field label="metric">
		<select value={metric} on:change={(e) => commitPatch({ metric: (e.currentTarget as HTMLSelectElement).value as TransformDriftCompareParams['metric'] }, true)}>
			<option value="psi">psi</option>
			<option value="jsd">jsd</option>
			<option value="ks">ks</option>
		</select>
	</Field>

	<Field label="threshold">
		<Input type="number" min="0" step="0.01" value={threshold} onInput={(e) => commitPatch({ threshold: Number((e.currentTarget as HTMLInputElement).value || 0) })} />
	</Field>

	<Field label="fail on drift">
		<input type="checkbox" checked={failOnDrift} on:change={(e) => commitPatch({ failOnDrift: (e.currentTarget as HTMLInputElement).checked }, true)} />
	</Field>
</Section>
