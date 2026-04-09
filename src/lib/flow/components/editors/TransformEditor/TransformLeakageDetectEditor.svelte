<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import type { TransformLeakageDetectParams } from '$lib/flow/schema/transform';
	import { uniqueStrings } from '$lib/flow/components/editors/shared';
	import { getTransformMeta } from '$lib/flow/schema/transformMeta';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';
	import ConditionalHint from './ConditionalHint.svelte';
	import ColumnTokenInput from './ColumnTokenInput.svelte';

	export let selectedNode: Node<PipelineNodeData>;
	export let params: Partial<TransformLeakageDetectParams> | Record<string, unknown>;
	export let onDraft: (patch: Partial<TransformLeakageDetectParams>) => void;
	export let onCommit: (patch: Partial<TransformLeakageDetectParams>) => void;
	export let inputColumns: string[] = [];
	export let inputSchemaColumns: Array<{ name: string; type?: string }> = [];

	$: void selectedNode?.id;
	const nested = (params as any)?.leakage_detect ?? params ?? {};
	$: splitColumn = String((nested as any)?.splitColumn ?? 'split');
	$: labelColumn = String((nested as any)?.labelColumn ?? '');
	$: maxAllowedOverlap = Number((nested as any)?.maxAllowedOverlap ?? 0);
	$: keyColumns = Array.isArray((nested as any)?.keyColumns)
		? uniqueStrings((nested as any).keyColumns.map((v: unknown) => String(v ?? '').trim()).filter(Boolean))
		: [];
	$: schemaColumns = uniqueStrings(
		inputSchemaColumns.map((c) => String(c?.name ?? '').trim()).filter(Boolean)
	).sort((a, b) => a.localeCompare(b));
	$: fallbackColumns = uniqueStrings(inputColumns.map((c) => String(c ?? '').trim()).filter(Boolean)).sort(
		(a, b) => a.localeCompare(b)
	);
	$: columnOptions = schemaColumns.length > 0 ? schemaColumns : fallbackColumns;
	const meta = getTransformMeta('leakage_detect');

	function commitPatch(next: Partial<TransformLeakageDetectParams>, immediate = false): void {
		const merged = { splitColumn, keyColumns, labelColumn, maxAllowedOverlap, ...next };
		onDraft({ op: 'leakage_detect', leakage_detect: merged } as unknown as Partial<TransformLeakageDetectParams>);
		if (immediate) onCommit({ op: 'leakage_detect', leakage_detect: merged } as unknown as Partial<TransformLeakageDetectParams>);
	}
</script>

<Section title={meta.label}>
	<ConditionalHint text={meta.description} />

	<Field label="split column">
		<Input value={splitColumn} onInput={(e) => commitPatch({ splitColumn: (e.currentTarget as HTMLInputElement).value })} />
	</Field>

	<Field label="key columns">
		<ColumnTokenInput value={keyColumns} schema={columnOptions} onChange={(next) => commitPatch({ keyColumns: next })} placeholder="Add key column" />
	</Field>

	<Field label="label column (optional)">
		<Input value={labelColumn} onInput={(e) => commitPatch({ labelColumn: (e.currentTarget as HTMLInputElement).value })} />
	</Field>

	<Field label="max allowed overlap">
		<Input type="number" min="0" max="1" step="0.01" value={maxAllowedOverlap} onInput={(e) => commitPatch({ maxAllowedOverlap: Number((e.currentTarget as HTMLInputElement).value || 0) })} />
	</Field>
</Section>
