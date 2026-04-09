<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import type { TransformQualityProfileParams } from '$lib/flow/schema/transform';
	import { uniqueStrings } from '$lib/flow/components/editors/shared';
	import { getTransformMeta } from '$lib/flow/schema/transformMeta';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import ConditionalHint from './ConditionalHint.svelte';
	import ColumnTokenInput from './ColumnTokenInput.svelte';

	export let selectedNode: Node<PipelineNodeData>;
	export let params: Partial<TransformQualityProfileParams> | Record<string, unknown>;
	export let onDraft: (patch: Partial<TransformQualityProfileParams>) => void;
	export let onCommit: (patch: Partial<TransformQualityProfileParams>) => void;
	export let inputColumns: string[] = [];
	export let inputSchemaColumns: Array<{ name: string; type?: string }> = [];

	$: void selectedNode?.id;
	const nested = (params as any)?.quality_profile ?? params ?? {};
	$: columns = Array.isArray((nested as any)?.columns)
		? uniqueStrings((nested as any).columns.map((v: unknown) => String(v ?? '').trim()).filter(Boolean))
		: [];
	$: includeHistograms = Boolean((nested as any)?.includeHistograms ?? true);
	$: includeSamples = Boolean((nested as any)?.includeSamples ?? true);
	$: schemaColumns = uniqueStrings(
		inputSchemaColumns.map((c) => String(c?.name ?? '').trim()).filter(Boolean)
	).sort((a, b) => a.localeCompare(b));
	$: fallbackColumns = uniqueStrings(inputColumns.map((c) => String(c ?? '').trim()).filter(Boolean)).sort(
		(a, b) => a.localeCompare(b)
	);
	$: columnOptions = schemaColumns.length > 0 ? schemaColumns : fallbackColumns;
	const meta = getTransformMeta('quality_profile');

	function commitPatch(next: Partial<TransformQualityProfileParams>, immediate = false): void {
		const merged = { columns, includeHistograms, includeSamples, ...next };
		onDraft({ op: 'quality_profile', quality_profile: merged } as unknown as Partial<TransformQualityProfileParams>);
		if (immediate) onCommit({ op: 'quality_profile', quality_profile: merged } as unknown as Partial<TransformQualityProfileParams>);
	}
</script>

<Section title={meta.label}>
	<ConditionalHint text={meta.description} />

	<Field label="columns">
		<ColumnTokenInput value={columns} schema={columnOptions} onChange={(next) => commitPatch({ columns: next })} placeholder="Add profile column" />
	</Field>

	<Field label="include histograms">
		<input type="checkbox" checked={includeHistograms} on:change={(e) => commitPatch({ includeHistograms: (e.currentTarget as HTMLInputElement).checked }, true)} />
	</Field>

	<Field label="include samples">
		<input type="checkbox" checked={includeSamples} on:change={(e) => commitPatch({ includeSamples: (e.currentTarget as HTMLInputElement).checked }, true)} />
	</Field>
</Section>
