<script lang="ts">
	import type { TransformEmbeddingParams } from '$lib/flow/schema/transform';
	import { getTransformMeta } from '$lib/flow/schema/transformMeta';
	import { uniqueStrings } from '$lib/flow/components/editors/shared';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';
	import ConditionalHint from './ConditionalHint.svelte';
	import ColumnTokenInput from './ColumnTokenInput.svelte';

	export let params: Partial<TransformEmbeddingParams> | Record<string, unknown>;
	export let onDraft: (patch: Partial<TransformEmbeddingParams>) => void;
	export let onCommit: (patch: Partial<TransformEmbeddingParams>) => void;
	export let inputColumns: string[] = [];
	export let inputSchemaColumns: Array<{ name: string; type?: string }> = [];

	const nested = (params as any)?.embedding ?? params ?? {};
	$: provider = String((nested as any)?.provider ?? 'local_hash');
	$: model = String((nested as any)?.model ?? 'text-embedding-3-small');
	$: dimensions = Number((nested as any)?.dimensions ?? 16);
	$: batchSize = Number((nested as any)?.batchSize ?? 64);
	$: cacheEmbeddings = Boolean((nested as any)?.cacheEmbeddings ?? true);
	$: outputColumn = String((nested as any)?.outputColumn ?? 'embedding');
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
	const meta = getTransformMeta('embedding');

	function commitPatch(next: Partial<TransformEmbeddingParams>, immediate = false) {
		const merged = {
			provider,
			model,
			dimensions,
			batchSize,
			cacheEmbeddings,
			outputColumn,
			columns,
			...next
		};
		onDraft({ op: 'embedding', embedding: merged } as unknown as Partial<TransformEmbeddingParams>);
		if (immediate) {
			onCommit({ op: 'embedding', embedding: merged } as unknown as Partial<TransformEmbeddingParams>);
		}
	}
</script>

<Section title={meta.label}>
	<ConditionalHint text={meta.description} />
	<Field label="columns">
		<ColumnTokenInput value={columns} schema={columnOptions} onChange={(next) => commitPatch({ columns: next })} placeholder="Add text column" />
	</Field>
	<Field label="provider">
		<select value={provider} on:change={(e) => commitPatch({ provider: (e.currentTarget as HTMLSelectElement).value as any }, true)}>
			<option value="local_hash">local_hash</option>
			<option value="openai">openai</option>
			<option value="ollama">ollama</option>
		</select>
	</Field>
	<Field label="model">
		<Input value={model} onInput={(e) => commitPatch({ model: (e.currentTarget as HTMLInputElement).value })} />
	</Field>
	<Field label="dimensions">
		<Input type="number" min="1" max="4096" step="1" value={String(dimensions)} onInput={(e) => commitPatch({ dimensions: Number((e.currentTarget as HTMLInputElement).value || 1) })} />
	</Field>
	<Field label="batch size">
		<Input type="number" min="1" max="2048" step="1" value={String(batchSize)} onInput={(e) => commitPatch({ batchSize: Number((e.currentTarget as HTMLInputElement).value || 1) })} />
	</Field>
	<Field label="cache embeddings">
		<input type="checkbox" checked={cacheEmbeddings} on:change={(e) => commitPatch({ cacheEmbeddings: (e.currentTarget as HTMLInputElement).checked }, true)} />
	</Field>
	<Field label="output column">
		<Input value={outputColumn} onInput={(e) => commitPatch({ outputColumn: (e.currentTarget as HTMLInputElement).value })} />
	</Field>
</Section>
