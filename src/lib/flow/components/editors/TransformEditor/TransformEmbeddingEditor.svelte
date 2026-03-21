<script lang="ts">
	import type { TransformEmbeddingParams } from '$lib/flow/schema/transform';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';

	export let params: Partial<TransformEmbeddingParams> | Record<string, unknown>;
	export let onDraft: (patch: Partial<TransformEmbeddingParams>) => void;
	export let onCommit: (patch: Partial<TransformEmbeddingParams>) => void;

	const nested = (params as any)?.embedding ?? params ?? {};
	$: provider = String((nested as any)?.provider ?? 'local_hash');
	$: model = String((nested as any)?.model ?? 'text-embedding-3-small');
	$: dimensions = Number((nested as any)?.dimensions ?? 16);
	$: outputColumn = String((nested as any)?.outputColumn ?? 'embedding');
	$: columnsText = Array.isArray((nested as any)?.columns) ? (nested as any).columns.join(', ') : '';

	function parseColumns(raw: string): string[] {
		return raw.split(',').map((v) => v.trim()).filter((v) => v.length > 0);
	}

	function commitPatch(next: Partial<TransformEmbeddingParams>) {
		const merged = { provider, model, dimensions, outputColumn, columns: parseColumns(columnsText), ...next };
		onDraft({ op: 'embedding', embedding: merged } as unknown as Partial<TransformEmbeddingParams>);
		onCommit({ op: 'embedding', embedding: merged } as unknown as Partial<TransformEmbeddingParams>);
	}
</script>

<Section title="Embedding">
	<Field label="columns (comma-separated)">
		<Input value={columnsText} onInput={(e) => commitPatch({ columns: parseColumns((e.currentTarget as HTMLInputElement).value) })} />
	</Field>
	<Field label="provider">
		<select value={provider} on:change={(e) => commitPatch({ provider: (e.currentTarget as HTMLSelectElement).value as any })}>
			<option value="local_hash">local_hash</option>
			<option value="openai">openai</option>
			<option value="ollama">ollama</option>
		</select>
	</Field>
	<Field label="model">
		<Input value={model} onInput={(e) => commitPatch({ model: (e.currentTarget as HTMLInputElement).value })} />
	</Field>
	<Field label="dimensions">
		<Input value={String(dimensions)} onInput={(e) => commitPatch({ dimensions: Number((e.currentTarget as HTMLInputElement).value || 1) })} />
	</Field>
	<Field label="output column">
		<Input value={outputColumn} onInput={(e) => commitPatch({ outputColumn: (e.currentTarget as HTMLInputElement).value })} />
	</Field>
</Section>
