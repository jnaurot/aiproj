<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import type { TransformTokenizeChunkParams } from '$lib/flow/schema/transform';
	import { getTransformMeta } from '$lib/flow/schema/transformMeta';
	import { uniqueStrings } from '$lib/flow/components/editors/shared';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';
	import ConditionalHint from './ConditionalHint.svelte';
	import ColumnTokenInput from './ColumnTokenInput.svelte';

	export let selectedNode: Node<PipelineNodeData>;
	export let params: Partial<TransformTokenizeChunkParams> | Record<string, unknown>;
	export let onDraft: (patch: Partial<TransformTokenizeChunkParams>) => void;
	export let onCommit: (patch: Partial<TransformTokenizeChunkParams>) => void;
	export let inputColumns: string[] = [];
	export let inputSchemaColumns: Array<{ name: string; type?: string }> = [];

	$: void selectedNode?.id;
	$: void onCommit;
	$: nested = readNested(params);
	$: columns = normalizeColumns(nested.columns);
	$: tokenizer = nested.tokenizer ?? 'whitespace';
	$: tokenPattern = nested.tokenPattern ?? '\\w+';
	$: maxTokens = Number(nested.maxTokens ?? 256);
	$: overlap = Number(nested.overlap ?? 32);
	$: sentenceAware = nested.sentenceAware ?? true;
	$: outColumn = nested.outColumn ?? 'chunk';
	$: overlapValid = overlap < maxTokens;
	const meta = getTransformMeta('tokenize_chunk');
	$: schemaColumns = uniqueStrings(
		inputSchemaColumns.map((c) => String(c?.name ?? '').trim()).filter(Boolean)
	).sort((a, b) => a.localeCompare(b));
	$: fallbackColumns = uniqueStrings(inputColumns.map((c) => String(c ?? '').trim()).filter(Boolean)).sort(
		(a, b) => a.localeCompare(b)
	);
	$: columnOptions = schemaColumns.length > 0 ? schemaColumns : fallbackColumns;

	function isObject(v: unknown): v is Record<string, unknown> {
		return Boolean(v) && typeof v === 'object' && !Array.isArray(v);
	}

	function readNested(raw: unknown): Partial<TransformTokenizeChunkParams> {
		if (!isObject(raw)) return {};
		if (isObject(raw.tokenize_chunk)) return raw.tokenize_chunk as Partial<TransformTokenizeChunkParams>;
		return raw as Partial<TransformTokenizeChunkParams>;
	}

	function isWrapped(raw: unknown): boolean {
		return isObject(raw) && ('op' in raw || 'tokenize_chunk' in raw);
	}

	function normalizeColumns(raw: unknown): string[] {
		if (!Array.isArray(raw)) return [];
		return uniqueStrings(raw.map((v) => String(v ?? '').trim()).filter((v) => v.length > 0));
	}

	function commitPatch(next: Partial<TransformTokenizeChunkParams>): void {
		const merged = {
			columns,
			tokenizer,
			tokenPattern,
			maxTokens,
			overlap,
			sentenceAware,
			outColumn,
			...next
		};
		if (isWrapped(params)) {
			onDraft({ op: 'tokenize_chunk', tokenize_chunk: merged } as unknown as Partial<TransformTokenizeChunkParams>);
			return;
		}
		onDraft(merged);
	}
</script>

<Section title={meta.label}>
	<ConditionalHint text={meta.description} />

	<Field label="columns">
		<ColumnTokenInput value={columns} schema={columnOptions} onChange={(next) => commitPatch({ columns: next })} placeholder="Add text column" />
	</Field>

	<Field label="tokenizer">
		<select value={tokenizer} on:change={(event) => commitPatch({ tokenizer: (event.currentTarget as HTMLSelectElement).value as TransformTokenizeChunkParams['tokenizer'] })}>
			<option value="whitespace">whitespace</option>
			<option value="regex">regex</option>
		</select>
	</Field>

	{#if tokenizer === 'regex'}
		<ConditionalHint text="Regex tokenizer requires a token pattern." />
		<Field label="token pattern">
			<Input
				value={tokenPattern}
				placeholder="\\w+"
				onInput={(event) => commitPatch({ tokenPattern: (event.currentTarget as HTMLInputElement).value })}
			/>
		</Field>
	{/if}

	<Field label="max tokens">
		<Input
			type="number"
			min="1"
			max="100000"
			step="1"
			value={maxTokens}
			onInput={(event) => commitPatch({ maxTokens: Number((event.currentTarget as HTMLInputElement).value || 256) })}
		/>
	</Field>

	<Field label="overlap">
		<Input
			type="number"
			min="0"
			max="50000"
			step="1"
			value={overlap}
			onInput={(event) => commitPatch({ overlap: Number((event.currentTarget as HTMLInputElement).value || 0) })}
		/>
	</Field>
	{#if !overlapValid}
		<ConditionalHint tone="warn" text="Overlap must be less than max tokens." />
	{/if}

	<Field label="out column">
		<Input
			value={outColumn}
			placeholder="chunk"
			onInput={(event) => commitPatch({ outColumn: (event.currentTarget as HTMLInputElement).value })}
		/>
	</Field>

	<label class="check">
		<input type="checkbox" checked={sentenceAware} on:change={(event) => commitPatch({ sentenceAware: (event.currentTarget as HTMLInputElement).checked })} />
		<span>Sentence-aware boundaries</span>
	</label>
</Section>

<style>
	.check {
		display: flex;
		align-items: center;
		gap: 8px;
		font-size: 13px;
	}
</style>
