<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import type { TransformTokenizeChunkParams } from '$lib/flow/schema/transform';
	import { uniqueStrings } from '$lib/flow/components/editors/shared';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';

	export let selectedNode: Node<PipelineNodeData>;
	export let params: Partial<TransformTokenizeChunkParams> | Record<string, unknown>;
	export let onDraft: (patch: Partial<TransformTokenizeChunkParams>) => void;
	export let onCommit: (patch: Partial<TransformTokenizeChunkParams>) => void;
	export let inputColumns: string[] = [];
	export let inputSchemaColumns: Array<{ name: string; type?: string }> = [];

	$: void selectedNode?.id;
	$: nested = readNested(params);
	$: columns = normalizeColumns(nested.columns);
	$: tokenizer = nested.tokenizer ?? 'whitespace';
	$: tokenPattern = nested.tokenPattern ?? '\\w+';
	$: maxTokens = Number(nested.maxTokens ?? 256);
	$: overlap = Number(nested.overlap ?? 32);
	$: sentenceAware = nested.sentenceAware ?? true;
	$: outColumn = nested.outColumn ?? 'chunk';
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
			onCommit({ op: 'tokenize_chunk', tokenize_chunk: merged } as unknown as Partial<TransformTokenizeChunkParams>);
			return;
		}
		onDraft(merged);
		onCommit(merged);
	}

	function addColumn(col: string): void {
		const value = String(col ?? '').trim();
		if (!value) return;
		commitPatch({ columns: uniqueStrings([...columns, value]) });
	}

	function removeColumn(col: string): void {
		const value = String(col ?? '').trim();
		commitPatch({ columns: columns.filter((c) => c !== value) });
	}
</script>

<Section title="Tokenize Chunk">
	<div class="hint">Chunk text by token budget with overlap and sentence-aware boundaries.</div>

	<Field label="columns">
		<select on:change={(event) => addColumn((event.currentTarget as HTMLSelectElement).value)}>
			<option value="">Select column...</option>
			{#each columnOptions as col (col)}
				<option value={col}>{col}</option>
			{/each}
		</select>
		{#if columns.length > 0}
			<div class="chips">
				{#each columns as col (col)}
					<button type="button" class="chip" on:click={() => removeColumn(col)}>{col} ×</button>
				{/each}
			</div>
		{/if}
	</Field>

	<Field label="tokenizer">
		<select value={tokenizer} on:change={(event) => commitPatch({ tokenizer: (event.currentTarget as HTMLSelectElement).value as TransformTokenizeChunkParams['tokenizer'] })}>
			<option value="whitespace">whitespace</option>
			<option value="regex">regex</option>
		</select>
	</Field>

	{#if tokenizer === 'regex'}
		<Field label="token pattern">
			<Input
				value={tokenPattern}
				placeholder="\\w+"
				onInput={(event) => onDraft({ tokenPattern: (event.currentTarget as HTMLInputElement).value })}
				onBlur={() => commitPatch({ tokenPattern: tokenPattern || '\\w+' })}
			/>
		</Field>
	{/if}

	<Field label="max tokens">
		<Input
			type="number"
			min="1"
			step="1"
			value={maxTokens}
			onInput={(event) => onDraft({ maxTokens: Number((event.currentTarget as HTMLInputElement).value || 256) })}
			onBlur={() => commitPatch({ maxTokens: Math.max(1, Number(maxTokens) || 256) })}
		/>
	</Field>

	<Field label="overlap">
		<Input
			type="number"
			min="0"
			step="1"
			value={overlap}
			onInput={(event) => onDraft({ overlap: Number((event.currentTarget as HTMLInputElement).value || 0) })}
			onBlur={() => commitPatch({ overlap: Math.max(0, Math.min((Number(maxTokens) || 256) - 1, Number(overlap) || 0)) })}
		/>
	</Field>

	<Field label="out column">
		<Input
			value={outColumn}
			placeholder="chunk"
			onInput={(event) => onDraft({ outColumn: (event.currentTarget as HTMLInputElement).value })}
			onBlur={() => commitPatch({ outColumn: outColumn.trim() || 'chunk' })}
		/>
	</Field>

	<label class="check">
		<input type="checkbox" checked={sentenceAware} on:change={(event) => commitPatch({ sentenceAware: (event.currentTarget as HTMLInputElement).checked })} />
		<span>Sentence-aware boundaries</span>
	</label>
</Section>

<style>
	.hint {
		font-size: 12px;
		opacity: 0.75;
		margin-top: 6px;
	}
	.chips {
		display: flex;
		flex-wrap: wrap;
		gap: 6px;
		margin-top: 8px;
	}
	.chip {
		border: 1px solid rgba(148, 163, 184, 0.4);
		background: rgba(15, 23, 42, 0.45);
		color: #e2e8f0;
		border-radius: 999px;
		padding: 2px 10px;
		font-size: 12px;
		cursor: pointer;
	}
	.check {
		display: flex;
		align-items: center;
		gap: 8px;
		font-size: 13px;
	}
</style>
