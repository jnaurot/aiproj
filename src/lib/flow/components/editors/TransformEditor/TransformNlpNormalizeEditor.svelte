<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import type { TransformNlpNormalizeParams } from '$lib/flow/schema/transform';
	import { uniqueStrings } from '$lib/flow/components/editors/shared';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';

	export let selectedNode: Node<PipelineNodeData>;
	export let params: Partial<TransformNlpNormalizeParams> | Record<string, unknown>;
	export let onDraft: (patch: Partial<TransformNlpNormalizeParams>) => void;
	export let onCommit: (patch: Partial<TransformNlpNormalizeParams>) => void;
	export let inputColumns: string[] = [];
	export let inputSchemaColumns: Array<{ name: string; type?: string }> = [];

	$: void selectedNode?.id;
	$: nested = readNested(params);
	$: columns = normalizeColumns(nested.columns);
	$: language = nested.language ?? 'en';
	$: removeStopwords = nested.removeStopwords ?? true;
	$: stemmer = nested.stemmer ?? 'none';
	$: lemmatizer = nested.lemmatizer ?? 'none';
	$: tokenPattern = nested.tokenPattern ?? '\\w+';
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

	function readNested(raw: unknown): Partial<TransformNlpNormalizeParams> {
		if (!isObject(raw)) return {};
		if (isObject(raw.nlp_normalize)) return raw.nlp_normalize as Partial<TransformNlpNormalizeParams>;
		return raw as Partial<TransformNlpNormalizeParams>;
	}

	function isWrapped(raw: unknown): boolean {
		return isObject(raw) && ('op' in raw || 'nlp_normalize' in raw);
	}

	function normalizeColumns(raw: unknown): string[] {
		if (!Array.isArray(raw)) return [];
		return uniqueStrings(raw.map((v) => String(v ?? '').trim()).filter((v) => v.length > 0));
	}

	function commitPatch(next: Partial<TransformNlpNormalizeParams>): void {
		const merged = {
			columns,
			language,
			removeStopwords,
			stemmer,
			lemmatizer,
			tokenPattern,
			...next
		};
		if (isWrapped(params)) {
			onDraft({ op: 'nlp_normalize', nlp_normalize: merged } as unknown as Partial<TransformNlpNormalizeParams>);
			onCommit({ op: 'nlp_normalize', nlp_normalize: merged } as unknown as Partial<TransformNlpNormalizeParams>);
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

<Section title="NLP Normalize">
	<div class="hint">Stopwords + stem/lemma normalization with explicit language config.</div>

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

	<Field label="language">
		<Input
			value={language}
			placeholder="en"
			onInput={(event) => onDraft({ language: (event.currentTarget as HTMLInputElement).value })}
			onBlur={() => commitPatch({ language: language.trim() || 'en' })}
		/>
	</Field>

	<Field label="token pattern">
		<Input
			value={tokenPattern}
			placeholder="\\w+"
			onInput={(event) => onDraft({ tokenPattern: (event.currentTarget as HTMLInputElement).value })}
			onBlur={() => commitPatch({ tokenPattern: tokenPattern || '\\w+' })}
		/>
	</Field>

	<Field label="stemmer">
		<select value={stemmer} on:change={(event) => commitPatch({ stemmer: (event.currentTarget as HTMLSelectElement).value as TransformNlpNormalizeParams['stemmer'] })}>
			<option value="none">none</option>
			<option value="porter">porter</option>
		</select>
	</Field>

	<Field label="lemmatizer">
		<select value={lemmatizer} on:change={(event) => commitPatch({ lemmatizer: (event.currentTarget as HTMLSelectElement).value as TransformNlpNormalizeParams['lemmatizer'] })}>
			<option value="none">none</option>
			<option value="rule_based">rule_based</option>
		</select>
	</Field>

	<label class="check">
		<input type="checkbox" checked={removeStopwords} on:change={(event) => commitPatch({ removeStopwords: (event.currentTarget as HTMLInputElement).checked })} />
		<span>Remove stopwords</span>
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
