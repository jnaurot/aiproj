<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import type { TransformNlpNormalizeParams } from '$lib/flow/schema/transform';
	import { getTransformMeta } from '$lib/flow/schema/transformMeta';
	import { uniqueStrings } from '$lib/flow/components/editors/shared';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';
	import ConditionalHint from './ConditionalHint.svelte';
	import ColumnTokenInput from './ColumnTokenInput.svelte';

	export let selectedNode: Node<PipelineNodeData>;
	export let params: Partial<TransformNlpNormalizeParams> | Record<string, unknown>;
	export let onDraft: (patch: Partial<TransformNlpNormalizeParams>) => void;
	export let onCommit: (patch: Partial<TransformNlpNormalizeParams>) => void;
	export let inputColumns: string[] = [];
	export let inputSchemaColumns: Array<{ name: string; type?: string }> = [];

	$: void selectedNode?.id;
	$: void onCommit;
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
	const meta = getTransformMeta('nlp_normalize');

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
			return;
		}
		onDraft(merged);
	}
</script>

<Section title={meta.label}>
	<ConditionalHint text={meta.description} />

	<Field label="columns">
		<ColumnTokenInput value={columns} schema={columnOptions} onChange={(next) => commitPatch({ columns: next })} placeholder="Add NLP column" />
	</Field>

	<Field label="language">
		<Input
			value={language}
			placeholder="en"
			onInput={(event) => commitPatch({ language: (event.currentTarget as HTMLInputElement).value })}
		/>
	</Field>

	<Field label="token pattern">
		<Input
			value={tokenPattern}
			placeholder="\\w+"
			onInput={(event) => commitPatch({ tokenPattern: (event.currentTarget as HTMLInputElement).value })}
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
	.check {
		display: flex;
		align-items: center;
		gap: 8px;
		font-size: 13px;
	}
</style>
