<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import type { TransformTextCleanParams } from '$lib/flow/schema/transform';
	import { uniqueStrings } from '$lib/flow/components/editors/shared';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';

	export let selectedNode: Node<PipelineNodeData>;
	export let params: Partial<TransformTextCleanParams> | Record<string, unknown>;
	export let onDraft: (patch: Partial<TransformTextCleanParams>) => void;
	export let onCommit: (patch: Partial<TransformTextCleanParams>) => void;
	export let inputColumns: string[] = [];
	export let inputSchemaColumns: Array<{ name: string; type?: string }> = [];

	$: void selectedNode?.id;
	$: nested = readNested(params);
	$: columns = normalizeColumns(nested.columns);
	$: lowercase = nested.lowercase ?? true;
	$: unicodeNormalize = nested.unicodeNormalize ?? 'nfkc';
	$: removePunctuation = nested.removePunctuation ?? false;
	$: removeUrls = nested.removeUrls ?? true;
	$: removeEmails = nested.removeEmails ?? true;
	$: removeEmoji = nested.removeEmoji ?? false;
	$: normalizeWhitespace = nested.normalizeWhitespace ?? true;
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

	function readNested(raw: unknown): Partial<TransformTextCleanParams> {
		if (!isObject(raw)) return {};
		if (isObject(raw.text_clean)) return raw.text_clean as Partial<TransformTextCleanParams>;
		return raw as Partial<TransformTextCleanParams>;
	}

	function isWrapped(raw: unknown): boolean {
		return isObject(raw) && ('op' in raw || 'text_clean' in raw);
	}

	function normalizeColumns(raw: unknown): string[] {
		if (!Array.isArray(raw)) return [];
		return uniqueStrings(raw.map((v) => String(v ?? '').trim()).filter((v) => v.length > 0));
	}

	function commitPatch(next: Partial<TransformTextCleanParams>): void {
		const merged = {
			columns,
			lowercase,
			unicodeNormalize,
			removePunctuation,
			removeUrls,
			removeEmails,
			removeEmoji,
			normalizeWhitespace,
			...next
		};
		if (isWrapped(params)) {
			onDraft({ op: 'text_clean', text_clean: merged } as unknown as Partial<TransformTextCleanParams>);
			onCommit({ op: 'text_clean', text_clean: merged } as unknown as Partial<TransformTextCleanParams>);
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

<Section title="Text Clean">
	<div class="hint">Normalize text for NLP/LLM preprocessing.</div>

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

	<Field label="unicode normalize">
		<select
			value={unicodeNormalize}
			on:change={(event) =>
				commitPatch({
					unicodeNormalize: (event.currentTarget as HTMLSelectElement).value as TransformTextCleanParams['unicodeNormalize']
				})}
		>
			<option value="none">none</option>
			<option value="nfc">nfc</option>
			<option value="nfkc">nfkc</option>
		</select>
	</Field>

	<div class="checks">
		<label><input type="checkbox" checked={lowercase} on:change={(e) => commitPatch({ lowercase: (e.currentTarget as HTMLInputElement).checked })} /> lowercase</label>
		<label><input type="checkbox" checked={removePunctuation} on:change={(e) => commitPatch({ removePunctuation: (e.currentTarget as HTMLInputElement).checked })} /> remove punctuation</label>
		<label><input type="checkbox" checked={removeUrls} on:change={(e) => commitPatch({ removeUrls: (e.currentTarget as HTMLInputElement).checked })} /> remove urls</label>
		<label><input type="checkbox" checked={removeEmails} on:change={(e) => commitPatch({ removeEmails: (e.currentTarget as HTMLInputElement).checked })} /> remove emails</label>
		<label><input type="checkbox" checked={removeEmoji} on:change={(e) => commitPatch({ removeEmoji: (e.currentTarget as HTMLInputElement).checked })} /> remove emoji</label>
		<label><input type="checkbox" checked={normalizeWhitespace} on:change={(e) => commitPatch({ normalizeWhitespace: (e.currentTarget as HTMLInputElement).checked })} /> normalize whitespace</label>
	</div>
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
	.checks {
		display: grid;
		gap: 6px;
		margin-top: 8px;
		font-size: 13px;
	}
</style>
