<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import type { TransformSplitParams } from '$lib/flow/schema/transform';
	import { uniqueStrings } from '$lib/flow/components/editors/shared';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';

	export let selectedNode: Node<PipelineNodeData>;
	export let params: Partial<TransformSplitParams> | Record<string, unknown>;
	export let onDraft: (patch: Partial<TransformSplitParams>) => void;
	export let onCommit: (patch: Partial<TransformSplitParams>) => void;
	export let inputColumns: string[] = [];
	export let inputSchemaColumns: Array<{ name: string; type?: string }> = [];

	const modes: Array<TransformSplitParams['mode']> = ['sentences', 'lines', 'regex', 'delimiter'];
	const lineBreakModes: Array<{ value: NonNullable<TransformSplitParams['lineBreak']>; label: string }> = [
		{ value: 'any', label: 'any (\\r\\n, \\n, \\r)' },
		{ value: 'lf', label: '\\n (LF)' },
		{ value: 'crlf', label: '\\r\\n (CRLF)' },
		{ value: 'cr', label: '\\r (CR)' }
	];

	$: void selectedNode?.id;
	$: splitParams = readSplitParams(params);
	$: sourceColumn = splitParams.sourceColumn ?? 'text';
	$: outColumn = splitParams.outColumn ?? 'part';
	$: mode = splitParams.mode ?? 'sentences';
	$: pattern = splitParams.pattern ?? '';
	$: delimiter = splitParams.delimiter ?? '';
	$: lineBreak = splitParams.lineBreak ?? 'any';
	$: flags = splitParams.flags ?? '';
	$: trim = splitParams.trim ?? true;
	$: dropEmpty = splitParams.dropEmpty ?? true;
	$: emitIndex = splitParams.emitIndex ?? true;
	$: emitSourceRow = splitParams.emitSourceRow ?? true;
	$: maxParts = splitParams.maxParts ?? 5000;
	$: schemaTypeByName = buildSchemaTypeMap(inputSchemaColumns);
	$: schemaColumns = uniqueStrings(
		inputSchemaColumns.map((c) => String(c?.name ?? '').trim()).filter(Boolean)
	).sort((a, b) => a.localeCompare(b));
	$: knownSchema = schemaColumns.length > 0;
	$: fallbackColumns = uniqueStrings(inputColumns.map((c) => String(c ?? '').trim()).filter(Boolean)).sort(
		(a, b) => a.localeCompare(b)
	);
	$: sourceOptions = knownSchema ? schemaColumns : fallbackColumns;
	$: visibleSourceColumn =
		knownSchema && !sourceOptions.includes(sourceColumn) ? (sourceOptions[0] ?? 'text') : sourceColumn;
	$: sourceColumnType = schemaTypeByName.get(visibleSourceColumn) ?? 'unknown';
	$: modeHint =
		mode === 'sentences'
			? 'Split by . ! ? terminators; internal whitespace is normalized.'
			: mode === 'lines'
				? 'Split on line endings.'
				: mode === 'regex'
					? 'Split using a regular expression. Flags: i, m, s.'
					: 'Split on an exact delimiter string.';
	$: flagsError = /^[ims]*$/.test(flags) ? null : 'Flags can only contain i, m, s.';
	$: modeError =
		mode === 'regex'
			? pattern.trim()
				? null
				: 'Pattern is required for regex mode.'
			: mode === 'delimiter'
				? delimiter.length > 0
					? null
					: 'Delimiter is required for delimiter mode.'
				: null;
	$: canCommit = !flagsError && !modeError;

	function isObject(v: unknown): v is Record<string, unknown> {
		return Boolean(v) && typeof v === 'object' && !Array.isArray(v);
	}

	function readSplitParams(raw: unknown): Partial<TransformSplitParams> {
		if (!isObject(raw)) return {};
		if (isObject(raw.split)) return raw.split as Partial<TransformSplitParams>;
		return raw as Partial<TransformSplitParams>;
	}

	function isWrappedParams(raw: unknown): boolean {
		return isObject(raw) && ('op' in raw || 'split' in raw);
	}

	function patchDraft(next: Partial<TransformSplitParams>): void {
		if (isWrappedParams(params)) {
			onDraft({ op: 'split', split: { ...splitParams, ...next } } as unknown as Partial<TransformSplitParams>);
			return;
		}
		onDraft(next);
	}

	function patchCommit(next: Partial<TransformSplitParams>): void {
		if (!canCommit) return;
		if (isWrappedParams(params)) {
			onCommit({ op: 'split', split: { ...splitParams, ...next } } as unknown as Partial<TransformSplitParams>);
			return;
		}
		onCommit(next);
	}

	function buildSchemaTypeMap(columns: Array<{ name: string; type?: string }>): Map<string, string> {
		const out = new Map<string, string>();
		for (const col of columns) {
			const name = String(col?.name ?? '').trim();
			if (!name) continue;
			const nextType = String(col?.type ?? 'unknown').trim() || 'unknown';
			const prevType = out.get(name);
			if (!prevType || prevType === 'unknown') {
				out.set(name, nextType);
				continue;
			}
			if (nextType !== 'unknown') out.set(name, nextType);
		}
		return out;
	}

	function pickSourceColumn(col: string): void {
		const next = String(col ?? '').trim();
		if (!next) return;
		patchDraft({ sourceColumn: next });
		patchCommit({ sourceColumn: next });
	}
</script>

<Section title="Split">
	<Field label="mode">
		<select
			value={mode}
			on:change={(event) => {
				const value = (event.currentTarget as HTMLSelectElement).value as TransformSplitParams['mode'];
				patchDraft({ mode: value });
				patchCommit({ mode: value });
			}}
		>
			{#each modes as m}
				<option value={m}>{m}</option>
			{/each}
		</select>
	</Field>

	<div class="hint">{modeHint}</div>

	<div class="colsWrap">
		<div class="colsHeader">Available Cols</div>
		<div class="colsList">
			{#if knownSchema}
				{#each schemaColumns as col}
					<button
						type="button"
						class="colChip"
						class:selected={col === visibleSourceColumn}
						on:click={() => pickSourceColumn(col)}
					>
						<span class="colName">{col}</span>
						<span class="colType">{schemaTypeByName.get(col) ?? 'unknown'}</span>
					</button>
				{/each}
			{:else if fallbackColumns.length > 0}
				{#each fallbackColumns as col}
					<div class="colChip fallback">
						<span class="colName">{col}</span>
						<span class="colType">unknown</span>
					</div>
				{/each}
			{:else}
				<div class="empty">Schema unavailable (run upstream)</div>
			{/if}
		</div>
	</div>

	<Field label="source column">
		{#if knownSchema}
			<div class="selectedSourceValue">{visibleSourceColumn || 'no column selected'}</div>
			<div class="hint">Selected type: {sourceColumnType}</div>
		{:else}
			<Input
				value={sourceColumn}
				placeholder="type source column name"
				onInput={(event) => patchDraft({ sourceColumn: (event.currentTarget as HTMLInputElement).value })}
			/>
		{/if}
	</Field>

	<Field label="output column">
		<Input
			value={outColumn}
			placeholder="type output column name"
			onInput={(event) => patchDraft({ outColumn: (event.currentTarget as HTMLInputElement).value })}
		/>
	</Field>

	{#if mode === 'lines'}
		<Field label="line break">
			<select
				value={lineBreak}
				on:change={(event) => {
					const value = (event.currentTarget as HTMLSelectElement).value as NonNullable<TransformSplitParams['lineBreak']>;
					patchDraft({ lineBreak: value });
					patchCommit({ lineBreak: value });
				}}
			>
				{#each lineBreakModes as opt}
					<option value={opt.value}>{opt.label}</option>
				{/each}
			</select>
		</Field>
	{/if}

	{#if mode === 'regex'}
		<Field label="regex pattern">
			<Input
				value={pattern}
				placeholder="type regex pattern"
				onInput={(event) => patchDraft({ pattern: (event.currentTarget as HTMLInputElement).value })}
			/>
		</Field>
		<Field label="regex flags">
			<Input
				value={flags}
				placeholder="type flags"
				onInput={(event) => patchDraft({ flags: (event.currentTarget as HTMLInputElement).value })}
			/>
		</Field>
	{/if}

	{#if mode === 'delimiter'}
		<Field label="delimiter value">
			<Input
				value={delimiter}
				placeholder="type delimiter string"
				onInput={(event) => patchDraft({ delimiter: (event.currentTarget as HTMLInputElement).value })}
			/>
		</Field>
	{/if}

	<Field label="trim">
		<Input
			type="checkbox"
			checked={trim}
			onChange={(event) => {
				const value = (event.currentTarget as HTMLInputElement).checked;
				patchDraft({ trim: value });
				patchCommit({ trim: value });
			}}
		/>
	</Field>

	<Field label="drop empty">
		<Input
			type="checkbox"
			checked={dropEmpty}
			onChange={(event) => {
				const value = (event.currentTarget as HTMLInputElement).checked;
				patchDraft({ dropEmpty: value });
				patchCommit({ dropEmpty: value });
			}}
		/>
	</Field>

	<Field label="emit index">
		<Input
			type="checkbox"
			checked={emitIndex}
			onChange={(event) => {
				const value = (event.currentTarget as HTMLInputElement).checked;
				patchDraft({ emitIndex: value });
				patchCommit({ emitIndex: value });
			}}
		/>
	</Field>

	<Field label="emit source row">
		<Input
			type="checkbox"
			checked={emitSourceRow}
			onChange={(event) => {
				const value = (event.currentTarget as HTMLInputElement).checked;
				patchDraft({ emitSourceRow: value });
				patchCommit({ emitSourceRow: value });
			}}
		/>
	</Field>

	<Field label="max parts">
		<Input
			type="number"
			min="1"
			max="100000"
			step="1"
			value={maxParts}
			placeholder="type max parts"
			onInput={(event) => patchDraft({ maxParts: Number((event.currentTarget as HTMLInputElement).value || 5000) })}
		/>
	</Field>

	{#if modeError}
		<div class="warn">{modeError}</div>
	{/if}
	{#if flagsError}
		<div class="warn">{flagsError}</div>
	{/if}
	<div class="hint">maxParts caps per-input-row emitted segments.</div>
</Section>

<style>
	.hint {
		font-size: 12px;
		opacity: 0.8;
		margin-top: 6px;
	}

	.colsWrap {
		margin-top: 8px;
	}

	.colsHeader {
		font-size: 12px;
		font-weight: 700;
		margin-bottom: 6px;
	}

	.colsList {
		min-height: 42px;
		max-height: 160px;
		overflow-y: auto;
		border: 1px solid rgba(255, 255, 255, 0.16);
		border-radius: 10px;
		padding: 8px;
		display: grid;
		grid-template-columns: repeat(3, minmax(0, 1fr));
		gap: 6px;
	}

	.colChip {
		padding: 4px 6px;
		font-size: 11px;
		border-radius: 8px;
		border: 1px solid rgba(255, 255, 255, 0.18);
		background: rgba(255, 255, 255, 0.04);
		color: inherit;
		cursor: pointer;
		text-align: left;
		display: grid;
		gap: 2px;
	}

	.colChip.selected {
		border-color: rgba(96, 165, 250, 0.8);
		background: rgba(59, 130, 246, 0.16);
	}

	.colChip.fallback {
		cursor: default;
	}

	.colName {
		white-space: nowrap;
		overflow: hidden;
		text-overflow: ellipsis;
	}

	.colType {
		font-size: 10px;
		opacity: 0.75;
	}

	.empty {
		font-size: 12px;
		opacity: 0.75;
	}

	.selectedSourceValue {
		min-height: 34px;
		padding: 6px 10px;
		border: 1px solid rgba(255, 255, 255, 0.18);
		border-radius: 8px;
		background: rgba(255, 255, 255, 0.04);
		font-size: 14px;
		display: flex;
		align-items: center;
	}

	.warn {
		font-size: 12px;
		color: #fca5a5;
		margin-top: 6px;
	}
</style>
