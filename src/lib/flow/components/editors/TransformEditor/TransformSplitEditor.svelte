<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import type { TransformSplitParams } from '$lib/flow/schema/transform';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Field from '$lib/flow/components/ui/Field.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';

	export let selectedNode: Node<PipelineNodeData>;
	export let params: Partial<TransformSplitParams>;
	export let onDraft: (patch: Partial<TransformSplitParams>) => void;
	export let onCommit: (patch: Partial<TransformSplitParams>) => void;
	export let inputColumns: string[] = [];

	const modes: Array<TransformSplitParams['mode']> = ['sentences', 'lines', 'regex', 'delimiter'];

	$: void selectedNode?.id;
	$: sourceColumn = params?.sourceColumn ?? 'text';
	$: outColumn = params?.outColumn ?? 'part';
	$: mode = params?.mode ?? 'sentences';
	$: pattern = params?.pattern ?? '';
	$: delimiter = params?.delimiter ?? '';
	$: flags = params?.flags ?? '';
	$: trim = params?.trim ?? true;
	$: dropEmpty = params?.dropEmpty ?? true;
	$: emitIndex = params?.emitIndex ?? true;
	$: emitSourceRow = params?.emitSourceRow ?? true;
	$: maxParts = params?.maxParts ?? 5000;
	$: modeHint =
		mode === 'sentences'
			? 'Split by . ! ? terminators; internal whitespace is normalized.'
			: mode === 'lines'
				? 'Split on line breaks.'
				: mode === 'regex'
					? 'Split using a regular expression.'
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
	$: maxPartsSafe = Number.isFinite(maxParts) ? Math.max(1, Math.min(100000, Math.trunc(maxParts))) : 5000;
	$: canCommit = !flagsError && !modeError;

	function decodeEscapes(raw: string): string {
		return raw
			.replace(/\\r/g, '\r')
			.replace(/\\n/g, '\n')
			.replace(/\\t/g, '\t');
	}

	function commitSafe(patch: Partial<TransformSplitParams>): void {
		if (!canCommit) return;
		onCommit(patch);
	}
</script>

<Section title="Split">
	<Field label="mode">
		<select
			value={mode}
			on:change={(event) => {
				const value = (event.currentTarget as HTMLSelectElement).value as TransformSplitParams['mode'];
				onDraft({ mode: value });
				commitSafe({ mode: value });
			}}
		>
			{#each modes as m}
				<option value={m}>{m}</option>
			{/each}
		</select>
	</Field>

	<div class="hint">{modeHint}</div>

	<Field label="source column">
		<input
			list="split-source-columns"
			value={sourceColumn}
			on:input={(event) => onDraft({ sourceColumn: (event.currentTarget as HTMLInputElement).value })}
			on:blur={() => commitSafe({ sourceColumn: sourceColumn.trim() || 'text' })}
		/>
		<datalist id="split-source-columns">
			{#each inputColumns as col}
				<option value={col} />
			{/each}
		</datalist>
	</Field>

	<Field label="output column">
		<Input
			value={outColumn}
			onInput={(event) => onDraft({ outColumn: (event.currentTarget as HTMLInputElement).value })}
			onBlur={() => commitSafe({ outColumn: outColumn.trim() || 'part' })}
		/>
	</Field>

	{#if mode === 'regex'}
		<Field label="pattern">
			<Input
				value={pattern}
				placeholder="e.g. \\s+"
				onInput={(event) => onDraft({ pattern: (event.currentTarget as HTMLInputElement).value })}
				onBlur={() => commitSafe({ pattern: pattern.trim() })}
			/>
		</Field>
		<Field label="flags">
			<Input
				value={flags}
				placeholder="ims"
				onInput={(event) => onDraft({ flags: (event.currentTarget as HTMLInputElement).value })}
				onBlur={() => commitSafe({ flags: flags.trim() })}
			/>
		</Field>
	{/if}

	{#if mode === 'delimiter'}
		<Field label="delimiter">
			<Input
				value={delimiter}
				placeholder="e.g. \\n or ,"
				onInput={(event) => onDraft({ delimiter: (event.currentTarget as HTMLInputElement).value })}
				onBlur={() => commitSafe({ delimiter: decodeEscapes(delimiter) })}
			/>
		</Field>
	{/if}

	<Field label="trim">
		<Input
			type="checkbox"
			checked={trim}
			onChange={(event) => {
				const value = (event.currentTarget as HTMLInputElement).checked;
				onDraft({ trim: value });
				commitSafe({ trim: value });
			}}
		/>
	</Field>
	<Field label="drop empty">
		<Input
			type="checkbox"
			checked={dropEmpty}
			onChange={(event) => {
				const value = (event.currentTarget as HTMLInputElement).checked;
				onDraft({ dropEmpty: value });
				commitSafe({ dropEmpty: value });
			}}
		/>
	</Field>
	<Field label="emit index">
		<Input
			type="checkbox"
			checked={emitIndex}
			onChange={(event) => {
				const value = (event.currentTarget as HTMLInputElement).checked;
				onDraft({ emitIndex: value });
				commitSafe({ emitIndex: value });
			}}
		/>
	</Field>
	<Field label="emit source row">
		<Input
			type="checkbox"
			checked={emitSourceRow}
			onChange={(event) => {
				const value = (event.currentTarget as HTMLInputElement).checked;
				onDraft({ emitSourceRow: value });
				commitSafe({ emitSourceRow: value });
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
			onInput={(event) =>
				onDraft({
					maxParts: Number((event.currentTarget as HTMLInputElement).value || 5000)
				})}
			onBlur={() => commitSafe({ maxParts: maxPartsSafe })}
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

	.warn {
		font-size: 12px;
		color: #fca5a5;
		margin-top: 6px;
	}
</style>
