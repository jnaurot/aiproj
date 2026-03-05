<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import type { TransformRenameParams } from '$lib/flow/schema/transform';
	import type { NodeExecutionError } from '$lib/flow/store/graphStore';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';
	import { computeColumnUniverse, normalizeColumnNames, toSchemaColumns } from './columnSelectionModel';
	import {
		availableRenameColumnsFromError,
		computeRenameIssues,
		computeRenamePreview,
		findPreviewCollisions,
		mapToPairs,
		missingRenameColumnsFromError,
		normalizeRenameParams,
		pairsToMap,
		readRenameMap,
		type RenamePair
	} from './renameModel';

	export let selectedNode: Node<PipelineNodeData>;
	export let params: Partial<TransformRenameParams>;
	export let onDraft: (patch: Partial<TransformRenameParams>) => void;
	export let onCommit: (patch: Partial<TransformRenameParams>) => void;
	export let inputColumns: string[] = [];
	export let nodeError: NodeExecutionError | null = null;
	$: void onCommit;

	const defaults: RenamePair[] = [{ from: '', to: '' }];

	let rows: RenamePair[] = [];
	let stickyKnownColumns: string[] = [];
	let lastNodeId = '';
	let lastParamsSignature = '';
	let suppressParamSync = false;

	$: void selectedNode?.id;
	$: normalized = normalizeRenameParams(params);
	$: paramsSignature = JSON.stringify(normalized.map ?? {});
	$: errorAvailableColumns = availableRenameColumnsFromError(nodeError);
	$: schemaColumns = toSchemaColumns([...inputColumns, ...errorAvailableColumns]);
	$: selectedSources = normalizeColumnNames(rows.map((r) => r.from));
	$: if (schemaColumns.length > 0) {
		stickyKnownColumns = [...schemaColumns];
	}
	$: universe = computeColumnUniverse({
		stickyColumns: stickyKnownColumns,
		schemaColumns,
		selectedColumns: selectedSources
	});
	$: knownColumns = universe.knownColumns;
	$: availableColumns = universe.availableColumns;
	$: hasKnownSchema = universe.hasKnownSchema;
	$: missingFromError = missingRenameColumnsFromError(nodeError);
	$: committedMap = pairsToMap(rows);
	$: previewRows = computeRenamePreview(knownColumns, committedMap);
	$: previewCollisions = findPreviewCollisions(previewRows);
	$: issues = computeRenameIssues(rows, schemaColumns);
	$: changedCount = previewRows.filter((row) => row.changed).length;

	$: if ((selectedNode?.id ?? '') !== lastNodeId) {
		lastNodeId = selectedNode?.id ?? '';
		const fromParams = mapToPairs(readRenameMap(params as any));
		rows = fromParams.length > 0 ? fromParams : defaults.map((r) => ({ ...r }));
		lastParamsSignature = paramsSignature;
	}

	$: if (!suppressParamSync && (selectedNode?.id ?? '') === lastNodeId && paramsSignature !== lastParamsSignature) {
		const fromParams = mapToPairs(readRenameMap(params as any));
		rows = fromParams.length > 0 ? fromParams : defaults.map((r) => ({ ...r }));
		lastParamsSignature = paramsSignature;
	}

	function markLocalEdit() {
		suppressParamSync = true;
		queueMicrotask(() => {
			suppressParamSync = false;
		});
	}

	function normalizedLocalRows(input: RenamePair[]): RenamePair[] {
		const next = (input ?? []).map((row) => ({
			from: String(row?.from ?? ''),
			to: String(row?.to ?? '')
		}));
		return next.length > 0 ? next : defaults.map((r) => ({ ...r }));
	}

	function commitRows(nextRows: RenamePair[]): void {
		const localRows = normalizedLocalRows(nextRows);
		const committed = normalizeRenameParams({ map: pairsToMap(localRows) });
		markLocalEdit();
		rows = localRows;
		onDraft(committed);
	}

	function updatePair(index: number, key: 'from' | 'to', value: string): void {
		const next = rows.map((pair, current) => (current === index ? { ...pair, [key]: value } : pair));
		commitRows(next);
	}

	function addRow(prefillFrom = ''): void {
		commitRows([...rows, { from: String(prefillFrom ?? ''), to: '' }]);
	}

	function removeRow(index: number): void {
		const next = rows.filter((_, current) => current !== index);
		commitRows(next.length > 0 ? next : defaults.map((r) => ({ ...r })));
	}

	function fillNextSource(col: string): void {
		const value = String(col ?? '').trim();
		if (!value) return;
		const existingIdx = rows.findIndex((row) => String(row.from ?? '').trim() === value);
		if (existingIdx >= 0) return;
		const emptyIdx = rows.findIndex((row) => !String(row.from ?? '').trim());
		if (emptyIdx >= 0) {
			updatePair(emptyIdx, 'from', value);
			return;
		}
		addRow(value);
	}
</script>

<Section title="Rename Columns">
	<div class="hint">Rows unchanged. Data unchanged. Schema names only.</div>
	<div class="hint">Schema preview: renaming {changedCount} column{changedCount === 1 ? '' : 's'}.</div>

	{#if hasKnownSchema}
		<div class="colsWrap">
			<div class="listHeader">Available Cols</div>
			<div class="colsList">
				{#if availableColumns.length === 0}
					<div class="emptySel">{knownColumns.length > 0 ? 'No more column names' : 'Schema unavailable (run upstream)'}</div>
				{:else}
					{#each availableColumns as col}
						<button class="chipBtn" type="button" on:click={() => fillNextSource(col)}>{col}</button>
					{/each}
				{/if}
			</div>
		</div>
	{/if}

	{#each rows as pair, index}
		<div class="row">
			<Input
				value={pair.from}
				placeholder="old column"
				onInput={(event) => updatePair(index, 'from', (event.currentTarget as HTMLInputElement).value)}
			/>
			<div class="arrow">to</div>
			<Input
				value={pair.to}
				placeholder="new column"
				onInput={(event) => updatePair(index, 'to', (event.currentTarget as HTMLInputElement).value)}
			/>
			<button class="small danger" type="button" on:click={() => removeRow(index)}>
				-
			</button>
		</div>
	{/each}

	<div class="actions">
		<button class="small" type="button" on:click={() => addRow()}>
			+ Add mapping
		</button>
	</div>

	<div class="previewWrap">
		<div class="listHeader">Schema Preview</div>
		<div class="previewList">
			{#if previewRows.length === 0}
				<div class="emptySel">Schema unavailable.</div>
			{:else}
				{#each previewRows as row}
					<div class="previewRow {row.changed ? 'changed' : ''} {previewCollisions.includes(row.output) ? 'collision' : ''}">
						<span class="inName">{row.input}</span>
						<span class="arrow">→</span>
						<span class="outName">{row.output}</span>
					</div>
				{/each}
			{/if}
		</div>
	</div>

	{#if Object.keys(committedMap).length === 0}
		<div class="warn">At least one valid mapping is required.</div>
	{/if}
	{#if issues.unknownSources.length > 0}
		<div class="warn">Unknown source columns: {issues.unknownSources.join(', ')}</div>
	{/if}
	{#if issues.duplicateSources.length > 0}
		<div class="warn">Duplicate source mappings: {issues.duplicateSources.join(', ')}</div>
	{/if}
	{#if issues.duplicateTargets.length > 0}
		<div class="warn">Duplicate target names: {issues.duplicateTargets.join(', ')}</div>
	{/if}
	{#if issues.noOps.length > 0}
		<div class="hint">No-op renames: {issues.noOps.join(', ')}</div>
	{/if}
	{#if previewCollisions.length > 0}
		<div class="warn">Preview collisions: {previewCollisions.join(', ')}</div>
	{/if}
	{#if missingFromError.length > 0}
		<div class="warn">Runtime mismatch: {missingFromError.join(', ')}</div>
	{/if}
</Section>

<style>
	.row {
		display: grid;
		grid-template-columns: minmax(0, 1fr) auto minmax(0, 1fr) auto;
		gap: 8px;
		align-items: center;
		margin: 8px 0;
	}

	.colsWrap {
		margin-top: 8px;
	}

	.colsList,
	.previewList {
		min-height: 42px;
		max-height: 188px;
		overflow-y: auto;
		border: 1px solid rgba(255, 255, 255, 0.16);
		border-radius: 10px;
		padding: 8px;
		display: grid;
		gap: 6px;
	}

	.listHeader {
		font-size: 12px;
		font-weight: 700;
		opacity: 0.9;
		margin-top: 10px;
		margin-bottom: 6px;
	}

	.emptySel {
		font-size: 12px;
		opacity: 0.75;
	}

	.chipBtn {
		padding: 4px 8px;
		font-size: 12px;
		border-radius: 8px;
		border: 1px solid rgba(255, 255, 255, 0.16);
		background: rgba(255, 255, 255, 0.04);
		color: inherit;
		cursor: pointer;
		text-align: left;
	}

	.previewRow {
		display: grid;
		grid-template-columns: minmax(0, 1fr) auto minmax(0, 1fr);
		gap: 6px;
		font-size: 12px;
		align-items: center;
	}

	.previewRow.changed .outName {
		font-weight: 700;
	}

	.previewRow.collision {
		color: #fca5a5;
	}

	.arrow {
		font-size: 12px;
		opacity: 0.75;
	}

	.hint,
	.warn {
		font-size: 12px;
		margin-top: 6px;
	}

	.hint {
		opacity: 0.75;
	}

	.warn {
		color: #fca5a5;
	}

	.actions {
		display: flex;
		justify-content: flex-end;
		margin-top: 8px;
	}

	button.small {
		padding: 6px 10px;
		font-size: 12px;
		border-radius: 10px;
		border: 1px solid rgba(255, 255, 255, 0.16);
		background: rgba(255, 255, 255, 0.06);
		color: inherit;
		cursor: pointer;
	}

	button.danger {
		border-color: rgba(239, 68, 68, 0.5);
		background: rgba(239, 68, 68, 0.14);
	}
</style>
