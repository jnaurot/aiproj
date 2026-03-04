<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import type { TransformDedupeParams } from '$lib/flow/schema/transform';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';
	import {
		canCommitDedupeDraft,
		missingDedupeColumnsFromError,
		normalizeDedupeParams,
		resolveDedupeAvailableColumns
	} from './dedupeModel';
	import { computeColumnUniverse, normalizeColumnNames } from './columnSelectionModel';
	import type { NodeExecutionError } from '$lib/flow/store/graphStore';

	export let selectedNode: Node<PipelineNodeData>;
	export let params: Partial<TransformDedupeParams>;
	export let onDraft: (patch: Partial<TransformDedupeParams>) => void;
	export let onCommit: (patch: Partial<TransformDedupeParams>) => void;
	export let inputColumns: string[] = [];
	export let nodeError: NodeExecutionError | null = null;

	const defaults: TransformDedupeParams = {
		allColumns: false,
		by: []
	};

	let by: string[] = [];
	let useByColumns = false;
	let lastNodeId = '';
	let stickyAvailableColumns: string[] = [];
	let lastParamsSignature = '';
	let suppressParamSync = false;

	$: void selectedNode?.id;
	$: normalized = normalizeDedupeParams(params);
	$: allColumns = normalized.allColumns;
	$: errorAvailableColumns = Array.isArray(nodeError?.availableColumns)
		? nodeError.availableColumns.map((c) => String(c).trim()).filter(Boolean)
		: [];
	$: resolvedBaseColumns = resolveDedupeAvailableColumns(inputColumns, errorAvailableColumns, []);
	$: if (resolvedBaseColumns.length > 0) {
		stickyAvailableColumns = [...resolvedBaseColumns];
	}
	$: selectedFromBy = normalizeColumnNames((by ?? []) as unknown[]);
	$: universe = computeColumnUniverse({
		stickyColumns: stickyAvailableColumns,
		schemaColumns: resolvedBaseColumns,
		selectedColumns: selectedFromBy
	});
	$: availableColumns = universe.knownColumns;
	$: validRuntimeColumns = universe.knownColumns;
	$: missingByColumns = missingDedupeColumnsFromError(nodeError);
	$: isMissingColumnError = missingByColumns.length > 0;
	$: dedupeAll = allColumns;
	$: canCommit = canCommitDedupeDraft(useByColumns, by);
	$: selectedColumns = normalizeColumnNames(
		(by ?? [])
			.map((c) => String(c).trim())
			.filter((c) => c && validRuntimeColumns.includes(c)) as unknown[]
	);
	$: selectableColumns = validRuntimeColumns.filter((c) => !selectedColumns.includes(c));
	$: selectedColumnsSorted = [...selectedColumns].sort((a, b) => a.localeCompare(b));
	$: selectableColumnsSorted = [...selectableColumns].sort((a, b) => a.localeCompare(b));
	$: paramsSignature = `${String(allColumns)}|${JSON.stringify(normalized.by ?? [])}`;

	$: if ((selectedNode?.id ?? '') !== lastNodeId) {
		lastNodeId = selectedNode?.id ?? '';
		useByColumns = !allColumns;
		by = Array.isArray(normalized.by) ? [...normalized.by] : [];
		lastParamsSignature = paramsSignature;
	}

	$: if (!suppressParamSync && (selectedNode?.id ?? '') === lastNodeId && paramsSignature !== lastParamsSignature) {
		useByColumns = !allColumns;
		by = Array.isArray(normalized.by) ? [...normalized.by] : [];
		lastParamsSignature = paramsSignature;
	}

	function markLocalEdit() {
		suppressParamSync = true;
		queueMicrotask(() => {
			suppressParamSync = false;
		});
	}

	function setDraft(next: Partial<TransformDedupeParams>) {
		const merged = normalizeDedupeParams({ allColumns, by: [...by], ...next });
		console.log('[dedupe-ui] setDraft', {
			nodeId: selectedNode?.id,
			next,
			merged,
			currentNormalized: normalized,
			useByColumns
		});
		onDraft(merged);
	}

	function setCommit(next: Partial<TransformDedupeParams>) {
		const merged = normalizeDedupeParams({ allColumns, by: [...by], ...next });
		const mergedUseByColumns = merged.allColumns === false;
		const allowed = canCommitDedupeDraft(mergedUseByColumns, merged.by ?? []);
		console.log('[dedupe-ui] setCommit', {
			nodeId: selectedNode?.id,
			next,
			merged,
			mergedUseByColumns,
			allowed
		});
		if (!allowed) return;
		onCommit(merged);
	}

	function setCommitAllowInvalid(next: Partial<TransformDedupeParams>) {
		const merged = normalizeDedupeParams({ allColumns, by: [...by], ...next });
		merged.by = normalizeColumnNames((merged.by ?? []) as unknown[]);
		console.log('[dedupe-ui] setCommitAllowInvalid', {
			nodeId: selectedNode?.id,
			next,
			merged,
			useByColumns
		});
		onCommit(merged);
	}

	function addSelectedColumn(candidate: string) {
		const next = normalizeColumnNames([...by, candidate] as unknown[]);
		markLocalEdit();
		by = next;
		setDraft({ allColumns: false, by: next });
		setCommit({ allColumns: false, by: next });
	}

	function removeSelectedColumn(column: string) {
		const next = selectedColumns.filter((c) => c !== column);
		markLocalEdit();
		by = next;
		setDraft({ allColumns: false, by: next });
		if (next.length === 0) {
			setCommitAllowInvalid({ allColumns: false, by: [] });
			return;
		}
		setCommit({ allColumns: false, by: next });
	}

	function resetFromCommitted(): void {
		const committedParams = (selectedNode?.data?.params ?? {}) as Record<string, unknown>;
		const committedDedupe = normalizeDedupeParams(
			((committedParams.dedupe as Partial<TransformDedupeParams> | undefined) ??
				(committedParams as Partial<TransformDedupeParams>)) as Partial<TransformDedupeParams>
		);
		const next = committedDedupe ?? defaults;
		markLocalEdit();
		useByColumns = !next.allColumns;
		by = Array.isArray(next.by) ? [...next.by] : [];
		onDraft(next);
		onCommit(next);
	}
</script>

<Section title="Deduplicate">
	<div class="hint">
		Removes duplicate rows based on selected columns.
		Enable <b>Deduplicate on all columns</b> to remove rows that are identical across the entire row.
	</div>
	<div class="hint">Keeps: first row (stable order)</div>

	<div class="actions">
		<label class="toggle">
			<Input
				type="checkbox"
				checked={dedupeAll}
				onChange={(event) => {
					const checked = (event.currentTarget as HTMLInputElement).checked;
					console.log('[dedupe-ui] checkbox:onChange', {
						nodeId: selectedNode?.id,
						checked,
						before: { allColumns, by, useByColumns, dedupeAll }
					});
					if (checked) {
						useByColumns = false;
						setDraft({ allColumns: true, by: [] });
						setCommit({ allColumns: true, by: [] });
						return;
					}
					useByColumns = true;
					const seeded = normalizeColumnNames((by ?? []) as unknown[]);
					setDraft({ allColumns: false, by: seeded });
					setCommitAllowInvalid({ allColumns: false, by: seeded });
				}}
			/>
			<span>Deduplicate on all columns</span>
		</label>

		<button
			class="small ghost"
			type="button"
			on:click={resetFromCommitted}
		>
			Reset
		</button>
	</div>

	{#if useByColumns}
		<div class="selectorGrid">
			<div class="listCol">
				<div class="listHeader">Dedupe Cols</div>
				<div class="selectedList">
					{#if selectedColumnsSorted.length === 0}
						<div class="emptySel">No columns selected</div>
					{:else}
						{#each selectedColumnsSorted as column}
							<button class="chipBtn" type="button" on:click={() => removeSelectedColumn(column)}>
								{column}
							</button>
						{/each}
					{/if}
				</div>
			</div>

			<div class="listCol">
				<div class="listHeader">Available Cols</div>
				<div class="availableList">
					{#if selectableColumnsSorted.length === 0}
						<div class="emptySel">
							{validRuntimeColumns.length > 0 ? 'No more column names' : 'Schema unavailable (run upstream)'}
						</div>
					{:else}
						{#each selectableColumnsSorted as col}
							<button class="chipBtn" type="button" on:click={() => addSelectedColumn(col)}>
								{col}
							</button>
						{/each}
					{/if}
				</div>
			</div>
		</div>

		<!-- <div class="actions">
			<button
				class="small ghost"
				type="button"
				disabled={!canCommit}
				on:click={() => setCommit({ allColumns: false, by })}
			>
				Commit
			</button>
		</div> -->
		{#if !canCommit}
			<div class="warn">Select at least one column.</div>
		{/if}
	{/if}
</Section>

<style>
	.selectorGrid {
		display: grid;
		grid-template-columns: repeat(2, minmax(0, 1fr));
		gap: 10px;
		align-items: start;
		width: 100%;
		min-width: 0;
	}

	.listCol {
		min-width: 0;
		width: 100%;
	}

	.listHeader {
		font-size: 12px;
		font-weight: 700;
		margin-bottom: 6px;
		opacity: 0.9;
	}

	.selectedList,
	.availableList {
		min-height: 42px;
		max-height: 188px;
		overflow-y: auto;
		overflow-x: hidden;
		border: 1px solid rgba(255, 255, 255, 0.16);
		border-radius: 10px;
		padding: 8px;
		display: grid;
		grid-template-columns: 1fr;
		gap: 6px;
		align-items: flex-start;
		align-content: flex-start;
		box-sizing: border-box;
		width: 100%;
		max-width: 100%;
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
		width: 100%;
		max-width: 100%;
		text-align: left;
		white-space: nowrap;
		overflow: hidden;
		text-overflow: ellipsis;
		box-sizing: border-box;
	}

	.toggle {
		display: inline-flex;
		gap: 8px;
		align-items: center;
	}

	.hint {
		font-size: 12px;
		opacity: 0.75;
		margin-top: 6px;
	}

	.actions {
		display: flex;
		gap: 8px;
		justify-content: space-between;
		align-items: center;
		margin-top: 8px;
		flex-wrap: wrap;
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

	button.ghost {
		background: transparent;
	}

	button.danger {
		border-color: rgba(239, 68, 68, 0.5);
		background: rgba(239, 68, 68, 0.14);
	}

	button:disabled {
		opacity: 0.5;
		cursor: not-allowed;
	}

	.warn {
		font-size: 12px;
		color: #fca5a5;
		margin-top: 6px;
	}

	.selectedList:has(.chipBtn),
	.availableList:has(.chipBtn) {
		background: rgba(255, 255, 255, 0.02);
	}

	@media (max-width: 560px) {
		.selectorGrid {
			grid-template-columns: 1fr;
		}
	}
</style>
