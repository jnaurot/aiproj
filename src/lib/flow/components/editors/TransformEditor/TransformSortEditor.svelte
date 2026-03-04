<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import type { TransformSortParams } from '$lib/flow/schema/transform';
	import type { NodeExecutionError } from '$lib/flow/store/graphStore';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';
	import {
		computeSortColumnState,
		missingSortColumnsFromError,
		normalizeSortDraftColumn,
		normalizeSortItems,
		readSortBy,
		schemaColumnsFromInput,
		type SortItem,
		type SortEditorParams
	} from './sortModel';

	export let selectedNode: Node<PipelineNodeData>;
	export let params: SortEditorParams;
	export let onDraft: (patch: Partial<TransformSortParams>) => void;
	export let onCommit: (patch: Partial<TransformSortParams>) => void;
	export let inputColumns: string[] = [];
	export let nodeError: NodeExecutionError | null = null;

	let addColDraft = '';
	let items: SortItem[] = [];
	let stickyKnownColumns: string[] = [];
	let lastNodeId = '';
	let lastParamsSignature = '';
	let suppressParamSync = false;

	$: void selectedNode?.id;
	$: paramsItems = normalizeSortItems(readSortBy(params));
	$: paramsSignature = JSON.stringify(paramsItems);
	$: if ((selectedNode?.id ?? '') !== lastNodeId) {
		lastNodeId = selectedNode?.id ?? '';
		items = [...paramsItems];
		lastParamsSignature = paramsSignature;
	}
	$: if (!suppressParamSync && (selectedNode?.id ?? '') === lastNodeId && paramsSignature !== lastParamsSignature) {
		items = [...paramsItems];
		lastParamsSignature = paramsSignature;
	}
	$: selectedCols = items.map((item) => item.col);
	$: schemaColumns = schemaColumnsFromInput(inputColumns);
	$: hasKnownSchema = schemaColumns.length > 0;
	$: if (hasKnownSchema) {
		stickyKnownColumns = [...schemaColumns];
	}
	$: columnState = computeSortColumnState(inputColumns, stickyKnownColumns, selectedCols);
	$: knownColumns = columnState.knownColumns;
	$: availableCols = columnState.availableCols;
	$: unknownFromSchema = columnState.unknownFromSchema;
	$: missingFromError = missingSortColumnsFromError(nodeError);

	function markLocalEdit() {
		suppressParamSync = true;
		queueMicrotask(() => {
			suppressParamSync = false;
		});
	}

	function commitItems(next: SortItem[]): void {
		markLocalEdit();
		items = [...next];
		onDraft({ by: next });
		onCommit({ by: next });
	}

	function addColumn(col: string): void {
		const name = normalizeSortDraftColumn(col);
		if (!name) return;
		if (items.some((x) => x.col === name)) return;
		stickyKnownColumns = [...new Set([...stickyKnownColumns, name])];
		commitItems([...items, { col: name, dir: 'asc' }]);
	}

	function removeColumn(col: string): void {
		commitItems(items.filter((x) => x.col !== col));
	}

	function toggleDir(col: string): void {
		const next = items.map((x) => (x.col === col ? { ...x, dir: x.dir === 'asc' ? 'desc' : 'asc' } : x));
		commitItems(next);
	}
</script>

<Section title="Sort">
	<div class="hint">Sort rows by one or more columns. Sort order is applied in insertion order.</div>

	{#if !hasKnownSchema}
		<div class="addRow">
			<Input
				value={addColDraft}
				placeholder="type column name"
				onInput={(event) => (addColDraft = (event.currentTarget as HTMLInputElement).value)}
				onKeydown={(event) => {
					if ((event as KeyboardEvent).key !== 'Enter') return;
					event.preventDefault();
					const next = addColDraft.trim();
					addColumn(next);
					addColDraft = '';
				}}
			/>
			<button
				class="small"
				type="button"
				on:click={() => {
					const next = addColDraft.trim();
					addColumn(next);
					addColDraft = '';
				}}
			>
				+
			</button>
		</div>
	{/if}

	<div class="selectorGrid">
		<div class="listCol">
			<div class="listHeader">Column Sort</div>
			<div class="selectedList">
				{#if items.length === 0}
					<div class="emptySel">No columns selected</div>
				{:else}
					{#each items as item}
						<div class="sortRow">
							<button class="chipBtn" type="button" on:click={() => removeColumn(item.col)} title="Remove from sort">
								{item.col}
							</button>
							<button class="dirBtn" type="button" on:click={() => toggleDir(item.col)} title="Toggle sort direction">
								{item.dir}
							</button>
						</div>
					{/each}
				{/if}
			</div>
		</div>

		<div class="listCol">
			<div class="listHeader">Available Cols</div>
			<div class="availableList">
				{#if availableCols.length === 0}
					<div class="emptySel">{knownColumns.length > 0 ? 'No more column names' : 'Schema unavailable (run upstream)'}</div>
				{:else}
					{#each availableCols as col}
						<button class="chipBtn" type="button" on:click={() => addColumn(col)}>{col}</button>
					{/each}
				{/if}
			</div>
		</div>
	</div>

	{#if unknownFromSchema.length > 0}
		<div class="warn">Unknown columns from schema: {unknownFromSchema.join(', ')}</div>
	{/if}
	{#if missingFromError.length > 0}
		<div class="warn">Runtime mismatch: {missingFromError.join(', ')}</div>
	{/if}
</Section>

<style>
	.hint {
		font-size: 12px;
		opacity: 0.75;
		margin-top: 6px;
	}

	.addRow {
		display: grid;
		grid-template-columns: minmax(0, 1fr) auto;
		gap: 8px;
		align-items: center;
		margin-top: 8px;
	}

	.selectorGrid {
		display: grid;
		grid-template-columns: repeat(2, minmax(0, 1fr));
		gap: 10px;
		align-items: start;
		width: 100%;
		min-width: 0;
		margin-top: 10px;
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

	.sortRow {
		display: grid;
		grid-template-columns: minmax(0, 1fr) auto;
		gap: 6px;
		align-items: center;
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

	.dirBtn {
		padding: 4px 8px;
		font-size: 12px;
		border-radius: 8px;
		border: 1px solid rgba(255, 255, 255, 0.16);
		background: rgba(255, 255, 255, 0.08);
		color: inherit;
		cursor: pointer;
		min-width: 56px;
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

	.warn {
		font-size: 12px;
		color: #fca5a5;
		margin-top: 8px;
	}

	@media (max-width: 560px) {
		.selectorGrid {
			grid-template-columns: 1fr;
		}
	}
</style>
