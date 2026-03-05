<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import type { TransformSelectParams } from '$lib/flow/schema/transform';
	import type { NodeExecutionError } from '$lib/flow/store/graphStore';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';
	import { uniqueStrings } from '$lib/flow/components/editors/shared';

	type SelectMode = 'include' | 'exclude';
	type KeepOrder = 'input' | 'custom';

	export let selectedNode: Node<PipelineNodeData>;
	export let params: Partial<TransformSelectParams>;
	export let onDraft: (patch: Partial<TransformSelectParams>) => void;
	export let onCommit: (patch: Partial<TransformSelectParams>) => void;
	export let inputColumns: string[] = [];
	export let nodeError: NodeExecutionError | null = null;

	let addColDraft = '';
	let selectedColumns: string[] = [];
	let stickyKnownColumns: string[] = [];
	let lastNodeId = '';
	let lastParamsSignature = '';
	let suppressParamSync = false;
	$: isWrappedParams = isObject(params) && ('op' in (params as Record<string, unknown>) || 'select' in (params as Record<string, unknown>));

	$: void selectedNode?.id;
	$: normalized = normalizeSelectParams(readSelectParams(params));
	$: mode = normalized.mode;
	$: keepOrder = normalized.keepOrder;
	$: strict = normalized.strict;
	$: paramsColumns = normalized.columns;
	$: paramsSignature = JSON.stringify(normalized);
	$: if ((selectedNode?.id ?? '') !== lastNodeId) {
		lastNodeId = selectedNode?.id ?? '';
		selectedColumns = [...paramsColumns];
		lastParamsSignature = paramsSignature;
	}
	$: if (!suppressParamSync && (selectedNode?.id ?? '') === lastNodeId && paramsSignature !== lastParamsSignature) {
		selectedColumns = [...paramsColumns];
		lastParamsSignature = paramsSignature;
	}

	$: errorAvailableColumns = Array.isArray(nodeError?.availableColumns)
		? uniqueStrings(nodeError.availableColumns.map((c) => String(c).trim()).filter(Boolean))
		: [];
	$: schemaColumns = uniqueStrings(
		[...inputColumns, ...errorAvailableColumns].map((c) => String(c).trim()).filter(Boolean)
	).sort((a, b) => a.localeCompare(b));
	$: hasKnownSchema = schemaColumns.length > 0;
	$: if (hasKnownSchema) {
		stickyKnownColumns = [...schemaColumns];
	}
	$: knownColumns = uniqueStrings([...stickyKnownColumns, ...schemaColumns, ...selectedColumns]).sort((a, b) =>
		a.localeCompare(b)
	);
	$: availableColumns = knownColumns.filter((col) => !selectedColumns.includes(col));
	$: unknownFromSchema = hasKnownSchema
		? selectedColumns.filter((col) => !schemaColumns.includes(col))
		: [];
	$: missingFromError = missingSelectColumnsFromError(nodeError);
	$: duplicateFromError = duplicateSelectColumnsFromError(nodeError);
	$: outputColumns = computeOutputColumns({
		mode,
		keepOrder,
		selectedColumns,
		knownColumns,
		schemaColumns,
		hasKnownSchema
	});

	function isObject(v: unknown): v is Record<string, unknown> {
		return Boolean(v) && typeof v === 'object' && !Array.isArray(v);
	}

	function readSelectParams(raw: unknown): Partial<TransformSelectParams> {
		if (!isObject(raw)) return {};
		const nested = raw.select;
		if (isObject(nested)) return nested as Partial<TransformSelectParams>;
		return raw as Partial<TransformSelectParams>;
	}

	function normalizeSelectParams(raw: Partial<TransformSelectParams>): TransformSelectParams {
		const mode = raw?.mode === 'exclude' ? 'exclude' : 'include';
		const columns = uniqueStrings((raw?.columns ?? []).map((c) => String(c ?? '').trim()).filter(Boolean));
		const keepOrder = raw?.keepOrder === 'input' || raw?.keepOrder === 'custom'
			? raw.keepOrder
			: mode === 'exclude'
				? 'input'
				: 'custom';
		const strict = raw?.strict ?? true;
		return { mode, columns, keepOrder, strict };
	}

	function missingSelectColumnsFromError(err: NodeExecutionError | null): string[] {
		const code = String(err?.errorCode ?? '');
		const path = String(err?.paramPath ?? '');
		if (code !== 'MISSING_COLUMN') return [];
		if (!(path === 'select.columns' || path === 'params.select.columns' || path.endsWith('.select.columns'))) {
			return [];
		}
		return uniqueStrings((err?.missingColumns ?? []).map((c) => String(c).trim()).filter(Boolean));
	}

	function duplicateSelectColumnsFromError(err: NodeExecutionError | null): string[] {
		const code = String(err?.errorCode ?? '');
		const path = String(err?.paramPath ?? '');
		if (code !== 'DUPLICATE_COLUMN') return [];
		if (!(path === 'select.columns' || path === 'params.select.columns' || path.endsWith('.select.columns'))) {
			return [];
		}
		return uniqueStrings((err?.missingColumns ?? []).map((c) => String(c).trim()).filter(Boolean));
	}

	function computeOutputColumns(input: {
		mode: SelectMode;
		keepOrder: KeepOrder;
		selectedColumns: string[];
		knownColumns: string[];
		schemaColumns: string[];
		hasKnownSchema: boolean;
	}): string[] {
		const orderedKnown = input.hasKnownSchema ? input.schemaColumns : input.knownColumns;
		if (input.mode === 'exclude') {
			return orderedKnown.filter((c) => !input.selectedColumns.includes(c));
		}
		if (input.keepOrder === 'input') {
			return orderedKnown.filter((c) => input.selectedColumns.includes(c));
		}
		return [...input.selectedColumns];
	}

	function markLocalEdit() {
		suppressParamSync = true;
		queueMicrotask(() => {
			suppressParamSync = false;
		});
	}

	function commitSelect(next: Partial<TransformSelectParams> = {}): void {
		const payload = normalizeSelectParams({
			mode,
			columns: selectedColumns,
			keepOrder,
			strict,
			...next
		});
		markLocalEdit();
		selectedColumns = [...payload.columns];
		if (isWrappedParams) {
			const wrapped = { op: 'select', select: payload } as unknown as Partial<TransformSelectParams>;
			onDraft(wrapped);
			onCommit(wrapped);
			return;
		}
		onDraft(payload);
		onCommit(payload);
	}

	function addColumn(col: string): void {
		const name = String(col ?? '').trim();
		if (!name) return;
		if (selectedColumns.includes(name)) return;
		stickyKnownColumns = uniqueStrings([...stickyKnownColumns, name]);
		commitSelect({ columns: [...selectedColumns, name] });
	}

	function removeColumn(col: string): void {
		commitSelect({ columns: selectedColumns.filter((c) => c !== col) });
	}

	function addAll(): void {
		commitSelect({ columns: [...knownColumns] });
	}

	function removeAll(): void {
		commitSelect({ columns: [] });
	}

	function resetInputOrder(): void {
		if (!hasKnownSchema) return;
		commitSelect({ columns: schemaColumns.filter((c) => selectedColumns.includes(c)) });
	}

	function sortSelectedAsc(): void {
		commitSelect({ columns: [...selectedColumns].sort((a, b) => a.localeCompare(b)) });
	}
</script>

<Section title="Select Columns">
	<div class="hint">Rows unchanged. Data unchanged. Schema shape only.</div>

	<div class="modeRow">
		<button class:active={mode === 'include'} type="button" on:click={() => commitSelect({ mode: 'include' })}>
			Keep only these columns
		</button>
		<button class:active={mode === 'exclude'} type="button" on:click={() => commitSelect({ mode: 'exclude' })}>
			Drop these columns
		</button>
	</div>

	<div class="optsRow">
		<label class="opt">
			<input type="checkbox" checked={strict} on:change={(e) => commitSelect({ strict: (e.currentTarget as HTMLInputElement).checked })} />
			<span>Strict</span>
		</label>
		{#if mode === 'include'}
			<label class="opt">
				<span>Order</span>
				<select value={keepOrder} on:change={(e) => commitSelect({ keepOrder: (e.currentTarget as HTMLSelectElement).value as KeepOrder })}>
					<option value="custom">custom</option>
					<option value="input">input</option>
				</select>
			</label>
		{/if}
	</div>

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

	<div class="actions">
		<button class="small" type="button" on:click={addAll}>Add all</button>
		<button class="small" type="button" on:click={removeAll}>Remove all</button>
		<button class="small" type="button" on:click={resetInputOrder} disabled={!hasKnownSchema}>Reset input order</button>
		<button class="small" type="button" on:click={sortSelectedAsc}>Sort A-Z</button>
	</div>

	<div class="selectorGrid">
		<div class="listCol">
			<div class="listHeader">{mode === 'include' ? 'Selected Cols' : 'Excluded Cols'}</div>
			<div class="selectedList">
				{#if selectedColumns.length === 0}
					<div class="emptySel">No columns selected</div>
				{:else}
					{#each selectedColumns as col}
						<button class="chipBtn" type="button" on:click={() => removeColumn(col)}>{col}</button>
					{/each}
				{/if}
			</div>
		</div>

		<div class="listCol">
			<div class="listHeader">Available Cols</div>
			<div class="availableList">
				{#if availableColumns.length === 0}
					<div class="emptySel">{knownColumns.length > 0 ? 'No more column names' : 'Schema unavailable (run upstream)'}</div>
				{:else}
					{#each availableColumns as col}
						<button class="chipBtn" type="button" on:click={() => addColumn(col)}>{col}</button>
					{/each}
				{/if}
			</div>
		</div>
	</div>

	<div class="previewWrap">
		<div class="listHeader">Output Columns ({outputColumns.length})</div>
		<div class="previewList">
			{#if outputColumns.length === 0}
				<div class="emptySel">No output columns.</div>
			{:else}
				{#each outputColumns as col}
					<div class="previewRow">{col}</div>
				{/each}
			{/if}
		</div>
	</div>

	{#if unknownFromSchema.length > 0}
		<div class="warn">Unknown columns from schema: {unknownFromSchema.join(', ')}</div>
	{/if}
	{#if missingFromError.length > 0}
		<div class="warn">Runtime missing columns: {missingFromError.join(', ')}</div>
	{/if}
	{#if duplicateFromError.length > 0}
		<div class="warn">Duplicate selected columns: {duplicateFromError.join(', ')}</div>
	{/if}
</Section>

<style>
	.hint {
		font-size: 12px;
		opacity: 0.75;
		margin-top: 6px;
	}

	.modeRow {
		display: grid;
		grid-template-columns: repeat(2, minmax(0, 1fr));
		gap: 8px;
		margin-top: 8px;
	}

	.modeRow button {
		padding: 6px 8px;
		font-size: 12px;
		border-radius: 10px;
		border: 1px solid rgba(255, 255, 255, 0.16);
		background: rgba(255, 255, 255, 0.05);
		color: inherit;
		cursor: pointer;
	}

	.modeRow button.active {
		background: rgba(59, 130, 246, 0.2);
		border-color: rgba(59, 130, 246, 0.6);
	}

	.optsRow {
		margin-top: 8px;
		display: flex;
		flex-wrap: wrap;
		gap: 10px;
		align-items: center;
	}

	.opt {
		display: inline-flex;
		align-items: center;
		gap: 6px;
		font-size: 12px;
	}

	.opt select {
		border-radius: 8px;
		font-size: 12px;
		padding: 4px 6px;
	}

	.addRow {
		display: grid;
		grid-template-columns: minmax(0, 1fr) auto;
		gap: 8px;
		align-items: center;
		margin-top: 8px;
	}

	.actions {
		display: flex;
		flex-wrap: wrap;
		gap: 8px;
		margin-top: 10px;
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
	.availableList,
	.previewList {
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

	.previewWrap {
		margin-top: 10px;
	}

	.previewRow {
		padding: 4px 8px;
		border-radius: 8px;
		font-size: 12px;
		border: 1px solid rgba(255, 255, 255, 0.14);
		background: rgba(255, 255, 255, 0.03);
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

	.warn {
		font-size: 12px;
		color: #fca5a5;
		margin-top: 8px;
	}

	@media (max-width: 560px) {
		.selectorGrid {
			grid-template-columns: 1fr;
		}
		.modeRow {
			grid-template-columns: 1fr;
		}
	}
</style>
