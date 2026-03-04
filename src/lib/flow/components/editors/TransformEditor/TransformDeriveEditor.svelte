<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import type { TransformDeriveParams } from '$lib/flow/schema/transform';
	import type { NodeExecutionError } from '$lib/flow/store/graphStore';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';
	import { computeColumnUniverse, normalizeColumnNames, toSchemaColumns } from './columnSelectionModel';

	type DeriveColumn = { name: string; expr: string };

	export let selectedNode: Node<PipelineNodeData>;
	export let params: Partial<TransformDeriveParams>;
	export let onDraft: (patch: Partial<TransformDeriveParams>) => void;
	export let onCommit: (patch: Partial<TransformDeriveParams>) => void;
	export let inputColumns: string[] = [];
	export let nodeError: NodeExecutionError | null = null;

	const defaults: DeriveColumn[] = [
		{ name: 'length_text', expr: 'length(text)' },
		{ name: 'is_long', expr: 'length(text) > 50' }
	];

	let columns: DeriveColumn[] = [];
	let stickyKnownColumns: string[] = [];
	let activeExprIndex = 0;
	let lastNodeId = '';
	let lastParamsSignature = '';
	let suppressParamSync = false;

	$: void selectedNode?.id;
	$: normalized = normalizeDeriveParams(params);
	$: paramsSignature = JSON.stringify(normalized.columns);
	$: errorAvailableColumns = availableDeriveColumnsFromError(nodeError);
	$: schemaColumns = toSchemaColumns([...inputColumns, ...errorAvailableColumns]);
	$: hasKnownSchema = schemaColumns.length > 0;
	$: if (hasKnownSchema) {
		stickyKnownColumns = [...schemaColumns];
	}
	$: universe = computeColumnUniverse({
		stickyColumns: stickyKnownColumns,
		schemaColumns,
		selectedColumns: []
	});
	$: knownColumns = universe.knownColumns;
	$: missingFromError = missingDeriveColumnsFromError(nodeError);
	$: unknownRefsFromSchema = hasKnownSchema
		? deriveUnknownRefs(columns, schemaColumns)
		: [];
	$: validRowCount = columns.filter((item) => isFilled(item.name) && isFilled(item.expr)).length;

	$: if ((selectedNode?.id ?? '') !== lastNodeId) {
		lastNodeId = selectedNode?.id ?? '';
		columns = normalized.columns.map((item) => ({ ...item }));
		lastParamsSignature = paramsSignature;
	}
	$: if (!suppressParamSync && (selectedNode?.id ?? '') === lastNodeId && paramsSignature !== lastParamsSignature) {
		columns = normalized.columns.map((item) => ({ ...item }));
		lastParamsSignature = paramsSignature;
	}

	function normalizeLocalColumns(items: DeriveColumn[]): DeriveColumn[] {
		const next = (items ?? []).map((item) => ({
			name: String(item?.name ?? ''),
			expr: String(item?.expr ?? '')
		}));
		return next.length > 0 ? next : defaults.map((item) => ({ ...item }));
	}

	function normalizeCommitColumns(items: DeriveColumn[]): DeriveColumn[] {
		return items
			.map((item) => ({ name: String(item.name ?? '').trim(), expr: String(item.expr ?? '').trim() }))
			.filter((item) => item.name.length > 0 && item.expr.length > 0)
			.filter((item, index, arr) => arr.findIndex((c) => c.name === item.name) === index);
	}

	function normalizeDeriveParams(raw: Partial<TransformDeriveParams> | undefined): TransformDeriveParams {
		const local = normalizeLocalColumns(Array.isArray(raw?.columns) ? raw.columns : defaults);
		const committed = normalizeCommitColumns(local);
		return {
			columns: committed.length > 0 ? committed : defaults.map((item) => ({ ...item }))
		};
	}

	function isFilled(value: string): boolean {
		return String(value ?? '').trim().length > 0;
	}

	function markLocalEdit(): void {
		suppressParamSync = true;
		queueMicrotask(() => {
			suppressParamSync = false;
		});
	}

	function commitColumns(nextLocal: DeriveColumn[]): void {
		const local = normalizeLocalColumns(nextLocal);
		const committed = normalizeCommitColumns(local);
		const safeCommitted = committed.length > 0 ? committed : defaults.map((item) => ({ ...item }));
		markLocalEdit();
		columns = local;
		onDraft({ columns: safeCommitted });
		onCommit({ columns: safeCommitted });
	}

	function updateColumn(index: number, key: keyof DeriveColumn, value: string): void {
		const next = columns.map((item, current) => (current === index ? { ...item, [key]: value } : item));
		commitColumns(next);
	}

	function addColumn(): void {
		commitColumns([...columns, { name: '', expr: '' }]);
		activeExprIndex = Math.max(0, columns.length);
	}

	function removeColumn(index: number): void {
		const next = columns.filter((_, current) => current !== index);
		commitColumns(next.length > 0 ? next : [{ name: '', expr: '' }]);
		if (activeExprIndex >= next.length) {
			activeExprIndex = Math.max(0, next.length - 1);
		}
	}

	function resetDefaults(): void {
		commitColumns(defaults.map((item) => ({ ...item })));
		activeExprIndex = 0;
	}

	function insertColumnRef(columnName: string): void {
		if (!columnName || columns.length === 0) return;
		const token = `"${columnName}"`;
		const index =
			activeExprIndex >= 0 && activeExprIndex < columns.length
				? activeExprIndex
				: 0;
		const currentExpr = String(columns[index]?.expr ?? '');
		const spacer = currentExpr.trim().length > 0 && !/\s$/.test(currentExpr) ? ' ' : '';
		updateColumn(index, 'expr', `${currentExpr}${spacer}${token}`);
	}

	function extractQuotedIdentifiers(expr: string): string[] {
		const source = String(expr ?? '');
		const matches = source.matchAll(/"([^"]+)"|`([^`]+)`/g);
		const out: string[] = [];
		for (const match of matches) {
			const token = String(match[1] ?? match[2] ?? '').trim();
			if (token) out.push(token);
		}
		return normalizeColumnNames(out);
	}

	function deriveUnknownRefs(items: DeriveColumn[], schema: string[]): string[] {
		const schemaSet = new Set(schema);
		const quotedRefs = normalizeColumnNames(
			items.flatMap((item) => extractQuotedIdentifiers(item.expr)) as unknown[]
		);
		return quotedRefs.filter((ref) => !schemaSet.has(ref));
	}

	function missingDeriveColumnsFromError(err: NodeExecutionError | null): string[] {
		if (!err) return [];
		const code = String(err.errorCode ?? '');
		const path = String(err.paramPath ?? '');
		if (code !== 'MISSING_COLUMN') return [];
		if (!(path === 'derive.columns' || path === 'params.derive.columns' || path.endsWith('.derive.columns'))) {
			return [];
		}
		return normalizeColumnNames((Array.isArray(err.missingColumns) ? err.missingColumns : []) as unknown[]);
	}

	function availableDeriveColumnsFromError(err: NodeExecutionError | null): string[] {
		if (!err) return [];
		const code = String(err.errorCode ?? '');
		const path = String(err.paramPath ?? '');
		if (code !== 'MISSING_COLUMN') return [];
		if (!(path === 'derive.columns' || path === 'params.derive.columns' || path.endsWith('.derive.columns'))) {
			return [];
		}
		return normalizeColumnNames((Array.isArray(err.availableColumns) ? err.availableColumns : []) as unknown[]);
	}
</script>

<Section title="Derive Columns">
	<div class="hint">
		Add computed columns with DuckDB expressions. Each row becomes <code>(expr) AS name</code>.
	</div>
	<div class="hint">Quote source columns for schema checks, e.g. <code>length("text")</code>.</div>

	{#if knownColumns.length > 0}
		<div class="colsWrap">
			<div class="colsHeader">Available Cols</div>
			<div class="colsList">
				{#each knownColumns as col}
					<button class="chipBtn" type="button" on:click={() => insertColumnRef(col)}>{col}</button>
				{/each}
			</div>
		</div>
	{:else}
		<div class="hint">Schema unavailable (run upstream) to populate column names.</div>
	{/if}

	{#each columns as column, index}
		<div class="deriveRule">
			<div class="ruleTopRow">
				<Input
					value={column.name}
					placeholder="name"
					onInput={(event) => updateColumn(index, 'name', (event.currentTarget as HTMLInputElement).value)}
				/>
				<button class="small danger" type="button" on:click={() => removeColumn(index)}>
					-
				</button>
			</div>
			<div class="ruleBottomRow">
				<Input
					multiline={true}
					rows={3}
					value={column.expr}
					placeholder="function"
					onInput={(event) => updateColumn(index, 'expr', (event.currentTarget as HTMLTextAreaElement).value)}
					onFocus={() => (activeExprIndex = index)}
				/>
			</div>
		</div>
	{/each}

	<div class="actions">
		<button class="small" type="button" on:click={addColumn}>
			+ Add derived column
		</button>
		<button class="small ghost" type="button" on:click={resetDefaults}>
			Reset defaults
		</button>
	</div>

	{#if validRowCount === 0}
		<div class="warn">At least one derived column is required.</div>
	{/if}
	{#if unknownRefsFromSchema.length > 0}
		<div class="warn">Unknown referenced columns: {unknownRefsFromSchema.join(', ')}</div>
	{/if}
	{#if missingFromError.length > 0}
		<div class="warn">Runtime mismatch: {missingFromError.join(', ')}</div>
	{/if}
</Section>

<style>
	.deriveRule {
		display: grid;
		gap: 6px;
		margin: 8px 0;
	}

	.ruleTopRow {
		display: grid;
		grid-template-columns: minmax(0, 1fr) auto;
		gap: 8px;
		align-items: center;
	}

	.ruleBottomRow {
		display: grid;
		grid-template-columns: 1fr;
		gap: 8px;
		align-items: start;
	}

	.colsWrap {
		margin-top: 8px;
	}

	.colsHeader {
		font-size: 12px;
		font-weight: 700;
		opacity: 0.9;
		margin-bottom: 6px;
	}

	.colsList {
		min-height: 42px;
		max-height: 188px;
		overflow-y: auto;
		border: 1px solid rgba(255, 255, 255, 0.16);
		border-radius: 10px;
		padding: 8px;
		display: flex;
		flex-wrap: wrap;
		gap: 6px;
	}

	.chipBtn {
		padding: 4px 8px;
		font-size: 12px;
		border-radius: 8px;
		border: 1px solid rgba(255, 255, 255, 0.16);
		background: rgba(255, 255, 255, 0.04);
		color: inherit;
		cursor: pointer;
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
		gap: 8px;
		justify-content: flex-end;
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

	code {
		font-family: ui-monospace, Menlo, Consolas, monospace;
		font-size: 12px;
	}
</style>
