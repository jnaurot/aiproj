<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import type { TransformDeriveParams } from '$lib/flow/schema/transform';
	import type { NodeExecutionError } from '$lib/flow/store/graphStore';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';
	import DeriveFormulaBuilder from './DeriveFormulaBuilder.svelte';
	import {
		defaultDeriveRules,
		defaultDeriveSqlColumns,
		normalizeDeriveParams,
		type DeriveMode,
		type DeriveRule,
		type DeriveSqlColumn
	} from './deriveRulesModel';
	import { normalizeColumnNames, toSchemaColumns } from './columnSelectionModel';

	export let selectedNode: Node<PipelineNodeData>;
	export let params: Partial<TransformDeriveParams>;
	export let onDraft: (patch: Partial<TransformDeriveParams>) => void;
	export let onCommit: (patch: Partial<TransformDeriveParams>) => void;
	export let inputColumns: string[] = [];
	export let nodeError: NodeExecutionError | null = null;

	let mode: DeriveMode = 'rules';
	let sqlColumnsDraft: DeriveSqlColumn[] = defaultDeriveSqlColumns();
	let rulesDraft: DeriveRule[] = defaultDeriveRules();
	let activeExprIndex = 0;
	let lastNodeId = '';
	let lastParamsSignature = '';
	let suppressParamSync = false;
	$: isWrappedParams =
		isObject(params) && ('op' in (params as Record<string, unknown>) || 'derive' in (params as Record<string, unknown>));
	$: effectiveParams = (() => {
		if (!isObject(params)) return {};
		const record = params as Record<string, unknown>;
		const nested = record.derive;
		if (isObject(nested)) return nested as Record<string, unknown>;
		return record;
	})();

	$: void selectedNode?.id;
	$: normalized = normalizeDeriveParams(effectiveParams);
	$: paramsSignature = JSON.stringify({ mode: normalized.mode, columns: normalized.columns, rules: normalized.rules });
	$: errorAvailableColumns = availableDeriveColumnsFromError(nodeError);
	$: schemaColumns = toSchemaColumns([...inputColumns, ...errorAvailableColumns]);
	$: hasKnownSchema = schemaColumns.length > 0;
	$: missingFromError = missingDeriveColumnsFromError(nodeError);
	$: knownColumns = schemaColumns;
	$: unknownRefsFromSchema = hasKnownSchema && mode === 'sql' ? deriveUnknownRefs(sqlColumnsDraft, schemaColumns) : [];
	$: validSqlRowCount = sqlColumnsDraft.filter((item) => isFilled(item.name) && isFilled(item.expr)).length;

	$: if ((selectedNode?.id ?? '') !== lastNodeId) {
		lastNodeId = selectedNode?.id ?? '';
		mode = normalized.mode;
		sqlColumnsDraft = normalized.columns.map((item) => ({ ...item }));
		rulesDraft = normalized.rules.map(cloneRule);
		lastParamsSignature = paramsSignature;
	}
	$: if (!suppressParamSync && (selectedNode?.id ?? '') === lastNodeId && paramsSignature !== lastParamsSignature) {
		mode = normalized.mode;
		sqlColumnsDraft = normalized.columns.map((item) => ({ ...item }));
		rulesDraft = normalized.rules.map(cloneRule);
		lastParamsSignature = paramsSignature;
	}

	function isObject(value: unknown): value is Record<string, unknown> {
		return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
	}

	function cloneRule(rule: DeriveRule): DeriveRule {
		return {
			name: rule.name,
			op: rule.op,
			args: (rule.args ?? []).map((arg) => ({ ...arg }))
		};
	}

	function normalizeSqlRows(items: DeriveSqlColumn[]): DeriveSqlColumn[] {
		return (items ?? [])
			.map((item) => ({
				name: String(item?.name ?? ''),
				expr: String(item?.expr ?? '')
			}))
			.filter((item) => item.name.trim().length > 0 || item.expr.trim().length > 0);
	}

	function normalizeCommittedSqlRows(items: DeriveSqlColumn[]): DeriveSqlColumn[] {
		return normalizeSqlRows(items)
			.map((item) => ({ name: item.name.trim(), expr: item.expr.trim() }))
			.filter((item) => item.name.length > 0 && item.expr.length > 0)
			.filter((item, index, arr) => arr.findIndex((candidate) => candidate.name === item.name) === index);
	}

	function parseLiteral(text: string): string | number | boolean | null {
		const trimmed = String(text ?? '').trim();
		if (trimmed.length === 0) return '';
		if (/^-?\d+(\.\d+)?$/.test(trimmed)) return Number(trimmed);
		if (trimmed.toLowerCase() === 'true') return true;
		if (trimmed.toLowerCase() === 'false') return false;
		if (trimmed.toLowerCase() === 'null') return null;
		return trimmed;
	}

	function toSchemaRules(rules: DeriveRule[]): NonNullable<TransformDeriveParams['rules']> {
		return (rules ?? [])
			.map((rule) => ({
				name: String(rule.name ?? '').trim(),
				formula: {
					op: rule.op,
					args: (rule.args ?? []).map((arg) => {
						if (arg.source === 'column') return { column: String(arg.column ?? '').trim() };
						if (arg.source === 'param_config') {
							return {
								valueFrom: {
									handle: 'param_config',
									path: String(arg.paramPath ?? '').trim()
								}
							};
						}
						return parseLiteral(String(arg.literalValue ?? ''));
					})
				}
			}))
			.filter((rule) => rule.name.length > 0);
	}

	function commitPatch(
		next: {
			mode?: DeriveMode;
			sqlColumns?: DeriveSqlColumn[];
			rules?: DeriveRule[];
		},
		commit = false
	): void {
		suppressParamSync = true;
		mode = next.mode ?? mode;
		sqlColumnsDraft = (next.sqlColumns ?? sqlColumnsDraft).map((item) => ({ ...item }));
		rulesDraft = (next.rules ?? rulesDraft).map(cloneRule);
		const patch: Partial<TransformDeriveParams> = {
			mode,
			columns: normalizeCommittedSqlRows(sqlColumnsDraft),
			rules: toSchemaRules(rulesDraft)
		};
		if (isWrappedParams) {
			const wrapped = { op: 'derive', derive: patch } as unknown as Partial<TransformDeriveParams>;
			onDraft(wrapped);
			if (commit) onCommit(wrapped);
		} else {
			onDraft(patch);
			if (commit) onCommit(patch);
		}
		queueMicrotask(() => {
			suppressParamSync = false;
		});
	}

	function setMode(nextMode: DeriveMode): void {
		commitPatch({ mode: nextMode }, false);
	}

	function updateSqlColumn(index: number, key: keyof DeriveSqlColumn, value: string): void {
		const nextRows = sqlColumnsDraft.map((item, current) => (current === index ? { ...item, [key]: value } : item));
		commitPatch({ sqlColumns: nextRows }, false);
	}

	function addSqlColumn(): void {
		const nextRows = [...sqlColumnsDraft, { name: '', expr: '' }];
		commitPatch({ sqlColumns: nextRows }, false);
		activeExprIndex = Math.max(0, nextRows.length - 1);
	}

	function removeSqlColumn(index: number): void {
		const nextRows = sqlColumnsDraft.filter((_, current) => current !== index);
		commitPatch({ sqlColumns: nextRows.length > 0 ? nextRows : [{ name: '', expr: '' }] }, false);
		if (activeExprIndex >= nextRows.length) activeExprIndex = Math.max(0, nextRows.length - 1);
	}

	function insertColumnRef(columnName: string): void {
		const col = String(columnName ?? '').trim();
		if (!col || sqlColumnsDraft.length === 0) return;
		const index = activeExprIndex >= 0 && activeExprIndex < sqlColumnsDraft.length ? activeExprIndex : 0;
		const token = `"${col}"`;
		const currentExpr = String(sqlColumnsDraft[index]?.expr ?? '');
		const spacer = currentExpr.trim().length > 0 && !/\s$/.test(currentExpr) ? ' ' : '';
		updateSqlColumn(index, 'expr', `${currentExpr}${spacer}${token}`);
	}

	function isFilled(value: string): boolean {
		return String(value ?? '').trim().length > 0;
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

	function deriveUnknownRefs(items: DeriveSqlColumn[], schema: string[]): string[] {
		const schemaSet = new Set(schema);
		const quotedRefs = normalizeColumnNames(items.flatMap((item) => extractQuotedIdentifiers(item.expr)) as unknown[]);
		return quotedRefs.filter((ref) => !schemaSet.has(ref));
	}

	function missingDeriveColumnsFromError(err: NodeExecutionError | null): string[] {
		if (!err) return [];
		const code = String(err.errorCode ?? '');
		const path = String(err.paramPath ?? '');
		if (code !== 'MISSING_COLUMN') return [];
		if (!(path === 'derive.columns' || path === 'params.derive.columns' || path.endsWith('.derive.columns'))) return [];
		return normalizeColumnNames((Array.isArray(err.missingColumns) ? err.missingColumns : []) as unknown[]);
	}

	function availableDeriveColumnsFromError(err: NodeExecutionError | null): string[] {
		if (!err) return [];
		const code = String(err.errorCode ?? '');
		const path = String(err.paramPath ?? '');
		if (code !== 'MISSING_COLUMN') return [];
		if (!(path === 'derive.columns' || path === 'params.derive.columns' || path.endsWith('.derive.columns'))) return [];
		return normalizeColumnNames((Array.isArray(err.availableColumns) ? err.availableColumns : []) as unknown[]);
	}
</script>

<Section title="Derive Columns">
	<div class="hint">Create computed columns using rules (default) or SQL expressions.</div>
	<div class="modeRow">
		<button class={`small ${mode === 'rules' ? 'active' : ''}`} type="button" on:click={() => setMode('rules')}>
			Rules
		</button>
		<button class={`small ${mode === 'sql' ? 'active' : ''}`} type="button" on:click={() => setMode('sql')}>
			SQL (advanced)
		</button>
	</div>

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

	{#if mode === 'rules'}
		<div class="hint">Use formula ops plus literal, column, or `param_config.path` args.</div>
		<DeriveFormulaBuilder
			rules={rulesDraft}
			columns={knownColumns}
			onChange={(next) => commitPatch({ rules: next }, false)}
		/>
	{:else}
		<div class="hint">Each row compiles to <code>(expr) AS name</code>. Example: <code>length("text")</code>.</div>
		{#each sqlColumnsDraft as column, index}
			<div class="deriveRule">
				<div class="ruleTopRow">
					<Input
						value={column.name}
						placeholder="name"
						onInput={(event) => updateSqlColumn(index, 'name', (event.currentTarget as HTMLInputElement).value)}
					/>
					<button class="small danger" type="button" on:click={() => removeSqlColumn(index)}>-</button>
				</div>
				<div class="ruleBottomRow">
					<Input
						multiline={true}
						rows={3}
						value={column.expr}
						placeholder="function"
						onInput={(event) => updateSqlColumn(index, 'expr', (event.currentTarget as HTMLTextAreaElement).value)}
						onFocus={() => (activeExprIndex = index)}
					/>
				</div>
			</div>
		{/each}
		<div class="actions">
			<button class="small" type="button" on:click={addSqlColumn}>+ Add derived column</button>
		</div>
		{#if validSqlRowCount === 0}
			<div class="warn">At least one derived column is required in SQL mode.</div>
		{/if}
		{#if unknownRefsFromSchema.length > 0}
			<div class="warn">Unknown referenced columns: {unknownRefsFromSchema.join(', ')}</div>
		{/if}
	{/if}

	{#if missingFromError.length > 0}
		<div class="warn">Runtime mismatch: {missingFromError.join(', ')}</div>
	{/if}
</Section>

<style>
	.modeRow {
		display: flex;
		gap: 8px;
		margin-top: 8px;
	}
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
	button.small.active {
		background: rgba(59, 130, 246, 0.24);
		border-color: rgba(59, 130, 246, 0.6);
	}
	button.commit {
		margin-left: auto;
		border-color: rgba(59, 130, 246, 0.5);
		background: rgba(59, 130, 246, 0.14);
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
