<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import type { TransformFilterParams } from '$lib/flow/schema/transform';
	import type { NodeExecutionError } from '$lib/flow/store/graphStore';
	import type { InputSchemaView } from './inputSchema';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';
	import FilterRulesBuilder from './FilterRulesBuilder.svelte';
	import {
		defaultFilterRules,
		normalizeFilterParams,
		type FilterCondition,
		type FilterGroup,
		type FilterMode,
		type FilterRuleNode
	} from './filterRulesModel';
	import { uniqueStrings } from '$lib/flow/components/editors/shared';

	export let selectedNode: Node<PipelineNodeData>;
	export let params: Partial<TransformFilterParams>;
	export let onDraft: (patch: Partial<TransformFilterParams>) => void;
	export let onCommit: (patch: Partial<TransformFilterParams>) => void;
	export let inputColumns: string[] = [];
	export let inputSchemaColumns: Array<{ name: string; type?: string }> = [];
	export let inputSchemas: InputSchemaView[] = [];
	export let nodeError: NodeExecutionError | null = null;

	let mode: FilterMode = 'rules';
	let exprDraft = '';
	let rulesDraft: FilterGroup = defaultFilterRules();
	let lastNodeId = '';
	let lastParamsSignature = '';
	let suppressParamSync = false;
	$: isWrappedParams = isObject(params) && ('op' in (params as Record<string, unknown>) || 'filter' in (params as Record<string, unknown>));
	$: effectiveParams = (() => {
		if (!isObject(params)) return {};
		const record = params as Record<string, unknown>;
		const nested = record.filter;
		if (isObject(nested)) return nested as Record<string, unknown>;
		return record;
	})();

	$: void selectedNode?.id;
	$: normalized = normalizeFilterParams(effectiveParams);
	$: paramsSignature = JSON.stringify({ mode: normalized.mode, expr: normalized.expr, rules: normalized.rules });
	$: if ((selectedNode?.id ?? '') !== lastNodeId) {
		lastNodeId = selectedNode?.id ?? '';
		mode = normalized.mode;
		exprDraft = normalized.expr;
		rulesDraft = structuredClone(normalized.rules);
		lastParamsSignature = paramsSignature;
	}
	$: if (!suppressParamSync && (selectedNode?.id ?? '') === lastNodeId && paramsSignature !== lastParamsSignature) {
		mode = normalized.mode;
		exprDraft = normalized.expr;
		rulesDraft = structuredClone(normalized.rules);
		lastParamsSignature = paramsSignature;
	}
	$: errorAvailableColumns = Array.isArray(nodeError?.availableColumns)
		? uniqueStrings(nodeError.availableColumns.map((c) => String(c).trim()).filter(Boolean))
		: [];
	$: schemaTypeByName = buildSchemaTypeMap(
		(inputSchemaColumns?.length ?? 0) > 0
			? inputSchemaColumns
			: (inputSchemas ?? []).flatMap((schema) => schema.columns ?? [])
	);
	$: columnNames = uniqueStrings(
		[...inputColumns, ...Array.from(schemaTypeByName.keys()), ...errorAvailableColumns]
			.map((c) => String(c).trim())
			.filter(Boolean)
	).sort((a, b) => a.localeCompare(b));
	$: columns = columnNames.map((name) => ({ name, type: schemaTypeByName.get(name) ?? 'unknown' }));
	$: missingColumns = missingFilterColumnsFromError(nodeError);

	function missingFilterColumnsFromError(err: NodeExecutionError | null): string[] {
		const code = String(err?.errorCode ?? '');
		const path = String(err?.paramPath ?? '');
		if (code !== 'MISSING_COLUMN') return [];
		if (!(path === 'filter.expr' || path === 'params.filter.expr' || path.endsWith('.filter.expr') || path === 'params.expr')) {
			return [];
		}
		return uniqueStrings((err?.missingColumns ?? []).map((c) => String(c).trim()).filter(Boolean));
	}

	function isObject(v: unknown): v is Record<string, unknown> {
		return Boolean(v) && typeof v === 'object' && !Array.isArray(v);
	}

	function buildSchemaTypeMap(
		columns: Array<{ name: string; type?: string }>
	): Map<string, string> {
		const out = new Map<string, string>();
		for (const col of columns) {
			const name = String(col?.name ?? '').trim();
			if (!name) continue;
			const nextType = String(col?.type ?? 'unknown').trim() || 'unknown';
			const prevType = out.get(name);
			if (!prevType || prevType === 'unknown' || prevType.length === 0) {
				out.set(name, nextType);
				continue;
			}
			if (nextType !== 'unknown' && nextType.length > 0) out.set(name, nextType);
		}
		return out;
	}

	function toSchemaRuleNode(node: FilterRuleNode): Record<string, unknown> {
		if (node.kind === 'group') {
			return {
				kind: 'group',
				op: node.op,
				conditions: node.conditions.map((entry) => toSchemaRuleNode(entry))
			};
		}
		const out: Record<string, unknown> = {
			kind: 'condition',
			column: String(node.column ?? '').trim(),
			op: node.op
		};
		const needsValue = !['is_null', 'not_null'].includes(node.op);
		if (needsValue) {
			if (node.valueSource === 'param_config') {
				out.value = {
					valueFrom: {
						handle: 'param_config',
						path: String(node.paramPath ?? '').trim()
					}
				};
			} else {
				const rawLiteral = String(node.literalValue ?? '').trim();
				if ((node.op === 'in' || node.op === 'not_in') && rawLiteral.length > 0) {
					out.value = rawLiteral.split(',').map((item) => item.trim()).filter((item) => item.length > 0);
				} else {
					out.value = rawLiteral;
				}
			}
		}
		return out;
	}

	function commitPatch(next: { mode?: FilterMode; expr?: string; rules?: FilterGroup }, commit = false): void {
		suppressParamSync = true;
		mode = next.mode ?? mode;
		exprDraft = next.expr ?? exprDraft;
		rulesDraft = next.rules ?? rulesDraft;
		const patch: Partial<TransformFilterParams> = {
			mode,
			expr: exprDraft,
			rules: toSchemaRuleNode(rulesDraft) as unknown as TransformFilterParams['rules']
		};
		if (isWrappedParams) {
			const wrapped = { op: 'filter', filter: patch } as unknown as Partial<TransformFilterParams>;
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

	function setMode(nextMode: FilterMode): void {
		if (nextMode === mode) return;
		const leavingRules = mode === 'rules' && nextMode === 'sql' && (rulesDraft.conditions?.length ?? 0) > 0;
		const leavingSql = mode === 'sql' && nextMode === 'rules' && exprDraft.trim().length > 0;
		const shouldConfirm = leavingRules || leavingSql;
		if (shouldConfirm && typeof window !== 'undefined') {
			const message =
				nextMode === 'sql'
					? 'Switching to SQL hides current rules. Continue?'
					: 'Switching to Rules hides the current SQL expression. Continue?';
			if (!window.confirm(message)) return;
		}
		commitPatch({ mode: nextMode }, true);
	}

	function draftExpr(nextExpr: string): void {
		const normalizedExpr = String(nextExpr ?? '').replace(/\r\n/g, '\n').replace(/\r/g, '\n');
		commitPatch({ expr: normalizedExpr }, false);
	}

	function insertColumnName(col: string): void {
		const name = String(col ?? '').trim();
		if (!name) return;
		const quoted = `"${name.replaceAll('"', '""')}"`;
		const next = exprDraft.trim().length === 0 ? quoted : `${exprDraft} ${quoted}`;
		draftExpr(next);
	}
</script>

<Section title="Filter">
	<div class="hint">Rows may decrease. Columns unchanged.</div>

	<div class="modeRow">
		<button class={`small ${mode === 'rules' ? 'active' : ''}`} type="button" on:click={() => setMode('rules')}>
			Rules
		</button>
		<button class={`small ${mode === 'sql' ? 'active' : ''}`} type="button" on:click={() => setMode('sql')}>
			SQL (advanced)
		</button>
	</div>

	<div class="stickyColsWrap">
		<div class="listHeader">Available Cols</div>
		<div class="colsList">
			{#if columns.length === 0}
				<div class="emptySel">Schema unavailable (run upstream)</div>
			{:else}
				{#each columns as col}
					<button class="chipBtn" type="button" on:click={() => insertColumnName(col.name)}>
						<span class="chipName">{col.name}</span>
						<span class="chipType">{col.type}</span>
					</button>
				{/each}
			{/if}
		</div>
	</div>

	{#if mode === 'rules'}
		<div class="hint">Use groups to combine conditions. `param_config.path` pulls runtime values.</div>
		<FilterRulesBuilder
			group={rulesDraft}
			columns={columns}
			onChange={(next) => commitPatch({ rules: next }, false)}
		/>
	{:else}
		<div class="exprWrap">
			<Input
				multiline={true}
				rows={5}
				value={exprDraft}
				placeholder={'type boolean WHERE expression\nexample: "qty" > 0 AND "price" IS NOT NULL'}
				onInput={(event) => draftExpr((event.currentTarget as HTMLTextAreaElement).value)}
			/>
		</div>
	{/if}

	{#if missingColumns.length > 0}
		<div class="warn">Unknown columns: {missingColumns.join(', ')}</div>
	{/if}
</Section>

<style>
	.hint {
		font-size: 12px;
		opacity: 0.75;
		margin-top: 6px;
	}
	.modeRow {
		display: flex;
		gap: 8px;
		margin-top: 8px;
	}
	button.small {
		padding: 6px 10px;
		font-size: 12px;
		border-radius: 10px;
		border: 1px solid rgba(255, 255, 255, 0.16);
		background: rgba(255, 255, 255, 0.04);
		color: inherit;
		cursor: pointer;
	}
	button.small.active {
		background: rgba(59, 130, 246, 0.24);
		border-color: rgba(59, 130, 246, 0.6);
	}
	.exprWrap {
		margin-top: 8px;
	}
	.stickyColsWrap {
		position: sticky;
		top: 0;
		z-index: 2;
		background: inherit;
		padding-top: 8px;
		margin-top: 6px;
		margin-bottom: 8px;
	}
	.listHeader {
		font-size: 12px;
		font-weight: 700;
		margin-bottom: 6px;
		opacity: 0.9;
	}
	.colsList {
		min-height: 42px;
		max-height: 140px;
		overflow-y: auto;
		border: 1px solid rgba(255, 255, 255, 0.16);
		border-radius: 10px;
		padding: 8px;
		display: grid;
		grid-template-columns: repeat(3, minmax(0, 1fr));
		gap: 6px;
	}
	.emptySel {
		font-size: 12px;
		opacity: 0.75;
	}
	.chipBtn {
		padding: 3px 6px;
		font-size: 11px;
		border-radius: 8px;
		border: 1px solid rgba(255, 255, 255, 0.16);
		background: rgba(255, 255, 255, 0.04);
		color: inherit;
		cursor: pointer;
		text-align: left;
		display: grid;
		grid-template-columns: minmax(0, 1fr);
		gap: 2px;
	}
	.chipName {
		white-space: nowrap;
		overflow: hidden;
		text-overflow: ellipsis;
	}
	.chipType {
		font-size: 10px;
		opacity: 0.75;
	}
	.warn {
		font-size: 12px;
		color: #fca5a5;
		margin-top: 8px;
	}
</style>
