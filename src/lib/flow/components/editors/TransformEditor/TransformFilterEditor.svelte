<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import type { TransformFilterParams } from '$lib/flow/schema/transform';
	import type { NodeExecutionError } from '$lib/flow/store/graphStore';
	import type { InputSchemaView } from './inputSchema';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import Input from '$lib/flow/components/ui/Input.svelte';
	import { uniqueStrings } from '$lib/flow/components/editors/shared';

	export let selectedNode: Node<PipelineNodeData>;
	export let params: Partial<TransformFilterParams>;
	export let onDraft: (patch: Partial<TransformFilterParams>) => void;
	export let onCommit: (patch: Partial<TransformFilterParams>) => void;
	export let inputColumns: string[] = [];
	export let inputSchemaColumns: Array<{ name: string; type?: string }> = [];
	export let inputSchemas: InputSchemaView[] = [];
	export let nodeError: NodeExecutionError | null = null;
	$: void onCommit;

	let exprDraft = '';
	let lastNodeId = '';
	let lastExpr = '';
	let suppressParamSync = false;
	$: isWrappedParams = isObject(params) && ('op' in (params as Record<string, unknown>) || 'filter' in (params as Record<string, unknown>));

	$: void selectedNode?.id;
	$: expr = readFilterExpr(params);
	$: if ((selectedNode?.id ?? '') !== lastNodeId) {
		lastNodeId = selectedNode?.id ?? '';
		exprDraft = expr;
		lastExpr = expr;
	}
	$: if (!suppressParamSync && (selectedNode?.id ?? '') === lastNodeId && expr !== lastExpr) {
		exprDraft = expr;
		lastExpr = expr;
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
	$: if (columns.length > 0) {
		console.debug('[filter-schema-prop] TransformFilterEditor.columns', {
			nodeId: selectedNode?.id ?? '',
			inputSchemaColumns,
			inputSchemasCount: (inputSchemas ?? []).length,
			schemaTypeByName: Array.from(schemaTypeByName.entries()),
			columns
		});
	}

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

	function readFilterExpr(raw: unknown): string {
		if (!isObject(raw)) return '';
		const direct = raw.expr;
		if (typeof direct === 'string') return direct;
		const nested = raw.filter;
		if (isObject(nested) && typeof nested.expr === 'string') return String(nested.expr);
		return '';
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
			// Prefer concrete inferred types over unknown when multiple inputs share a name.
			if (!prevType || prevType === 'unknown' || prevType.length === 0) {
				out.set(name, nextType);
				continue;
			}
			if (nextType !== 'unknown' && nextType.length > 0) {
				out.set(name, nextType);
			}
		}
		return out;
	}

	function draftExpr(nextExpr: string): void {
		const normalized = String(nextExpr ?? '').replace(/\r\n/g, '\n').replace(/\r/g, '\n');
		suppressParamSync = true;
		exprDraft = normalized;
		lastExpr = normalized;
		if (isWrappedParams) {
			onDraft({ op: 'filter', filter: { expr: normalized } } as unknown as Partial<TransformFilterParams>);
		} else {
			onDraft({ expr: normalized });
		}
		queueMicrotask(() => {
			suppressParamSync = false;
		});
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
	<div class="hint">Rows may decrease. Columns unchanged. Empty expression passes through.</div>

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

	<div class="exprWrap">
		<Input
			multiline={true}
			rows={5}
			value={exprDraft}
			placeholder={'type boolean WHERE expression\nexample: "qty" > 0 AND "price" IS NOT NULL'}
			onInput={(event) => draftExpr((event.currentTarget as HTMLTextAreaElement).value)}
		/>
	</div>

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
