<script lang="ts">
	import type { Node } from '@xyflow/svelte';
	import type { PipelineNodeData } from '$lib/flow/types';
	import type { TransformJsonFilterParams } from '$lib/flow/schema/transform';
	import type { InputSchemaView } from './inputSchema';
	import Section from '$lib/flow/components/ui/Section.svelte';
	import FilterRulesBuilder from './FilterRulesBuilder.svelte';
	import { defaultFilterRules, normalizeFilterParams, type FilterGroup } from './filterRulesModel';
	import { uniqueStrings } from '$lib/flow/components/editors/shared';

	export let selectedNode: Node<PipelineNodeData>;
	export let params: Partial<TransformJsonFilterParams>;
	export let onDraft: (patch: Partial<TransformJsonFilterParams>) => void;
	export let onCommit: (patch: Partial<TransformJsonFilterParams>) => void;
	export let inputColumns: string[] = [];
	export let inputSchemaColumns: Array<{ name: string; type?: string }> = [];
	export let inputSchemas: InputSchemaView[] = [];

	let rulesDraft: FilterGroup = defaultFilterRules();
	let routeReject = true;
	let includeRejectMeta = true;
	let lastNodeId = '';
	let lastParamsSignature = '';
	let suppressParamSync = false;

	$: isWrappedParams = isObject(params) && ('op' in (params as Record<string, unknown>) || 'json_filter' in (params as Record<string, unknown>));
	$: effectiveParams = (() => {
		if (!isObject(params)) return {};
		const record = params as Record<string, unknown>;
		const nested = record.json_filter;
		if (isObject(nested)) return nested as Record<string, unknown>;
		return record;
	})();
	$: normalized = normalizeFilterParams({ mode: 'rules', rules: (effectiveParams as any)?.rules ?? defaultFilterRules(), expr: '' });
	$: routeRejectNorm = Boolean((effectiveParams as any)?.route_reject ?? true);
	$: includeRejectMetaNorm = Boolean((effectiveParams as any)?.include_reject_meta ?? true);
	$: paramsSignature = JSON.stringify({
		rules: normalized.rules,
		route_reject: routeRejectNorm,
		include_reject_meta: includeRejectMetaNorm
	});
	$: if ((selectedNode?.id ?? '') !== lastNodeId) {
		lastNodeId = selectedNode?.id ?? '';
		rulesDraft = structuredClone(normalized.rules);
		routeReject = routeRejectNorm;
		includeRejectMeta = includeRejectMetaNorm;
		lastParamsSignature = paramsSignature;
	}
	$: if (!suppressParamSync && (selectedNode?.id ?? '') === lastNodeId && paramsSignature !== lastParamsSignature) {
		rulesDraft = structuredClone(normalized.rules);
		routeReject = routeRejectNorm;
		includeRejectMeta = includeRejectMetaNorm;
		lastParamsSignature = paramsSignature;
	}

	$: schemaTypeByName = buildSchemaTypeMap(
		(inputSchemaColumns?.length ?? 0) > 0 ? inputSchemaColumns : (inputSchemas ?? []).flatMap((schema) => schema.columns ?? [])
	);
	$: columnNames = uniqueStrings(
		[...inputColumns, ...Array.from(schemaTypeByName.keys())].map((c) => String(c).trim()).filter(Boolean)
	).sort((a, b) => a.localeCompare(b));
	$: columns = columnNames.map((name) => ({ name, type: schemaTypeByName.get(name) ?? 'unknown' }));

	function isObject(v: unknown): v is Record<string, unknown> {
		return Boolean(v) && typeof v === 'object' && !Array.isArray(v);
	}

	function buildSchemaTypeMap(columns: Array<{ name: string; type?: string }>): Map<string, string> {
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

	function toJsonFilterRuleNode(node: any): Record<string, unknown> {
		if (node.kind === 'group') {
			return {
				kind: 'group',
				op: node.op,
				conditions: (node.conditions ?? []).map((entry: any) => toJsonFilterRuleNode(entry))
			};
		}
		const path = String(node.column ?? '').trim();
		const out: Record<string, unknown> = {
			kind: 'condition',
			path,
			op: node.op
		};
		const needsValue = !['exists', 'is_null'].includes(String(node.op ?? ''));
		if (needsValue) {
			if ((node.valueSource ?? 'literal') === 'param_config') {
				out.value = {
					valueFrom: {
						handle: 'param_config',
						path: String(node.paramPath ?? '').trim()
					}
				};
			} else {
				const rawLiteral = String(node.literalValue ?? '').trim();
				if ((node.op === 'in' || node.op === 'between') && rawLiteral.length > 0) {
					out.value = rawLiteral.split(',').map((item) => item.trim()).filter((item) => item.length > 0);
				} else {
					out.value = rawLiteral;
				}
			}
		}
		return out;
	}

	function emitPatch(commit = false): void {
		suppressParamSync = true;
		const payload: Partial<TransformJsonFilterParams> = {
			mode: 'rules',
			rules: toJsonFilterRuleNode(rulesDraft) as TransformJsonFilterParams['rules'],
			route_reject: routeReject,
			include_reject_meta: includeRejectMeta
		};
		if (isWrappedParams) {
			const wrapped = { op: 'json_filter', json_filter: payload } as unknown as Partial<TransformJsonFilterParams>;
			onDraft(wrapped);
			if (commit) onCommit(wrapped);
		} else {
			onDraft(payload);
			if (commit) onCommit(payload);
		}
		queueMicrotask(() => {
			suppressParamSync = false;
		});
	}
</script>

<Section title="JSON Filter">
	<div class="hint">Filters JSON objects directly and routes rejects to <code>out_reject</code>.</div>
	<FilterRulesBuilder group={rulesDraft} columns={columns} onChange={(next) => { rulesDraft = next; emitPatch(false); }} />
	<div class="toggles">
		<label><input type="checkbox" checked={routeReject} on:change={(e) => { routeReject = (e.currentTarget as HTMLInputElement).checked; emitPatch(true); }} /> route rejects to out_reject</label>
		<label><input type="checkbox" checked={includeRejectMeta} on:change={(e) => { includeRejectMeta = (e.currentTarget as HTMLInputElement).checked; emitPatch(true); }} /> include reject metadata</label>
	</div>
</Section>

<style>
	.hint {
		font-size: 12px;
		opacity: 0.8;
	}
	.toggles {
		display: grid;
		gap: 6px;
		margin-top: 8px;
		font-size: 12px;
	}
</style>
