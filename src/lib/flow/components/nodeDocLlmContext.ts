import type { Edge } from '@xyflow/svelte';
import type { GraphState } from '$lib/flow/store/graphStore';
import type { PipelineEdgeData, PipelineNodeData } from '$lib/flow/types';

export type NodeDocLlmContext = {
	node_id: string;
	node_label: string;
	node_kind: string;
	node_subtype: string;
	settings: Record<string, string>;
	planes: {
		data_inputs: string[];
		data_outputs: string[];
		param_inputs: string[];
		control_inputs: string[];
	};
	runtime: {
		pending_input_count: number;
		inflight: number;
		ready_work: boolean;
		blocked_reason_code: string;
	};
};

function normalized(value: unknown): string {
	return String(value ?? '').trim();
}

function truncate(value: string, limit = 240): string {
	const next = normalized(value);
	if (next.length <= limit) return next;
	return `${next.slice(0, Math.max(0, limit - 3))}...`;
}

function addSetting(target: Record<string, string>, key: string, value: unknown): void {
	const next = truncate(normalized(value));
	if (!next) return;
	target[key] = next;
}

function getNodeSubtype(data: PipelineNodeData): string {
	const kind = normalized((data as any)?.kind).toLowerCase();
	if (kind === 'source') return normalized((data as any)?.sourceKind).toLowerCase();
	if (kind === 'transform') return normalized((data as any)?.transformKind).toLowerCase();
	if (kind === 'model' || kind === 'llm') return normalized((data as any)?.llmKind || (data as any)?.modelKind).toLowerCase();
	if (kind === 'tool') return normalized((data as any)?.toolKind || (data as any)?.params?.provider).toLowerCase();
	if (kind === 'component') return normalized((data as any)?.componentKind).toLowerCase();
	return '';
}

function edgeMode(edge: Edge<PipelineEdgeData>): 'work' | 'param' | 'control' {
	const mode = normalized((edge as any)?.data?.mode).toLowerCase();
	if (mode === 'param' || mode === 'control') return mode;
	return 'work';
}

function collectPlaneHandles(state: GraphState, nodeId: string): NodeDocLlmContext['planes'] {
	const data_inputs: string[] = [];
	const data_outputs: string[] = [];
	const param_inputs: string[] = [];
	const control_inputs: string[] = [];
	for (const edge of state.edges ?? []) {
		const source = normalized((edge as any)?.source);
		const target = normalized((edge as any)?.target);
		const mode = edgeMode(edge as any);
		if (target === nodeId) {
			const handle = normalized((edge as any)?.targetHandle) || 'in';
			if (mode === 'work') data_inputs.push(handle);
			if (mode === 'param') param_inputs.push(handle);
			if (mode === 'control') control_inputs.push(handle);
		}
		if (source === nodeId) {
			const handle = normalized((edge as any)?.sourceHandle) || 'out';
			if (mode === 'work') data_outputs.push(handle);
		}
	}
	return {
		data_inputs: Array.from(new Set(data_inputs)).sort(),
		data_outputs: Array.from(new Set(data_outputs)).sort(),
		param_inputs: Array.from(new Set(param_inputs)).sort(),
		control_inputs: Array.from(new Set(control_inputs)).sort()
	};
}

function buildSettingsSummary(data: PipelineNodeData): Record<string, string> {
	const settings: Record<string, string> = {};
	const kind = normalized((data as any)?.kind).toLowerCase();
	const params = ((data as any)?.params ?? {}) as Record<string, unknown>;
	if (kind === 'source') {
		const sourceKind = normalized((data as any)?.sourceKind).toLowerCase();
		addSetting(settings, 'source_kind', sourceKind || 'unknown');
		if (sourceKind === 'file') {
			addSetting(
				settings,
				'file_name',
				(params as any)?.file?.name ?? (params as any)?.file_name ?? (params as any)?.snapshot?.name
			);
		} else if (sourceKind === 'api') {
			addSetting(settings, 'api_url', (params as any)?.url ?? (params as any)?.api?.url);
			addSetting(settings, 'api_method', (params as any)?.method ?? (params as any)?.api?.method ?? 'GET');
		} else if (sourceKind === 'database') {
			addSetting(settings, 'database_table', (params as any)?.table ?? (params as any)?.db?.table);
			addSetting(settings, 'database_query', (params as any)?.query ?? (params as any)?.db?.query);
		}
	}
	if (kind === 'transform') {
		addSetting(settings, 'transform_kind', (data as any)?.transformKind);
		addSetting(settings, 'operation', (params as any)?.op ?? (params as any)?.operation);
	}
	if (kind === 'model' || kind === 'llm') {
		addSetting(settings, 'provider', (data as any)?.llmKind ?? (params as any)?.provider);
		addSetting(settings, 'model', (params as any)?.model);
		addSetting(settings, 'output_mode', (params as any)?.output?.mode ?? (params as any)?.output_mode);
	}
	if (kind === 'tool') {
		addSetting(settings, 'provider', (params as any)?.provider ?? (data as any)?.toolKind);
		addSetting(settings, 'tool_id', (params as any)?.toolId ?? (params as any)?.name);
	}
	if (kind === 'component') {
		addSetting(settings, 'component_id', (params as any)?.componentRef?.componentId);
		addSetting(settings, 'revision_id', (params as any)?.componentRef?.revisionId);
	}
	return settings;
}

function runtimeSummary(state: GraphState, nodeId: string): NodeDocLlmContext['runtime'] {
	const rows = Array.isArray((state as any)?.queueRuntime?.schedulerSnapshot?.perNode)
		? ((state as any).queueRuntime.schedulerSnapshot.perNode as Array<Record<string, unknown>>)
		: [];
	const row = rows.find((entry) => normalized(entry?.nodeId) === nodeId) ?? {};
	return {
		pending_input_count: Math.max(0, Number((row as any)?.pendingInputCount ?? 0)),
		inflight: Math.max(0, Number((row as any)?.inflight ?? 0)),
		ready_work: Boolean((row as any)?.readyWork ?? false),
		blocked_reason_code: normalized((row as any)?.lastBlockedReasonCode)
	};
}

export function buildNodeDocLlmContext(state: GraphState, nodeIdRaw: string): NodeDocLlmContext | null {
	const nodeId = normalized(nodeIdRaw);
	if (!nodeId) return null;
	const node = (state.nodes ?? []).find((entry) => normalized(entry?.id) === nodeId);
	if (!node?.data) return null;
	const data = node.data as PipelineNodeData;
	return {
		node_id: nodeId,
		node_label: normalized((data as any)?.label) || 'Node',
		node_kind: normalized((data as any)?.kind).toLowerCase() || 'node',
		node_subtype: getNodeSubtype(data),
		settings: buildSettingsSummary(data),
		planes: collectPlaneHandles(state, nodeId),
		runtime: runtimeSummary(state, nodeId)
	};
}

export function buildNodeDocLlmContextSignature(context: NodeDocLlmContext | null): string {
	if (!context) return 'missing-context';
	return JSON.stringify([
		context.node_kind,
		context.node_subtype,
		context.settings,
		context.planes,
		context.runtime
	]);
}

