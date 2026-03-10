// src/lib/flow/store/graphStore.ts
import { writable, get, derived } from 'svelte/store';
import type { Node, Edge } from '@xyflow/svelte';

import type {
	NodeStatus,
	NodeKind,
	PipelineNodeData,
	PipelineEdgeData,
	PipelineGraphDTO,
	PortType
} from '$lib/flow/types';
import { isPortType } from '$lib/flow/types/base';
import { defaultSourceParamsByKind } from '$lib/flow/schema/sourceDefaults';
import { defaultLlmParamsByKind } from '$lib/flow/schema/llmDefaults';
import { defaultTransformParamsByKind } from '$lib/flow/schema/transformDefaults';
import { defaultToolParamsByProvider, type ToolProvider } from '$lib/flow/schema/toolDefaults';
import { evaluateSchemaCoercion } from '$lib/flow/schema/coercionPolicy';
import type { SchemaDiagnosticCode } from '$lib/flow/schema/diagnosticsContract';
import { TOOL_BUILTIN_PROFILE_IDS } from '$lib/flow/schema/toolBuiltinProfiles';
import { validateCustomPackageDraft } from '$lib/flow/schema/toolBuiltinCustomPackages';
import { defaultNodeData } from '$lib/flow/schema/defaults';
import { updateNodeParamsValidated } from './graph';
import { saveGraphToLocalStorage, loadGraphFromLocalStorage, emptyGraph, clearGraphDraft } from './persist';
import {
	getLatestGraphRevision,
	getGraphRevision,
	listGraphRevisions,
	createGraphRevision,
	listGraphs as listGraphsClient,
	deleteGraph as deleteGraphClient,
	deleteGraphRevision as deleteGraphRevisionClient
} from '$lib/flow/client/graphs';
import {
	getComponentRevision,
	listComponentRevisions,
	listComponents,
	createComponentRevision,
	renameComponent,
	deleteComponent,
	deleteComponentRevision,
	type ComponentApiContract
} from '$lib/flow/client/components';
import {
	acceptNodeParams,
	createEventBatcher,
	createRun,
	getRun,
	resolveSourceNode,
	streamRunEvents
} from '$lib/flow/client/runs';
import type { KnownRunEvent } from '$lib/flow/types/run';
import type { SourceKind, LlmKind, TransformKind } from '$lib/flow/types/paramsMap';
import { getAllowedPortsForNode, getStrictSchemaFeatureFlags } from '$lib/flow/portCapabilities';
import {
	buildRunCreateRequest,
	computeGraphFreshness,
	computePlannedNodeSet,
	displayStatusFromBinding,
	getStaleFlipNodeIds,
	isBindingStale,
	mergeBindingsSticky,
	type ActiveRunMode,
	type GraphFreshness as ScopeFreshness
} from './runScope';

type NodeOutputInfo = {
	mimeType?: string;
	portType?: string;
	preview?: string;
	cached?: boolean;
	cacheDecision?: 'cache_hit' | 'cache_miss' | 'cache_hit_contract_mismatch';
	expectedContractFingerprint?: string;
	actualContractFingerprint?: string;
	mismatchKind?: string;
	lastError?: NodeExecutionError | null;
};

export type NodeExecutionError = {
	message?: string;
	errorCode?: string;
	op?: string;
	paramPath?: string;
	missingColumns?: string[];
	availableColumns?: string[];
	availableColumnsSource?: 'schema' | 'inferred' | string;
};
type NodeBindingInfo = {
	status?: string;
	current?: { execKey?: string | null; artifactId?: string | null } | null;
	last?: { execKey?: string | null; artifactId?: string | null } | null;
	lastArtifactId?: string | null; // legacy
	lastRunId?: string | null;
	lastExecKey?: string | null; // legacy
	currentExecKey?: string | null; // legacy
	currentArtifactId?: string | null; // legacy
	currentRunId?: string | null;
	isUpToDate?: boolean;
	cacheValid?: boolean;
	staleReason?: string | null;
};
type BindingPair = { execKey: string | null; artifactId: string | null };
export type NormalizedNodeBinding = NodeBindingInfo & {
	status: string;
	isUpToDate: boolean;
	cacheValid: boolean;
	currentRunId: string | null;
	staleReason: string | null;
	current: BindingPair;
	last: BindingPair;
};
type EdgeExec = 'idle' | 'active' | 'done';
type LogLevel = 'info' | 'warn' | 'error';
type RunLog = {
	id: number; // âœ… ADD
	ts: string;
	level: LogLevel;
	message: string;
	nodeId?: string;
	componentPath?: string[];
};
const RUN_IDLE = "idle"
type RunStatus = typeof RUN_IDLE | 'running' | 'succeeded' | 'failed' | 'canceled' | 'cancelled';
type GraphLastRunStatus = 'succeeded' | 'failed' | 'cancelled' | 'never_run';
type AuditContext = {
	source: 'event' | 'accept_params' | 'hydrate_snapshot' | 'graph_edit' | 'unknown';
	evt?: KnownRunEvent;
	allowedNodeIds?: Set<string>;
	snapshotNodeIds?: Set<string>;
	expectedDirtyTransition?: boolean;
};
type InspectorState = {
	nodeId: string | null;
	draftParams: Record<string, any>;
	dirty: boolean;
	systemNotice?: string | null;
	uiByNodeId: Record<string, ApiEditorUiState>;
};

export type InspectorDraftPatchIntent = 'user_edit' | 'system_canonicalize';

export type ApiEditorUiState = {
	requestOpen: boolean;
	authOpen: boolean;
	transportOpen: boolean;
	executionOpen: boolean;
	debugOpen: boolean;
	queryOpen: boolean;
	headersOpen: boolean;
	bodyOpen: boolean;
};

type SavePreflightSeverity = 'error' | 'warning';
export type SavePreflightDiagnostic = {
	code: string;
	path: string;
	message: string;
	severity: SavePreflightSeverity;
};
export type SavePreflightResult = {
	ok: boolean;
	diagnostics: SavePreflightDiagnostic[];
};

export type EditorContext = 'graph' | 'component';

export type ComponentEditSessionSnapshot = {
	graphId: string;
	nodes: Node<PipelineNodeData & Record<string, unknown>>[];
	edges: Edge<PipelineEdgeData & Record<string, unknown>>[];
	selectedNodeId: string | null;
	inspector: InspectorState;
	logs: RunLog[];
	runStatus: RunStatus;
	lastRunStatus: GraphLastRunStatus;
	freshness: ScopeFreshness;
	staleNodeCount: number;
	activeRunMode: ActiveRunMode;
	activeRunFrom: string | null;
	activeRunNodeSet: Set<string>;
	nodeOutputs: Record<string, NodeOutputInfo>;
	nodeBindings: Record<string, NormalizedNodeBinding>;
	activeRunId: string | null;
};

export type ComponentEditSession = {
	componentId: string;
	revisionId: string;
	entryNodeId: string | null;
	snapshot: ComponentEditSessionSnapshot;
};

const IDLE: NodeStatus = 'idle';
const SUCCEEDED: NodeStatus = 'succeeded';
const allowedPorts = new Set(['table', 'text', 'json', 'binary', 'embeddings']);
const allowedBuiltinProfileIds = new Set<string>(TOOL_BUILTIN_PROFILE_IDS);
const initialInspector: InspectorState = {
	nodeId: null,
	draftParams: {},
	dirty: false,
	systemNotice: null,
	uiByNodeId: {}
};

function hasAnyKeys(value: unknown): boolean {
	return Boolean(value && typeof value === 'object' && Object.keys(value as Record<string, unknown>).length > 0);
}

function defaultApiEditorUiState(params?: Record<string, any>): ApiEditorUiState {
	const authType = String(params?.auth_type ?? 'none');
	const bodyMode = String(params?.bodyMode ?? params?.body_mode ?? 'none');
	return {
		requestOpen: true,
		authOpen: authType !== 'none',
		transportOpen: false,
		executionOpen: false,
		debugOpen: false,
		queryOpen: hasAnyKeys(params?.query),
		headersOpen: hasAnyKeys(params?.headers),
		bodyOpen: bodyMode !== 'none'
	};
}

function normalizeComponentPortType(value: unknown): PortType | null {
	const t = String(value ?? '').trim().toLowerCase();
	if (t === 'table' || t === 'text' || t === 'json' || t === 'binary' || t === 'embeddings') {
		return t as PortType;
	}
	return null;
}

function derivePortsFromComponentApi(api: unknown): { in?: PortType | null; out?: PortType | null } {
	const contract = (api ?? {}) as ComponentApiContract;
	const inputs = Array.isArray(contract?.inputs) ? contract.inputs : [];
	const inPort = normalizeComponentPortType(inputs[0]?.portType ?? null);
	// Component output routing/type comes from API outputs + sourceHandle, not ports.out.
	return { in: inPort, out: null };
}

function sanitizeComponentDraftParams(params: Record<string, any>): Record<string, any> {
	const api = params?.api;
	const bindings = params?.bindings;
	if (!api || typeof api !== 'object' || !bindings || typeof bindings !== 'object') {
		return params;
	}
	const outputs = Array.isArray((api as any).outputs) ? ((api as any).outputs as any[]) : [];
	const validOutputNames = new Set(
		outputs
			.map((out) => String((out as any)?.name ?? '').trim())
			.filter((name) => name.length > 0)
	);
	const currentOutputs =
		bindings && typeof (bindings as any).outputs === 'object' && (bindings as any).outputs
			? ((bindings as any).outputs as Record<string, any>)
			: {};
	const nextOutputs: Record<string, any> = {};
	for (const name of validOutputNames) {
		if (Object.prototype.hasOwnProperty.call(currentOutputs, name)) {
			nextOutputs[name] = currentOutputs[name];
		}
	}
	return {
		...params,
		bindings: {
			...(bindings as Record<string, any>),
			outputs: nextOutputs
		}
	};
}

function validateComponentDraftForAccept(params: Record<string, any>): { ok: true } | { ok: false; errors: string[] } {
	const api = params?.api;
	const bindings = params?.bindings;
	const outputs = Array.isArray(api?.outputs) ? (api.outputs as any[]) : [];
	const outputBindings =
		bindings && typeof bindings.outputs === 'object' && bindings.outputs
			? (bindings.outputs as Record<string, any>)
			: {};
	const errors: string[] = [];
	const seenOutputNames = new Set<string>();
	for (const output of outputs) {
		const outputName = String(output?.name ?? '').trim();
		if (!outputName) {
			errors.push('Component output name is required before Accept.');
			continue;
		}
		const outputNameKey = outputName.toLowerCase();
		if (seenOutputNames.has(outputNameKey)) {
			errors.push(`Component output "${outputName}" duplicates another declared output.`);
			continue;
		}
		seenOutputNames.add(outputNameKey);
		const binding = outputBindings[outputName];
		const boundNodeId = String(binding?.nodeId ?? '').trim();
		if (!boundNodeId) {
			errors.push(`Component output "${outputName}" requires a bound internal node before Accept.`);
		}
		const artifactMode = String(binding?.artifact ?? 'current').trim();
		if (artifactMode !== 'current' && artifactMode !== 'last') {
			errors.push(
				`Component output "${outputName}" must set artifact mode to "current" or "last" before Accept.`
			);
		}
		const portType = normalizeComponentPortType(output?.portType);
		if (portType == null) {
			errors.push(`Component output "${outputName}" has no derivable output type.`);
		}
		const typedSchemaType = normalizeComponentPortType(output?.typedSchema?.type);
		if (typedSchemaType == null || typedSchemaType !== portType) {
			errors.push(`Component output "${outputName}" must keep typedSchema.type aligned with portType.`);
		}
	}
	if (errors.length > 0) return { ok: false, errors };
	return { ok: true };
}

type InspectorDraftAcceptValidation =
	| { ok: true; errors: [] }
	| { ok: false; errors: string[] };

function validateInspectorDraftForAccept(state: GraphState): InspectorDraftAcceptValidation {
	const nodeId = state.inspector.nodeId;
	if (!nodeId) return { ok: false, errors: ['No node selected.'] };
	const node = state.nodes.find((n) => n.id === nodeId);
	if (!node) return { ok: false, errors: ['Selected node no longer exists.'] };
	if (node.data.kind !== 'component') return { ok: true, errors: [] };
	const paramsForCommit = sanitizeComponentDraftParams(
		(state.inspector.draftParams ?? {}) as Record<string, any>
	);
	const validation = validateComponentDraftForAccept(paramsForCommit);
	if (!validation.ok) return { ok: false, errors: validation.errors };
	return { ok: true, errors: [] };
}

function listComponentOutputNames(node: Node<PipelineNodeData>): string[] {
	if (node.data.kind !== 'component') return [];
	const outputs = Array.isArray((node.data as any)?.params?.api?.outputs)
		? ((node.data as any).params.api.outputs as any[])
		: [];
	return outputs
		.map((o) => String((o as any)?.name ?? '').trim())
		.filter((name): name is string => name.length > 0);
}

function canonicalComponentSourceHandleForEdge(
	nodes: Node<PipelineNodeData>[],
	edge: Edge<PipelineEdgeData>
): string | null {
	const sourceNode = nodes.find((n) => n.id === edge.source);
	if (!sourceNode || sourceNode.data.kind !== 'component') {
		return normalizeHandleId((edge as any).sourceHandle, 'out');
	}
	const outputNames = listComponentOutputNames(sourceNode);
	if (outputNames.length === 0) return null;
	const raw = String((edge as any).sourceHandle ?? '').trim();
	if (!raw || raw === 'out') {
		return outputNames.length === 1 ? outputNames[0] : null;
	}
	return outputNames.includes(raw) ? raw : null;
}

function dedupeEdgesBySignature(
	edges: Edge<PipelineEdgeData>[]
): { edges: Edge<PipelineEdgeData>[]; removedIds: string[] } {
	const seen = new Set<string>();
	const next: Edge<PipelineEdgeData>[] = [];
	const removedIds: string[] = [];
	for (const edge of edges) {
		const key = [
			String(edge.source ?? ''),
			String((edge as any).sourceHandle ?? 'out'),
			String(edge.target ?? ''),
			String((edge as any).targetHandle ?? 'in')
		].join('|');
		if (seen.has(key)) {
			removedIds.push(String(edge.id ?? ''));
			continue;
		}
		seen.add(key);
		next.push(edge);
	}
	return { edges: next, removedIds };
}

function reconcileComponentOutgoingEdges(
	nodeId: string,
	nextNode: Node<PipelineNodeData>,
	edges: Edge<PipelineEdgeData>[],
	previousOutputNames: string[]
): { edges: Edge<PipelineEdgeData>[]; removedIds: string[] } {
	if (nextNode.data.kind !== 'component') return { edges, removedIds: [] };
	const nextOutputNames = listComponentOutputNames(nextNode);
	const nextSet = new Set(nextOutputNames);
	const renameMap = new Map<string, string>();
	for (let i = 0; i < Math.min(previousOutputNames.length, nextOutputNames.length); i += 1) {
		const prevName = String(previousOutputNames[i] ?? '').trim();
		const nextName = String(nextOutputNames[i] ?? '').trim();
		if (!prevName || !nextName || prevName === nextName) continue;
		if (nextSet.has(prevName)) continue;
		renameMap.set(prevName, nextName);
	}

	const rewritten = edges
		.map((edge) => {
			if (edge.source !== nodeId) return edge;
			const rawHandle = String((edge as any).sourceHandle ?? '').trim();
			if (!rawHandle || rawHandle === 'out') {
				if (nextOutputNames.length === 1) {
					return { ...edge, sourceHandle: nextOutputNames[0] };
				}
				return edge;
			}
			if (nextSet.has(rawHandle)) return edge;
			const mapped = renameMap.get(rawHandle);
			if (mapped && nextSet.has(mapped)) {
				return { ...edge, sourceHandle: mapped };
			}
			if (nextOutputNames.length === 1) {
				return { ...edge, sourceHandle: nextOutputNames[0] };
			}
			return null;
		})
		.filter((edge): edge is Edge<PipelineEdgeData> => Boolean(edge));

	const removedIds = edges
		.filter((edge) => edge.source === nodeId)
		.filter((edge) => !rewritten.some((candidate) => candidate.id === edge.id))
		.map((edge) => String(edge.id ?? ''));

	const deduped = dedupeEdgesBySignature(rewritten);
	return { edges: deduped.edges, removedIds: [...removedIds, ...deduped.removedIds] };
}

let logSeq = 0;
const statusRegressionLogThrottle = new Map<string, number>();
const debugLastStatusChange = new Map<
	string,
	{
		ts: string;
		eventType: string;
		stack: string;
		prevDisplay: NodeStatus;
		nextDisplay: NodeStatus;
		prevNodeStatus?: NodeStatus;
		nextNodeStatus?: NodeStatus;
	}
>();
const DEV_MODE = (() => {
	try {
		return Boolean((import.meta as any)?.env?.DEV);
	} catch {
		return false;
	}
})();

function _pairFromLegacy(binding: NodeBindingInfo | undefined, which: 'current' | 'last') {
	const b = binding ?? {};
	if (which === 'current') {
		const hasStructured = Boolean(b.current && typeof b.current === 'object');
		let execKey = (b.current?.execKey ?? b.currentExecKey) ?? null;
		let artifactId = (b.current?.artifactId ?? b.currentArtifactId) ?? null;
		return {
			execKey: hasStructured ? execKey : (execKey ?? artifactId),
			artifactId: hasStructured ? artifactId : (artifactId ?? execKey)
		};
	}
	const hasStructured = Boolean(b.last && typeof b.last === 'object');
	let execKey = (b.last?.execKey ?? b.lastExecKey) ?? null;
	let artifactId = (b.last?.artifactId ?? b.lastArtifactId) ?? null;
	return {
		execKey: hasStructured ? execKey : (execKey ?? artifactId),
		artifactId: hasStructured ? artifactId : (artifactId ?? execKey)
	};
}

function _withPair(binding: NormalizedNodeBinding, which: 'current' | 'last', pair: BindingPair): NormalizedNodeBinding {
	const next: NormalizedNodeBinding = { ...binding };
	if (which === 'current') {
		next.current = {
			execKey: pair.execKey,
			artifactId: pair.artifactId
		};
		next.currentExecKey = pair.execKey;
		next.currentArtifactId = pair.artifactId;
	} else {
		next.last = {
			execKey: pair.execKey,
			artifactId: pair.artifactId
		};
		next.lastExecKey = pair.execKey;
		next.lastArtifactId = pair.artifactId;
	}
	return next;
}

function _assertBindingPairInvariant(
	binding: NodeBindingInfo | undefined,
	nodeId: string,
	context: string,
	force = false
): void {
	if ((!DEV_MODE && !force) || !binding) return;
	for (const which of ['current', 'last'] as const) {
		const pair = _pairFromLegacy(binding, which);
		const hasExec = Boolean(pair.execKey);
		const hasArt = Boolean(pair.artifactId);
		if (hasExec !== hasArt) {
			throw new Error(`[graphStore] INVALID_BINDING_PAIR ${context} node=${nodeId} pair=${which}`);
		}
	}
}

export function __assertBindingPairForTest(binding: NodeBindingInfo, nodeId = 'test', context = 'test'): void {
	_assertBindingPairInvariant(binding, nodeId, context, true);
}

function _normalizeBinding(binding: NodeBindingInfo | undefined, nodeId?: string): NormalizedNodeBinding {
	const b = { ...(binding ?? {}) };
	const hasCurrentFields =
		Object.prototype.hasOwnProperty.call(b, 'current') ||
		Object.prototype.hasOwnProperty.call(b, 'currentExecKey') ||
		Object.prototype.hasOwnProperty.call(b, 'currentArtifactId');
	const hasLastFields =
		Object.prototype.hasOwnProperty.call(b, 'last') ||
		Object.prototype.hasOwnProperty.call(b, 'lastExecKey') ||
		Object.prototype.hasOwnProperty.call(b, 'lastArtifactId');
	if (hasCurrentFields) b.current = _pairFromLegacy(b, 'current');
	if (hasLastFields) b.last = _pairFromLegacy(b, 'last');
	const current = (b.current as BindingPair | undefined) ?? { execKey: null, artifactId: null };
	const last = (b.last as BindingPair | undefined) ?? { execKey: null, artifactId: null };
	const normalized: NormalizedNodeBinding = {
		...b,
		status: String(b.status ?? 'idle'),
		isUpToDate: Boolean(b.isUpToDate ?? false),
		cacheValid: Boolean(b.cacheValid ?? false),
		currentRunId: (b.currentRunId ?? null) as string | null,
		staleReason: (b.staleReason ?? null) as string | null,
		current: {
			execKey: current.execKey ?? null,
			artifactId: current.artifactId ?? null
		},
		last: {
			execKey: last.execKey ?? null,
			artifactId: last.artifactId ?? null
		}
	};
	normalized.currentExecKey = normalized.current.execKey;
	normalized.currentArtifactId = normalized.current.artifactId;
	normalized.lastExecKey = normalized.last.execKey;
	normalized.lastArtifactId = normalized.last.artifactId;
	if (nodeId) _assertBindingPairInvariant(normalized, nodeId, 'normalize');
	return normalized;
}

export function __normalizeBindingForTest(
	binding: NodeBindingInfo | undefined,
	nodeId = 'test'
): NormalizedNodeBinding {
	return _normalizeBinding(binding, nodeId);
}

export type GraphState = {
	graphId: string;
	nodes: Node<PipelineNodeData & Record<string, unknown>>[];
	edges: Edge<PipelineEdgeData & Record<string, unknown>>[];
	selectedNodeId: string | null;
	inspector: InspectorState; // âœ… add this
	logs: RunLog[];
	runStatus: RunStatus;
	lastRunStatus: GraphLastRunStatus;
	freshness: ScopeFreshness;
	staleNodeCount: number;
	activeRunMode: ActiveRunMode;
	activeRunFrom: string | null;
	activeRunNodeSet: Set<string>;
	nodeOutputs: Record<string, NodeOutputInfo>;
	nodeBindings: Record<string, NormalizedNodeBinding>;
	activeRunId: string | null;
	editingContext: EditorContext;
	componentEditSession: ComponentEditSession | null;
};

export type InputResolution = {
	inPort: string;
	edge: { fromNodeId: string; fromPort: string } | null;
	status: 'resolved' | 'missing';
	reason?: 'DISCONNECTED' | 'UPSTREAM_NO_ARTIFACT' | 'UPSTREAM_FAILED' | 'UNKNOWN';
	artifactId?: string;
	artifactSource?: 'active_run' | 'bound';
	upstream: {
		nodeId: string;
		port: string;
		status?: string;
		isUpToDate?: boolean;
		staleReason?: string | null;
	};
	artifactSummary?: {
		mimeType?: string;
		schemaFingerprint?: string;
		contract?: string;
	};
};

function normalizeHandleId(handleId: string | null | undefined, fallback: 'in' | 'out'): string {
	const v = String(handleId ?? '').trim();
	return v ? v : fallback;
}

function isFailedBindingStatus(binding: NormalizedNodeBinding | undefined): boolean {
	const display = displayStatusFromBinding(binding as any);
	const raw = String(binding?.status ?? '').toLowerCase();
	return display === 'failed' || raw.startsWith('failed');
}

function resolveUpstreamArtifact(
	state: GraphState,
	upstreamBinding: NormalizedNodeBinding | undefined
): { artifactId?: string; artifactSource?: 'active_run' | 'bound' } {
	const currentArtifactId =
		upstreamBinding?.current?.artifactId ?? upstreamBinding?.currentArtifactId ?? null;
	const lastArtifactId = upstreamBinding?.last?.artifactId ?? upstreamBinding?.lastArtifactId ?? null;
	const activeRunId = state.activeRunId;
	if (
		activeRunId &&
		upstreamBinding?.currentRunId === activeRunId &&
		typeof currentArtifactId === 'string' &&
		currentArtifactId.length > 0
	) {
		return { artifactId: currentArtifactId, artifactSource: 'active_run' };
	}
	if (typeof currentArtifactId === 'string' && currentArtifactId.length > 0) {
		return { artifactId: currentArtifactId, artifactSource: 'bound' };
	}
	if (typeof lastArtifactId === 'string' && lastArtifactId.length > 0) {
		return { artifactId: lastArtifactId, artifactSource: 'bound' };
	}
	return {};
}

export function resolveNodeInputsFromState(state: GraphState, nodeId: string): InputResolution[] {
	const node = state.nodes.find((n) => n.id === nodeId);
	if (!node) return [];
	const inPortType = node.data?.ports?.in ?? null;
	const hasInputPort = inPortType !== null && inPortType !== undefined;
	const incoming = (state.edges ?? [])
		.filter((e) => e.target === nodeId)
		.slice()
		.sort((a, b) => String(a.id ?? '').localeCompare(String(b.id ?? '')));
	const inPorts = new Set<string>();
	if (hasInputPort) inPorts.add('in');
	for (const e of incoming) inPorts.add(normalizeHandleId((e as any).targetHandle, 'in'));
	const orderedInPorts = Array.from(inPorts).sort((a, b) => a.localeCompare(b));
	const resolutions: InputResolution[] = [];
	for (const inPort of orderedInPorts) {
		const edge = incoming.find((e) => normalizeHandleId((e as any).targetHandle, 'in') === inPort) ?? null;
		if (!edge) {
			resolutions.push({
				inPort,
				edge: null,
				status: 'missing',
				reason: 'DISCONNECTED',
				upstream: { nodeId: '', port: '' }
			});
			continue;
		}
		const fromNodeId = String(edge.source ?? '');
		const fromPort = normalizeHandleId((edge as any).sourceHandle, 'out');
		const upstreamBinding = state.nodeBindings?.[fromNodeId];
		const upstreamOut = state.nodeOutputs?.[fromNodeId];
		const resolved = resolveUpstreamArtifact(state, upstreamBinding);
		if (resolved.artifactId) {
			resolutions.push({
				inPort,
				edge: { fromNodeId, fromPort },
				status: 'resolved',
				artifactId: resolved.artifactId,
				artifactSource: resolved.artifactSource,
				upstream: {
					nodeId: fromNodeId,
					port: fromPort,
					status: displayStatusFromBinding(upstreamBinding as any),
					isUpToDate: upstreamBinding?.isUpToDate,
					staleReason: upstreamBinding?.staleReason ?? null
				},
				artifactSummary: {
					mimeType: upstreamOut?.mimeType,
					schemaFingerprint: upstreamOut?.actualContractFingerprint,
					contract: upstreamOut?.portType
				}
			});
			continue;
		}
		resolutions.push({
			inPort,
			edge: { fromNodeId, fromPort },
			status: 'missing',
			reason: isFailedBindingStatus(upstreamBinding) ? 'UPSTREAM_FAILED' : 'UPSTREAM_NO_ARTIFACT',
			upstream: {
				nodeId: fromNodeId,
				port: fromPort,
				status: displayStatusFromBinding(upstreamBinding as any),
				isUpToDate: upstreamBinding?.isUpToDate,
				staleReason: upstreamBinding?.staleReason ?? null
			}
		});
	}
	return resolutions;
}

function ensureNormalizedBindingsForNodes(
	nodes: Node<PipelineNodeData & Record<string, unknown>>[],
	nodeBindings: Record<string, NormalizedNodeBinding>
): Record<string, NormalizedNodeBinding> {
	const liveNodeIds = new Set((nodes ?? []).map((n) => n.id).filter(Boolean));
	const next: Record<string, NormalizedNodeBinding> = {};
	for (const [nodeId, binding] of Object.entries(nodeBindings ?? {})) {
		if (!liveNodeIds.has(nodeId)) continue;
		next[nodeId] = binding;
	}
	for (const node of nodes ?? []) {
		if (!node?.id) continue;
		next[node.id] = _normalizeBinding(next[node.id], node.id);
	}
	return next;
}

function pruneNodeOutputsForNodes(
	nodes: Node<PipelineNodeData & Record<string, unknown>>[],
	nodeOutputs: Record<string, NodeOutputInfo>
): Record<string, NodeOutputInfo> {
	const liveNodeIds = new Set((nodes ?? []).map((n) => n.id).filter(Boolean));
	const next: Record<string, NodeOutputInfo> = {};
	for (const [nodeId, output] of Object.entries(nodeOutputs ?? {})) {
		if (!liveNodeIds.has(nodeId)) continue;
		next[nodeId] = output;
	}
	return next;
}

function mintGraphId(): string {
	try {
		return `graph_${crypto.randomUUID()}`;
	} catch {
		return `graph_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 10)}`;
	}
}

function nowTs() {
	return new Date().toLocaleTimeString();
}

function _componentPathFromNodeId(state: GraphState, nodeId?: string): string[] | undefined {
	const raw = String(nodeId ?? '').trim();
	if (!raw.startsWith('cmp:')) return undefined;
	const componentInstanceIds: string[] = [];
	let cursor = raw;
	let guard = 0;
	while (cursor.startsWith('cmp:') && guard < 32) {
		guard += 1;
		const rest = cursor.slice(4);
		const sep = rest.indexOf(':');
		if (sep <= 0) break;
		const instanceId = rest.slice(0, sep).trim();
		if (!instanceId) break;
		componentInstanceIds.push(instanceId);
		cursor = rest.slice(sep + 1);
	}
	if (!componentInstanceIds.length) return undefined;
	const names = componentInstanceIds.map((instanceId) => {
		const node = state.nodes.find((n) => n.id === instanceId);
		const data = (node?.data ?? {}) as Record<string, any>;
		const ref = (data.params as Record<string, any> | undefined)?.componentRef as
			| Record<string, unknown>
			| undefined;
		const componentId = String(ref?.componentId ?? '').trim();
		return componentId || instanceId;
	});
	return names.length ? names : undefined;
}

function formatEnvProfileRunLogMessage(message: string): string {
	const raw = String(message ?? '').trim();
	if (!raw) return raw;
	const envCodePrefix = raw.match(/^(ENV_PROFILE_[A-Z_]+)\s*:\s*(.*)$/);
	if (envCodePrefix) {
		const code = String(envCodePrefix[1] ?? '').trim();
		const rest = String(envCodePrefix[2] ?? '').trim();
		if (code === 'ENV_PROFILE_MISSING' && !/Install profile:/i.test(rest)) {
			return `${code}: ${rest} Install profile: POST /env/profiles/install.`;
		}
		return raw;
	}
	if (!raw.startsWith('{') || !raw.endsWith('}')) return raw;
	try {
		const parsed = JSON.parse(raw) as Record<string, unknown>;
		const code = String(parsed?.errorCode ?? parsed?.code ?? '').trim().toUpperCase();
		if (!code.startsWith('ENV_PROFILE_')) return raw;
		const profileId = String(parsed?.profileId ?? '').trim() || 'core';
		const missingPackages = Array.isArray(parsed?.missingPackages)
			? (parsed.missingPackages as unknown[]).map((v) => String(v)).filter((v) => v.trim().length > 0)
			: [];
		const installHint = String(parsed?.installHint ?? '').trim() || 'POST /env/profiles/install';
		if (code === 'ENV_PROFILE_MISSING') {
			const suffix =
				missingPackages.length > 0
					? `missing packages: ${missingPackages.join(', ')}.`
					: 'is not installed.';
			return `${code}: profile '${profileId}' ${suffix} Install profile: ${installHint} (profileId='${profileId}').`;
		}
		if (code === 'ENV_PROFILE_INVALID') {
			return `${code}: profile '${profileId}' is invalid. Update profile selection in the tool editor.`;
		}
		if (code === 'ENV_PROFILE_PACKAGE_BLOCKED') {
			return `${code}: profile '${profileId}' has blocked package entries.`;
		}
		if (code === 'ENV_PROFILE_INSTALL_FAILED') {
			return `${code}: profile '${profileId}' install failed. Retry install via ${installHint} (profileId='${profileId}').`;
		}
		return raw;
	} catch {
		return raw;
	}
}

function logPush(
	state: GraphState,
	level: LogLevel,
	message: string,
	nodeId?: string,
	componentPath?: string[]
) {
	logSeq += 1;
	const resolvedComponentPath = componentPath?.length ? componentPath : _componentPathFromNodeId(state, nodeId);
	const normalizedMessage = formatEnvProfileRunLogMessage(message);
	return {
		...state,
		logs: [
			...state.logs,
			{ id: logSeq, ts: nowTs(), level, message: normalizedMessage, nodeId, componentPath: resolvedComponentPath }
		]
	};
}

function captureStack(label: string): string {
	try {
		return new Error(label).stack ?? '';
	} catch {
		return '';
	}
}

function isAllowedSucceededRegression(nodeId: string, ctx: AuditContext): boolean {
	if (ctx.expectedDirtyTransition && ctx.allowedNodeIds?.has(nodeId)) {
		return true;
	}
	if (ctx.source === 'accept_params') {
		if (ctx.expectedDirtyTransition) return true;
		return Boolean(ctx.allowedNodeIds?.has(nodeId));
	}
	if (ctx.source === 'hydrate_snapshot') return false;
	if (ctx.source === 'graph_edit') return true;
	if (ctx.source !== 'event' || !ctx.evt) return false;
	const evt = ctx.evt;
	if (
		(evt.type === 'node_started' || evt.type === 'node_finished' || evt.type === 'cache_decision') &&
		evt.nodeId === nodeId
	) {
		if (evt.type === 'cache_decision') return evt.decision !== 'cache_hit';
		return true;
	}
	if (evt.type === 'run_started') {
		return Boolean(ctx.allowedNodeIds?.has(nodeId));
	}
	return false;
}

function logSucceededRegression(
	channel: 'binding' | 'node_data',
	nodeId: string,
	prev: GraphState,
	next: GraphState,
	ctx: AuditContext,
	prevDisplay: NodeStatus,
	nextDisplay: NodeStatus,
	prevNodeStatus?: NodeStatus,
	nextNodeStatus?: NodeStatus
): void {
	const eventType = ctx.evt?.type ?? ctx.source;
	const stack = captureStack(`[graphStore] ${channel} succeeded regression ${nodeId}`);
	const payload = {
		channel,
		eventType,
		event: ctx.evt ?? null,
		nodeId,
		prevDisplay,
		nextDisplay,
		prevNodeStatus,
		nextNodeStatus,
		prevBinding: prev.nodeBindings?.[nodeId] ?? null,
		nextBinding: next.nodeBindings?.[nodeId] ?? null,
		prevOutput: prev.nodeOutputs?.[nodeId] ?? null,
		nextOutput: next.nodeOutputs?.[nodeId] ?? null,
		activeRunId: next.activeRunId,
		activeRunMode: next.activeRunMode,
		activeRunFrom: next.activeRunFrom,
		activeRunNodeSetSize: next.activeRunNodeSet?.size ?? 0,
		stack
	};

	const key = `${channel}:${eventType}:${nodeId}`;
	const now = Date.now();
	const last = statusRegressionLogThrottle.get(key) ?? 0;
	if (DEV_MODE || now - last > 2000) {
		console.error('[graphStore] SUCCEEDED_REGRESSION', payload);
		statusRegressionLogThrottle.set(key, now);
	}
	debugLastStatusChange.set(nodeId, {
		ts: new Date().toISOString(),
		eventType,
		stack,
		prevDisplay,
		nextDisplay,
		prevNodeStatus,
		nextNodeStatus
	});
}

function auditSucceededRegressions(prev: GraphState, next: GraphState, ctx: AuditContext): void {
	if (ctx.source === 'hydrate_snapshot') return;
	const ids = new Set([...Object.keys(prev.nodeBindings ?? {}), ...Object.keys(next.nodeBindings ?? {})]);
	for (const nodeId of ids) {
		const prevDisplay = displayStatusFromBinding(prev.nodeBindings?.[nodeId]);
		const nextDisplay = displayStatusFromBinding(next.nodeBindings?.[nodeId]);
		if (prevDisplay !== SUCCEEDED || nextDisplay === SUCCEEDED) continue;
		if (nextDisplay === 'running') continue;
		if (nextDisplay === 'idle' && !next.nodeBindings?.[nodeId]) continue;
		const allowed = isAllowedSucceededRegression(nodeId, ctx);
		if (allowed) {
			if (DEV_MODE) {
				console.debug('[graphStore] EXPECTED_DIRTY_TRANSITION', {
					nodeId,
					source: ctx.source,
					eventType: ctx.evt?.type ?? ctx.source,
					prevDisplay,
					nextDisplay
				});
			}
			continue;
		}
		logSucceededRegression('binding', nodeId, prev, next, ctx, prevDisplay, nextDisplay);
		if (DEV_MODE && !allowed) {
			throw new Error(`SUCCEEDED_REGRESSION(binding): node=${nodeId}, source=${ctx.source}`);
		}
	}
}

function assertHydrationBindingInvariants(prev: GraphState, next: GraphState, ctx: AuditContext): void {
	if (!DEV_MODE || ctx.source !== 'hydrate_snapshot') return;
	const prevBindings = prev.nodeBindings ?? {};
	const nextBindings = next.nodeBindings ?? {};
	const patchIds = ctx.snapshotNodeIds ?? new Set<string>();
	const dropped = Object.keys(prevBindings).filter((id) => !nextBindings[id]);
	if (dropped.length > 0) {
		console.error('[graphStore] BINDING_DROPPED_DURING_HYDRATION', {
			droppedNodeIds: dropped,
			patchNodeIds: Array.from(patchIds)
		});
		throw new Error(`BINDING_DROPPED_DURING_HYDRATION: ${dropped.join(',')}`);
	}
	for (const [id, prevBinding] of Object.entries(prevBindings)) {
		if (patchIds.has(id)) continue;
		const nextBinding = nextBindings[id];
		const same = JSON.stringify(prevBinding) === JSON.stringify(nextBinding);
		if (!same) {
			console.error('[graphStore] OUT_OF_SCOPE_BINDING_MUTATED_DURING_HYDRATION', {
				nodeId: id,
				prevBinding,
				nextBinding,
				patchNodeIds: Array.from(patchIds)
			});
			throw new Error(`OUT_OF_SCOPE_BINDING_MUTATED_DURING_HYDRATION: ${id}`);
		}
	}
}

function auditStateTransition(prev: GraphState, next: GraphState, ctx: AuditContext): void {
	assertHydrationBindingInvariants(prev, next, ctx);
	auditSucceededRegressions(prev, next, ctx);
}

function withGraphMeta(state: GraphState): GraphState {
	const normalizedBindings = ensureNormalizedBindingsForNodes(state.nodes, state.nodeBindings ?? {});
	const normalizedOutputs = pruneNodeOutputsForNodes(state.nodes, state.nodeOutputs ?? {});
	const { freshness, staleNodeCount } = computeGraphFreshness(normalizedBindings ?? {});
	let lastRunStatus = state.lastRunStatus;
	if (state.runStatus === 'succeeded') lastRunStatus = 'succeeded';
	if (state.runStatus === 'failed') lastRunStatus = 'failed';
	if (state.runStatus === 'canceled' || state.runStatus === 'cancelled') lastRunStatus = 'cancelled';
	if (freshness === 'never_run') lastRunStatus = 'never_run';
	return {
		...state,
		freshness,
		staleNodeCount,
		lastRunStatus,
		nodeBindings: normalizedBindings,
		nodeOutputs: normalizedOutputs
	};
}

function canApplyNodeEvent(state: GraphState, nodeId: string, evtRunId?: string): boolean {
	if (!nodeId) return false;
	if (!state.activeRunId) return true;
	if (evtRunId && evtRunId !== state.activeRunId) return false;
	// Event streams are run-scoped by runId; do not drop valid per-node events based on
	// planned sets because scheduler/runtime may execute additional upstream nodes.
	return true;
}

function isNodeStateFromActiveRunAndFresh(cur: GraphState, binding: NormalizedNodeBinding): boolean {
	// Guard only during an active in-flight run; completed runs must not block invalidation.
	if (cur.runStatus !== 'running') return false;
	if (!cur.activeRunId) return false;
	if (binding.currentRunId !== cur.activeRunId) return false;
	const status = String(binding.status ?? '').toLowerCase();
	return (
		status === 'running' ||
		status.startsWith('succeeded') ||
		binding.isUpToDate === true
	);
}

function changedBindingNodeIds(
	prev: Record<string, NormalizedNodeBinding>,
	next: Record<string, NormalizedNodeBinding>
): string[] {
	const ids = new Set([...Object.keys(prev ?? {}), ...Object.keys(next ?? {})]);
	const changed: string[] = [];
	for (const id of ids) {
		const a = prev?.[id] ?? null;
		const b = next?.[id] ?? null;
		if (a === b) continue;
		if (!a || !b) {
			changed.push(id);
			continue;
		}
		const keys = new Set([...Object.keys(a), ...Object.keys(b)]);
		let mutated = false;
		for (const k of keys) {
			if ((a as any)[k] !== (b as any)[k]) {
				mutated = true;
				break;
			}
		}
		if (mutated) changed.push(id);
	}
	return changed;
}

function debugLogOutOfScopeBindingMutation(prev: GraphState, next: GraphState, context: string): void {
	if (!DEV_MODE) return;
	if (!next.activeRunId || !next.activeRunNodeSet || next.activeRunNodeSet.size === 0) return;
	const changed = changedBindingNodeIds(prev.nodeBindings ?? {}, next.nodeBindings ?? {});
	const outside = changed.filter((id) => !next.activeRunNodeSet.has(id));
	if (outside.length === 0) return;
	console.warn('[graphStore] out-of-scope nodeBindings mutation', {
		context,
		outsideNodeIds: outside,
		activeRunId: next.activeRunId,
		activeRunNodeSet: Array.from(next.activeRunNodeSet)
	});
}

function debugLogStaleFlips(prev: GraphState, next: GraphState, context: string): void {
	if (!DEV_MODE) return;
	const flips = getStaleFlipNodeIds(prev.nodeBindings ?? {}, next.nodeBindings ?? {});
	if (flips.length === 0) return;
	console.warn('[graphStore] stale flip detected', {
		context,
		nodeIds: flips,
		activeRunId: next.activeRunId,
		activeRunNodeSet: next.activeRunNodeSet ? Array.from(next.activeRunNodeSet) : []
	});
}

function stableJson(value: unknown): string {
	try {
		return JSON.stringify(value ?? null);
	} catch {
		return '';
	}
}

function sourceOutputModeForPort(port: PortType | null | undefined): 'table' | 'text' | 'json' | 'binary' | null {
	if (!port) return null;
	if (port === 'table' || port === 'text' || port === 'json' || port === 'binary') return port;
	return null;
}

function downstreamNodeIds(edges: Edge<PipelineEdgeData>[], nodeId: string): Set<string> {
	const out = new Set<string>([nodeId]);
	const q = [nodeId];
	while (q.length > 0) {
		const cur = q.shift()!;
		for (const e of edges) {
			if (e.source !== cur) continue;
			const nxt = String(e.target ?? '');
			if (!nxt || out.has(nxt)) continue;
			out.add(nxt);
			q.push(nxt);
		}
	}
	return out;
}

export function __markStaleFromNodeForTest(state: GraphState, nodeId: string): GraphState {
	const candidateIds = downstreamNodeIds(state.edges, nodeId);
	const nodeBindings = { ...state.nodeBindings };
	let changed = false;
	for (const affectedId of candidateIds) {
		const prev = _normalizeBinding(nodeBindings[affectedId], affectedId);
		const hadArtifact = Boolean(prev.current?.artifactId || prev.last?.artifactId);
		if (!hadArtifact) continue;
		if (isNodeStateFromActiveRunAndFresh(state, prev)) continue;
		let next = {
			...prev,
			status: 'stale',
			isUpToDate: false,
			cacheValid: false,
			currentRunId: null,
			staleReason: affectedId === nodeId ? 'PARAMS_CHANGED' : 'UPSTREAM_CHANGED'
		};
		next = _withPair(next, 'current', { execKey: null, artifactId: null });
		nodeBindings[affectedId] = next;
		changed = true;
	}
	if (!changed) return state;
	return withGraphMeta({ ...state, nodeBindings });
}

function effectiveExecParamsForNode(node: Node<PipelineNodeData> | undefined): Record<string, unknown> {
	const raw = { ...(node?.data?.params ?? {}) } as Record<string, unknown>;
	for (const key of [
		'recentSnapshotIds',
		'recent_snapshot_ids',
		'snapshotMetadata',
		'snapshot_metadata',
		'recentSnapshots',
		'snapshotHistory'
	]) {
		delete raw[key];
	}
	return raw;
}

function committedNodeParamsForNode(
	state: GraphState,
	nodeId: string
): Record<string, any> {
	const node = state.nodes.find((x) => x.id === nodeId);
	return { ...((node?.data?.params ?? {}) as Record<string, any>) };
}

function clearNodeCacheUi(
	nodeOutputs: Record<string, NodeOutputInfo>,
	nodeId: string
): Record<string, NodeOutputInfo> {
	const prev = nodeOutputs?.[nodeId];
	if (!prev) return nodeOutputs;
	return {
		...nodeOutputs,
		[nodeId]: {
			...prev,
			cached: false,
			cacheDecision: undefined,
			expectedContractFingerprint: undefined,
			actualContractFingerprint: undefined,
			mismatchKind: undefined
		}
	};
}

function clearNodeCacheUiForNodes(
	nodeOutputs: Record<string, NodeOutputInfo>,
	nodeIds: Iterable<string>
): Record<string, NodeOutputInfo> {
	let next = nodeOutputs;
	for (const nodeId of nodeIds) {
		next = clearNodeCacheUi(next, nodeId);
	}
	return next;
}

function assertNoOutOfScopeStaleFlips(prev: GraphState, next: GraphState, context: string): void {
	if (!DEV_MODE) return;
	if (!next.activeRunId || !next.activeRunNodeSet || next.activeRunNodeSet.size === 0) return;
	const flips = getStaleFlipNodeIds(prev.nodeBindings ?? {}, next.nodeBindings ?? {});
	const outOfScope = flips.filter((id) => !next.activeRunNodeSet.has(id));
	for (const nodeId of outOfScope) {
		console.error('[graphStore] out-of-scope stale flip', {
			context,
			nodeId,
			prevBinding: prev.nodeBindings?.[nodeId] ?? null,
			nextBinding: next.nodeBindings?.[nodeId] ?? null,
			activeRunId: next.activeRunId,
			runMode: next.activeRunMode,
			runFrom: next.activeRunFrom,
			activeRunNodeSet: Array.from(next.activeRunNodeSet)
		});
	}
}

function assertRunStartedBindingTouchInScope(prev: GraphState, next: GraphState): void {
	if (!DEV_MODE) return;
	const changed = changedBindingNodeIds(prev.nodeBindings ?? {}, next.nodeBindings ?? {});
	if (changed.length === 0) return;
	const outOfScope = changed.filter((id) => !next.activeRunNodeSet?.has(id));
	if (outOfScope.length === 0) return;
	console.error('[graphStore] run_started mutated out-of-scope nodeBindings', {
		changedNodeIds: changed,
		outOfScopeNodeIds: outOfScope,
		activeRunId: next.activeRunId,
		runMode: next.activeRunMode,
		runFrom: next.activeRunFrom,
		activeRunNodeSet: next.activeRunNodeSet ? Array.from(next.activeRunNodeSet) : [],
		bindingsBefore: outOfScope.reduce(
			(acc, id) => ({ ...acc, [id]: prev.nodeBindings?.[id] ?? null }),
			{} as Record<string, NormalizedNodeBinding | null>
		),
		bindingsAfter: outOfScope.reduce(
			(acc, id) => ({ ...acc, [id]: next.nodeBindings?.[id] ?? null }),
			{} as Record<string, NormalizedNodeBinding | null>
		)
	});
}

function reduceRunEventState(state: GraphState, evt: KnownRunEvent, runId: string): GraphState {
	const evtGraphId = (evt as any)?.graphId;
	if (typeof evtGraphId === 'string' && evtGraphId && evtGraphId !== state.graphId) {
		return state;
	}
	switch (evt.type) {
		case 'node_output': {
			if (!canApplyNodeEvent(state, evt.nodeId, evt.runId)) return state;
			const prevBinding = _normalizeBinding(state.nodeBindings?.[evt.nodeId], evt.nodeId);
			let nextForNode: NormalizedNodeBinding = {
				...prevBinding,
				currentRunId: runId,
				lastRunId: runId,
				isUpToDate: prevBinding.isUpToDate
			};
			const currentPair = _pairFromLegacy(prevBinding, 'current');
			const boundExecKey = currentPair.execKey ?? _pairFromLegacy(prevBinding, 'last').execKey ?? evt.artifactId;
			nextForNode = _withPair(nextForNode, 'current', { execKey: boundExecKey, artifactId: evt.artifactId });
			nextForNode = _withPair(nextForNode, 'last', { execKey: boundExecKey, artifactId: evt.artifactId });
			_assertBindingPairInvariant(nextForNode, evt.nodeId, 'node_output');
			const nodeBindings = {
				...state.nodeBindings,
				[evt.nodeId]: nextForNode
			};
			const prevCacheDecision = state.nodeOutputs?.[evt.nodeId]?.cacheDecision;
			const nextCacheDecision =
				evt.cached === true
					? (prevCacheDecision ?? 'cache_hit')
					: 'cache_miss';
			const nodeOutputs = {
				...state.nodeOutputs,
				[evt.nodeId]: {
					mimeType: evt.mimeType,
					portType: evt.portType,
					preview: evt.preview ?? undefined,
					cached: evt.cached ?? false,
					cacheDecision: nextCacheDecision
				}
			};
			return withGraphMeta({
				...state,
				nodeOutputs,
				nodeBindings
			});
		}
		case 'cache_decision': {
			if (!canApplyNodeEvent(state, evt.nodeId, evt.runId)) return state;
			const prevBinding = _normalizeBinding(state.nodeBindings?.[evt.nodeId], evt.nodeId);
			const nextIsUpToDate =
				evt.decision === 'cache_hit'
					? true
					: evt.decision === 'cache_hit_contract_mismatch'
						? false
						: (typeof prevBinding.isUpToDate === 'boolean' ? prevBinding.isUpToDate : true);
			let nextBinding: NormalizedNodeBinding = {
				...prevBinding,
				cacheValid: evt.decision === 'cache_hit',
				isUpToDate: nextIsUpToDate,
				status: evt.decision === 'cache_hit_contract_mismatch' ? 'stale' : prevBinding.status,
				staleReason: evt.decision === 'cache_hit_contract_mismatch' ? 'CONTRACT_MISMATCH' : prevBinding.staleReason
			};
			if (evt.decision === 'cache_hit_contract_mismatch' && isNodeStateFromActiveRunAndFresh(state, prevBinding)) {
				nextBinding = {
					...prevBinding,
					cacheValid: false
				};
			}
			if (evt.decision === 'cache_hit') {
				const aid = evt.artifactId ?? prevBinding.current?.artifactId ?? prevBinding.last?.artifactId ?? null;
				nextBinding = _withPair(nextBinding, 'current', { execKey: evt.execKey, artifactId: aid });
			} else {
				nextBinding = _withPair(nextBinding, 'current', { execKey: null, artifactId: null });
			}
			_assertBindingPairInvariant(nextBinding, evt.nodeId, 'cache_decision');
			const nodeBindings = {
				...state.nodeBindings,
				[evt.nodeId]: nextBinding
			};
			const prev = state.nodeOutputs?.[evt.nodeId];
			const nextForNode: NodeOutputInfo = {
				mimeType: prev?.mimeType,
				portType: prev?.portType,
				preview: prev?.preview,
				cached: evt.decision === 'cache_hit',
				cacheDecision: evt.decision,
				expectedContractFingerprint:
					evt.decision === 'cache_hit_contract_mismatch'
						? String((evt as any).expectedContractFingerprint ?? '')
						: prev?.expectedContractFingerprint,
				actualContractFingerprint:
					evt.decision === 'cache_hit_contract_mismatch'
						? String((evt as any).actualContractFingerprint ?? '')
						: prev?.actualContractFingerprint,
				mismatchKind:
					evt.decision === 'cache_hit_contract_mismatch'
						? String((evt as any).mismatchKind ?? '')
						: prev?.mismatchKind
			};
			return withGraphMeta({
				...state,
				nodeBindings,
				nodeOutputs: {
					...state.nodeOutputs,
					[evt.nodeId]: nextForNode
				}
			});
		}
		case 'run_started': {
			const evtMode = ((evt as any).runMode ?? state.activeRunMode) as ActiveRunMode;
			const evtPlanned = Array.isArray((evt as any).plannedNodeIds)
				? new Set<string>((evt as any).plannedNodeIds as string[])
				: computePlannedNodeSet(
					state.nodes,
					state.edges,
					evt.runFrom ?? null,
					evtMode ?? (evt.runFrom ? 'from_selected_onward' : 'from_start')
				);
			const nodeBindings = { ...state.nodeBindings };
			for (const nodeId of evtPlanned) {
				const prevBinding = _normalizeBinding(nodeBindings[nodeId], nodeId);
				const hasArtifact = Boolean(
					prevBinding.current?.artifactId ??
					prevBinding.currentArtifactId ??
					prevBinding.last?.artifactId ??
					prevBinding.lastArtifactId
				);
				if (!hasArtifact) continue;
				if (isNodeStateFromActiveRunAndFresh(state, prevBinding)) continue;
				nodeBindings[nodeId] = {
					...prevBinding,
					status: 'stale',
					isUpToDate: false,
					cacheValid: false,
					currentRunId: null,
					staleReason: 'RUN_PENDING'
				};
			}
			const nodeOutputs = clearNodeCacheUiForNodes(state.nodeOutputs, evtPlanned);
			return withGraphMeta(
				logPush(
					{
						...state,
						activeRunId: evt.runId ?? state.activeRunId,
						activeRunMode: evtMode,
						activeRunFrom: evt.runFrom ?? state.activeRunFrom,
						activeRunNodeSet: evtPlanned,
						nodeBindings,
						nodeOutputs
					},
					'info',
					`Run started ${evt.runFrom ? `(from ${evt.runFrom})` : '(from start)'}`
				)
			);
		}
		case 'node_started': {
			if (!canApplyNodeEvent(state, evt.nodeId, evt.runId)) return state;
			const prevBinding = _normalizeBinding(state.nodeBindings?.[evt.nodeId], evt.nodeId);
			if (
				prevBinding.currentRunId === (evt.runId ?? runId) &&
				(prevBinding.status.startsWith('succeeded') || prevBinding.isUpToDate === true)
			) {
				return state;
			}
			const nodeBindings = {
				...state.nodeBindings,
				[evt.nodeId]: {
					...prevBinding,
					status: 'running',
					currentRunId: evt.runId ?? runId
				}
			};
			const nodeOutputs = {
				...state.nodeOutputs,
				[evt.nodeId]: {
					...(state.nodeOutputs?.[evt.nodeId] ?? {}),
					lastError: null
				}
			};
			return withGraphMeta(logPush({ ...state, nodeBindings, nodeOutputs }, 'info', 'Node started', evt.nodeId));
		}
		case 'component_started': {
			if (!canApplyNodeEvent(state, evt.nodeId, evt.runId)) return state;
			const prevBinding = _normalizeBinding(state.nodeBindings?.[evt.nodeId], evt.nodeId);
			const nodeBindings = {
				...state.nodeBindings,
				[evt.nodeId]: {
					...prevBinding,
					status: 'running',
					currentRunId: evt.runId ?? runId,
					staleReason: null
				}
			};
			const nodeOutputs = {
				...state.nodeOutputs,
				[evt.nodeId]: {
					...(state.nodeOutputs?.[evt.nodeId] ?? {}),
					lastError: null
				}
			};
			return withGraphMeta(
				logPush({ ...state, nodeBindings, nodeOutputs }, 'info', 'Component started', evt.nodeId)
			);
		}
		case 'component_finished': {
			if (!canApplyNodeEvent(state, evt.nodeId, evt.runId)) return state;
			const prevBinding = _normalizeBinding(state.nodeBindings?.[evt.nodeId], evt.nodeId);
			// Keep running until wrapper component node itself emits node_finished.
			const nodeBindings = {
				...state.nodeBindings,
				[evt.nodeId]: {
					...prevBinding,
					status: 'running',
					currentRunId: evt.runId ?? runId
				}
			};
			return withGraphMeta(logPush({ ...state, nodeBindings }, 'info', 'Component internals finished', evt.nodeId));
		}
		case 'component_failed': {
			if (!canApplyNodeEvent(state, evt.nodeId, evt.runId)) return state;
			const prevBinding = _normalizeBinding(state.nodeBindings?.[evt.nodeId], evt.nodeId);
			const nodeBindings = {
				...state.nodeBindings,
				[evt.nodeId]: {
					...prevBinding,
					status: 'failed',
					isUpToDate: false,
					cacheValid: false,
					currentRunId: evt.runId ?? runId,
					staleReason: 'COMPONENT_FAILED'
				}
			};
			const nodeOutputs = {
				...state.nodeOutputs,
				[evt.nodeId]: {
					...(state.nodeOutputs?.[evt.nodeId] ?? {}),
					lastError: {
						message: String((evt as any).error ?? 'Component failed'),
						errorCode: 'COMPONENT_FAILED'
					}
				}
			};
			return withGraphMeta(logPush({ ...state, nodeBindings, nodeOutputs }, 'error', 'Component failed', evt.nodeId));
		}
		case 'edge_exec': {
			const edges = state.edges.map((e) =>
				e.id === evt.edgeId ? { ...e, data: { ...(e.data ?? {}), exec: evt.exec } } : e
			);
			return { ...state, edges };
		}
		case 'log':
			return logPush(state, evt.level, evt.message, evt.nodeId, (evt as any).componentPath);
		case 'node_finished': {
			if (!canApplyNodeEvent(state, evt.nodeId, evt.runId)) return state;
			const prevBinding = _normalizeBinding(state.nodeBindings?.[evt.nodeId], evt.nodeId);
			const succeeded = evt.status === 'succeeded';
			const errorDetails = (evt as any).errorDetails as Record<string, unknown> | undefined;
			const errorPayload: NodeExecutionError | null = succeeded
				? null
				: {
					message: evt.error ? String(evt.error) : undefined,
					errorCode: typeof (evt as any).errorCode === 'string' ? String((evt as any).errorCode) : undefined,
					op: typeof errorDetails?.op === 'string' ? String(errorDetails.op) : undefined,
					paramPath: typeof errorDetails?.paramPath === 'string' ? String(errorDetails.paramPath) : undefined,
					missingColumns: Array.isArray(errorDetails?.missingColumns)
						? errorDetails?.missingColumns.map((c) => String(c))
						: undefined,
					availableColumns: Array.isArray(errorDetails?.availableColumns)
						? errorDetails?.availableColumns.map((c) => String(c))
						: undefined,
					availableColumnsSource:
						typeof errorDetails?.availableColumnsSource === 'string'
							? (errorDetails.availableColumnsSource as any)
							: undefined
				};
			let nextBinding: NormalizedNodeBinding = {
				...prevBinding,
				status: succeeded ? 'succeeded_up_to_date' : evt.status,
				currentRunId: evt.runId ?? runId,
				isUpToDate: succeeded ? true : false,
				cacheValid: succeeded ? true : false,
				staleReason: succeeded ? null : prevBinding.staleReason
			};
			if (succeeded) {
				const current = _pairFromLegacy(nextBinding, 'current');
				if (current.execKey && current.artifactId) {
					nextBinding = _withPair(nextBinding, 'last', current);
				}
			}
			_assertBindingPairInvariant(nextBinding, evt.nodeId, 'node_finished');
			const nodeBindings = {
				...state.nodeBindings,
				[evt.nodeId]: nextBinding
			};
			const prevOut = state.nodeOutputs?.[evt.nodeId];
			const nodeOutputs = {
				...state.nodeOutputs,
				[evt.nodeId]: {
					...prevOut,
					cacheDecision: prevOut?.cacheDecision ?? (succeeded ? 'cache_miss' : prevOut?.cacheDecision),
					lastError: errorPayload
				}
			};
			return withGraphMeta(
				logPush(
					{ ...state, nodeBindings, nodeOutputs },
					'info',
					`Node finished (${displayStatusFromBinding(nextBinding)})`,
					evt.nodeId
				)
			);
		}
		case 'run_finished': {
			return withGraphMeta(
				logPush({ ...state, runStatus: evt.status }, 'info', `Run finished (${evt.status})`)
			);
		}
		default:
			return state;
	}
}

type RunSnapshotLike = {
	graphId?: string;
	status?: string;
	runMode?: ActiveRunMode;
	plannedNodeIds?: string[];
	nodeStatus?: Record<string, string>;
	nodeOutputs?: Record<string, string>;
	nodeBindings?: Record<string, Record<string, unknown>>;
};

function hydrateFromRunSnapshotState(state: GraphState, snap: RunSnapshotLike): GraphState {
	if (typeof snap.graphId === 'string' && snap.graphId && snap.graphId !== state.graphId) {
		return state;
	}
	const nodeBindingsPatch: Record<string, NormalizedNodeBinding> = {};
	const nodeOutputs: Record<string, NodeOutputInfo> = { ...(state.nodeOutputs ?? {}) };
	for (const [nodeId, raw] of Object.entries(snap.nodeBindings ?? {})) {
		const b = _normalizeBinding(raw as NodeBindingInfo, nodeId);
		nodeBindingsPatch[nodeId] = b;
	}
	const nodeBindings = mergeBindingsSticky(state.nodeBindings ?? {}, nodeBindingsPatch);
	const runStatus = (snap.status as RunStatus) || state.runStatus;
	const runMode = snap.runMode ?? state.activeRunMode;
	const activeRunNodeSet = Array.isArray(snap.plannedNodeIds)
		? new Set<string>(snap.plannedNodeIds)
		: state.activeRunNodeSet;
	return withGraphMeta({
		...state,
		runStatus,
		nodeBindings,
		nodeOutputs,
		activeRunMode: runMode,
		activeRunFrom: state.activeRunFrom,
		activeRunNodeSet
	});
}

export function __applyRunEventForTest(state: GraphState, evt: KnownRunEvent, runId: string): GraphState {
	return reduceRunEventState(state, evt, runId);
}

export function __hydrateFromRunSnapshotForTest(
	state: GraphState,
	snap: RunSnapshotLike
): GraphState {
	return hydrateFromRunSnapshotState(state, snap);
}

function buildHardResetState(freshGraphId: string): GraphState {
	return {
		graphId: freshGraphId,
		nodes: [],
		edges: [],
		selectedNodeId: null,
		inspector: initialInspector,
		logs: [],
		runStatus: RUN_IDLE,//change or fix 
		lastRunStatus: 'never_run',
		freshness: 'never_run',
		staleNodeCount: 0,
		activeRunMode: 'from_start',
		activeRunFrom: null,
		activeRunNodeSet: new Set<string>(),
		nodeOutputs: {},
		nodeBindings: {},
		activeRunId: null,
		editingContext: 'graph',
		componentEditSession: null
	};
}

function captureComponentEditSnapshot(state: GraphState): ComponentEditSessionSnapshot {
	return {
		graphId: state.graphId,
		nodes: structuredClone(state.nodes),
		edges: structuredClone(state.edges),
		selectedNodeId: state.selectedNodeId,
		inspector: structuredClone(state.inspector),
		logs: structuredClone(state.logs),
		runStatus: state.runStatus,
		lastRunStatus: state.lastRunStatus,
		freshness: state.freshness,
		staleNodeCount: state.staleNodeCount,
		activeRunMode: state.activeRunMode,
		activeRunFrom: state.activeRunFrom,
		activeRunNodeSet: new Set(Array.from(state.activeRunNodeSet ?? [])),
		nodeOutputs: structuredClone(state.nodeOutputs),
		nodeBindings: structuredClone(state.nodeBindings),
		activeRunId: state.activeRunId
	};
}

export function __hardResetGraphForTest(_state: GraphState, freshGraphId = 'graph_test_reset'): GraphState {
	return buildHardResetState(freshGraphId);
}

function stripToDTO(
	nodes: Node<PipelineNodeData>[],
	edges: Edge<PipelineEdgeData>[],
	graphId?: string
): PipelineGraphDTO {
	const dto: PipelineGraphDTO = {
		version: 1,
		nodes,
		edges: recomputeEdgeContractsBestEffort(nodes, edges)
	};
	if (graphId) {
		dto.meta = { ...(dto.meta ?? {}), graphId } as any;
	}
	return dto;
}

function edgeStructuralSignature(edge: Edge<PipelineEdgeData>): string {
	const contract = (edge?.data as any)?.contract ?? {};
	const payload = (contract as any)?.payload ?? {};
	return [
		String(edge?.id ?? ''),
		String(edge?.source ?? ''),
		String((edge as any)?.sourceHandle ?? ''),
		String(edge?.target ?? ''),
		String((edge as any)?.targetHandle ?? ''),
		String((edge?.data as any)?.exec ?? ''),
		String((contract as any)?.out ?? ''),
		String((contract as any)?.in ?? ''),
		JSON.stringify((payload as any)?.source ?? null),
		JSON.stringify((payload as any)?.target ?? null)
	].join('|');
}

function shouldPreserveStoreEdgesOnCanvasSync(
	storeEdges: Edge<PipelineEdgeData>[],
	canvasEdges: Edge<PipelineEdgeData>[]
): boolean {
	if (canvasEdges.length >= storeEdges.length) return false;
	if (storeEdges.length === 0) return false;
	const storeById = new Map<string, Edge<PipelineEdgeData>>();
	for (const edge of storeEdges) {
		storeById.set(String(edge.id ?? ''), edge);
	}
	for (const edge of canvasEdges) {
		const id = String(edge.id ?? '');
		const existing = storeById.get(id);
		if (!existing) return false;
		// If the edge shape changed, this is not a stale node-drag sync.
		if (edgeStructuralSignature(edge) !== edgeStructuralSignature(existing)) return false;
	}
	return true;
}

function normalizeComponentPortTypeOrDefault(value: unknown, fallback: PortType = 'json'): PortType {
	const normalized = normalizeComponentPortType(value);
	return normalized ?? fallback;
}

function normalizeComponentNodeForMigration(
	node: Node<PipelineNodeData>
): { node: Node<PipelineNodeData>; outputNames: string[]; outputByName: Map<string, PortType>; bindingNames: string[] } {
	if (node.data.kind !== 'component') {
		return { node, outputNames: [], outputByName: new Map(), bindingNames: [] };
	}
	const params = (((node.data as any)?.params ?? {}) as Record<string, any>) || {};
	const api = (params.api ?? {}) as Record<string, any>;
	const outputsRaw = Array.isArray(api.outputs) ? (api.outputs as any[]) : [];
	const normalizedOutputs = outputsRaw
		.filter((out) => Boolean(out) && typeof out === 'object')
		.map((out) => {
			const outName = String((out as any)?.name ?? '').trim();
			const portType = normalizeComponentPortTypeOrDefault((out as any)?.portType, 'json');
			const typedSchemaRaw =
				(out as any)?.typedSchema && typeof (out as any).typedSchema === 'object'
					? ((out as any).typedSchema as Record<string, any>)
					: {};
			const fieldsRaw = Array.isArray(typedSchemaRaw.fields) ? (typedSchemaRaw.fields as any[]) : [];
			const normalizedFields =
				portType === 'table' || portType === 'json'
					? fieldsRaw
					: [];
			return {
				...(out as any),
				name: outName,
				portType,
				typedSchema: {
					type: portType,
					fields: normalizedFields
				}
			};
		})
		.filter((out) => String((out as any)?.name ?? '').trim().length > 0);

	const outputNames = normalizedOutputs.map((out) => String((out as any)?.name ?? '').trim());
	const outputSet = new Set(outputNames);
	const outputByName = new Map<string, PortType>();
	for (const out of normalizedOutputs) {
		const name = String((out as any)?.name ?? '').trim();
		const portType = normalizeComponentPortTypeOrDefault((out as any)?.portType, 'json');
		outputByName.set(name, portType);
	}

	const bindings = (params.bindings ?? {}) as Record<string, any>;
	const outputBindingsRaw =
		bindings.outputs && typeof bindings.outputs === 'object'
			? ({ ...(bindings.outputs as Record<string, any>) } as Record<string, any>)
			: {};
	if (outputNames.length === 1) {
		const only = outputNames[0];
		if (!Object.prototype.hasOwnProperty.call(outputBindingsRaw, only) && outputBindingsRaw.out_data) {
			outputBindingsRaw[only] = structuredClone(outputBindingsRaw.out_data);
		}
	}
	for (const key of Object.keys(outputBindingsRaw)) {
		if (!outputSet.has(String(key))) delete outputBindingsRaw[key];
	}

	const nextNode: Node<PipelineNodeData> = {
		...node,
		data: {
			...node.data,
			ports: {
				...((node.data as any)?.ports ?? {}),
				in:
					normalizeComponentPortType(
						(Array.isArray((api as any)?.inputs) ? (api as any).inputs[0]?.portType : null) ?? null
					) ?? null,
				out: null
			},
			params: {
				...params,
				api: {
					...(api as Record<string, any>),
					outputs: normalizedOutputs
				},
				bindings: {
					...(bindings as Record<string, any>),
					outputs: outputBindingsRaw
				}
			}
		}
	};
	const bindingNames = outputNames.filter((name) =>
		Object.prototype.hasOwnProperty.call(outputBindingsRaw, name)
	);
	return { node: nextNode, outputNames, outputByName, bindingNames };
}

function normalizeGraphForComponentMigration(
	nodes: Node<PipelineNodeData>[],
	edges: Edge<PipelineEdgeData>[]
): { nodes: Node<PipelineNodeData>[]; edges: Edge<PipelineEdgeData>[] } {
	const nodeInfo = new Map<
		string,
		{ outputNames: string[]; outputByName: Map<string, PortType>; bindingNames: string[] }
	>();
	const normalizedNodes = nodes.map((node) => {
		if (node.data.kind === 'tool') {
			const params = ((node.data as any)?.params ?? {}) as Record<string, any>;
			if (String(params?.provider ?? '').trim().toLowerCase() === 'builtin') {
				const builtin = (params?.builtin && typeof params.builtin === 'object'
					? params.builtin
					: {}) as Record<string, any>;
				const profileId = String(builtin.profileId ?? '').trim() || 'core';
				const customPackages = Array.isArray(builtin.customPackages)
					? builtin.customPackages
							.filter((pkg: unknown) => typeof pkg === 'string')
							.map((pkg: string) => pkg.trim())
							.filter((pkg: string) => pkg.length > 0)
					: [];
				const locked = typeof builtin.locked === 'string' ? builtin.locked.trim() : '';
				const nextBuiltin: Record<string, any> = {
					...builtin,
					profileId,
					customPackages
				};
				if (locked) nextBuiltin.locked = locked;
				else delete nextBuiltin.locked;
				node = {
					...node,
					data: {
						...node.data,
						params: {
							...params,
							builtin: nextBuiltin
						}
					}
				};
			}
		}
		const normalized = normalizeComponentNodeForMigration(node);
		if (node.data.kind === 'component') {
			nodeInfo.set(String(node.id), {
				outputNames: normalized.outputNames,
				outputByName: normalized.outputByName,
				bindingNames: normalized.bindingNames
			});
		}
		return normalized.node;
	});

	const normalizedEdges = edges.map((edge) => {
		const srcInfo = nodeInfo.get(String(edge.source));
		if (!srcInfo) return edge;
		const outputNames = srcInfo.outputNames;
		if (outputNames.length === 0) return edge;
		const sourceHandle = String((edge as any)?.sourceHandle ?? 'out').trim() || 'out';
		const outputSet = new Set(outputNames);
		const bindingNames = srcInfo.bindingNames;
		const edgeDataContract =
			(edge as any)?.data && typeof (edge as any).data === 'object'
				? ((edge as any).data?.contract as Record<string, any> | undefined)
				: undefined;
		const contractOut = normalizeComponentPortType(edgeDataContract?.out ?? null);
		let canonicalHandle = sourceHandle;
		if (canonicalHandle === 'out') {
			if (outputNames.length === 1) {
				canonicalHandle = outputNames[0];
			} else if (bindingNames.length === 1) {
				canonicalHandle = bindingNames[0];
			} else if (contractOut) {
				const candidates = outputNames.filter(
					(name) => srcInfo.outputByName.get(name) === contractOut
				);
				if (candidates.length === 1) canonicalHandle = candidates[0];
			}
		} else if (!outputSet.has(canonicalHandle)) {
			if (outputNames.length === 1) {
				canonicalHandle = outputNames[0];
			} else if (bindingNames.length === 1) {
				canonicalHandle = bindingNames[0];
			} else if (contractOut) {
				const candidates = outputNames.filter(
					(name) => srcInfo.outputByName.get(name) === contractOut
				);
				if (candidates.length === 1) canonicalHandle = candidates[0];
			}
		}
		if (canonicalHandle === sourceHandle) return edge;
		return {
			...edge,
			sourceHandle: canonicalHandle
		};
	});

	return {
		nodes: normalizedNodes,
		edges: recomputeEdgeContractsBestEffort(normalizedNodes, normalizedEdges)
	};
}

function buildPersistableGraphStrict(
	nodes: Node<PipelineNodeData>[],
	edges: Edge<PipelineEdgeData>[],
	graphId?: string
): { ok: true; graph: PipelineGraphDTO } | { ok: false; error: string } {
	const normalized = normalizeGraphForComponentMigration(nodes, edges);
	const canonicalized = canonicalizeComponentEdgeSourceHandles(normalized.nodes, normalized.edges, 'strict');
	if (!canonicalized.ok) return { ok: false, error: canonicalized.error };
	const rechecked = pruneAndRecontractEdgesStrict(normalized.nodes, canonicalized.edges);
	if (!rechecked.ok) return { ok: false, error: rechecked.error };
	return { ok: true, graph: stripToDTO(normalized.nodes, rechecked.edges, graphId) };
}

function toolBuiltinPreflightDiagnostics(node: Node<PipelineNodeData>): SavePreflightDiagnostic[] {
	if (node.data.kind !== 'tool') return [];
	const params = ((node.data as any)?.params ?? {}) as Record<string, any>;
	const provider = String(params?.provider ?? '').trim().toLowerCase();
	const builtin =
		params?.builtin && typeof params.builtin === 'object' ? (params.builtin as Record<string, any>) : null;
	if (!builtin && provider !== 'builtin') return [];
	const profileId = String((builtin?.profileId ?? 'core') ?? 'core').trim() || 'core';
	if (!allowedBuiltinProfileIds.has(profileId)) {
		return [
			{
				code: 'ENV_PROFILE_INVALID',
				path: `nodes.${String(node.id)}.params.builtin.profileId`,
				message: `Tool builtin profile "${profileId}" is invalid.`,
				severity: 'error'
			}
		];
	}
	if (profileId !== 'custom') return [];
	const customPackagesRaw = Array.isArray(builtin?.customPackages) ? (builtin?.customPackages as string[]) : [];
	if (customPackagesRaw.length === 0) {
		return [
			{
				code: 'ENV_PROFILE_MISSING',
				path: `nodes.${String(node.id)}.params.builtin.customPackages`,
				message: "Custom builtin profile requires at least one package before save.",
				severity: 'error'
			}
		];
	}
	const parsed = validateCustomPackageDraft(customPackagesRaw.join('\n'));
	const diagnostics: SavePreflightDiagnostic[] = [];
	if (parsed.blocked.length > 0) {
		diagnostics.push({
			code: 'ENV_PROFILE_PACKAGE_BLOCKED',
			path: `nodes.${String(node.id)}.params.builtin.customPackages`,
			message: `Custom builtin profile includes blocked package(s): ${parsed.blocked.join(', ')}`,
			severity: 'error'
		});
	}
	if (parsed.errors.length > 0) {
		diagnostics.push({
			code: 'ENV_PROFILE_INVALID',
			path: `nodes.${String(node.id)}.params.builtin.customPackages`,
			message: `Custom builtin profile has invalid package entries: ${parsed.errors.join('; ')}`,
			severity: 'error'
		});
	}
	return diagnostics;
}

function buildSavePreflightDiagnostics(
	nodes: Node<PipelineNodeData>[],
	edges: Edge<PipelineEdgeData>[]
): SavePreflightResult {
	const normalized = normalizeGraphForComponentMigration(nodes, edges);
	const workingNodes = normalized.nodes;
	const workingEdges = normalized.edges;
	const diagnostics: SavePreflightDiagnostic[] = [];
	for (const edge of workingEdges) {
		const sourceHandle = String((edge as any)?.sourceHandle ?? 'out').trim() || 'out';
		const sourceNode = workingNodes.find((n) => n.id === edge.source);
		if (sourceNode?.data?.kind === 'component') {
			const canonicalHandle = canonicalComponentSourceHandleForEdge(workingNodes, edge);
			if (canonicalHandle == null) {
				diagnostics.push({
					code: 'COMPONENT_OUTPUT_HANDLE_UNRESOLVED',
					path: `edges.${String(edge.id ?? '')}.sourceHandle`,
					message: `Component edge sourceHandle "${sourceHandle}" is not declared in API outputs.`,
					severity: 'error'
				});
			}
		}
		const edgeCheck = isEdgeStillValid(workingNodes, edge);
		if (!edgeCheck.ok) {
			if (edgeCheck.reason === 'type_mismatch') {
				diagnostics.push({
					code: 'CONTRACT_EDGE_PORT_TYPE_MISMATCH',
					path: `edges.${String(edge.id ?? '')}.data.contract`,
					message: `Edge has incompatible schemas (source=${String(edge.source ?? '')}:${sourceHandle} target=${String((edge as any)?.target ?? '')}:${String((edge as any)?.targetHandle ?? 'in')})${edgeCheck.suggestion ? ` ${edgeCheck.suggestion}` : ''}.`,
					severity: 'error'
				});
			} else if (edgeCheck.reason === 'typed_schema_missing') {
				diagnostics.push({
					code: 'CONTRACT_EDGE_TYPED_SCHEMA_MISSING',
					path: `edges.${String(edge.id ?? '')}.data.contract.payload.source`,
					message: `Edge is missing required typed schema coverage. Required columns: ${(edgeCheck.missingColumns ?? []).join(', ') || '(unknown)'}.`,
					severity: 'error'
				});
			} else if (edgeCheck.reason === 'schema_mismatch') {
				diagnostics.push({
					code: 'CONTRACT_EDGE_SCHEMA_MISMATCH',
					path: `edges.${String(edge.id ?? '')}.data.contract`,
					message: `Edge is missing required columns: ${(edgeCheck.missingColumns ?? []).join(', ') || '(unknown)'}.`,
					severity: 'error'
				});
			} else {
				diagnostics.push({
					code: 'CONTRACT_EDGE_PORT_TYPE_UNRESOLVED',
					path: `edges.${String(edge.id ?? '')}.data.contract`,
					message: `Edge has unresolved port types (source=${String(edge.source ?? '')}:${sourceHandle} target=${String((edge as any)?.target ?? '')}:${String((edge as any)?.targetHandle ?? 'in')}).`,
					severity: 'error'
				});
			}
		}
	}

	for (const node of workingNodes) {
		diagnostics.push(...toolBuiltinPreflightDiagnostics(node));
		if (node.data.kind !== 'component') continue;
		const componentParams = ((node.data as any)?.params ?? {}) as Record<string, any>;
		const apiOutputs = Array.isArray(componentParams?.api?.outputs)
			? (componentParams.api.outputs as any[])
			: [];
		const outputBindings =
			componentParams?.bindings && typeof componentParams.bindings.outputs === 'object' && componentParams.bindings.outputs
				? (componentParams.bindings.outputs as Record<string, any>)
				: {};
		for (let i = 0; i < apiOutputs.length; i += 1) {
			const out = apiOutputs[i] ?? {};
			const outputName = String(out?.name ?? '').trim();
			const pathBase = `nodes.${String(node.id)}.params.api.outputs[${i}]`;
			if (!outputName) {
				diagnostics.push({
					code: 'COMPONENT_OUTPUT_NAME_REQUIRED',
					path: `${pathBase}.name`,
					message: 'Component output name is required.',
					severity: 'error'
				});
				continue;
			}
			const binding = outputBindings[outputName] ?? {};
			const boundNodeId = String(binding?.nodeId ?? '').trim();
			if (!boundNodeId) {
				diagnostics.push({
					code: 'COMPONENT_OUTPUT_BINDING_MISSING',
					path: `nodes.${String(node.id)}.params.bindings.outputs.${outputName}.nodeId`,
					message: `Component output "${outputName}" requires a bound internal node.`,
					severity: 'error'
				});
			}
			const portType = normalizeComponentPortType(out?.portType);
			const typedSchemaType = normalizeComponentPortType(out?.typedSchema?.type);
			if (portType == null || typedSchemaType == null || portType !== typedSchemaType) {
				diagnostics.push({
					code: 'COMPONENT_OUTPUT_TYPED_SCHEMA_MISMATCH',
					path: `${pathBase}.typedSchema.type`,
					message: `Component output "${outputName}" must keep typedSchema.type aligned with portType.`,
					severity: 'error'
				});
			}
		}
	}

	return {
		ok: !diagnostics.some((d) => d.severity === 'error'),
		diagnostics
	};
}

function summarizeSavePreflightError(diagnostics: SavePreflightDiagnostic[]): string {
	const errors = diagnostics.filter((d) => d.severity === 'error');
	if (errors.length === 0) return 'Graph preflight failed.';
	return errors
		.slice(0, 5)
		.map((d, i) => `${i + 1}. [${d.code}] (${d.path}) ${d.message}`)
		.join('\n');
}

function getPortType(
	nodes: Node<PipelineNodeData>[],
	sourceId: string,
	whichPort: 'in' | 'out'
): PortType | null {
	const n = nodes.find((x) => x.id === sourceId);
	if (!n) return null;
	return (n.data.ports?.[whichPort as 'in' | 'out'] ?? null) as PortType | null;
}

function componentApiOutputPortType(
	node: Node<PipelineNodeData>,
	sourceHandle: string
): PortType | null {
	if (node.data.kind !== 'component') return null;
	const outputs = Array.isArray((node.data as any)?.params?.api?.outputs)
		? ((node.data as any).params.api.outputs as any[])
		: [];
	const handle = String(sourceHandle ?? '').trim();
	if (!handle || handle === 'out') {
		if (outputs.length === 1) {
			return normalizeComponentPortType((outputs[0] as any)?.portType ?? null);
		}
		return null;
	}
	const decl = outputs.find((o) => String((o as any)?.name ?? '').trim() === handle);
	return normalizeComponentPortType((decl as any)?.portType ?? null);
}

function sourcePortTypeForEdge(
	nodes: Node<PipelineNodeData>[],
	edge: Edge<PipelineEdgeData>
): PortType | null {
	const node = nodes.find((x) => x.id === edge.source);
	if (!node) return null;
	if (node.data.kind === 'component') {
		return componentApiOutputPortType(node, String((edge as any).sourceHandle ?? 'out'));
	}
	return (node.data.ports?.out ?? null) as PortType | null;
}

function sourcePayloadHint(
	node: Node<PipelineNodeData>,
	whichPort: 'in' | 'out',
	handleId: string = 'out'
) {
	if (node.data.kind === 'component' && whichPort === 'out') {
		const outputs = Array.isArray((node.data as any)?.params?.api?.outputs)
			? ((node.data as any).params.api.outputs as any[])
			: [];
		const handle = String(handleId ?? '').trim();
		const decl =
			handle && handle !== 'out'
				? outputs.find((o) => String((o as any)?.name ?? '').trim() === handle)
				: null;
		const typed = (decl as any)?.typedSchema ?? null;
		const typedType = String((typed as any)?.type ?? '').trim().toLowerCase();
		if (typedType === 'table') {
			const fields = normalizeSchemaFields((typed as any)?.fields);
			const columns = schemaFieldNames(fields);
			return columns.length > 0 ? { type: 'table', fields, columns } : { type: 'table' };
		}
		if (typedType === 'json') return { type: 'json' };
		if (typedType === 'text') return { type: 'string' };
		if (typedType === 'binary') return { type: 'binary' };
		const declared = normalizeComponentPortType((decl as any)?.portType ?? null);
		if (declared === 'table') return { type: 'table' };
		if (declared === 'json') return { type: 'json' };
		if (declared === 'text') return { type: 'string' };
		if (declared === 'binary') return { type: 'binary' };
		// Component edge payload hint must come from selected API output only.
		return { type: 'unknown' };
	}
	const port = node.data.ports?.[whichPort] ?? null;
	if (port === 'table') {
		let fields: SchemaField[] | undefined;
		if (node.data.kind === 'source') {
			const params: any = node.data.params ?? {};
			const sourceKind = node.data.sourceKind;
			if (sourceKind === 'file') {
				const fileFormat = String(params?.file_format ?? 'csv').toLowerCase();
				if (fileFormat === 'txt') {
					fields = normalizeSchemaFields([{ name: 'text', type: 'string', nullable: true }]);
				}
				if (fileFormat === 'pdf') {
					fields = normalizeSchemaFields([
						{ name: 'page_number', type: 'integer', nullable: false },
						{ name: 'text', type: 'string', nullable: true },
						{ name: 'has_tables', type: 'boolean', nullable: false },
						{ name: 'table_count', type: 'integer', nullable: false },
						{ name: 'tables', type: 'json', nullable: true }
					]);
				}
			}
		}
		const columns = schemaFieldNames(fields ?? []);
		return fields && fields.length > 0 ? { type: 'table', fields, columns } : { type: 'table' };
	}
	if (port === 'json') return { type: 'json' };
	if (port === 'text') return { type: 'string' };
	if (port === 'binary') return { type: 'binary' };
	return undefined;
}

function targetPayloadHint(node: Node<PipelineNodeData>) {
	if (node.data.kind !== 'transform') return sourcePayloadHint(node, 'in');

	const params: any = node.data.params ?? {};
	const op = params?.op ?? node.data.transformKind;
	if (op === 'select') {
		const cols = params?.select?.columns;
		if (Array.isArray(cols) && cols.length > 0) {
			const requiredFields = makeSchemaFieldsFromColumns(cols);
			return {
				type: 'table',
				required_fields: requiredFields,
				required_columns: schemaFieldNames(requiredFields)
			};
		}
	}
	if (op === 'split') {
		const sourceColumn = String(params?.split?.sourceColumn ?? '').trim();
		if (sourceColumn) {
			const requiredFields = makeSchemaFieldsFromColumns([sourceColumn]);
			return {
				type: 'table',
				required_fields: requiredFields,
				required_columns: schemaFieldNames(requiredFields)
			};
		}
	}
	if (op === 'quality_gate') {
		const checks = Array.isArray(params?.quality_gate?.checks) ? params.quality_gate.checks : [];
		const required = new Set<string>();
		for (const check of checks) {
			const kind = String(check?.kind ?? '').trim().toLowerCase();
			if (kind === 'leakage') {
				const feature = String(check?.featureColumn ?? '').trim();
				const target = String(check?.targetColumn ?? '').trim();
				if (feature) required.add(feature);
				if (target) required.add(target);
				continue;
			}
			const column = String(check?.column ?? '').trim();
			if (column) required.add(column);
		}
		if (required.size > 0) {
			const requiredFields = makeSchemaFieldsFromColumns(Array.from(required));
			return {
				type: 'table',
				required_fields: requiredFields,
				required_columns: schemaFieldNames(requiredFields)
			};
		}
	}
	return sourcePayloadHint(node, 'in');
}

function normalizeHintType(raw: unknown): string {
	const value = String(raw ?? '').trim().toLowerCase();
	if (value === 'string') return 'text';
	return value;
}

type SchemaField = {
	name: string;
	type: string;
	nullable: boolean;
	constraints?: Record<string, unknown>;
};

function normalizeSchemaField(raw: unknown): SchemaField | null {
	if (!raw || typeof raw !== 'object') return null;
	const name = String((raw as any).name ?? '').trim();
	if (!name) return null;
	const typeRaw = String((raw as any).type ?? 'unknown').trim().toLowerCase();
	const type = typeRaw.length > 0 ? typeRaw : 'unknown';
	const nullable =
		typeof (raw as any).nullable === 'boolean'
			? Boolean((raw as any).nullable)
			: String(type).toLowerCase() === 'unknown';
	const constraints =
		(raw as any).constraints && typeof (raw as any).constraints === 'object'
			? ({ ...(raw as any).constraints } as Record<string, unknown>)
			: undefined;
	return { name, type, nullable, constraints };
}

function normalizeSchemaFields(raw: unknown): SchemaField[] {
	if (!Array.isArray(raw)) return [];
	const preferred = new Map<string, SchemaField>();
	for (const item of raw) {
		const field = normalizeSchemaField(item);
		if (!field) continue;
		const key = field.name.toLowerCase();
		const existing = preferred.get(key);
		if (!existing) {
			preferred.set(key, field);
			continue;
		}
		const existingUnknown = existing.type === 'unknown';
		const nextUnknown = field.type === 'unknown';
		if (existingUnknown && !nextUnknown) {
			preferred.set(key, field);
			continue;
		}
		if (existing.type === field.type && existing.nullable && !field.nullable) {
			preferred.set(key, field);
		}
	}
	return Array.from(preferred.values());
}

function schemaFieldNames(fields: SchemaField[]): string[] {
	return fields.map((field) => String(field.name ?? '').trim()).filter((name) => name.length > 0);
}

function makeSchemaFieldsFromColumns(columns: unknown, fallbackType = 'unknown'): SchemaField[] {
	if (!Array.isArray(columns)) return [];
	return normalizeSchemaFields(
		columns
			.map((col) => String(col ?? '').trim())
			.filter((name) => name.length > 0)
			.map((name) => ({ name, type: fallbackType, nullable: true }))
	);
}

export function __normalizeSchemaFieldsForTest(raw: unknown): SchemaField[] {
	return normalizeSchemaFields(raw);
}

type AdapterTransformKind = 'text_to_table' | 'json_to_table' | 'table_to_json';

function adapterKindForTypes(providedType: string, requiredType: string): AdapterTransformKind | null {
	const key = `${providedType}->${requiredType}`;
	if (key === 'text->table') return 'text_to_table';
	if (key === 'json->table') return 'json_to_table';
	if (key === 'table->json') return 'table_to_json';
	return null;
}

function adapterSuggestionForTypes(providedType: string, requiredType: string): string | null {
	const adapterKind = adapterKindForTypes(providedType, requiredType);
	if (adapterKind === 'text_to_table') return "Insert Transform adapter: op='text_to_table'.";
	if (adapterKind === 'json_to_table') return "Insert Transform adapter: op='json_to_table'.";
	if (adapterKind === 'table_to_json') return "Insert Transform adapter: op='table_to_json'.";
	return null;
}

type SchemaCompatibility =
	| { ok: true; warning?: 'lossy_coercion'; suggestion?: string | null; adapterKind?: AdapterTransformKind | null }
	| {
			ok: false;
			reason: 'type_mismatch' | 'missing_required_columns' | 'missing_typed_schema';
			missingColumns?: string[];
			suggestion?: string | null;
			adapterKind?: AdapterTransformKind | null;
	  };

function isSchemaCompatible(
	providedSchema: Record<string, any> | undefined,
	requiredSchema: Record<string, any> | undefined
): SchemaCompatibility {
	const providedType = normalizeHintType(providedSchema?.type ?? 'unknown');
	const requiredType = normalizeHintType(requiredSchema?.type ?? 'unknown');
	const coercion = evaluateSchemaCoercion(providedType, requiredType);
	if (!coercion.allowed) {
		const adapterKind = adapterKindForTypes(providedType, requiredType);
		return {
			ok: false,
			reason: 'type_mismatch',
			suggestion: adapterSuggestionForTypes(providedType, requiredType),
			adapterKind
		};
	}
	const providedFields = normalizeSchemaFields(providedSchema?.fields);
	const providedColumns =
		providedFields.length > 0
			? schemaFieldNames(providedFields)
			: Array.isArray(providedSchema?.columns)
				? providedSchema.columns
						.map((c: unknown) => String(c ?? '').trim())
						.filter((c: string) => c.length > 0)
				: [];
	const requiredFields = normalizeSchemaFields(requiredSchema?.required_fields);
	const requiredColumns =
		requiredFields.length > 0
			? schemaFieldNames(requiredFields)
			: Array.isArray(requiredSchema?.required_columns)
				? requiredSchema.required_columns
						.map((c: unknown) => String(c ?? '').trim())
						.filter((c: string) => c.length > 0)
				: [];
	if (requiredColumns.length > 0 && providedColumns.length === 0) {
		return { ok: false, reason: 'missing_typed_schema', missingColumns: requiredColumns };
	}
	if (requiredColumns.length > 0 && providedColumns.length > 0) {
		const missing = requiredColumns.filter((c) => !providedColumns.includes(c));
		if (missing.length > 0) {
			return { ok: false, reason: 'missing_required_columns', missingColumns: missing };
		}
	}
	if (coercion.lossy) {
		const adapterKind = adapterKindForTypes(providedType, requiredType);
		return {
			ok: true,
			warning: 'lossy_coercion',
			suggestion: adapterSuggestionForTypes(providedType, requiredType),
			adapterKind
		};
	}
	return { ok: true };
}

export type EdgeSchemaConstraint = {
	edgeId: string;
	sourceNodeId: string;
	targetNodeId: string;
	providedSchema: Record<string, any>;
	requiredSchema: Record<string, any>;
	compatible: boolean;
	warning?: 'lossy_coercion';
	adapterKind?: AdapterTransformKind | null;
	reason?: 'type_mismatch' | 'missing_required_columns' | 'missing_typed_schema';
	missingColumns?: string[];
	suggestions: string[];
};

export type EdgeSchemaDiagnostic = {
	edgeId: string;
	code: SchemaDiagnosticCode;
	severity: 'error' | 'warning';
	message: string;
	details: {
		providedSchema: Record<string, any>;
		requiredSchema: Record<string, any>;
		missingColumns?: string[];
	};
	suggestions: string[];
};

function inferredTransformOutputHint(node: Node<PipelineNodeData>): Record<string, any> | undefined {
	if (node.data.kind !== 'transform') return undefined;
	const params: any = node.data.params ?? {};
	const op = String(params?.op ?? node.data.transformKind ?? '').trim().toLowerCase();
	if (op === 'table_to_json') return { type: 'json' };
	if (op === 'json_to_table' || op === 'text_to_table') return { type: 'table' };
	if (op === 'split') {
		const outColumn = String(params?.split?.outColumn ?? 'part').trim() || 'part';
		const fields: SchemaField[] = [{ name: outColumn, type: 'string', nullable: true }];
		if (Boolean(params?.split?.emitIndex ?? true)) {
			fields.push({ name: 'index', type: 'integer', nullable: false });
		}
		if (Boolean(params?.split?.emitSourceRow ?? true)) {
			fields.push({ name: 'source_row', type: 'integer', nullable: false });
		}
		return { type: 'table', fields, columns: schemaFieldNames(fields) };
	}
	if (op === 'select') {
		const mode = String(params?.select?.mode ?? 'include').trim().toLowerCase();
		const fields = makeSchemaFieldsFromColumns(params?.select?.columns);
		if (mode === 'include' && fields.length > 0) {
			return { type: 'table', fields, columns: schemaFieldNames(fields) };
		}
	}
	return undefined;
}

function buildProvidedSchema(
	node: Node<PipelineNodeData>,
	sourceHandle: string
): Record<string, any> {
	return (
		inferredTransformOutputHint(node) ??
		(sourcePayloadHint(node as any, 'out', sourceHandle) as Record<string, any> | undefined) ??
		{ type: 'unknown' }
	);
}

function buildRequiredSchema(node: Node<PipelineNodeData>): Record<string, any> {
	return (targetPayloadHint(node as any) as Record<string, any> | undefined) ?? { type: 'unknown' };
}

function computeEdgeSchemaConstraintsInternal(
	nodes: Node<PipelineNodeData>[],
	edges: Edge<PipelineEdgeData>[]
): Record<string, EdgeSchemaConstraint> {
	const byNodeId = new Map(nodes.map((n) => [n.id, n]));
	const out: Record<string, EdgeSchemaConstraint> = {};
	for (const edge of edges) {
		const edgeId = String(edge.id ?? '');
		if (!edgeId) continue;
		const sourceNodeId = String(edge.source ?? '');
		const targetNodeId = String(edge.target ?? '');
		const sourceNode = byNodeId.get(sourceNodeId);
		const targetNode = byNodeId.get(targetNodeId);
		const sourceHandle = String((edge as any)?.sourceHandle ?? 'out').trim() || 'out';
		if (!sourceNode || !targetNode) continue;
		const providedSchema = buildProvidedSchema(sourceNode as any, sourceHandle);
		const requiredSchema = buildRequiredSchema(targetNode as any);
		const check = isSchemaCompatible(providedSchema, requiredSchema);
		out[edgeId] = {
			edgeId,
			sourceNodeId,
			targetNodeId,
			providedSchema,
			requiredSchema,
			compatible: check.ok,
			warning: check.ok ? check.warning : undefined,
			adapterKind: check.adapterKind ?? null,
			reason: check.ok ? undefined : check.reason,
			missingColumns: check.ok ? undefined : check.missingColumns,
			suggestions:
				check.ok
					? check.warning && check.suggestion
						? [check.suggestion]
						: []
					: check.reason === 'type_mismatch'
						? check.suggestion
							? [check.suggestion]
							: []
						: []
		};
	}
	return out;
}

export function __computeEdgeSchemaConstraintsForTest(
	nodes: Node<PipelineNodeData>[],
	edges: Edge<PipelineEdgeData>[]
): Record<string, EdgeSchemaConstraint> {
	return computeEdgeSchemaConstraintsInternal(nodes, edges);
}

function computeEdgeSchemaDiagnosticsInternal(
	constraints: Record<string, EdgeSchemaConstraint>
): Record<string, EdgeSchemaDiagnostic | null> {
	const out: Record<string, EdgeSchemaDiagnostic | null> = {};
	for (const [edgeId, constraint] of Object.entries(constraints ?? {})) {
		if (constraint.compatible) {
			if (constraint.warning === 'lossy_coercion') {
				out[edgeId] = {
					edgeId,
					code: 'TYPE_MISMATCH',
					severity: 'warning',
					message: `Lossy coercion: ${String(constraint.providedSchema?.type ?? 'unknown')} -> ${String(constraint.requiredSchema?.type ?? 'unknown')}`,
					details: {
						providedSchema: constraint.providedSchema,
						requiredSchema: constraint.requiredSchema
					},
					suggestions: constraint.suggestions ?? []
				};
				continue;
			}
			out[edgeId] = null;
			continue;
		}
		if (constraint.reason === 'missing_required_columns') {
			out[edgeId] = {
				edgeId,
				code: 'PAYLOAD_SCHEMA_MISMATCH',
				severity: 'error',
				message: `Missing required columns: ${(constraint.missingColumns ?? []).join(', ') || '(unknown)'}`,
				details: {
					providedSchema: constraint.providedSchema,
					requiredSchema: constraint.requiredSchema,
					missingColumns: constraint.missingColumns
				},
				suggestions: constraint.suggestions ?? []
			};
			continue;
		}
		if (constraint.reason === 'missing_typed_schema') {
			out[edgeId] = {
				edgeId,
				code: 'PAYLOAD_SCHEMA_MISMATCH',
				severity: 'error',
				message: `Required typed schema coverage is missing. Required columns: ${(constraint.missingColumns ?? []).join(', ') || '(unknown)'}`,
				details: {
					providedSchema: constraint.providedSchema,
					requiredSchema: constraint.requiredSchema,
					missingColumns: constraint.missingColumns
				},
				suggestions: constraint.suggestions ?? []
			};
			continue;
		}
		out[edgeId] = {
			edgeId,
			code: 'TYPE_MISMATCH',
			severity: 'error',
			message: `Incompatible schema types: ${String(constraint.providedSchema?.type ?? 'unknown')} -> ${String(constraint.requiredSchema?.type ?? 'unknown')}`,
			details: {
				providedSchema: constraint.providedSchema,
				requiredSchema: constraint.requiredSchema
			},
			suggestions: constraint.suggestions ?? []
		};
	}
	return out;
}

export function __computeEdgeSchemaDiagnosticsForTest(
	constraints: Record<string, EdgeSchemaConstraint>
): Record<string, EdgeSchemaDiagnostic | null> {
	return computeEdgeSchemaDiagnosticsInternal(constraints);
}

type EdgeInvalidReason =
	| 'missing_port_type' // couldn't resolve out/in
	| 'type_mismatch'
	| 'schema_mismatch'
	| 'typed_schema_missing';
type EdgeCheck =
	| { ok: true; out?: PortType; in?: PortType }
	| {
			ok: false;
			reason: EdgeInvalidReason;
			missingColumns?: string[];
			suggestion?: string | null;
			adapterKind?: AdapterTransformKind | null;
	  };

function isEdgeStillValid(nodes: Node<PipelineNodeData>[], e: Edge<PipelineEdgeData>): EdgeCheck {
	const outPort = sourcePortTypeForEdge(nodes, e);
	const inPort = getPortType(nodes, e.target, 'in');

	if (outPort == null || inPort == null) {
		return { ok: false, reason: 'missing_port_type' };
	}
	const strictV2 = Boolean(getStrictSchemaFeatureFlags().STRICT_SCHEMA_EDGE_CHECKS_V2 ?? true);
	if (!strictV2) {
		if (outPort !== inPort) return { ok: false, reason: 'type_mismatch' };
		return { ok: true, out: outPort, in: inPort };
	}

	const sourceNode = nodes.find((n) => n.id === e.source);
	const targetNode = nodes.find((n) => n.id === e.target);
	const sourcePayload = sourceNode
		? sourcePayloadHint(sourceNode as any, 'out', String((e as any).sourceHandle ?? 'out'))
		: undefined;
	const targetPayload = targetNode ? targetPayloadHint(targetNode as any) : undefined;
	const schemaCheck = isSchemaCompatible(sourcePayload as any, targetPayload as any);
	if (!schemaCheck.ok) {
		if (schemaCheck.reason === 'missing_typed_schema') {
			return {
				ok: false,
				reason: 'typed_schema_missing',
				missingColumns: schemaCheck.missingColumns
			};
		}
		if (schemaCheck.reason === 'missing_required_columns') {
			return {
				ok: false,
				reason: 'schema_mismatch',
				missingColumns: schemaCheck.missingColumns
			};
		}
		return {
			ok: false,
			reason: 'type_mismatch',
			suggestion: schemaCheck.suggestion,
			adapterKind: schemaCheck.adapterKind
		};
	}

	return { ok: true, out: outPort, in: inPort };
}

function resetEdgesExec(edges: Edge<PipelineEdgeData>[]): Edge<PipelineEdgeData>[] {
	return edges.map((e) => ({ ...e, data: { ...e.data, exec: 'idle' as EdgeExec } }));
}

function setEdgeExec(
	edges: Edge<PipelineEdgeData>[],
	edgeId: string,
	exec: 'idle' | 'active' | 'done'
) {
	return edges.map((e) => (e.id === edgeId ? { ...e, data: { ...e.data, exec: exec } } : e));
}

function downstreamIds(startId: string, edges: Edge<PipelineEdgeData>[]) {
	const adj = new Map<string, string[]>();
	for (const e of edges) adj.set(e.source, [...(adj.get(e.source) ?? []), e.target]);

	const seen = new Set<string>();
	const q = [startId];
	while (q.length) {
		const cur = q.shift()!;
		for (const nxt of adj.get(cur) ?? []) {
			if (!seen.has(nxt)) {
				seen.add(nxt);
				q.push(nxt);
			}
		}
	}
	return seen;
}

function pruneAndRecontractEdgesStrict(
	nodes: Node<PipelineNodeData>[],
	edges: Edge<PipelineEdgeData>[]
):
	| { ok: true; edges: Edge<PipelineEdgeData>[]; prunedIds: string[] }
	| { ok: false; error: string } {
	const next: Edge<PipelineEdgeData>[] = [];
	const prunedIds: string[] = [];

	for (const e of edges) {
		const chk = isEdgeStillValid(nodes, e);

		if (chk.ok === false) {
			if (chk.reason === 'type_mismatch' || chk.reason === 'schema_mismatch') {
				// allowed prune
				prunedIds.push(e.id);
				continue;
			}

			// NOT allowed to silently prune: graph invariants broken
			return {
				ok: false,
				error: `Edge ${e.id} has unresolved port types (source=${e.source}:${e.sourceHandle ?? 'out'} target=${e.target}:${e.targetHandle ?? 'in'})`
			};
		}

		next.push({
			...e,
			data: {
				...(e.data ?? {}),
				exec: e.data?.exec ?? 'idle',
				contract: {
					out: chk.out,
					in: chk.in,
					payload: {
						source: sourcePayloadHint(
							nodes.find((n) => n.id === e.source)! as any,
							'out',
							String((e as any).sourceHandle ?? 'out')
						),
						target: targetPayloadHint(nodes.find((n) => n.id === e.target)! as any)
					}
				}
			}
		});
	}

	return { ok: true, edges: next, prunedIds };
}

function canonicalizeComponentEdgeSourceHandles(
	nodes: Node<PipelineNodeData>[],
	edges: Edge<PipelineEdgeData>[],
	mode: 'strict' | 'best_effort'
):
	| { ok: true; edges: Edge<PipelineEdgeData>[] }
	| { ok: false; error: string } {
	const next: Edge<PipelineEdgeData>[] = [];
	for (const edge of edges) {
		const sourceNode = nodes.find((n) => n.id === edge.source);
		if (!sourceNode || sourceNode.data.kind !== 'component') {
			next.push(edge);
			continue;
		}
		const canonicalSourceHandle = canonicalComponentSourceHandleForEdge(nodes, edge);
		if (canonicalSourceHandle == null) {
			if (mode === 'strict') {
				return {
					ok: false,
					error: `Edge ${String(edge.id ?? '')} has unresolved component source handle (source=${String(edge.source ?? '')}:${String((edge as any).sourceHandle ?? 'out')})`
				};
			}
			next.push(edge);
			continue;
		}
		next.push({
			...edge,
			sourceHandle: canonicalSourceHandle
		});
	}
	return { ok: true, edges: next };
}

function recomputeEdgeContractsBestEffort(
	nodes: Node<PipelineNodeData>[],
	edges: Edge<PipelineEdgeData>[]
): Edge<PipelineEdgeData>[] {
	const canonicalized = canonicalizeComponentEdgeSourceHandles(nodes, edges, 'best_effort');
	const working = canonicalized.ok ? canonicalized.edges : edges;
	return working.map((edge) => {
		const sourceNode = nodes.find((n) => n.id === edge.source);
		const targetNode = nodes.find((n) => n.id === edge.target);
		if (!sourceNode || !targetNode) return edge;
		const chk = isEdgeStillValid(nodes, edge);
		const existingContract = ((edge.data ?? {}) as any).contract ?? {};
		const sourceHandle = String((edge as any).sourceHandle ?? 'out');
		const payload = {
			source: sourcePayloadHint(sourceNode as any, 'out', sourceHandle),
			target: targetPayloadHint(targetNode as any)
		};
		if (chk.ok) {
			return {
				...edge,
				data: {
					...(edge.data ?? {}),
					contract: {
						out: chk.out,
						in: chk.in,
						payload
					}
				}
			};
		}
		return {
			...edge,
			data: {
				...(edge.data ?? {}),
				contract: {
					out: existingContract?.out,
					in: existingContract?.in,
					payload
				}
			}
		};
	});
}

function topoFrom(
	nodes: Node<PipelineNodeData>[],
	edges: Edge<PipelineEdgeData>[],
	startId: string | null
) {
	const inDeg = new Map<string, number>();
	const adj = new Map<string, string[]>();

	for (const n of nodes) {
		inDeg.set(n.id, 0);
		adj.set(n.id, []);
	}

	for (const e of edges) {
		adj.get(e.source)!.push(e.target);
		inDeg.set(e.target, (inDeg.get(e.target) ?? 0) + 1);
	}

	const startSet = new Set<string>();
	if (startId) {
		startSet.add(startId);
		for (const d of downstreamIds(startId, edges)) startSet.add(d);
	} else {
		for (const [id, deg] of inDeg.entries()) if (deg === 0) startSet.add(id);
		const roots = [...startSet];
		for (const r of roots) for (const d of downstreamIds(r, edges)) startSet.add(d);
	}

	const inDeg2 = new Map<string, number>();
	for (const id of startSet) inDeg2.set(id, 0);
	for (const e of edges) {
		if (startSet.has(e.source) && startSet.has(e.target)) {
			inDeg2.set(e.target, (inDeg2.get(e.target) ?? 0) + 1);
		}
	}

	const q: string[] = [];
	for (const [id, deg] of inDeg2.entries()) if (deg === 0) q.push(id);

	const order: string[] = [];
	while (q.length) {
		const cur = q.shift()!;
		order.push(cur);
		for (const nxt of adj.get(cur) ?? []) {
			if (!startSet.has(nxt)) continue;
			const nd = (inDeg2.get(nxt) ?? 0) - 1;
			inDeg2.set(nxt, nd);
			if (nd === 0) q.push(nxt);
		}
	}

	if (order.length !== startSet.size) return [...startSet].sort();
	return order;
}

const loaded = loadGraphFromLocalStorage(emptyGraph);
const loadedNodes = Array.isArray((loaded as any)?.nodes)
	? ((loaded as any).nodes as Node<PipelineNodeData>[])
	: [];
const loadedEdgesRaw = Array.isArray((loaded as any)?.edges)
	? ((loaded as any).edges as Edge<PipelineEdgeData>[])
	: [];
const loadedCanonicalized = canonicalizeComponentEdgeSourceHandles(
	loadedNodes,
	loadedEdgesRaw,
	'best_effort'
);
const loadedEdges = recomputeEdgeContractsBestEffort(
	loadedNodes,
	loadedCanonicalized.ok ? loadedCanonicalized.edges : loadedEdgesRaw
);

const initialState: GraphState = {
	graphId: String((loaded as any)?.meta?.graphId ?? mintGraphId()),
	nodes: loadedNodes,
	edges: loadedEdges,
	selectedNodeId: null,
	inspector: initialInspector,
	logs: [],
	runStatus: IDLE,
	lastRunStatus: 'never_run',
	freshness: 'never_run',
	staleNodeCount: 0,
	activeRunMode: 'from_start',
	activeRunFrom: null,
	activeRunNodeSet: new Set<string>(),
	nodeOutputs: {},
	nodeBindings: ensureNormalizedBindingsForNodes(loadedNodes, {}),
	activeRunId: null,
	editingContext: 'graph',
	componentEditSession: null
};

export const graphStore = (() => {
	const { subscribe, set, update: rawUpdate } = writable<GraphState>(initialState);

	const update = (
		recipe: (state: GraphState) => GraphState,
		ctx: AuditContext = { source: 'unknown' }
	) =>
		rawUpdate((state) => {
			const next = recipe(state);
			auditStateTransition(state, next, ctx);
			return next;
		});

	function updateNodeConfigImpl(
		nodeId: string,
		config: { params?: unknown; ports?: { in?: PortType | null; out?: PortType | null } }
	) {
		let out: { ok: boolean; error?: string; removedEdgeIds?: string[] } = { ok: true };
		let invalidateFromPortChange = false;

		update((s) => {
			let nodes = s.nodes;
			let edges = s.edges;
			let removedEdgeIds: string[] = [];

			// 0) Ensure node exists
			const node = nodes.find((n) => n.id === nodeId);
			if (!node) {
				out = { ok: false, error: 'Node not found' };
				return logPush(s, 'warn', out.error!, nodeId);
			}
			const previousComponentOutputNames =
				node.data.kind === 'component' ? listComponentOutputNames(node as Node<PipelineNodeData>) : [];

			// ---- 1) params (must be valid to commit) ----
			if (config.params !== undefined) {
				const res = updateNodeParamsValidated(nodes, nodeId, config.params);
				if (res.error) {
					out = { ok: false, error: res.error };
					return logPush(s, 'error', res.error, nodeId);
				}
				nodes = res.nodes;
			}

			const currentNode = nodes.find((n) => n.id === nodeId) ?? node;
			const componentDerivedPorts =
				currentNode.data.kind === 'component' ? derivePortsFromComponentApi((currentNode.data as any)?.params?.api) : null;
			const effectivePorts = config.ports ?? componentDerivedPorts ?? undefined;
			const previousOutPort = (currentNode.data.ports?.out ?? null) as PortType | null;

			// ---- 2) ports (must be valid to commit) ----
			if (effectivePorts) {
				const { in: inPort, out: outPort } = effectivePorts;

				if (inPort !== undefined && inPort !== null && !isPortType(inPort)) {
					out = { ok: false, error: `Invalid input port type: ${String(inPort)}` };
					return logPush(s, 'warn', out.error!, nodeId);
				}
				if (outPort !== undefined && outPort !== null && !isPortType(outPort)) {
					out = { ok: false, error: `Invalid output port type: ${String(outPort)}` };
					return logPush(s, 'warn', out.error!, nodeId);
				}

				const allowedIn = getAllowedPortsForNode(node as any, 'in');
				const allowedOut = getAllowedPortsForNode(node as any, 'out');
				if (inPort !== undefined && inPort !== null && !allowedIn.includes(inPort)) {
					out = {
						ok: false,
						error: `${node.data.kind} input port '${String(inPort)}' is not supported`
					};
					return logPush(s, 'warn', out.error!, nodeId);
				}
				if (outPort !== undefined && outPort !== null && !allowedOut.includes(outPort)) {
					out = {
						ok: false,
						error: `${node.data.kind} output port '${String(outPort)}' is not supported`
					};
					return logPush(s, 'warn', out.error!, nodeId);
				}

				// capture connectivity BEFORE changes
				const incoming = edges.filter((e) => e.target === nodeId);
				const outgoing = edges.filter((e) => e.source === nodeId);

				nodes = nodes.map((n) => {
					if (n.id !== nodeId) return n;

					const curPorts = n.data.ports ?? {};
					const mergedPorts = {
						in: inPort !== undefined ? inPort : (curPorts.in ?? null),
						out: outPort !== undefined ? outPort : (curPorts.out ?? null)
					};

					return {
						...n,
						data: {
							...n.data,
							ports: mergedPorts,
							meta: { ...(n.data.meta ?? {}), updatedAt: new Date().toISOString() }
						}
					};
				});

				// NOW read what we actually set
				const updatedNode = nodes.find((n) => n.id === nodeId)!;
				const pin = updatedNode.data.ports?.in ?? null;
				const pout = updatedNode.data.ports?.out ?? null;
				const outPortChanged = previousOutPort !== (pout as PortType | null);

				if (updatedNode.data.kind === 'component') {
					const reconciled = reconcileComponentOutgoingEdges(
						nodeId,
						updatedNode as Node<PipelineNodeData>,
						edges,
						previousComponentOutputNames
					);
					edges = reconciled.edges;
					if (reconciled.removedIds.length) {
						removedEdgeIds = [...removedEdgeIds, ...reconciled.removedIds];
					}
				}

				// Source output port controls execution output mode and must stay in sync with params.
				if (outPortChanged && updatedNode.data.kind === 'source') {
					const nextMode = sourceOutputModeForPort(pout as PortType | null);
					if (!nextMode) {
						out = { ok: false, error: `Unsupported source output port '${String(pout)}'` };
						return logPush(s, 'warn', out.error!, nodeId);
					}
					const existingOutput = ((updatedNode.data.params as any)?.output ?? {}) as Record<string, unknown>;
					const paramsPatch = { output: { ...existingOutput, mode: nextMode } };
					const paramsRes = updateNodeParamsValidated(nodes, nodeId, paramsPatch);
					if (paramsRes.error) {
						out = { ok: false, error: paramsRes.error };
						return logPush(s, 'error', paramsRes.error, nodeId);
					}
					nodes = paramsRes.nodes;
					invalidateFromPortChange = true;
				}

				// Invariant: cannot null a port that is currently used by edges
				if (incoming.length > 0 && pin == null) {
					out = {
						ok: false,
						error: 'Cannot set input port to null while node has incoming edges.'
					};
					return logPush(s, 'warn', out.error!, nodeId);
				}
				if (outgoing.length > 0 && pout == null && updatedNode.data.kind !== 'component') {
					out = {
						ok: false,
						error: 'Cannot set output port to null while node has outgoing edges.'
					};
					return logPush(s, 'warn', out.error!, nodeId);
				}

				const pr = pruneAndRecontractEdgesStrict(nodes, edges);
				if (pr.ok === false) {
					out = { ok: false, error: pr.error };
					return logPush(s, 'warn', pr.error, nodeId);
				}
				edges = pr.edges;
				if (pr.prunedIds?.length) {
					removedEdgeIds = [...removedEdgeIds, ...pr.prunedIds];
				}
			}
			if (removedEdgeIds.length) {
				const uniq = Array.from(new Set(removedEdgeIds.filter((id) => id.length > 0)));
				out.removedEdgeIds = uniq;
			}

			const next = logPush({ ...s, nodes, edges }, 'info', 'Node config updated', nodeId);
			persist(next);
			return next;
		});

		if (out.ok && invalidateFromPortChange) {
			applyLocalStaleInvalidation(nodeId, 'PORTS_CHANGED');
		}

		return out;
	}

	type UpdateNodeConfig = {
		params?: unknown;
		ports?: {
			in?: PortType | null; // apply to all input handles
			out?: PortType | null; // apply to all output handles
		};
	};

	type PreviewUpdateResult =
		| {
			ok: true;
			prunedEdgeIds: string[];
			nextNodes: Node<PipelineNodeData>[];
			nextEdges: Edge<PipelineEdgeData>[];
		}
		| { ok: false; error: string };

	//BEGIN
	function canonicalInspectorDraftForNode(
		node: Node<PipelineNodeData & Record<string, unknown>> | undefined,
		params: Record<string, any>
	): Record<string, any> {
		if (!node) return params;
		if (node.data?.kind === 'component') {
			return sanitizeComponentDraftParams(params);
		}
		return params;
	}

	function patchInspectorDraft(
		patch: Record<string, any>,
		opts?: { intent?: InspectorDraftPatchIntent; notice?: string | null }
	) {
		update((s) => {
			if (!s.inspector.nodeId) return s;
			const node = s.nodes.find((n) => n.id === s.inspector.nodeId);
			const nextDraftParams = { ...s.inspector.draftParams, ...patch };
			const intent: InspectorDraftPatchIntent = opts?.intent ?? 'user_edit';
			const baselineCanonical = canonicalInspectorDraftForNode(
				node as any,
				structuredClone((node?.data?.params ?? {}) as Record<string, any>)
			);
			const nextCanonical = canonicalInspectorDraftForNode(node as any, structuredClone(nextDraftParams));
			const changedVsBaseline = JSON.stringify(nextCanonical) !== JSON.stringify(baselineCanonical);
			const changedVsCurrent = JSON.stringify(nextDraftParams) !== JSON.stringify(s.inspector.draftParams ?? {});
			const nextDirty = intent === 'system_canonicalize' ? Boolean(s.inspector.dirty) : changedVsBaseline;
			const nextSystemNotice =
				intent === 'system_canonicalize' && changedVsCurrent
					? String(opts?.notice ?? 'Bindings normalized automatically.')
					: intent === 'user_edit'
						? null
						: s.inspector.systemNotice ?? null;
			return {
				...s,
				inspector: {
					...s.inspector,
					draftParams: nextDraftParams,
					dirty: nextDirty,
					systemNotice: nextSystemNotice
				}
			};
		});
	}

	// optional: dropdown commit (keeps draft consistent + commits)
	async function commitInspectorImmediate(patch: Record<string, any>) {
		const s = get({ subscribe } as any) as GraphState;
		const nodeId = s.inspector.nodeId;
		if (!nodeId) return { ok: false, error: 'No node selected' };
		const targetNode = s.nodes.find((x) => x.id === nodeId);
		const commitPatch =
			targetNode?.data?.kind === 'component'
				? sanitizeComponentDraftParams(patch)
				: patch;
		if (patch?.op === 'dedupe' || patch?.dedupe || s.inspector.draftParams?.op === 'dedupe') {
			console.log('[dedupe-store] commitInspectorImmediate:patch', {
				nodeId,
				patch: commitPatch,
				draftParams: s.inspector.draftParams
			});
		}
		const beforeNode = targetNode;
		const beforeExecParams = effectiveExecParamsForNode(beforeNode);

		// 2) commit patch (validated/stripped)
		const result = updateNodeConfigImpl(nodeId, { params: commitPatch });
		if (!result.ok) return result;

		const afterState = get({ subscribe } as any) as GraphState;
		const paramsForSubmit = committedNodeParamsForNode(afterState, nodeId);
		if (paramsForSubmit?.op === 'dedupe') {
			console.log('[dedupe-store] commitInspectorImmediate:paramsForSubmit', {
				nodeId,
				paramsForSubmit
			});
		}
		update((cur) => {
			if (cur.inspector.nodeId !== nodeId) return cur;
			return {
				...cur,
				inspector: {
					...cur.inspector,
					draftParams: structuredClone(paramsForSubmit),
					dirty: false
				}
			};
		});

		await syncAcceptParamsForNode(nodeId, paramsForSubmit, beforeExecParams);
		return result;
	}

	async function commitSnapshotSelection(patch: Record<string, any>) {
		const s = get({ subscribe } as any) as GraphState;
		const nodeId = s.inspector.nodeId;
		if (!nodeId) return { ok: false, error: 'No node selected' };
		const beforeNode = s.nodes.find((x) => x.id === nodeId);
		const beforeExecParams = effectiveExecParamsForNode(beforeNode);

		// Commit only the provided snapshot-related patch; do not merge with pending draft.
		const result = updateNodeConfigImpl(nodeId, { params: patch });
		if (!result.ok) return result;

		const afterState = get({ subscribe } as any) as GraphState;
		const paramsForSubmit = committedNodeParamsForNode(afterState, nodeId);
		update((cur) => {
			if (cur.inspector.nodeId !== nodeId) return cur;
			return {
				...cur,
				inspector: {
					...cur.inspector,
					draftParams: structuredClone(paramsForSubmit),
					dirty: false
				}
			};
		});

		await syncAcceptParamsForNode(nodeId, paramsForSubmit, beforeExecParams);
		return result;
	}

function applyLocalStaleInvalidation(nodeId: string, rootReason: string = 'PARAMS_CHANGED'): void {
		update((cur) => {
			const candidateIds = downstreamNodeIds(cur.edges, nodeId);
			const nodeBindings = { ...cur.nodeBindings };
			let nodeOutputs = { ...cur.nodeOutputs };
			let changed = false;
			for (const affectedId of candidateIds) {
				const prev = _normalizeBinding(nodeBindings[affectedId], affectedId);
				const hadArtifact = Boolean(prev.current?.artifactId || prev.last?.artifactId);
				if (!hadArtifact && affectedId !== nodeId) continue;
				if (isNodeStateFromActiveRunAndFresh(cur, prev)) continue;
				let next = {
					...prev,
					status: 'stale',
					isUpToDate: false,
					cacheValid: false,
					currentRunId: null,
					staleReason: affectedId === nodeId ? rootReason : 'UPSTREAM_CHANGED'
				};
				next = _withPair(next, 'current', { execKey: null, artifactId: null });
				_assertBindingPairInvariant(next, affectedId, 'applyLocalStaleInvalidation');
				nodeBindings[affectedId] = next;
				nodeOutputs = clearNodeCacheUi(nodeOutputs, affectedId);
				changed = true;
			}
			if (!changed) return cur;
			// Keep existing previews while stale so users can compare last known outputs.
			return withGraphMeta({ ...cur, nodeBindings, nodeOutputs });
		}, { source: 'accept_params', expectedDirtyTransition: true });
	}

function applyBackendAffectedStale(affectedNodeIds: string[], rootNodeId: string): void {
		if (!Array.isArray(affectedNodeIds) || affectedNodeIds.length === 0) return;
		update((cur) => {
			const nodeBindings = { ...cur.nodeBindings };
			const touchedIds: string[] = [];
			for (const affectedId of affectedNodeIds) {
				const prev = _normalizeBinding(nodeBindings[affectedId], affectedId);
				if (isNodeStateFromActiveRunAndFresh(cur, prev)) continue;
				let next = {
					...prev,
					status: 'stale',
					isUpToDate: false,
					cacheValid: false,
					currentRunId: null,
					staleReason: affectedId === rootNodeId ? 'PARAMS_CHANGED' : 'UPSTREAM_CHANGED'
				};
				next = _withPair(next, 'current', { execKey: null, artifactId: null });
				_assertBindingPairInvariant(next, affectedId, 'applyBackendAffectedStale');
				nodeBindings[affectedId] = next;
				touchedIds.push(affectedId);
			}
			const nodeOutputs =
				touchedIds.length > 0
					? clearNodeCacheUiForNodes({ ...cur.nodeOutputs }, touchedIds)
					: cur.nodeOutputs;
			return withGraphMeta({ ...cur, nodeBindings, nodeOutputs });
		}, { source: 'accept_params', allowedNodeIds: new Set(affectedNodeIds), expectedDirtyTransition: true });
	}

	function applySourceRehydration(nodeId: string, resolved: {
		execKey: string;
		artifactId: string | null;
		artifact?: { mimeType?: string; portType?: string };
	}): void {
		if (!resolved.artifactId) return;
		update((cur) => {
			const prevBinding = _normalizeBinding(cur.nodeBindings?.[nodeId], nodeId);
			let nextBinding: NormalizedNodeBinding = {
				...prevBinding,
				status: 'succeeded_up_to_date',
				cacheValid: true,
				isUpToDate: true,
				staleReason: null
			};
			nextBinding = _withPair(nextBinding, 'current', {
				execKey: resolved.execKey,
				artifactId: resolved.artifactId
			});
			nextBinding = _withPair(nextBinding, 'last', {
				execKey: resolved.execKey,
				artifactId: resolved.artifactId
			});
			_assertBindingPairInvariant(nextBinding, nodeId, 'applySourceRehydration');
			const nodeBindings = {
				...cur.nodeBindings,
				[nodeId]: nextBinding
			};
			const prevOut: NodeOutputInfo | undefined = cur.nodeOutputs?.[nodeId];
			const nodeOutputs = {
				...cur.nodeOutputs,
				[nodeId]: {
					...prevOut,
					mimeType: resolved.artifact?.mimeType ?? prevOut.mimeType,
					portType: resolved.artifact?.portType ?? prevOut.portType,
					preview: undefined,
					cached: true,
					cacheDecision: 'cache_hit' as const
				}
			};
			return withGraphMeta({ ...cur, nodeBindings, nodeOutputs });
		}, { source: 'accept_params', expectedDirtyTransition: true, allowedNodeIds: new Set([nodeId]) });
	}

	async function syncAcceptParamsForNode(
		nodeId: string,
		paramsForSubmit: Record<string, any>,
		beforeExecParams: Record<string, unknown>
	): Promise<void> {
		if (paramsForSubmit?.op === 'dedupe') {
			console.log('[dedupe-store] syncAcceptParamsForNode:begin', {
				nodeId,
				paramsForSubmit
			});
		}
		const st = get({ subscribe } as any) as GraphState;
		const afterNode = st.nodes.find((x) => x.id === nodeId);
		const afterExecParams = effectiveExecParamsForNode(afterNode);
		const execInputsChanged = stableJson(beforeExecParams) !== stableJson(afterExecParams);
		if (!execInputsChanged) return;
		const isSourceFile =
			String((afterNode as any)?.data?.kind ?? '') === 'source' &&
			String((afterNode as any)?.data?.sourceKind ?? 'file') === 'file';

		// Even when no active backend run handle exists, keep local UI and previews honest.
		if (!st.activeRunId) {
			applyLocalStaleInvalidation(nodeId);
			if (isSourceFile) {
				try {
					const resolved = await resolveSourceNode({
						graphId: st.graphId,
						graph: { version: 1, nodes: st.nodes, edges: st.edges },
						nodeId,
						params: paramsForSubmit
					});
					applySourceRehydration(nodeId, resolved);
				} catch {
					// keep stale state on resolve failure
				}
			}
			return;
		}

		try {
			const resp = await acceptNodeParams({
				runId: st.activeRunId,
				nodeId,
				graph: { version: 1, nodes: st.nodes, edges: st.edges },
				params: paramsForSubmit
			});
			applyBackendAffectedStale(resp.affectedNodeIds ?? [], nodeId);
			const snap = await getRun(st.activeRunId);
			update((cur) => hydrateFromRunSnapshot(cur, snap), {
				source: 'hydrate_snapshot',
				snapshotNodeIds: new Set(Object.keys(snap.nodeBindings ?? {}))
			});
			if (isSourceFile) {
				try {
					const resolved = await resolveSourceNode({
						graphId: st.graphId,
						graph: { version: 1, nodes: st.nodes, edges: st.edges },
						nodeId,
						params: paramsForSubmit
					});
					applySourceRehydration(nodeId, resolved);
				} catch {
					// keep stale state on resolve failure
				}
			}
		} catch (e) {
			// Backend sync failed; still keep local UX in stale state for changed effective inputs.
			applyLocalStaleInvalidation(nodeId);
			update((cur) => logPush(cur, 'warn', `accept-params sync failed: ${String(e)}`, nodeId));
		}
	}

	async function applyInspectorDraft() {
		const s = get({ subscribe } as any) as GraphState;
		const nodeId = s.inspector.nodeId;
		if (!nodeId) return { ok: false, error: 'No node selected' };
		const beforeNode = s.nodes.find((x) => x.id === nodeId);
		const beforeExecParams = effectiveExecParamsForNode(beforeNode);
		const paramsForCommit =
			beforeNode?.data?.kind === 'component'
				? sanitizeComponentDraftParams(s.inspector.draftParams as Record<string, any>)
				: (s.inspector.draftParams as Record<string, any>);
		if (beforeNode?.data?.kind === 'component') {
			const validation = validateComponentDraftForAccept(paramsForCommit);
			if (!validation.ok) {
				update((cur) => {
					let next = cur;
					for (const issue of validation.errors) {
						next = logPush(next, 'warn', issue, nodeId);
					}
					return next;
				});
				return {
					ok: false,
					reason: 'component_accept_blocked',
					error: validation.errors[0] ?? 'Component output bindings are invalid.',
					details: validation.errors
				} as const;
			}
		}

		const r = updateNodeConfigImpl(nodeId, { params: paramsForCommit });

		// only clear dirty if commit succeeded (fail-closed keeps draft)
		if (r.ok) {
			update((st) => {
				const n = st.nodes.find((x) => x.id === nodeId);
				return {
					...st,
					inspector: {
						nodeId,
						draftParams: structuredClone((n?.data.params ?? {}) as any),
						dirty: false,
						uiByNodeId: st.inspector.uiByNodeId
					}
				};
			});

			await syncAcceptParamsForNode(nodeId, paramsForCommit, beforeExecParams);
		}
		return r;
	}

	function revertInspectorDraft() {
		update((s) => {
			const nodeId = s.inspector.nodeId;
			if (!nodeId) return s;
			const n = s.nodes.find((x) => x.id === nodeId);
			return {
				...s,
				inspector: {
					nodeId,
					draftParams: structuredClone((n?.data.params ?? {}) as any),
					dirty: false,
					uiByNodeId: s.inspector.uiByNodeId
				}
			};
		});
	}

	//END

	function persist(state: GraphState) {
		saveGraphToLocalStorage(stripToDTO(state.nodes, state.edges, state.graphId));
	}

	function applyGraphDocument(
		graph: { nodes: unknown[]; edges: unknown[] },
		graphIdOverride?: string | null
	): { ok: boolean; reason?: string } {
		const nextNodes = Array.isArray(graph?.nodes) ? (graph.nodes as Node<PipelineNodeData>[]) : null;
		const nextEdges = Array.isArray(graph?.edges) ? (graph.edges as Edge<PipelineEdgeData>[]) : null;
		if (!nextNodes || !nextEdges) return { ok: false, reason: 'invalid_payload' };
		const normalized = normalizeGraphForComponentMigration(nextNodes, nextEdges);
		const canonicalized = canonicalizeComponentEdgeSourceHandles(normalized.nodes, normalized.edges, 'strict');
		if (!canonicalized.ok) return { ok: false, reason: canonicalized.error };
		const rechecked = pruneAndRecontractEdgesStrict(normalized.nodes, canonicalized.edges);
		if (!rechecked.ok) return { ok: false, reason: rechecked.error };
		update((s) => {
			const nextState = withGraphMeta({
				...s,
				graphId: String(graphIdOverride || s.graphId),
				nodes: normalized.nodes,
				edges: rechecked.edges,
				selectedNodeId: null,
				inspector: { ...initialInspector, uiByNodeId: s.inspector.uiByNodeId },
				logs: [],
				runStatus: IDLE,
				lastRunStatus: 'never_run',
				freshness: 'never_run',
				staleNodeCount: 0,
				activeRunMode: 'from_start',
				activeRunFrom: null,
				activeRunNodeSet: new Set<string>(),
				nodeOutputs: {},
				nodeBindings: ensureNormalizedBindingsForNodes(normalized.nodes as any, {}),
				activeRunId: null,
				editingContext: 'graph',
				componentEditSession: null
			});
			persist(nextState);
			return nextState;
		}, { source: 'graph_edit' });
		return { ok: true };
	}

	function hydrateFromRunSnapshot(
		state: GraphState,
		snap: RunSnapshotLike
	): GraphState {
		return hydrateFromRunSnapshotState(state, snap);
	}

	function getInspectorUi(nodeId: string, paramsHint?: Record<string, any>): ApiEditorUiState {
		const state = get({ subscribe } as any) as GraphState;
		const existing = state.inspector.uiByNodeId?.[nodeId];
		if (existing) return existing;
		const node = state.nodes.find((n) => n.id === nodeId);
		const params = paramsHint ?? ((node?.data?.params ?? {}) as Record<string, any>);
		return defaultApiEditorUiState(params);
	}

	function setInspectorUi(nodeId: string, patch: Partial<ApiEditorUiState>): void {
		update((s) => {
			const node = s.nodes.find((n) => n.id === nodeId);
			const base =
				s.inspector.uiByNodeId?.[nodeId] ??
				defaultApiEditorUiState((node?.data?.params ?? {}) as Record<string, any>);
			return {
				...s,
				inspector: {
					...s.inspector,
					uiByNodeId: {
						...(s.inspector.uiByNodeId ?? {}),
						[nodeId]: { ...base, ...patch }
					}
				}
			};
		});
	}

	function applySemanticSubtypeReset(
		nodeId: string,
		payload: Record<string, unknown>
	): void {
		applyLocalStaleInvalidation(nodeId, 'KIND_CHANGED');
		if (DEV_MODE) {
			const st = get({ subscribe } as any) as GraphState;
			const b = st.nodeBindings?.[nodeId];
			const o = st.nodeOutputs?.[nodeId];
			console.debug('[graphStore][subtype-switch] post-invalidate', {
				nodeId,
				...payload,
				status: b?.status,
				isUpToDate: b?.isUpToDate,
				cacheDecision: o?.cacheDecision,
				cached: o?.cached,
				currentArtifactId: b?.current?.artifactId ?? b?.currentArtifactId ?? null,
				currentExecKey: b?.current?.execKey ?? b?.currentExecKey ?? null,
				lastArtifactId: b?.last?.artifactId ?? b?.lastArtifactId ?? null
			});
		}
	}

	return {
		subscribe,
		patchInspectorDraft,
		commitInspectorImmediate,
		commitSnapshotSelection,
		applyInspectorDraft,
		revertInspectorDraft,
		getInspectorDraftAcceptValidation(stateOverride?: GraphState): InspectorDraftAcceptValidation {
			const state = stateOverride ?? (get({ subscribe } as any) as GraphState);
			return validateInspectorDraftForAccept(state);
		},
		getSavePreflight(stateOverride?: GraphState): SavePreflightResult {
			const state = stateOverride ?? (get({ subscribe } as any) as GraphState);
			return buildSavePreflightDiagnostics(state.nodes as any, state.edges as any);
		},
		getInspectorUi,
		setInspectorUi,
		resolveNodeInputs(nodeId: string): InputResolution[] {
			const s = get({ subscribe } as any) as GraphState;
			return resolveNodeInputsFromState(s, nodeId);
		},
		updateNodeConfig: updateNodeConfigImpl,

		setSourceKind(nodeId: string, nextKind: SourceKind) {
			const nextParams = structuredClone(defaultSourceParamsByKind[nextKind]);

			// 1) update structural subtype on the node
			update((s) => {
				const node = s.nodes.find((n) => n.id === nodeId);
				if (!node) return logPush(s, 'warn', 'Node not found', nodeId);

				const nodes = s.nodes.map((n) =>
					n.id === nodeId
						? {
							...n,
							data: {
								...n.data,
								sourceKind: nextKind, // âœ… structural
								meta: { ...(n.data.meta ?? {}), updatedAt: new Date().toISOString() }
							}
						}
						: n
				);

				const next = { ...s, nodes };
				persist(next);
				return next;
			});

			// 2) replace params via your validated path (schema stripping happens here)
			const r = updateNodeConfigImpl(nodeId, { params: nextParams });
			if (r.ok) {
				applySemanticSubtypeReset(nodeId, { kind: 'source', sourceKind: nextKind });
			}

			// 3) ensure inspector draft matches immediately after type switch
			if (r.ok) {
				update((s) => {
					const n = s.nodes.find((x) => x.id === nodeId);
					return {
						...s,
						inspector: {
							nodeId,
							draftParams: structuredClone((n?.data.params ?? {}) as any),
							dirty: false,
							uiByNodeId: s.inspector.uiByNodeId
						}
					};
				});
			}
			return r;
		},

		// graphStore.ts (inside your graphStore object)
		setLlmKind(nodeId: string, nextKind: LlmKind) {
			const nextParams = structuredClone(defaultLlmParamsByKind[nextKind]);

			// 1) update structural subtype on the node
			update((s) => {
				const node = s.nodes.find((n) => n.id === nodeId);
				if (!node) return logPush(s, 'warn', 'Node not found', nodeId);

				const nodes = s.nodes.map((n) =>
					n.id === nodeId
						? {
							...n,
							data: {
								...n.data,
								llmKind: nextKind, // âœ… structural
								meta: { ...(n.data.meta ?? {}), updatedAt: new Date().toISOString() }
							}
						}
						: n
				);

				const next = { ...s, nodes };
				persist(next);
				return next;
			});

			// 2) replace params via your validated path (schema stripping happens here)
			const r = updateNodeConfigImpl(nodeId, { params: nextParams });
			if (r.ok) {
				applySemanticSubtypeReset(nodeId, { kind: 'llm', llmKind: nextKind });
			}

			// 3) ensure inspector draft matches immediately after type switch
			if (r.ok) {
				update((s) => {
					const n = s.nodes.find((x) => x.id === nodeId);
					return {
						...s,
						inspector: {
							nodeId,
							draftParams: structuredClone((n?.data.params ?? {}) as any),
							dirty: false,
							uiByNodeId: s.inspector.uiByNodeId
						}
					};
				});
			}

			return r;
		},

		// graphStore.ts (inside your graphStore object)
		setTransformKind(nodeId: string, nextKind: TransformKind) {
			const nextParams = structuredClone(defaultTransformParamsByKind[nextKind]);
			const nextPorts: { in: PortType; out: PortType } =
				nextKind === 'json_to_table'
					? { in: 'json', out: 'table' }
					: nextKind === 'text_to_table'
						? { in: 'text', out: 'table' }
						: nextKind === 'table_to_json'
							? { in: 'table', out: 'json' }
							: { in: 'table', out: 'table' };

			// 1) update structural subtype on the node
			update((s) => {
				const node = s.nodes.find((n) => n.id === nodeId);
				if (!node) return logPush(s, 'warn', 'Node not found', nodeId);

				const nodes = s.nodes.map((n) =>
					n.id === nodeId
						? {
							...n,
							data: {
								...n.data,
								transformKind: nextKind, // âœ… structural
								ports: nextPorts,
								meta: { ...(n.data.meta ?? {}), updatedAt: new Date().toISOString() }
							}
						}
						: n
				);

				const next = { ...s, nodes };
				persist(next);
				return next;
			});

			// 2) replace params via your validated path (schema stripping happens here)
			const r = updateNodeConfigImpl(nodeId, { params: nextParams });
			if (r.ok) {
				applySemanticSubtypeReset(nodeId, { kind: 'transform', transformKind: nextKind });
			}

			// 3) ensure inspector draft matches immediately after type switch
			if (r.ok) {
				update((s) => {
					const n = s.nodes.find((x) => x.id === nodeId);
					return {
						...s,
						inspector: {
							nodeId,
							draftParams: structuredClone((n?.data.params ?? {}) as any),
							dirty: false,
							uiByNodeId: s.inspector.uiByNodeId
						}
					};
				});
			}

			return r;
		},

		setToolProvider(nodeId: string, nextProvider: ToolProvider) {
			const nextParams = structuredClone(defaultToolParamsByProvider[nextProvider]);
			const r = updateNodeConfigImpl(nodeId, { params: nextParams });
			if (r.ok) {
				applySemanticSubtypeReset(nodeId, { kind: 'tool', provider: nextProvider });
			}

			if (r.ok) {
				update((s) => {
					const n = s.nodes.find((x) => x.id === nodeId);
					return {
						...s,
						inspector: {
							nodeId,
							draftParams: structuredClone((n?.data.params ?? {}) as any),
							dirty: false,
							uiByNodeId: s.inspector.uiByNodeId
						}
					};
				});
			}

			return r;
		},

		setToolKind(nodeId: string, nextProvider: ToolProvider) {
			return this.setToolProvider(nodeId, nextProvider);
		},

		// ----- sync entrypoints (because SvelteFlow uses bind:nodes/bind:edges) -----
		syncFromCanvas(nodes: Node<PipelineNodeData>[], edges: Edge<PipelineEdgeData>[]) {
			update((s) => {
				const nextEdges = shouldPreserveStoreEdgesOnCanvasSync(s.edges, edges) ? s.edges : edges;
				// avoid needless churn if same references
				if (s.nodes === nodes && s.edges === nextEdges) return s;
				const next = {
					...s,
					nodes,
					edges: nextEdges,
					nodeBindings: ensureNormalizedBindingsForNodes(nodes, s.nodeBindings ?? {})
				};
				persist(next);
				return next;
			});
		},

		// ----- selection -----
		selectNode(nodeId: string | null) {
			update((s) => {
				if (!nodeId) {
					return {
						...s,
						selectedNodeId: null,
						inspector: { ...initialInspector, uiByNodeId: s.inspector.uiByNodeId }
					};
				}

				const n = s.nodes.find((x) => x.id === nodeId);
				return {
					...s,
					selectedNodeId: nodeId,
					inspector: {
						nodeId,
						draftParams: structuredClone((n?.data.params ?? {}) as any),
						dirty: false,
						uiByNodeId: s.inspector.uiByNodeId
					}
				};
			});
		},

		// ----- node CRUD -----
		addNode(kind: NodeKind, position: { x: number; y: number }) {
			const id = `n_${crypto.randomUUID()}`;
			const node: Node<PipelineNodeData> = {
				id,
				type: kind,
				position,
				data: defaultNodeData(kind)
			};

			update((s) => {
				const nodeBindings = {
					...s.nodeBindings,
					[id]: _normalizeBinding(s.nodeBindings?.[id], id)
				};
				const next = logPush(
					{ ...s, nodes: [...s.nodes, node], selectedNodeId: id, nodeBindings },
					'info',
					`Added node ${id} (${kind})`,
					id
				);
				persist(next);
				return next;
			});

			return id;
		},

		deleteNode(nodeId: string) {
			update((s) => {
				const nodes = s.nodes.filter((n) => n.id !== nodeId);
				const edges = s.edges.filter((e) => e.source !== nodeId && e.target !== nodeId);
				const selectedNodeId = s.selectedNodeId === nodeId ? null : s.selectedNodeId;
				const { [nodeId]: _dropBinding, ...nodeBindings } = s.nodeBindings;
				const { [nodeId]: _dropOutput, ...nodeOutputs } = s.nodeOutputs;

				const next = logPush(
					{ ...s, nodes, edges, selectedNodeId, nodeBindings, nodeOutputs },
					'info',
					`Deleted node ${nodeId}`,
					nodeId
				);
				const withMeta = withGraphMeta(next);
				persist(withMeta);
				return withMeta;
			});
		},

		// ----- edge CRUD -----
		deleteEdge(edgeId: string) {
			update((s) => {
				const edges = s.edges.filter((e) => e.id !== edgeId);
				const next = logPush({ ...s, edges }, 'info', `Deleted edge ${edgeId}`);
				persist(next);
				return next;
			});
		},

		addEdge(edge: Edge<PipelineEdgeData>) {
			let out: {
				ok: boolean;
				id?: string;
				error?: string;
				suggestion?: string | null;
				adapterKind?: AdapterTransformKind | null;
			} = { ok: true };
			update((s) => {
				const strictV2 = Boolean(getStrictSchemaFeatureFlags().STRICT_SCHEMA_EDGE_CHECKS_V2 ?? true);
				// basic sanity checks
				const sourceExists = s.nodes.some((n) => n.id === edge.source);
				const targetExists = s.nodes.some((n) => n.id === edge.target);
				if (!sourceExists || !targetExists) {
					out = { ok: false, error: 'Source or target node not found' };
					return s;
				}

				// default id if absent
				const id = edge.id ?? `e_${crypto.randomUUID()}`;

				// duplicate id?
				if (s.edges.some((ee) => ee.id === id)) {
					out = { ok: false, error: 'Edge id already exists' };
					return s;
				}

				// no self-connection
				if (edge.source === edge.target) {
					out = { ok: false, error: 'Cannot connect node to itself' };
					return s;
				}

				// basic cycle prevention: if target reaches source already, adding would create cycle
				const adj = new Map<string, string[]>();
				for (const ee of s.edges) adj.set(ee.source, [...(adj.get(ee.source) ?? []), ee.target]);

				const seen = new Set<string>();
				const q = [edge.target];
				let createsCycle = false;
				while (q.length) {
					const cur = q.shift()!;
					if (cur === edge.source) {
						createsCycle = true;
						break;
					}
					for (const nxt of adj.get(cur) ?? []) {
						if (!seen.has(nxt)) {
							seen.add(nxt);
							q.push(nxt);
						}
					}
				}
				if (createsCycle) {
					out = { ok: false, error: 'Connection would create a cycle' };
					return s;
				}

				const canonicalSourceHandle = canonicalComponentSourceHandleForEdge(
					s.nodes,
					{ ...edge, id } as Edge<PipelineEdgeData>
				);
				if (canonicalSourceHandle == null) {
					out = {
						ok: false,
						error: 'Component output handle is required and must match a declared output.'
					};
					return s;
				}
				const edgeForValidation: Edge<PipelineEdgeData> = {
					...edge,
					id,
					sourceHandle: canonicalSourceHandle
				};

				// validate port types + refresh contract
				const chk = isEdgeStillValid(s.nodes, edgeForValidation);
				if (chk.ok === false) {
					out = {
						ok: false,
						suggestion: chk.suggestion,
						adapterKind: chk.adapterKind,
						error:
							chk.reason === 'type_mismatch'
								? `Incompatible schemas${chk.suggestion ? `. ${chk.suggestion}` : ''}`
								: chk.reason === 'schema_mismatch'
									? `Missing required columns: ${(chk.missingColumns ?? []).join(', ') || '(unknown)'}`
								: 'Cannot resolve port types for this connection'
					};
					if (!strictV2) return s;
					return logPush(
						s,
						'info',
						`[schema-edge-checks-v2] decision=block edge=${id} reason=${chk.reason}`,
						edge.source
					);
				}
				const sourceNode = s.nodes.find((n) => n.id === edgeForValidation.source)!;
				const targetNode = s.nodes.find((n) => n.id === edgeForValidation.target)!;
				const sourceHint = sourcePayloadHint(
					sourceNode as any,
					'out',
					String((edgeForValidation as any).sourceHandle ?? 'out')
				);
				const targetHint = targetPayloadHint(targetNode as any);
				const providedType = normalizeHintType(sourceHint?.type ?? chk.out ?? 'unknown');
				const requiredType = normalizeHintType(targetHint?.type ?? chk.in ?? 'unknown');
				const coercion = evaluateSchemaCoercion(providedType, requiredType);
				const adapterKind = adapterKindForTypes(providedType, requiredType);
				if (adapterKind) {
					out.adapterKind = adapterKind;
					out.suggestion = adapterSuggestionForTypes(providedType, requiredType);
				}

				const nextEdge: Edge<PipelineEdgeData> = {
					...edgeForValidation,
					id,
					data: {
						...(edge.data ?? {}),
						exec: edge.data?.exec ?? 'idle',
						contract: {
							out: chk.out,
							in: chk.in,
							payload: {
								source: sourceHint,
								target: targetHint
							}
						}
					}
				};

				const decision = adapterKind ? 'adapter' : coercion.mode === 'native' ? 'native' : 'coerced';
				let nextState: GraphState = { ...s, edges: [...s.edges, nextEdge] };
				if (strictV2) {
					nextState = logPush(
						nextState,
						'info',
						`[schema-edge-checks-v2] decision=${decision} edge=${id} source=${providedType} target=${requiredType}`
					);
				}
				const next = logPush(nextState, 'info', `Added edge ${id}`);
				persist(next);
				out.id = id;
				return next;
			});

			return out;
		},

		insertSchemaAdapterForEdgeConnection(input: {
			source: string;
			target: string;
			sourceHandle?: string | null;
			targetHandle?: string | null;
			adapterKind?: AdapterTransformKind | null;
		}) {
			const source = String(input?.source ?? '').trim();
			const target = String(input?.target ?? '').trim();
			if (!source || !target) {
				return { ok: false as const, error: 'Missing source or target for adapter insertion' };
			}

			const state = get({ subscribe } as any) as GraphState;
			const sourceNode = state.nodes.find((n) => n.id === source);
			const targetNode = state.nodes.find((n) => n.id === target);
			if (!sourceNode || !targetNode) {
				return { ok: false as const, error: 'Source or target node not found' };
			}

			const sourceHandleRaw = String(input?.sourceHandle ?? '').trim();
			const sourceHandle = sourceHandleRaw.length > 0 ? sourceHandleRaw : undefined;
			const targetHandleRaw = String(input?.targetHandle ?? '').trim();
			const targetHandle = targetHandleRaw.length > 0 ? targetHandleRaw : undefined;
			const sourceHint = sourcePayloadHint(sourceNode as any, 'out', sourceHandle ?? 'out');
			const targetHint = targetPayloadHint(targetNode as any);
			const providedType = normalizeHintType(sourceHint?.type ?? sourceNode.data?.ports?.out ?? 'unknown');
			const requiredType = normalizeHintType(targetHint?.type ?? targetNode.data?.ports?.in ?? 'unknown');
			const adapterKind = (input?.adapterKind ?? adapterKindForTypes(providedType, requiredType)) as
				| AdapterTransformKind
				| null;
			if (!adapterKind) {
				return {
					ok: false as const,
					error: `No adapter available for ${providedType}->${requiredType}`
				};
			}

			const midX = (Number(sourceNode.position?.x ?? 0) + Number(targetNode.position?.x ?? 0)) / 2;
			const midY = (Number(sourceNode.position?.y ?? 0) + Number(targetNode.position?.y ?? 0)) / 2;
			const adapterNodeId = this.addNode('transform', { x: midX, y: midY });
			const subtypeRes = this.setTransformKind(adapterNodeId, adapterKind);
			if (!subtypeRes.ok) {
				this.deleteNode(adapterNodeId);
				return {
					ok: false as const,
					error: String(subtypeRes.error ?? 'Failed to configure adapter node')
				};
			}

			const incomingRes = this.addEdge({
				id: `e_${crypto.randomUUID()}`,
				source,
				target: adapterNodeId,
				sourceHandle,
				targetHandle: 'in',
				data: { exec: 'idle' }
			} as Edge<PipelineEdgeData>);
			if (!incomingRes.ok) {
				this.deleteNode(adapterNodeId);
				return {
					ok: false as const,
					error: String(incomingRes.error ?? 'Failed to connect source to adapter')
				};
			}

			const outgoingRes = this.addEdge({
				id: `e_${crypto.randomUUID()}`,
				source: adapterNodeId,
				target,
				sourceHandle: 'out',
				targetHandle,
				data: { exec: 'idle' }
			} as Edge<PipelineEdgeData>);
			if (!outgoingRes.ok) {
				if (incomingRes.id) this.deleteEdge(incomingRes.id);
				this.deleteNode(adapterNodeId);
				return {
					ok: false as const,
					error: String(outgoingRes.error ?? 'Failed to connect adapter to target')
				};
			}

			return {
				ok: true as const,
				adapterKind,
				adapterNodeId,
				incomingEdgeId: incomingRes.id ?? null,
				outgoingEdgeId: outgoingRes.id ?? null
			};
		},

		updateNodeTitle(nodeId: string, label: string) {
			update((s) => {
				const nodes = s.nodes.map((n) =>
					n.id === nodeId ? { ...n, data: { ...n.data, label } } : n
				);
				const next = { ...s, nodes };
				persist(next);
				return next;
			});
		},

		setNodeMeta(nodeId: string, patch: Record<string, unknown>) {
			update((s) => {
				const node = s.nodes.find((n) => n.id === nodeId);
				if (!node) return s;
				const nodes = s.nodes.map((n) =>
					n.id === nodeId
						? {
								...n,
								data: {
									...n.data,
									meta: {
										...(n.data.meta ?? {}),
										...patch,
										updatedAt: new Date().toISOString()
									}
								}
							}
						: n
				);
				const next = { ...s, nodes };
				persist(next);
				return next;
			});
		},

		//before extensive renovations

		// ----- clear edges of prior run's status (uses edge highlighting) -----
		resetRunUi() {
			update((s) => {
				const edges = resetEdgesExec(s.edges);
				const next = withGraphMeta({ ...s, edges, logs: [], runStatus: IDLE });
				persist(next);
				return next;
			});
		},

		hardResetGraph() {
			const freshGraphId = mintGraphId();
			const next = buildHardResetState(freshGraphId);
			persist(next);
			set(next);
		},

		clearDraft() {
			clearGraphDraft();
		},

		loadGraphDocument(graph: { nodes: unknown[]; edges: unknown[] }, graphIdOverride?: string | null) {
			const applied = applyGraphDocument(graph, graphIdOverride);
			if (!applied.ok) return { ok: false, reason: 'invalid_payload' as const };
			return { ok: true };
		},

		async saveGraph(message?: string, opts?: { graphName?: string }) {
			const current = get({ subscribe } as any) as GraphState;
			const graphId = String(current.graphId ?? '').trim();
			if (!graphId) return { ok: false, reason: 'missing_graph_id' as const };
			const preflight = buildSavePreflightDiagnostics(current.nodes as any, current.edges as any);
			if (!preflight.ok) {
				return {
					ok: false,
					reason: 'preflight_failed' as const,
					error: summarizeSavePreflightError(preflight.diagnostics),
					diagnostics: preflight.diagnostics
				};
			}
			const strictGraph = buildPersistableGraphStrict(current.nodes as any, current.edges as any, graphId);
			if (!strictGraph.ok) return { ok: false, reason: 'invalid_graph' as const, error: strictGraph.error };
			const graph = strictGraph.graph;
			try {
				const created = await createGraphRevision({
					graphId,
					graphName: String(opts?.graphName ?? '').trim() || undefined,
					revisionKind: 'save_graph',
					message: String(message ?? '').trim() || undefined,
					graph
				});
				return {
					ok: true,
					graphId: String(created.graphId),
					graphName: created.graphName ?? null,
					revisionId: String(created.revisionId),
					createdAt: String(created.createdAt)
				};
			} catch (error) {
				return { ok: false, reason: 'save_failed' as const, error: String(error) };
			}
		},

		async saveGraphVersion(versionName: string, message?: string) {
			const current = get({ subscribe } as any) as GraphState;
			const graphId = String(current.graphId ?? '').trim();
			const nextVersionName = String(versionName ?? '').trim();
			if (!graphId) return { ok: false, reason: 'missing_graph_id' as const };
			if (!nextVersionName) return { ok: false, reason: 'missing_version_name' as const };
			const preflight = buildSavePreflightDiagnostics(current.nodes as any, current.edges as any);
			if (!preflight.ok) {
				return {
					ok: false,
					reason: 'preflight_failed' as const,
					error: summarizeSavePreflightError(preflight.diagnostics),
					diagnostics: preflight.diagnostics
				};
			}
			const strictGraph = buildPersistableGraphStrict(current.nodes as any, current.edges as any, graphId);
			if (!strictGraph.ok) return { ok: false, reason: 'invalid_graph' as const, error: strictGraph.error };
			const graph = strictGraph.graph;
			try {
				const created = await createGraphRevision({
					graphId,
					versionName: nextVersionName,
					revisionKind: 'save_version',
					message: String(message ?? '').trim() || undefined,
					graph
				});
				return {
					ok: true,
					graphId: String(created.graphId),
					revisionId: String(created.revisionId),
					versionName: created.versionName ?? null,
					createdAt: String(created.createdAt)
				};
			} catch (error) {
				return { ok: false, reason: 'save_failed' as const, error: String(error) };
			}
		},

		async saveGraphAs(graphName: string, message?: string, versionName?: string) {
			const nextGraphName = String(graphName ?? '').trim();
			if (!nextGraphName) return { ok: false, reason: 'missing_graph_name' as const };
			const current = get({ subscribe } as any) as GraphState;
			const preflight = buildSavePreflightDiagnostics(current.nodes as any, current.edges as any);
			if (!preflight.ok) {
				return {
					ok: false,
					reason: 'preflight_failed' as const,
					error: summarizeSavePreflightError(preflight.diagnostics),
					diagnostics: preflight.diagnostics
				};
			}
			const strictGraph = buildPersistableGraphStrict(
				current.nodes as any,
				current.edges as any,
				current.graphId
			);
			if (!strictGraph.ok) return { ok: false, reason: 'invalid_graph' as const, error: strictGraph.error };
			const graph = strictGraph.graph;
			try {
				const created = await createGraphRevision({
					graphName: nextGraphName,
					versionName: String(versionName ?? '').trim() || undefined,
					revisionKind: 'save_graph_as',
					message: String(message ?? '').trim() || undefined,
					graph
				});
				update((s) => {
					const next = { ...s, graphId: String(created.graphId) };
					persist(next);
					return next;
				});
				return {
					ok: true,
					graphId: String(created.graphId),
					graphName: created.graphName ?? null,
					revisionId: String(created.revisionId),
					createdAt: String(created.createdAt)
				};
			} catch (error) {
				return { ok: false, reason: 'save_failed' as const, error: String(error) };
			}
		},

		// Backward-compatible aliases while UI migrates.
		async saveGraphRevision(message?: string) {
			return await this.saveGraph(message);
		},

		async saveAsGraphRevision(nextGraphId: string, message?: string) {
			const targetGraphId = String(nextGraphId ?? '').trim();
			if (!targetGraphId) return { ok: false, reason: 'missing_graph_id' as const };
			const current = get({ subscribe } as any) as GraphState;
			const preflight = buildSavePreflightDiagnostics(current.nodes as any, current.edges as any);
			if (!preflight.ok) {
				return {
					ok: false,
					reason: 'preflight_failed' as const,
					error: summarizeSavePreflightError(preflight.diagnostics),
					diagnostics: preflight.diagnostics
				};
			}
			const strictGraph = buildPersistableGraphStrict(
				current.nodes as any,
				current.edges as any,
				targetGraphId
			);
			if (!strictGraph.ok) return { ok: false, reason: 'invalid_graph' as const, error: strictGraph.error };
			const graph = strictGraph.graph;
			try {
				const created = await createGraphRevision({
					graphId: targetGraphId,
					revisionKind: 'save_graph_as',
					message: String(message ?? '').trim() || undefined,
					graph
				});
				update((s) => {
					const next = { ...s, graphId: targetGraphId };
					persist(next);
					return next;
				});
				return {
					ok: true,
					graphId: String(created.graphId),
					revisionId: String(created.revisionId),
					createdAt: String(created.createdAt)
				};
			} catch (error) {
				return { ok: false, reason: 'save_failed' as const, error: String(error) };
			}
		},

		async listGraphs(limit = 50, offset = 0) {
			try {
				const listed = await listGraphsClient(limit, offset);
				return {
					ok: true,
					graphs: Array.isArray(listed.graphs) ? listed.graphs : []
				};
			} catch (error) {
				return { ok: false, reason: 'list_failed' as const, error: String(error) };
			}
		},

		async listGraphRevisionHistory(limit = 30, offset = 0) {
			const current = get({ subscribe } as any) as GraphState;
			const graphId = String(current.graphId ?? '').trim();
			if (!graphId) return { ok: false, reason: 'missing_graph_id' as const };
			try {
				const listed = await listGraphRevisions(graphId, limit, offset);
				return {
					ok: true,
					graphId,
					revisions: Array.isArray(listed.revisions) ? listed.revisions : []
				};
			} catch (error) {
				return { ok: false, reason: 'list_failed' as const, error: String(error) };
			}
		},

		async listGraphRevisionHistoryForGraph(graphId: string, limit = 30, offset = 0) {
			const gid = String(graphId ?? '').trim();
			if (!gid) return { ok: false, reason: 'missing_graph_id' as const };
			try {
				const listed = await listGraphRevisions(gid, limit, offset);
				return {
					ok: true,
					graphId: gid,
					revisions: Array.isArray(listed.revisions) ? listed.revisions : []
				};
			} catch (error) {
				return { ok: false, reason: 'list_failed' as const, error: String(error) };
			}
		},

		async restoreGraphRevision(revisionId: string) {
			const current = get({ subscribe } as any) as GraphState;
			const graphId = String(current.graphId ?? '').trim();
			const rid = String(revisionId ?? '').trim();
			if (!graphId) return { ok: false, reason: 'missing_graph_id' as const };
			if (!rid) return { ok: false, reason: 'missing_revision_id' as const };
			try {
				const restored = await getGraphRevision(graphId, rid);
				const graph = (restored?.graph ?? {}) as any;
				const applied = applyGraphDocument(graph, restored.graphId);
				if (!applied.ok) return { ok: false, reason: 'invalid_payload' as const };
				return {
					ok: true,
					graphId: String(restored.graphId),
					revisionId: String(restored.revisionId)
				};
			} catch (error) {
				return { ok: false, reason: 'restore_failed' as const, error: String(error) };
			}
		},

		async loadGraphRevision(graphId: string, revisionId: string) {
			const gid = String(graphId ?? '').trim();
			const rid = String(revisionId ?? '').trim();
			if (!gid) return { ok: false, reason: 'missing_graph_id' as const };
			if (!rid) return { ok: false, reason: 'missing_revision_id' as const };
			try {
				const restored = await getGraphRevision(gid, rid);
				const graph = (restored?.graph ?? {}) as any;
				const applied = applyGraphDocument(graph, restored.graphId);
				if (!applied.ok) return { ok: false, reason: 'invalid_payload' as const };
				return {
					ok: true,
					graphId: String(restored.graphId),
					graphName: restored.graphName ?? null,
					revisionId: String(restored.revisionId)
				};
			} catch (error) {
				return { ok: false, reason: 'restore_failed' as const, error: String(error) };
			}
		},

		async deleteGraph(graphId: string) {
			const gid = String(graphId ?? '').trim();
			if (!gid) return { ok: false, reason: 'missing_graph_id' as const };
			try {
				const deleted = await deleteGraphClient(gid);
				return { ok: true, deleted };
			} catch (error) {
				return { ok: false, reason: 'delete_failed' as const, error: String(error) };
			}
		},

		async deleteGraphRevision(graphId: string, revisionId: string) {
			const gid = String(graphId ?? '').trim();
			const rid = String(revisionId ?? '').trim();
			if (!gid) return { ok: false, reason: 'missing_graph_id' as const };
			if (!rid) return { ok: false, reason: 'missing_revision_id' as const };
			try {
				const deleted = await deleteGraphRevisionClient(gid, rid);
				return { ok: true, deleted };
			} catch (error) {
				return { ok: false, reason: 'delete_failed' as const, error: String(error) };
			}
		},

		async hydrateLatestGraphFromBackend() {
			if (typeof window === 'undefined') return { ok: false, reason: 'non_browser' as const };
			try {
				const current = get({ subscribe } as any) as GraphState;
				const graphId = String(current.graphId ?? '').trim();
				if (!graphId) return { ok: false, reason: 'missing_graph_id' as const };
				const latest = await getLatestGraphRevision(graphId);
				const graph = (latest?.graph ?? {}) as any;
				const applied = applyGraphDocument(graph, latest.graphId);
				if (!applied.ok) return { ok: false, reason: 'invalid_payload' as const };
				return {
					ok: true,
					graphId: String(latest.graphId),
					graphName: latest.graphName ?? null,
					revisionId: String(latest.revisionId)
				};
			} catch (error) {
				return { ok: false, reason: 'read_failed' as const, error: String(error) };
			}
		},

		async listComponentCatalog(limit = 100, offset = 0) {
			try {
				const components = await listComponents(limit, offset);
				return { ok: true, components };
			} catch (error) {
				return { ok: false, reason: 'list_components_failed' as const, error: String(error) };
			}
		},

		async listComponentRevisionHistory(componentId: string, limit = 100, offset = 0) {
			try {
				const revisions = await listComponentRevisions(componentId, limit, offset);
				return { ok: true, revisions };
			} catch (error) {
				return { ok: false, reason: 'list_revisions_failed' as const, error: String(error) };
			}
		},

		async getComponentRevisionDetail(componentId: string, revisionId: string) {
			try {
				const detail = await getComponentRevision(componentId, revisionId);
				return { ok: true, detail };
			} catch (error) {
				return { ok: false, reason: 'get_revision_failed' as const, error: String(error) };
			}
		},

		async openComponentRevisionForEditing(componentId: string, revisionId: string, entryNodeId?: string | null) {
			const cid = String(componentId ?? '').trim();
			const rid = String(revisionId ?? '').trim();
			if (!cid || !rid) return { ok: false, reason: 'missing_component_ref' as const };
			try {
				const before = get({ subscribe } as any) as GraphState;
				const snapshot = captureComponentEditSnapshot(before);
				const detail = await getComponentRevision(cid, rid);
				const graph = (detail?.definition?.graph ?? {}) as { nodes?: unknown[]; edges?: unknown[] };
				const applied = applyGraphDocument(
					{
						nodes: Array.isArray(graph?.nodes) ? graph.nodes : [],
						edges: Array.isArray(graph?.edges) ? graph.edges : []
					},
					null
				);
				if (!applied.ok) {
					return { ok: false, reason: 'invalid_payload' as const, error: String(applied.reason ?? 'invalid_payload') };
				}
				update((s) => {
					const next = {
						...s,
						editingContext: 'component' as const,
						componentEditSession: {
							componentId: cid,
							revisionId: rid,
							entryNodeId: String(entryNodeId ?? '').trim() || null,
							snapshot
						},
						lastRunStatus: 'never_run' as const,
						logs: [
							...(Array.isArray(s.logs) ? s.logs : []),
							{
								id: ++logSeq,
								ts: new Date().toLocaleTimeString(),
								level: 'info' as const,
								message: `[component-edit] Loaded internals: ${cid}@${rid}`
							}
						]
					};
					persist(next);
					return next;
				});
				return { ok: true, detail };
			} catch (error) {
				return { ok: false, reason: 'open_component_failed' as const, error: String(error) };
			}
		},

		returnFromComponentEditSession() {
			const state = get({ subscribe } as any) as GraphState;
			const session = state.componentEditSession;
			if (!session) return { ok: false as const, reason: 'no_component_edit_session' as const };
			const snapshot = session.snapshot;
			update((s) => {
				const next: GraphState = {
					...s,
					graphId: snapshot.graphId,
					nodes: structuredClone(snapshot.nodes),
					edges: structuredClone(snapshot.edges),
					selectedNodeId: snapshot.selectedNodeId,
					inspector: structuredClone(snapshot.inspector),
					logs: [
						...structuredClone(snapshot.logs),
						{
							id: ++logSeq,
							ts: new Date().toLocaleTimeString(),
							level: 'info',
							message: `[component-edit] Returned to graph context from ${session.componentId}@${session.revisionId}`
						}
					],
					runStatus: snapshot.runStatus,
					lastRunStatus: snapshot.lastRunStatus,
					freshness: snapshot.freshness,
					staleNodeCount: snapshot.staleNodeCount,
					activeRunMode: snapshot.activeRunMode,
					activeRunFrom: snapshot.activeRunFrom,
					activeRunNodeSet: new Set(Array.from(snapshot.activeRunNodeSet ?? [])),
					nodeOutputs: structuredClone(snapshot.nodeOutputs),
					nodeBindings: ensureNormalizedBindingsForNodes(
						structuredClone(snapshot.nodes) as any,
						structuredClone(snapshot.nodeBindings) as any
					),
					activeRunId: snapshot.activeRunId,
					editingContext: 'graph',
					componentEditSession: null
				};
				persist(next);
				return withGraphMeta(next);
			}, { source: 'graph_edit' });
			return { ok: true as const };
		},

		updateComponentEditSessionRevision(revisionId: string) {
			const rid = String(revisionId ?? '').trim();
			if (!rid) return { ok: false as const, reason: 'missing_revision_id' as const };
			let updated = false;
			update((s) => {
				const session = s.componentEditSession;
				if (!session) return s;
				if (String(session.revisionId ?? '').trim() === rid) return s;
				updated = true;
				const next: GraphState = {
					...s,
					componentEditSession: {
						...session,
						revisionId: rid
					},
					logs: [
						...(Array.isArray(s.logs) ? s.logs : []),
						{
							id: ++logSeq,
							ts: new Date().toLocaleTimeString(),
							level: 'info',
							message: `[component-edit] Active revision updated: ${session.componentId}@${rid}`
						}
					]
				};
				persist(next);
				return next;
			});
			if (!updated) return { ok: false as const, reason: 'no_component_edit_session' as const };
			return { ok: true as const, revisionId: rid };
		},

		applySavedComponentRevisionToReturnGraph(
			componentId: string,
			fromRevisionId: string,
			toRevisionId: string,
			scope: 'none' | 'one' | 'all'
		) {
			const cid = String(componentId ?? '').trim();
			const fromRid = String(fromRevisionId ?? '').trim();
			const toRid = String(toRevisionId ?? '').trim();
			const mode = scope === 'all' || scope === 'none' ? scope : 'one';
			if (!cid || !fromRid || !toRid) return { ok: false as const, reason: 'missing_revision_context' as const };
			let applied = false;
			let matchedCount = 0;
			let updatedCount = 0;
			let entryMatched = false;
			update((s) => {
				const session = s.componentEditSession;
				if (!session) return s;
				const snapshot = session.snapshot;
				const matchingNodeIds = (snapshot.nodes ?? [])
					.filter((n) => {
						if (n.data?.kind !== 'component') return false;
						const ref = (((n.data as any)?.params ?? {}) as any)?.componentRef ?? {};
						const nodeComponentId = String(ref?.componentId ?? '').trim();
						const nodeRevisionId = String(ref?.revisionId ?? '').trim();
						return nodeComponentId === cid && nodeRevisionId === fromRid;
					})
					.map((n) => String(n.id));
				matchedCount = matchingNodeIds.length;
				const targetIds = new Set<string>();
				if (mode === 'all') {
					for (const id of matchingNodeIds) targetIds.add(id);
				} else if (mode === 'one') {
					const entryNodeId = String(session.entryNodeId ?? '').trim();
					if (entryNodeId && matchingNodeIds.includes(entryNodeId)) {
						entryMatched = true;
						targetIds.add(entryNodeId);
					}
				}
				const nextSnapshotNodes = (snapshot.nodes ?? []).map((n) => {
					if (!targetIds.has(String(n.id))) return n;
					updatedCount += 1;
					const params = structuredClone(((n.data as any)?.params ?? {}) as Record<string, unknown>);
					const existingRef = ((params as any)?.componentRef ?? {}) as Record<string, unknown>;
					return {
						...n,
						data: {
							...n.data,
							params: {
								...params,
								componentRef: {
									...existingRef,
									componentId: cid,
									revisionId: toRid
								}
							},
							meta: {
								...(n.data?.meta ?? {}),
								componentLatestRevisionId: toRid,
								componentHasUpdate: false,
								updatedAt: new Date().toISOString()
							}
						}
					};
				});
				const modeLabel = mode === 'all' ? 'all' : mode === 'none' ? 'none' : 'one';
				const nextLogMessage = `[component-edit] Save apply scope=${modeLabel} updated=${updatedCount}/${matchedCount} ${cid}@${fromRid} -> ${cid}@${toRid}`;
				const nextSnapshotLogs = [
					...(Array.isArray(snapshot.logs) ? structuredClone(snapshot.logs) : []),
					{
						id: ++logSeq,
						ts: new Date().toLocaleTimeString(),
						level: 'info' as const,
						message: nextLogMessage
					}
				];
				const next: GraphState = {
					...s,
					componentEditSession: {
						...session,
						revisionId: toRid,
						snapshot: {
							...snapshot,
							nodes: nextSnapshotNodes,
							logs: nextSnapshotLogs
						}
					},
					logs: [
						...(Array.isArray(s.logs) ? s.logs : []),
						{
							id: ++logSeq,
							ts: new Date().toLocaleTimeString(),
							level: 'info',
							message: nextLogMessage
						}
					]
				};
				applied = true;
				persist(next);
				return next;
			});
			if (!applied) return { ok: false as const, reason: 'no_component_edit_session' as const };
			return { ok: true as const, scope: mode, matchedCount, updatedCount, entryMatched };
		},

		async forkComponentRevisionToNode(
			nodeId: string,
			fromComponentId: string,
			fromRevisionId: string,
			nextComponentId: string,
			opts?: { revisionId?: string; message?: string }
		) {
			const sourceComponentId = String(fromComponentId ?? '').trim();
			const sourceRevisionId = String(fromRevisionId ?? '').trim();
			const targetComponentId = String(nextComponentId ?? '').trim();
			const targetRevisionId = String(opts?.revisionId ?? '').trim();
			const message = String(opts?.message ?? '').trim() || `fork:${sourceComponentId}@${sourceRevisionId}`;
			if (!nodeId) return { ok: false, reason: 'missing_node_id' as const };
			if (!sourceComponentId || !sourceRevisionId) {
				return { ok: false, reason: 'missing_source_ref' as const };
			}
			if (!targetComponentId) return { ok: false, reason: 'missing_target_component_id' as const };
			try {
				const source = await getComponentRevision(sourceComponentId, sourceRevisionId);
				const created = await createComponentRevision({
					componentId: targetComponentId,
					revisionId: targetRevisionId || undefined,
					parentRevisionId: undefined,
					message,
					schemaVersion: Number(source?.schemaVersion ?? 1) || 1,
					graph: {
						nodes: structuredClone(((source?.definition?.graph as any)?.nodes ?? []) as unknown[]),
						edges: structuredClone(((source?.definition?.graph as any)?.edges ?? []) as unknown[])
					},
					api: structuredClone(
						((source?.definition?.api as ComponentApiContract | undefined) ?? {
							inputs: [],
							outputs: []
						}) as ComponentApiContract
					),
					configSchema: structuredClone((source?.definition?.configSchema ?? {}) as Record<string, unknown>)
				});
				const apply = await this.applyComponentRevisionToNode(
					nodeId,
					String(created.componentId ?? targetComponentId),
					String(created.revisionId ?? '')
				);
				if (!(apply as any)?.ok) {
					return {
						ok: false,
						reason: 'fork_apply_failed' as const,
						error: String((apply as any)?.error ?? (apply as any)?.reason ?? 'unknown')
					};
				}
				return { ok: true, created, applied: apply };
			} catch (error) {
				return { ok: false, reason: 'fork_failed' as const, error: String(error) };
			}
		},

		async renameComponent(componentId: string, nextComponentId: string) {
			try {
				const renamed = await renameComponent(componentId, nextComponentId);
				const fromId = String(componentId ?? '').trim();
				const toId = String((renamed as any)?.componentId ?? nextComponentId ?? '').trim();
				if (fromId && toId && fromId !== toId) {
					const state = get({ subscribe } as any) as GraphState;
					const componentNodeIds = state.nodes
						.filter((n) => {
							if (n.data.kind !== 'component') return false;
							const currentId = String(((n.data.params as any)?.componentRef?.componentId ?? '')).trim();
							return currentId === fromId;
						})
						.map((n) => n.id);
					for (const nodeId of componentNodeIds) {
						const node = (get({ subscribe } as any) as GraphState).nodes.find((n) => n.id === nodeId);
						const existingRef = ((node?.data?.params as any)?.componentRef ?? {}) as Record<string, unknown>;
						const patch = {
							componentRef: {
								...existingRef,
								componentId: toId
							}
						};
						const result = updateNodeConfigImpl(nodeId, { params: patch });
						if (!result.ok) {
							return {
								ok: false,
								reason: 'rename_component_failed' as const,
								error: String(result.error ?? 'Failed to update component node reference')
							};
						}
					}
					update((s) => {
						const draftComponentRef = ((s.inspector.draftParams ?? {}) as Record<string, any>)
							.componentRef as Record<string, any> | undefined;
						if (String(draftComponentRef?.componentId ?? '').trim() !== fromId) return s;
						return {
							...s,
							inspector: {
								...s.inspector,
								draftParams: {
									...(s.inspector.draftParams ?? {}),
									componentRef: {
										...(draftComponentRef ?? {}),
										componentId: toId
									}
								}
							}
						};
					});
				}
				return { ok: true, renamed };
			} catch (error) {
				return { ok: false, reason: 'rename_component_failed' as const, error: String(error) };
			}
		},

		async deleteComponent(componentId: string) {
			try {
				const deleted = await deleteComponent(componentId);
				return { ok: true, deleted };
			} catch (error) {
				return { ok: false, reason: 'delete_component_failed' as const, error: String(error) };
			}
		},

		async deleteComponentRevision(componentId: string, revisionId: string) {
			try {
				const deleted = await deleteComponentRevision(componentId, revisionId);
				return { ok: true, deleted };
			} catch (error) {
				return { ok: false, reason: 'delete_component_revision_failed' as const, error: String(error) };
			}
		},

		async applyComponentRevisionToNode(nodeId: string, componentId: string, revisionId: string) {
			const cid = String(componentId ?? '').trim();
			const rid = String(revisionId ?? '').trim();
			if (!cid || !rid) return { ok: false, reason: 'missing_component_ref' as const };
			const node = (get({ subscribe } as any) as GraphState).nodes.find((n) => n.id === nodeId);
			if (!node || node.data.kind !== 'component') return { ok: false, reason: 'node_not_component' as const };
			try {
				const detail = await getComponentRevision(cid, rid);
				const api = (detail.definition?.api ?? { inputs: [], outputs: [] }) as ComponentApiContract;
				const internalGraph = (detail.definition?.graph ?? { nodes: [], edges: [] }) as {
					nodes?: Array<{ id?: string }>;
					edges?: Array<{ source?: string; target?: string }>;
				};
				const internalNodes = Array.isArray(internalGraph.nodes) ? internalGraph.nodes : [];
				const internalEdges = Array.isArray(internalGraph.edges) ? internalGraph.edges : [];
				const nodeIds = new Set(
					internalNodes
						.map((n) => String(n?.id ?? '').trim())
						.filter((id) => id.length > 0)
				);
				const outDegree = new Map<string, number>();
				for (const id of nodeIds) outDegree.set(id, 0);
				for (const e of internalEdges) {
					const src = String(e?.source ?? '').trim();
					if (nodeIds.has(src)) outDegree.set(src, (outDegree.get(src) ?? 0) + 1);
				}
				const leafNodeId =
					Array.from(nodeIds).find((id) => (outDegree.get(id) ?? 0) === 0) ?? '';
				const firstNodeId = Array.from(nodeIds)[0] ?? '';

				const prevBindings = ((node.data.params as any)?.bindings ?? {}) as {
					inputs?: Record<string, string>;
					config?: Record<string, string>;
					outputs?: Record<string, { nodeId?: string; artifact?: 'current' | 'last' }>;
				};
				const prevOutputs = (prevBindings.outputs ?? {}) as Record<
					string,
					{ nodeId?: string; artifact?: 'current' | 'last' }
				>;
				const nextOutputs: Record<string, { nodeId?: string; artifact?: 'current' | 'last' }> = {};
				const apiOutputs = Array.isArray(api.outputs) ? api.outputs : [];
				for (const out of apiOutputs) {
					const outName = String((out as any)?.name ?? '').trim();
					if (!outName) continue;
					const existing = prevOutputs[outName] ?? {};
					const existingNodeId = String(existing?.nodeId ?? '').trim();
					nextOutputs[outName] = {
						nodeId: existingNodeId || leafNodeId || firstNodeId || undefined,
						artifact: existing?.artifact === 'last' ? 'last' : 'current'
					};
				}
				const paramsPatch = {
					componentRef: {
						componentId: cid,
						revisionId: rid,
						apiVersion: String((node.data.params as any)?.componentRef?.apiVersion ?? 'v1')
					},
					bindings: {
						inputs: { ...(prevBindings.inputs ?? {}) },
						config: { ...(prevBindings.config ?? {}) },
						outputs: nextOutputs
					},
					api
				};
				const portsPatch = derivePortsFromComponentApi(api);
				const result = updateNodeConfigImpl(nodeId, {
					params: paramsPatch,
					ports: portsPatch
				});
				if (!result.ok) return { ok: false, reason: 'update_failed' as const, error: result.error };
				const revisions = await listComponentRevisions(cid, 20, 0);
				const latestRevisionId = String(revisions?.[0]?.revisionId ?? '').trim() || null;
				update((s) => {
					const target = s.nodes.find((n) => n.id === nodeId);
					if (!target) return s;
					const refreshedParams = structuredClone((target.data.params ?? {}) as Record<string, unknown>);
					const nodes = s.nodes.map((n) =>
						n.id === nodeId
							? {
									...n,
									data: {
										...n.data,
										params: refreshedParams,
										meta: {
											...(n.data.meta ?? {}),
											componentLatestRevisionId: latestRevisionId,
											componentHasUpdate: Boolean(latestRevisionId && latestRevisionId !== rid),
											updatedAt: new Date().toISOString()
										}
									}
								}
							: n
					);
					const next = { ...s, nodes };
					persist(next);
					return next;
				});
				const stateAfter = get({ subscribe } as any) as GraphState;
				const refreshedNode = stateAfter.nodes.find((n) => n.id === nodeId);
				const refreshedParams = structuredClone((refreshedNode?.data.params ?? {}) as Record<string, unknown>);
				update((s) => {
					if (s.inspector.nodeId !== nodeId) return s;
					return {
						...s,
						inspector: {
							...s.inspector,
							draftParams: refreshedParams,
							dirty: false
						}
					};
				});
				return {
					ok: true,
					detail,
					latestRevisionId,
					hasUpdate: Boolean(latestRevisionId && latestRevisionId !== rid)
				};
			} catch (error) {
				return { ok: false, reason: 'apply_revision_failed' as const, error: String(error) };
			}
		},

		async runRemote(
			runFrom: string | null,
			runMode?: ActiveRunMode,
			cacheMode?: 'default_on' | 'force_off' | 'force_on'
		) {
			// prevent concurrent runs
			const s0 = get({ subscribe } as any) as GraphState;
			if (s0.runStatus === 'running') return;

			// reset UI
			this.resetRunUi();
			update((s) => withGraphMeta({ ...s, runStatus: 'running' }));

			// snapshot graph DTO
			const s1 = get({ subscribe } as any) as GraphState;
			const effectiveRunMode: ActiveRunMode = runMode ?? (runFrom ? 'from_selected_onward' : 'from_start');
			const dirtyNodeIds =
				effectiveRunMode === 'from_start'
					? Object.entries(s1.nodeBindings ?? {})
							.filter(([, binding]) => isBindingStale(binding))
							.map(([nodeId]) => nodeId)
					: [];
			const payload = buildRunCreateRequest(
				{ version: 1, nodes: s1.nodes, edges: s1.edges },
				s1.graphId,
				runFrom,
				effectiveRunMode,
				dirtyNodeIds,
				cacheMode
			);
			const plannedNodeSet = computePlannedNodeSet(s1.nodes, s1.edges, runFrom, effectiveRunMode);

			// create run
			let runId: string;

			try {
				const created = await createRun(payload);
				runId = created.runId;
				update((s) =>
					withGraphMeta({
						...s,
						graphId: created.graphId || s.graphId,
						activeRunId: runId,
						activeRunMode: effectiveRunMode,
						activeRunFrom: runFrom,
						activeRunNodeSet: plannedNodeSet
					})
				);
				try {
					const snap = await getRun(runId);
					update((s) => hydrateFromRunSnapshot(s, snap), {
						source: 'hydrate_snapshot',
						snapshotNodeIds: new Set(Object.keys(snap.nodeBindings ?? {}))
					});
				} catch {
					// non-fatal: stream events can still drive updates
				}
			} catch (e) {
				update((s) =>
					withGraphMeta(
						logPush({ ...s, runStatus: 'failed' }, 'error', `Run create failed: ${String(e)}`)
					)
				);
				return;
			}

			await new Promise<void>((resolve) => {
				let subHandle: { close: () => void } | null = null;
				let settled = false;
				const settle = () => {
					if (settled) return;
					settled = true;
					resolve();
				};
				const applyEventBatch = (events: KnownRunEvent[]) => {
					for (const evt of events) {
						const cur = get({ subscribe } as any) as GraphState;
						const evtGraphId = (evt as any)?.graphId;
						if (typeof evtGraphId === 'string' && evtGraphId && evtGraphId !== cur.graphId) {
							continue;
						}
						const auditCtx: AuditContext =
							evt.type === 'run_started'
								? {
										source: 'event',
										evt,
										expectedDirtyTransition: true,
										allowedNodeIds: new Set<string>(
											Array.isArray((evt as any).plannedNodeIds)
												? ((evt as any).plannedNodeIds as string[])
												: []
										)
									}
								: { source: 'event', evt };
						update((s) => {
							const nextState = reduceRunEventState(s, evt, runId);
							debugLogOutOfScopeBindingMutation(s, nextState, evt.type);
							debugLogStaleFlips(s, nextState, evt.type);
							assertNoOutOfScopeStaleFlips(s, nextState, evt.type);
							if (evt.type === 'run_started') {
								assertRunStartedBindingTouchInScope(s, nextState);
							}
							return nextState;
						}, auditCtx);

						if (evt.type === 'run_finished') {
							const current = get({ subscribe } as any) as GraphState;
							persist(current);
							void getRun(runId)
								.then((snap) => {
									const latest = get({ subscribe } as any) as GraphState;
									if (typeof snap.graphId === 'string' && snap.graphId && snap.graphId !== latest.graphId) {
										return;
									}
									update((s) => hydrateFromRunSnapshot(s, snap), {
										source: 'hydrate_snapshot',
										snapshotNodeIds: new Set(Object.keys(snap.nodeBindings ?? {}))
									});
								})
								.catch(() => {});
							subHandle?.close();
							settle();
						}
					}
				};
				const batcher = createEventBatcher<KnownRunEvent>(applyEventBatch, {
					maxBatchSize: 48,
					maxDelayMs: 16
				});
				subHandle = streamRunEvents(
					runId,
					(evt: KnownRunEvent) => {
						batcher.push(evt);
					},
					() => {
						const cur = get({ subscribe } as any) as GraphState;
						const isTerminalForThisRun =
							cur.activeRunId !== runId ||
							cur.runStatus === 'succeeded' ||
							cur.runStatus === 'failed' ||
							cur.runStatus === 'canceled' ||
							cur.runStatus === 'cancelled';
						if (isTerminalForThisRun) {
							settle();
							return;
						}
						batcher.flush();
						update((s) =>
							withGraphMeta(logPush({ ...s, runStatus: 'failed' }, 'error', 'Event stream error'))
						);
						settle();
					}
				);
			});
		}
	};
})();

export const selectedNode = derived(graphStore, ($s) =>
	$s.selectedNodeId ? ($s.nodes.find((n) => n.id === $s.selectedNodeId) ?? null) : null
);

export const edgeSchemaConstraints = derived(graphStore, ($s) =>
	computeEdgeSchemaConstraintsInternal($s.nodes as any, $s.edges as any)
);

export const edgeSchemaDiagnostics = derived(edgeSchemaConstraints, ($constraints) =>
	computeEdgeSchemaDiagnosticsInternal($constraints as any)
);

export type NodeSchemaContractEdge = {
	edgeId: string;
	direction: 'incoming' | 'outgoing';
	sourceNodeId: string;
	targetNodeId: string;
	sourceHandle: string | null;
	targetHandle: string | null;
	providedSchema: Record<string, any>;
	requiredSchema: Record<string, any>;
	severity: 'clean' | 'warning' | 'error';
	suggestions: string[];
	adapterKind: AdapterTransformKind | null;
};

export type NodeSchemaContractSnapshot = {
	nodeId: string;
	status: 'clean' | 'warning' | 'error';
	edges: NodeSchemaContractEdge[];
};

function buildNodeSchemaContractSnapshotInternal(
	state: GraphState,
	nodeIdRaw: string
): NodeSchemaContractSnapshot {
	const nodeId = String(nodeIdRaw ?? '').trim();
	if (!nodeId) return { nodeId: '', status: 'clean', edges: [] };
	const constraints = computeEdgeSchemaConstraintsInternal(state.nodes as any, state.edges as any);
	const diagnostics = computeEdgeSchemaDiagnosticsInternal(constraints as any);
	const edges: NodeSchemaContractEdge[] = [];
	for (const edge of state.edges ?? []) {
		const edgeId = String(edge.id ?? '');
		if (!edgeId) continue;
		if (String(edge.source ?? '') !== nodeId && String(edge.target ?? '') !== nodeId) continue;
		const constraint = constraints[edgeId];
		if (!constraint) continue;
		const diag = diagnostics[edgeId];
		const severity: 'clean' | 'warning' | 'error' =
			diag?.severity === 'error' ? 'error' : diag?.severity === 'warning' ? 'warning' : 'clean';
		edges.push({
			edgeId,
			direction: String(edge.target ?? '') === nodeId ? 'incoming' : 'outgoing',
			sourceNodeId: String(edge.source ?? ''),
			targetNodeId: String(edge.target ?? ''),
			sourceHandle: String((edge as any).sourceHandle ?? '').trim() || null,
			targetHandle: String((edge as any).targetHandle ?? '').trim() || null,
			providedSchema: constraint.providedSchema,
			requiredSchema: constraint.requiredSchema,
			severity,
			suggestions: constraint.suggestions ?? [],
			adapterKind: constraint.adapterKind ?? null
		});
	}
	const status: 'clean' | 'warning' | 'error' = edges.some((edge) => edge.severity === 'error')
		? 'error'
		: edges.some((edge) => edge.severity === 'warning')
			? 'warning'
			: 'clean';
	return { nodeId, status, edges };
}

export function __buildNodeSchemaContractSnapshotForTest(
	state: GraphState,
	nodeId: string
): NodeSchemaContractSnapshot {
	return buildNodeSchemaContractSnapshotInternal(state, nodeId);
}
