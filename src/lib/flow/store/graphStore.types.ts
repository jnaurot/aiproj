// src/lib/flow/store/graphStore.types.ts
//
// Single source of truth for all types, interfaces, and shared constants that
// were previously scattered through graphStore.ts.  Nothing in here has any
// runtime side-effects – it is pure type/constant declarations so that every
// downstream module can import from one place without creating circular deps.

// What's in it: Every type, interface, and shared constant that was previously scattered
//  through the 10k-line file — 42 named exports total.
// One meaningful improvement beyond a mechanical move: QueueRuntime was previously
//  an anonymous inline type nested inside GraphState (the massive queueRuntime?:
//  block). It's now a named export, so any module that needs to work with queue runtime data
//  can reference it directly instead of writing GraphState['queueRuntime'].
// The two raw string constants (IDLE, SUCCEEDED) are renamed to NODE_STATUS_IDLE /
//  NODE_STATUS_SUCCEEDED to be less ambiguous when read out of context.
//  initialInspector becomes INITIAL_INSPECTOR for the same reason.

import type { Node, Edge } from '@xyflow/svelte';
import type {
	NodeStatus,
	PipelineNodeData,
	PipelineEdgeData,
} from '$lib/flow/types';
import type { ActiveRunMode, GraphFreshness as ScopeFreshness } from './runScope';
import type { KnownRunEvent } from '$lib/flow/types/run';
import type { BindingPair } from './graphStore.bindings';
import type { NodeDocExplanationMode, NodeDocTrainingMode } from '$lib/flow/schema/nodeDocs';
import type { SchemaDiagnosticCode } from '$lib/flow/schema/diagnosticsContract';
import type { CheckpointRegistry, CheckpointStaleness } from '$lib/flow/types/checkpoint';
import type { SchemaPlaneResult, SchemaPlaneState } from '$lib/flow/types/schemaPlane';

// ---------------------------------------------------------------------------
// Primitive aliases
// ---------------------------------------------------------------------------

export type EdgeExec = 'idle' | 'active' | 'done';
export type LogLevel = 'info' | 'warn' | 'error';
export type SavePreflightSeverity = 'error' | 'warning';
export type EditorContext = 'graph' | 'component';
export type InspectorDraftPatchIntent = 'user_edit' | 'system_canonicalize';
export type AdapterTransformKind = 'text_to_table' | 'json_to_table' | 'table_to_json';
export type EdgeInvalidReason =
	| 'type_mismatch'
	| 'schema_mismatch'
	| 'typed_schema_missing'
	| 'mode_mismatch';

// ---------------------------------------------------------------------------
// Run status
// ---------------------------------------------------------------------------

export const RUN_IDLE = 'idle' as const;

export type RunStatus =
	| typeof RUN_IDLE
	| 'running'
	| 'pausing'
	| 'paused'
	| 'resuming'
	| 'succeeded'
	| 'failed'
	| 'canceled';

export type GraphLastRunStatus = 'succeeded' | 'failed' | 'canceled' | 'never_run';

export type RunBlockedReason =
	| {
			type: 'unsaved_checkpoint_changes';
			componentNodeIds: string[];
			message: string;
	  }
	| {
			type: 'schema_errors_in_run_path';
			nodeIds: string[];
			message: string;
			errors?: Array<{ nodeId: string; code?: string; message: string }>;
	  };

export type GraphViewMode = 'execution' | 'schema';

// ---------------------------------------------------------------------------
// Node execution / binding
// ---------------------------------------------------------------------------

export type NodeExecutionError = {
	message?: string;
	errorCode?: string;
	op?: string;
	paramPath?: string;
	missingColumns?: string[];
	availableColumns?: string[];
	availableColumnsSource?: 'schema' | 'inferred' | string;
};

export type NodeOutputInfo = {
	mimeType?: string;
	payloadType?: string;
	preview?: string;
	sourceObservability?: Record<string, unknown>;
	primingArtifact?: Record<string, unknown>;
	cached?: boolean;
	cacheDecision?: 'cache_hit' | 'cache_miss' | 'cache_hit_contract_mismatch';
	pinnedByCheckpoint?: boolean;
	expectedContractFingerprint?: string;
	actualContractFingerprint?: string;
	mismatchKind?: string;
	lastError?: NodeExecutionError | null;
};

export type NodeBindingInfo = {
	status?: string;
	current?: { execKey?: string | null; artifactId?: string | null } | null;
	last?: { execKey?: string | null; artifactId?: string | null } | null;
	outputLineage?: Record<string, { execKey?: string | null; artifactId?: string | null } | null> | null;
	memoState?: {
		decision: 'reuse' | 'compute' | 'skip_non_memoizable';
		memoKey?: string;
		resolvedAt?: string;
	} | null;
	checkpointable?: boolean;
	lastArtifactId?: string | null;     // legacy
	lastRunId?: string | null;
	lastExecKey?: string | null;        // legacy
	currentExecKey?: string | null;     // legacy
	currentArtifactId?: string | null;  // legacy
	currentRunId?: string | null;
	isUpToDate?: boolean;
	cacheValid?: boolean;
	staleReason?: string | null;
};

export type NormalizedNodeBinding = NodeBindingInfo & {
	status: string;
	isUpToDate: boolean;
	cacheValid: boolean;
	currentRunId: string | null;
	staleReason: string | null;
	current: BindingPair;
	last: BindingPair;
};

// ---------------------------------------------------------------------------
// Run snapshot (used by hydration / SSE reconciliation)
// ---------------------------------------------------------------------------

export type RunSnapshotLike = {
	graphId?: string;
	status?: string;
	runMode?: ActiveRunMode;
	plannedNodeIds?: string[];
	nodeStatus?: Record<string, string>;
	nodeOutputs?: Record<string, string>;
	nodeBindings?: Record<string, Record<string, unknown>>;
	checkpoint_outcomes?: Record<string, CheckpointStaleness | string>;
};

// ---------------------------------------------------------------------------
// Audit
// ---------------------------------------------------------------------------

export type AuditContext = {
	source: 'event' | 'accept_params' | 'hydrate_snapshot' | 'graph_edit' | 'unknown';
	evt?: KnownRunEvent;
	allowedNodeIds?: Set<string>;
	snapshotNodeIds?: Set<string>;
	expectedDirtyTransition?: boolean;
};

// ---------------------------------------------------------------------------
// Logging
// ---------------------------------------------------------------------------

export type RunLog = {
	id: number;
	ts: string;
	level: LogLevel;
	message: string;
	nodeId?: string;
	edgeId?: string;
	componentPath?: string[];
};

// ---------------------------------------------------------------------------
// Inspector / editor UI
// ---------------------------------------------------------------------------

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

export type InspectorState = {
	nodeId: string | null;
	draftParams: Record<string, any>;
	dirty: boolean;
	systemNotice?: string | null;
	uiByNodeId: Record<string, ApiEditorUiState>;
};

export type InspectorDraftAcceptValidation =
	| { ok: true; errors: [] }
	| { ok: false; errors: string[] };

// ---------------------------------------------------------------------------
// Save / preflight
// ---------------------------------------------------------------------------

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

export type SaveConsistencyEntity = {
	id: string;
	label: string;
};

export type SaveConsistencyMismatch = {
	canvasNodeCount: number;
	persistedNodeCount: number;
	canvasEdgeCount: number;
	persistedEdgeCount: number;
	missingNodes: SaveConsistencyEntity[];
	addedNodes: SaveConsistencyEntity[];
	changedNodes: SaveConsistencyEntity[];
	missingEdges: SaveConsistencyEntity[];
	addedEdges: SaveConsistencyEntity[];
	changedEdges: SaveConsistencyEntity[];
};

// ---------------------------------------------------------------------------
// Component edit session
// ---------------------------------------------------------------------------

export type ComponentEditSessionSnapshot = {
	graphId: string;
	nodes: Node<PipelineNodeData & Record<string, unknown>>[];
	edges: Edge<PipelineEdgeData & Record<string, unknown>>[];
	checkpointRegistry: CheckpointRegistry;
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
	runBlockedReason?: RunBlockedReason | null;
	nodeOutputs: Record<string, NodeOutputInfo>;
	nodeBindings: Record<string, NormalizedNodeBinding>;
	activeRunId: string | null;
};

export type ComponentEditSession = {
	componentId: string;
	revisionId: string;
	entryNodeId: string | null;
	contractDraftParams: Record<string, any>;
	snapshot: ComponentEditSessionSnapshot;
	parentSession: ComponentEditSession | null;
};

// ---------------------------------------------------------------------------
// Edge schema / contract types
// ---------------------------------------------------------------------------

export type SchemaCompatibility =
	| { ok: true; warning?: 'lossy_coercion'; suggestion?: string | null; adapterKind?: AdapterTransformKind | null }
	| {
			ok: false;
			reason: 'type_mismatch' | 'missing_required_columns' | 'missing_typed_schema';
			missingColumns?: string[];
			suggestion?: string | null;
			adapterKind?: AdapterTransformKind | null;
	  };

export type EdgeCheck =
	| { ok: true; out?: string; in?: string }
	| {
			ok: false;
			reason: EdgeInvalidReason;
			missingColumns?: string[];
			suggestion?: string | null;
			adapterKind?: AdapterTransformKind | null;
	  };

export type EdgeSchemaConstraint = {
	edgeId: string;
	mode: 'work' | 'param' | 'control';
	sourceNodeId: string;
	targetNodeId: string;
	sourceHandle: string;
	targetHandle: string;
	sourceAffinity: 'work' | 'param' | 'control';
	targetAffinity: 'work' | 'param' | 'control';
	providedSchema: Record<string, any>;
	requiredSchema: Record<string, any>;
	compatible: boolean;
	warning?: 'lossy_coercion';
	adapterKind?: AdapterTransformKind | null;
	reason?: 'type_mismatch' | 'missing_required_columns' | 'missing_typed_schema';
	missingColumns?: string[];
	snapshotSourceSchemaFingerprint?: string;
	snapshotTargetSchemaFingerprint?: string;
	currentSourceSchemaFingerprint?: string;
	currentTargetSchemaFingerprint?: string;
	snapshotDrift?: boolean;
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
		targetHandle?: string;
		sourceHandle?: string;
		sourceNodeId?: string;
		targetNodeId?: string;
		mode?: 'work' | 'param' | 'control';
		sourceAffinity?: 'work' | 'param' | 'control';
		targetAffinity?: 'work' | 'param' | 'control';
		snapshotSourceSchemaFingerprint?: string;
		snapshotTargetSchemaFingerprint?: string;
		currentSourceSchemaFingerprint?: string;
		currentTargetSchemaFingerprint?: string;
		snapshotDrift?: boolean;
	};
	suggestions: string[];
};

export type NodeSchemaContractEdge = {
	edgeId: string;
	mode: 'work' | 'param' | 'control';
	direction: 'incoming' | 'outgoing';
	sourceNodeId: string;
	targetNodeId: string;
	sourceHandle: string | null;
	targetHandle: string | null;
	providedSchema: Record<string, any>;
	requiredSchema: Record<string, any>;
	severity: 'clean' | 'warning' | 'error';
	snapshotDrift?: boolean;
	snapshotSourceSchemaFingerprint?: string;
	snapshotTargetSchemaFingerprint?: string;
	currentSourceSchemaFingerprint?: string;
	currentTargetSchemaFingerprint?: string;
	suggestions: string[];
	adapterKind: AdapterTransformKind | null;
};

export type NodeSchemaContractSnapshot = {
	nodeId: string;
	status: 'clean' | 'warning' | 'error';
	edges: NodeSchemaContractEdge[];
};

// ---------------------------------------------------------------------------
// Input resolution
// ---------------------------------------------------------------------------

export type InputResolution = {
	inputHandle: string;
	edge: { fromNodeId: string; sourceHandle: string } | null;
	status: 'resolved' | 'missing';
	reason?: 'DISCONNECTED' | 'UPSTREAM_NO_ARTIFACT' | 'UPSTREAM_FAILED' | 'UNKNOWN';
	artifactId?: string;
	artifactSource?: 'active_run' | 'bound';
	upstream: {
		nodeId: string;
		sourceHandle: string;
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

// ---------------------------------------------------------------------------
// Root graph state
// ---------------------------------------------------------------------------

export type GraphState = {
	graphId: string;
	nodeDocExplanationMode: NodeDocExplanationMode;
	nodeDocTrainingMode: NodeDocTrainingMode;
	nodeDocTooltipEnabled: boolean;
	nodeDocTooltipOpenDelayMs: number;
	nodeDocPlanesExpansionEnabled: boolean;
	nodeDocPlanesExpansionDelayMs: number;
	nodeDocExplainModel: string;
	nodeDocExplainTemperature: number;
	nodeDocExplainTopP: number;
	nodeDocExplainMaxTokens: number;
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
	runBlockedReason: RunBlockedReason | null;
	viewMode: GraphViewMode;
	schemaWarningDismissCount: number;
	nodeOutputs: Record<string, NodeOutputInfo>;
	nodeBindings: Record<string, NormalizedNodeBinding>;
	activeRunId: string | null;
	queueRuntime?: QueueRuntime;
	editingContext: EditorContext;
	componentEditSession: ComponentEditSession | null;
	componentContractDraftCache: Record<string, Record<string, any>>;
	checkpointRegistry: CheckpointRegistry;
	schemaPlane: SchemaPlaneState;
};

// Pulled out of GraphState to keep it readable.
export type QueueRuntime = {
	metrics?: Record<string, unknown>;
	nodeMetrics?: Record<string, unknown>;
	runtimeItemMetrics?: Record<string, unknown>;
	runScoped?: {
		runId?: string;
		scope?: string;
		metrics?: Record<string, unknown>;
		nodeMetrics?: Record<string, unknown>;
		runtimeItemMetrics?: Record<string, unknown>;
	};
	aggregateDiagnostics?: {
		queueMetricEvents: number;
		itemsEnqueued: number;
		itemsDequeued: number;
		itemsAccepted: number;
		itemsRejected: number;
	};
	schedulerSnapshot?: {
		readyCount: number;
		inflightCount: number;
		pendingQueueDepth: number;
		runnableNodeCount: number;
		stalled: boolean;
		perNode?: Array<{
			nodeId: string;
			readyWork: boolean;
			inflight: number;
			pendingInputCount: number;
			lastBlockedReasonCode?: string;
		}>;
		updatedAt?: string;
	};
	llmLease?: {
		state: 'waiting' | 'acquired' | 'released';
		nodeId?: string;
		holderNodeId?: string | null;
		activeNodeIds?: string[];
		waitQueueLength?: number;
		waitingNodeIds?: string[];
		updatedAt?: string;
	};
	currentRunSummary?: {
		runId: string;
		maxPendingQueueDepth: number;
		hadStalledSnapshot: boolean;
		blockedEvents: number;
		runtimeMs?: number;
		peakConcurrency?: number;
	};
	runHistory?: Array<{
		runId: string;
		finishedAt: string;
		status: RunStatus;
		runtimeMs: number;
		peakConcurrency: number;
		maxPendingQueueDepth: number;
		hadStalledSnapshot: boolean;
		blockedEvents: number;
	}>;
	adaptiveDecisions?: Array<{
		at: string;
		runId: string;
		mode: 'off' | 'observe' | 'enforce' | string;
		enforced: boolean;
		inputs: Record<string, unknown>;
		reasons: string[];
		hardCaps: Record<string, number>;
		minCaps: Record<string, number>;
		proposedCaps: Record<string, number>;
		effectiveCaps: Record<string, number>;
		changedCaps: Record<string, { from: number; to: number }>;
	}>;
	handleStates?: Record<string, { state: string; updatedAt?: string }>;
	handleTimeline?: Array<{
		nodeId: string;
		handle: string;
		signal: string;
		at: string;
	}>;
	branchCascade?: Array<{
		originNodeId: string;
		blockedNodeIds: string[];
		reasonCode?: string;
		at?: string;
	}>;
	handleSatisfaction?: Record<
		string,
		{
			nodeId: string;
			handle: string;
			status: 'all' | 'partial' | 'none';
			connectedEdges: number;
			providedEdges: number;
			updatedAt?: string;
		}
	>;
	paramControlWarnings?: Record<
		string,
		{
			nodeId: string;
			handle: string;
			edgeId: string;
			plane: 'param' | 'control';
			code: 'PARAM_CONTROL_EMPTY_INPUT';
			reasonCode?: string;
			upstreamNodeId?: string;
			updatedAt?: string;
		}
	>;
	warningSummary?: Record<
		string,
		{
			warningKey: string;
			nodeId: string;
			handle: string;
			code: string;
			plane?: string;
			edgeId?: string;
			reasonCode?: string;
			upstreamNodeId?: string;
			count: number;
			firstAt?: string;
			updatedAt?: string;
		}
	>;
	blockedByNode?: Record<
		string,
		{
			nodeId: string;
			reasonCode: string;
			handle?: string;
			plane?: 'work' | 'param' | 'control';
			missingEdgeIds?: string[];
			waitingOnNodeIds?: string[];
			details?: Record<string, unknown>;
			updatedAt?: string;
		}
	>;
	softFailByNode?: Record<
		string,
		{
			count: number;
			itemsRejected: number;
			lastAt?: string;
		}
	>;
	controlPlaneEdgeState?: Record<
		string,
		{
			edgeId: string;
			open: boolean;
			closed: boolean;
			depth: number;
			blocked: boolean;
			lastSeq: number;
			updatedAt?: string;
		}
	>;
	controlPlaneNodeState?: Record<
		string,
		{
			nodeId: string;
			lastSignal: string;
			terminalReasonCode?: string;
			lastSeq: number;
			updatedAt?: string;
		}
	>;
	appliedControlSeq?: number;
};

// ---------------------------------------------------------------------------
// Shared constants
// ---------------------------------------------------------------------------

// Typed as NodeStatus (from $lib/flow/types) so they can be used as status values.
export const NODE_STATUS_IDLE = 'idle' as NodeStatus;
export const NODE_STATUS_SUCCEEDED = 'succeeded' as NodeStatus;

/** The canonical empty inspector state. Kept here so every module that needs
 *  to reset to it imports from one place instead of reconstructing the literal. */
export const INITIAL_INSPECTOR: InspectorState = {
	nodeId: null,
	draftParams: {},
	dirty: false,
	systemNotice: null,
	uiByNodeId: {}
};
