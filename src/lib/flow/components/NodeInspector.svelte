<script lang="ts">
	// lib/flow/components/NodeInspector.svelte
	import { SourceEditorByKind } from '$lib/flow/components/editors/SourceEditor/SourceEditor';
	import SourceGuidedQuickEditor from '$lib/flow/components/editors/SourceEditor/SourceGuidedQuickEditor.svelte';
	import { LlmEditorByKind } from '$lib/flow/components/editors/LlmEditor/LlmEditor'; // <-- your new registry
	import {
		guidedControlsForModelKind,
		taskKindsForModelKind
	} from '$lib/flow/components/editors/LlmEditor/modelAssist';
	import { buildModelAutoFixes } from '$lib/flow/components/editors/LlmEditor/modelErrorAssist';
	import { buildModelPreviewDiff } from '$lib/flow/components/editors/LlmEditor/modelPreview';
	import { TransformEditorByKind } from '$lib/flow/components/editors/TransformEditor/TransformEditor';
	import { ToolEditorByProvider } from '$lib/flow/components/editors/ToolEditor/ToolEditor';
	import ToolEditor from '$lib/flow/components/editors/ToolEditor/ToolEditor.svelte';
	import ComponentEditor from '$lib/flow/components/editors/ComponentEditor/ComponentEditor.svelte';
	import ThemedSelect, { type ThemedSelectOption } from '$lib/flow/components/ui/ThemedSelect.svelte';
	import { getArtifactMetaUrl, getArtifactPreviewUrl } from '$lib/flow/client/runs';
	import { parseInputSchemaView, type InputSchemaView } from '$lib/flow/components/editors/TransformEditor/inputSchema';
	import { buildTransformSchemaProps } from '$lib/flow/components/editors/TransformEditor/schemaPropagation';
	import {
		buildTransformAutoFixes,
		buildTransformPreviewDiff,
		guidedControlsForTransform,
		suggestNextTransformOps
	} from '$lib/flow/components/editors/TransformEditor/transformAssist';
	import {
		buildSourceCapabilityNotices,
		resolveSourceCapabilityDescriptor
	} from '$lib/flow/sourceCapabilities';

	import type { PipelineNodeData } from '$lib/flow/types';
	import {
		graphStore,
		__buildNodeSchemaContractSnapshotForTest,
		type NodeSchemaContractEdge
	} from '$lib/flow/store/graphStore';
	import {
		collectExpectedInputHandles,
		collectNodeHandleStates,
		schemaEdgeContractBadges,
		groupSchemaEdgesByMode,
		schemaEdgeCounterpartyName,
		schemaEdgeDriftGuidance,
		type ExpectedInputHandleSummary
	} from '$lib/flow/components/nodeInspectorSchema';
	import { formatUserLocalTime } from '$lib/flow/components/localTime';

	import { selectedNode as selectedNodeStore } from '$lib/flow/store/graphStore';

	import type { LlmKind, TransformKind, ToolProvider } from '$lib/flow/types/paramsMap';
	import type { ModelKind, ModelTaskKind } from '$lib/flow/schema/llm';
	// import type { LlmKind } from '$lib/flow/types/paramsMap'; // adjust path if yours differs
	// import type { TransformKind } from '$lib/flow/types/paramsMap';

	$: selectedNode = $selectedNodeStore;

	// kind discriminators
	$: kind = selectedNode?.data?.kind as PipelineNodeData['kind'] | undefined;
	$: isSource = kind === 'source';
	$: isLlm = kind === 'llm' || kind === 'model';
	$: isTool = kind === 'tool';
	$: isTransform = kind === 'transform';
	$: isComponent = kind === 'component';

	// inspector draft params (single source of truth for editors)
	$: params = $graphStore.inspector?.draftParams ?? {};
	$: nodeDebugEnabled = Boolean((params as any)?.debug?.enabled ?? false);
	$: nodeDebugLogInputPreview = Boolean((params as any)?.debug?.log_input_preview ?? false);
	$: nodeDebugLogRawOutput = Boolean((params as any)?.debug?.log_raw_output ?? false);
	$: nodeError = selectedNode ? ($graphStore.nodeOutputs?.[selectedNode.id]?.lastError ?? null) : null;
	$: sourceObservability = selectedNode
		? (($graphStore.nodeOutputs?.[selectedNode.id]?.sourceObservability ?? null) as Record<string, unknown> | null)
		: null;
	$: sourcePrimingArtifact = selectedNode
		? (($graphStore.nodeOutputs?.[selectedNode.id]?.primingArtifact ?? null) as Record<string, unknown> | null)
		: null;
	$: sourcePrimingDrift = (() => {
		const drift = sourcePrimingArtifact?.drift;
		return drift && typeof drift === 'object' ? (drift as Record<string, unknown>) : null;
	})();
	$: sourceObsWarnings = (() => {
		if (!sourceObservability) return [] as string[];
		const warnings: string[] = [];
		const nullRatio = Number(sourceObservability?.null_ratio ?? 0);
		if (Number.isFinite(nullRatio) && nullRatio >= 0.2) {
			warnings.push(`High null ratio: ${(nullRatio * 100).toFixed(1)}%`);
		}
		const retryCount = Number(sourceObservability?.retry_count ?? 0);
		if (Number.isFinite(retryCount) && retryCount > 0) {
			warnings.push(`Retries observed: ${retryCount}`);
		}
		return warnings;
	})();
	$: sourceObsDetailsText = (() => {
		if (!sourceObservability) return '';
		const omit = new Set([
			'source_kind',
			'output_mode',
			'input_bytes',
			'output_rows',
			'null_ratio',
			'retry_count',
			'partition_count',
			'execution_ms',
			'cost_estimate_usd',
			'table_columns'
		]);
		const details: Record<string, unknown> = {};
		for (const [k, v] of Object.entries(sourceObservability)) {
			if (!omit.has(k)) details[k] = v;
		}
		return Object.keys(details).length > 0 ? JSON.stringify(details, null, 2) : '';
	})();

	// sub-kinds / kinds
	$: sourceKind = (selectedNode?.data as any)?.sourceKind ?? 'file';
	$: sourceCapability = isSource ? resolveSourceCapabilityDescriptor(sourceKind) : null;
	$: sourceCapabilityNotices =
		isSource && sourceCapability
			? buildSourceCapabilityNotices(sourceCapability, (params as Record<string, unknown>) ?? null)
			: [];

	// LLM kind source of truth: node discriminator (optionally draft override), never node.data.kind.
	$: llmKind = (((params as any)?.llmKind ?? (selectedNode?.data as any)?.llmKind ?? 'ollama') as LlmKind);
	$: modelKind = (((selectedNode?.data as any)?.modelKind ?? 'llm') as ModelKind);
	$: taskKind = (((selectedNode?.data as any)?.taskKind ?? 'generate') as ModelTaskKind);
	$: modelTaskOptions = taskKindsForModelKind(modelKind);
	$: modelGuidedControls = isLlm ? guidedControlsForModelKind(modelKind) : [];

	$: transformKind = (selectedNode?.data as any)?.transformKind ?? 'select';
	$: toolProvider = ((params as any)?.provider ??
		(selectedNode?.data as any)?.params?.provider ??
		'mcp') as ToolProvider;
	$: schemaProps = buildTransformSchemaProps(transformKind as TransformKind, inputSchemas);

	const consumeModeOptions: ThemedSelectOption[] = [
		{ value: 'once', label: 'once' },
		{ value: 'single_item', label: 'single_item' },
		{ value: 'batch', label: 'batch' }
	];

	const directionOptions: ThemedSelectOption[] = [
		{ value: 'in', label: 'in' },
		{ value: 'out', label: 'out' }
	];

	const planeOptions: ThemedSelectOption[] = [
		{ value: 'work', label: 'work' },
		{ value: 'param', label: 'param' },
		{ value: 'control', label: 'control' }
	];

	const cardinalityOptions: ThemedSelectOption[] = [
		{ value: 'many', label: 'many' },
		{ value: 'one', label: 'one' }
	];

	const overflowOptions: ThemedSelectOption[] = [
		{ value: 'block', label: 'block' },
		{ value: 'spill', label: 'spill' },
		{ value: 'error', label: 'error' }
	];

	const arbitrationOptions: ThemedSelectOption[] = [
		{ value: 'fifo', label: 'fifo (default)' },
		{ value: 'round_robin', label: 'round_robin (preview)' }
	];

	const itemModeOptions: ThemedSelectOption[] = [
		{ value: 'artifact', label: 'artifact' },
		{ value: 'json_items', label: 'json_items' },
		{ value: 'table_rows', label: 'table_rows' }
	];
	$: schemaContract = selectedNode
		? __buildNodeSchemaContractSnapshotForTest($graphStore as any, selectedNode.id)
		: { nodeId: '', status: 'clean', edges: [] as NodeSchemaContractEdge[] };
	$: schemaContractGroups = groupSchemaEdgesByMode(schemaContract.edges ?? []);
	$: schemaDeprecationNotices = (() => {
		if (!selectedNode) return [] as string[];
		const notices: string[] = [];
		const nodeData = (selectedNode.data ?? {}) as Record<string, any>;
		const schema = nodeData?.schema && typeof nodeData.schema === 'object' ? (nodeData.schema as Record<string, any>) : {};
		if (schema.expectedInputSchema && typeof schema.expectedInputSchema === 'object') {
			notices.push(
				'Legacy data.schema.expectedInputSchema detected. Migrate to data.schema.expectedInputSchemas.<handle> before 2026-06-30.'
			);
		}
		const hasPortDeclarations =
			nodeData?.portDeclarations && typeof nodeData.portDeclarations === 'object';
		const hasPortContracts =
			nodeData?.portContracts &&
			typeof nodeData.portContracts === 'object' &&
			Object.keys(nodeData.portContracts as Record<string, unknown>).length > 0;
		if (!hasPortDeclarations && hasPortContracts) {
			notices.push(
				'Legacy data.portContracts is acting as the primary port model. Declare data.portDeclarations before 2026-06-30.'
			);
		}
		const connectedEdges = ($graphStore.edges ?? []).filter(
			(edge) =>
				String((edge as any)?.source ?? '') === String(selectedNode.id) ||
				String((edge as any)?.target ?? '') === String(selectedNode.id)
		);
		if (
			connectedEdges.some(
				(edge) => String((edge as any)?.data?.queue?.policy ?? 'fifo').trim().toLowerCase() === 'round_robin'
			)
		) {
			notices.push('One or more connected edges use queue.policy=round_robin (preview).');
		}
		return notices;
	})();
	$: guidedControls = isTransform ? guidedControlsForTransform(transformKind as TransformKind) : [];
	$: transformPreviewDiff = isTransform
		? buildTransformPreviewDiff({
				kind: transformKind as TransformKind,
				params: (params ?? {}) as Record<string, unknown>,
				inputColumns: Array.from(new Set([...(schemaProps.inputColumns ?? []), ...inputPreviewColumns])),
				sampleRows: inputPreviewRows
			})
		: {
				beforeColumns: [],
				afterColumns: [],
				beforeRows: [],
				afterRows: [],
				notes: []
			};
	$: transformAutoFixes = isTransform
		? buildTransformAutoFixes({
				kind: transformKind as TransformKind,
				params: (params ?? {}) as Record<string, unknown>,
				nodeError,
				availableColumns: schemaProps.inputColumns ?? []
			})
		: [];
	$: nextTransformOps = isTransform
		? suggestNextTransformOps({
				kind: transformKind as TransformKind,
				nodeError,
				schemaEdges: (schemaContract.edges ?? []).filter((e) => e.direction === 'incoming')
			})
		: [];
	$: modelAutoFixes = isLlm
		? buildModelAutoFixes({
				nodeError,
				params: (params ?? {}) as Record<string, unknown>,
				schemaEdges: schemaContract.edges ?? []
			})
		: [];
	$: modelPreviewDiff = isLlm
		? buildModelPreviewDiff({
				params: (params ?? {}) as Record<string, unknown>,
				inputSchemas,
				sampleRows: inputPreviewRows
			})
		: {
				inputType: 'unknown',
				outputType: 'text',
				inputColumns: [],
				outputColumns: [],
				sampleInput: null,
				sampleOutput: null,
				notes: []
			};
	$: nodeProcessingPolicy = (() => {
		const policyRaw =
			((selectedNode?.data as any)?.processingPolicy ?? {}) as Record<string, unknown>;
		const consumeMode = String(policyRaw?.consume_mode ?? 'once').trim().toLowerCase();
		const readOnceRaw =
			(policyRaw as any)?.read_once ?? (policyRaw as any)?.readOnce ?? consumeMode === 'once';
		return {
			consume_mode: (
				consumeMode === 'single_item' || consumeMode === 'batch' ? consumeMode : 'once'
			) as 'once' | 'single_item' | 'batch',
			batch_size: Math.max(1, Number(policyRaw?.batch_size ?? 1)),
			max_inflight: Math.max(1, Number(policyRaw?.max_inflight ?? 1)),
			read_once: Boolean(readOnceRaw)
		};
	})();
	$: nodeProcessingPolicyByHandle = (() => {
		const policyRaw =
			((selectedNode?.data as any)?.processingPolicy ?? {}) as Record<string, unknown>;
		const inputHandlesRaw =
			policyRaw?.input_handles && typeof policyRaw.input_handles === 'object'
				? (policyRaw.input_handles as Record<string, unknown>)
				: {};
		const out: Record<
			string,
			{ consume_mode: 'once' | 'single_item' | 'batch'; batch_size: number; max_inflight: number; read_once: boolean }
		> = {};
		for (const handleSummary of expectedInputHandles ?? []) {
			const handle = String(handleSummary?.handle ?? '').trim();
			if (!handle) continue;
			const handleRaw =
				inputHandlesRaw && typeof inputHandlesRaw[handle] === 'object'
					? (inputHandlesRaw[handle] as Record<string, unknown>)
					: {};
			const consumeMode = String(
				(handleRaw as any)?.consume_mode ?? nodeProcessingPolicy.consume_mode ?? 'once'
			)
				.trim()
				.toLowerCase();
			const readOnceRaw =
				(handleRaw as any)?.read_once ?? (handleRaw as any)?.readOnce ?? consumeMode === 'once';
			out[handle] = {
				consume_mode: (
					consumeMode === 'single_item' || consumeMode === 'batch' ? consumeMode : 'once'
				) as 'once' | 'single_item' | 'batch',
				batch_size: Math.max(
					1,
					Number((handleRaw as any)?.batch_size ?? nodeProcessingPolicy.batch_size ?? 1)
				),
				max_inflight: Math.max(
					1,
					Number((handleRaw as any)?.max_inflight ?? nodeProcessingPolicy.max_inflight ?? 1)
				),
				read_once: Boolean(readOnceRaw)
			};
		}
		return out;
	})();

	let inputSchemas: InputSchemaView[] = [];
	let inputSchemaReqSeq = 0;
	let lastInputSignature = '';
	let transformGuidedMode = false;
	let sourceGuidedMode = false;
	let modelGuidedMode = false;
	let modelAdvancedOpen = false;
	let modelEditorNodeId = '';
	let inputPreviewRows: Array<Record<string, unknown>> = [];
	let inputPreviewColumns: string[] = [];
	let expectedInputHandles: ExpectedInputHandleSummary[] = [];
	type SchemaAssistState = 'fresh' | 'partial' | 'stale' | 'unknown';
	type SchemaAssistSummary = {
		state: SchemaAssistState;
		source: string;
		hasSchema: boolean;
	};

	function artifactIdFromBinding(binding: any): string {
		return String(
			binding?.current?.artifactId ??
				binding?.currentArtifactId ??
				binding?.last?.artifactId ??
				binding?.lastArtifactId ??
				''
		);
	}

	function typedSchemaPayloadFromNode(node: Record<string, any> | undefined): Record<string, unknown> | null {
		const observation =
			(node?.data as any)?.schema?.observedSchema ??
			(node?.data as any)?.schema?.inferredSchema ??
			null;
		const typed = observation?.typedSchema;
		const typedType = String(typed?.type ?? '').trim().toLowerCase();
		const source = String(observation?.source ?? 'unknown');
		const state = String(observation?.state ?? 'unknown');
		if (typedType === 'table') {
			const fields = Array.isArray(typed?.fields) ? typed.fields : [];
			const columns = fields
				.map((field: Record<string, unknown>) => ({
					name: String(field?.name ?? '').trim(),
					type: String(field?.type ?? 'unknown').trim() || 'unknown'
				}))
				.filter((col: { name: string }) => col.name.length > 0);
			return {
				type: 'table',
				schema: {
					contract: 'TABLE_V1',
					source,
					state,
					table: { columns }
				}
			};
		}
		if (typedType === 'text') return { type: 'text', source, state };
		if (typedType === 'json') return { type: 'json', source, state };
		if (typedType === 'binary') return { type: 'binary', source, state };
		if (typedType === 'embeddings') return { type: 'embeddings', source, state };
		return null;
	}

	function schemaAssistStateRank(state: string): number {
		const normalized = String(state ?? 'unknown').toLowerCase();
		if (normalized === 'stale') return 3;
		if (normalized === 'partial') return 2;
		if (normalized === 'fresh') return 1;
		return 0;
	}

	function summarizeSchemaAssist(inputSchemasRaw: InputSchemaView[]): SchemaAssistSummary {
		const inputSchemas = Array.isArray(inputSchemasRaw) ? inputSchemasRaw : [];
		if (inputSchemas.length === 0) {
			return { state: 'unknown', source: 'unknown', hasSchema: false };
		}
		let topState: SchemaAssistState = 'unknown';
		let source = 'unknown';
		for (const view of inputSchemas) {
			const candidateState = String(view?.schemaState ?? 'unknown').toLowerCase();
			if (schemaAssistStateRank(candidateState) > schemaAssistStateRank(topState)) {
				topState = candidateState as SchemaAssistState;
			}
			if (source === 'unknown') {
				const candidateSource = String(view?.schemaSource ?? 'unknown').trim().toLowerCase();
				if (candidateSource && candidateSource !== 'unknown') source = candidateSource;
			}
		}
		return { state: topState, source, hasSchema: true };
	}

	$: schemaAssist = summarizeSchemaAssist(inputSchemas);
	$: schemaDriftSummary = (() => {
		const expectedEnvelope = (selectedNode?.data as any)?.schema?.expectedSchema;
		const observedEnvelope = (selectedNode?.data as any)?.schema?.observedSchema;
		const expected = expectedEnvelope?.typedSchema;
		const observed = observedEnvelope?.typedSchema;
		if (!expected || !observed) return null;
		const expectedAt = Date.parse(String(expectedEnvelope?.updatedAt ?? ''));
		const observedAt = Date.parse(String(observedEnvelope?.updatedAt ?? ''));
		if (Number.isFinite(expectedAt) && Number.isFinite(observedAt) && observedAt < expectedAt) {
			// Ignore stale observed schema; show drift only after a run newer than the expected declaration.
			return null;
		}
		const expectedType = String(expected?.type ?? 'unknown').trim().toLowerCase();
		const observedType = String(observed?.type ?? 'unknown').trim().toLowerCase();
		const issues: string[] = [];
		if (expectedType !== 'unknown' && observedType !== 'unknown' && expectedType !== observedType) {
			issues.push(`type ${expectedType}->${observedType}`);
		}
		if (expectedType === 'table' && observedType === 'table') {
			const expectedFields = Array.isArray(expected?.fields) ? expected.fields : [];
			const observedFields = Array.isArray(observed?.fields) ? observed.fields : [];
			const observedNames = new Set(
				observedFields.map((f: Record<string, unknown>) => String(f?.name ?? '').trim().toLowerCase())
			);
			const missing = expectedFields
				.map((f: Record<string, unknown>) => String(f?.name ?? '').trim())
				.filter((name: string) => name.length > 0 && !observedNames.has(name.toLowerCase()));
			if (missing.length > 0) issues.push(`missing: ${missing.join(', ')}`);
		}
		return issues.length > 0 ? issues.join(' | ') : null;
	})();
	$: expectedInputHandles = collectExpectedInputHandles(
		selectedNode as any,
		(schemaContract.edges ?? []) as NodeSchemaContractEdge[]
	);
	$: nodePortDeclarations = (() => {
		const decls =
			((selectedNode?.data as any)?.portDeclarations ?? null) as
				| { in?: Record<string, any>; out?: Record<string, any> }
				| null;
		return {
			in: decls?.in && typeof decls.in === 'object' ? decls.in : {},
			out: decls?.out && typeof decls.out === 'object' ? decls.out : {}
		};
	})();
	$: nodeQueuePortStats = (() => {
		const nodeId = String(selectedNode?.id ?? '').trim();
		if (!nodeId) return [] as Array<Record<string, unknown>>;
		const runtimeEdges = (($graphStore as any)?.queueRuntime?.metrics as any)?.edges;
		if (!runtimeEdges || typeof runtimeEdges !== 'object') return [] as Array<Record<string, unknown>>;
		const out: Array<Record<string, unknown>> = [];
		for (const edge of ($graphStore?.edges ?? []) as any[]) {
			const edgeId = String(edge?.id ?? '').trim();
			if (!edgeId) continue;
			if (String(edge?.target ?? '') === nodeId) {
				const handle = String(edge?.targetHandle ?? 'in').trim() || 'in';
				const metric = (runtimeEdges as Record<string, any>)[`${edgeId}:${handle}`] ?? null;
				out.push({
					direction: 'in',
					edgeId,
					handle,
					metric
				});
			}
			if (String(edge?.source ?? '') === nodeId) {
				const handle = String(edge?.targetHandle ?? 'in').trim() || 'in';
				const metric = (runtimeEdges as Record<string, any>)[`${edgeId}:${handle}`] ?? null;
				out.push({
					direction: 'out',
					edgeId,
					handle,
					metric
				});
			}
		}
		return out;
	})();
	$: graphNodesForInspector = (($graphStore?.nodes ?? []) as any[]).map((node) => ({
		id: String(node?.id ?? ''),
		label: String((node?.data as any)?.label ?? node?.id ?? '')
	}));
	$: runScopedQueueSummary = (() => {
		const runScoped = (($graphStore as any)?.queueRuntime?.runScoped ?? null) as Record<string, any> | null;
		if (!runScoped || typeof runScoped !== 'object') return null;
		const itemStats = (runScoped.runtimeItemMetrics ?? {}) as Record<string, unknown>;
		return {
			runId: String(runScoped.runId ?? '').trim(),
			scope: String(runScoped.scope ?? 'run').trim(),
			enq: Number(itemStats.itemsEnqueued ?? 0),
			deq: Number(itemStats.itemsDequeued ?? 0),
			accepted: Number(itemStats.itemsAccepted ?? 0),
			rejected: Number(itemStats.itemsRejected ?? 0)
		};
	})();
	$: aggregateQueueSummary = (() => {
		const aggregate =
			(($graphStore as any)?.queueRuntime?.aggregateDiagnostics ?? null) as Record<string, unknown> | null;
		if (!aggregate || typeof aggregate !== 'object') return null;
		return {
			events: Number(aggregate.queueMetricEvents ?? 0),
			enq: Number(aggregate.itemsEnqueued ?? 0),
			deq: Number(aggregate.itemsDequeued ?? 0),
			accepted: Number(aggregate.itemsAccepted ?? 0),
			rejected: Number(aggregate.itemsRejected ?? 0)
		};
	})();
	$: nodeHandleStates = (() => {
		const nodeId = String(selectedNode?.id ?? '').trim();
		const raw =
			(($graphStore as any)?.queueRuntime?.handleStates &&
			typeof ($graphStore as any).queueRuntime.handleStates === 'object'
				? (($graphStore as any).queueRuntime.handleStates as Record<string, unknown>)
				: null) ?? null;
		return collectNodeHandleStates(nodeId, raw);
	})();
	$: nodeBranchCascade = (() => {
		const nodeId = String(selectedNode?.id ?? '').trim();
		if (!nodeId) return [] as Array<{ originNodeId: string; blockedNodeIds: string[]; reasonCode?: string; at?: string }>;
		const entries = Array.isArray(($graphStore as any)?.queueRuntime?.branchCascade)
			? (((($graphStore as any).queueRuntime.branchCascade as unknown[]) ?? []) as Array<Record<string, unknown>>)
			: [];
		return entries
			.map((entry) => {
				const originNodeId = String(entry?.originNodeId ?? '').trim();
				const blockedNodeIds = Array.isArray(entry?.blockedNodeIds)
					? (entry.blockedNodeIds as unknown[]).map((item) => String(item ?? '').trim()).filter(Boolean)
					: [];
				const reasonCode = String(entry?.reasonCode ?? '').trim();
				const at = String(entry?.at ?? '').trim();
				return { originNodeId, blockedNodeIds, reasonCode, at };
			})
			.filter((entry) => entry.originNodeId === nodeId || entry.blockedNodeIds.includes(nodeId));
	})();
	$: nodeHandleSatisfaction = (() => {
		const nodeId = String(selectedNode?.id ?? '').trim();
		if (!nodeId) return [] as Array<{ handle: string; status: string; connectedEdges: number; providedEdges: number; updatedAt?: string }>;
		const map =
			(($graphStore as any)?.queueRuntime?.handleSatisfaction &&
			typeof ($graphStore as any).queueRuntime.handleSatisfaction === 'object'
				? (($graphStore as any).queueRuntime.handleSatisfaction as Record<string, unknown>)
				: null) ?? null;
		if (!map) return [] as Array<{ handle: string; status: string; connectedEdges: number; providedEdges: number; updatedAt?: string }>;
		const out: Array<{ handle: string; status: string; connectedEdges: number; providedEdges: number; updatedAt?: string }> = [];
		for (const [key, value] of Object.entries(map)) {
			if (!String(key).startsWith(`${nodeId}:`)) continue;
			const row = (value ?? {}) as Record<string, unknown>;
			out.push({
				handle: String(row.handle ?? '').trim(),
				status: String(row.status ?? 'all').trim(),
				connectedEdges: Number(row.connectedEdges ?? 0),
				providedEdges: Number(row.providedEdges ?? 0),
				updatedAt: String(row.updatedAt ?? '').trim()
			});
		}
		out.sort((a, b) => a.handle.localeCompare(b.handle));
		return out;
	})();
	$: nodeParamControlWarnings = (() => {
		const nodeId = String(selectedNode?.id ?? '').trim();
		if (!nodeId)
			return [] as Array<{
				handle: string;
				edgeId: string;
				plane: 'param' | 'control';
				code: string;
				reasonCode?: string;
				upstreamNodeId?: string;
				updatedAt?: string;
			}>;
		const map =
			(($graphStore as any)?.queueRuntime?.paramControlWarnings &&
			typeof ($graphStore as any).queueRuntime.paramControlWarnings === 'object'
				? (($graphStore as any).queueRuntime.paramControlWarnings as Record<string, unknown>)
				: null) ?? null;
		if (!map)
			return [] as Array<{
				handle: string;
				edgeId: string;
				plane: 'param' | 'control';
				code: string;
				reasonCode?: string;
				upstreamNodeId?: string;
				updatedAt?: string;
			}>;
		const out: Array<{
			handle: string;
			edgeId: string;
			plane: 'param' | 'control';
			code: string;
			reasonCode?: string;
			upstreamNodeId?: string;
			updatedAt?: string;
		}> = [];
		for (const [key, value] of Object.entries(map)) {
			if (!String(key).startsWith(`${nodeId}:`)) continue;
			const row = (value ?? {}) as Record<string, unknown>;
			const planeRaw = String(row.plane ?? '').trim().toLowerCase();
			out.push({
				handle: String(row.handle ?? '').trim(),
				edgeId: String(row.edgeId ?? '').trim(),
				plane: planeRaw === 'control' ? 'control' : 'param',
				code: String(row.code ?? '').trim(),
				reasonCode: String(row.reasonCode ?? '').trim() || undefined,
				upstreamNodeId: String(row.upstreamNodeId ?? '').trim() || undefined,
				updatedAt: String(row.updatedAt ?? '').trim() || undefined
			});
		}
		out.sort((a, b) => `${a.handle}:${a.edgeId}`.localeCompare(`${b.handle}:${b.edgeId}`));
		return out;
	})();
	$: nodeBlockedStatus = (() => {
		const nodeId = String(selectedNode?.id ?? '').trim();
		if (!nodeId) return null as null | Record<string, unknown>;
		const map =
			(($graphStore as any)?.queueRuntime?.blockedByNode &&
			typeof ($graphStore as any).queueRuntime.blockedByNode === 'object'
				? (($graphStore as any).queueRuntime.blockedByNode as Record<string, unknown>)
				: null) ?? null;
		if (!map) return null as null | Record<string, unknown>;
		const row = map[nodeId];
		return row && typeof row === 'object' ? (row as Record<string, unknown>) : null;
	})();
	$: schedulerSnapshot = (() => {
		const row =
			(($graphStore as any)?.queueRuntime?.schedulerSnapshot &&
			typeof ($graphStore as any).queueRuntime.schedulerSnapshot === 'object'
				? (($graphStore as any).queueRuntime.schedulerSnapshot as Record<string, unknown>)
				: null) ?? null;
		return row;
	})();
	$: llmLeaseStatus = (() => {
		const row =
			(($graphStore as any)?.queueRuntime?.llmLease &&
			typeof ($graphStore as any).queueRuntime.llmLease === 'object'
				? (($graphStore as any).queueRuntime.llmLease as Record<string, unknown>)
				: null) ?? null;
		return row;
	})();
	let expectedInputSchemaDraftByHandle: Record<string, string> = {};
	let expectedInputSchemaErrorByHandle: Record<string, string> = {};
	let expectedInputSchemaNodeId = '';
	let edgeConfigErrors: Record<string, string> = {};
	let expectedSchemaDraft = '';
	let expectedSchemaError = '';
	let expectedSchemaNodeId = '';
	let headerSchemaLoading = false;
	let newPortHandle = '';
	let newPortDirection: 'in' | 'out' = 'in';
	let newPortPlane: 'work' | 'param' | 'control' = 'work';

	type EdgeRuntimeConfig = {
		mode: 'work' | 'param' | 'control';
		fatal: boolean;
		queue: {
			max: number;
			overflow: 'block' | 'spill' | 'error';
			policy: 'fifo' | 'round_robin';
		};
		work: {
			item_mode: 'artifact' | 'json_items' | 'table_rows';
			max_items: number;
		};
	};

	function readEdgeRuntimeConfig(edgeId: string): EdgeRuntimeConfig {
		const edge = ($graphStore?.edges ?? []).find((candidate) => String(candidate.id ?? '') === edgeId);
		const mode = String((edge?.data as any)?.mode ?? 'work').trim().toLowerCase();
		const overflow = String((edge?.data as any)?.queue?.overflow ?? 'block').trim().toLowerCase();
		const queuePolicy = String((edge?.data as any)?.queue?.policy ?? 'fifo').trim().toLowerCase();
		const itemMode = String((edge?.data as any)?.work?.item_mode ?? (edge?.data as any)?.work?.itemMode ?? 'artifact')
			.trim()
			.toLowerCase();
		return {
			mode: (mode === 'param' || mode === 'control' ? mode : 'work') as 'work' | 'param' | 'control',
			fatal: Boolean((edge?.data as any)?.fatal ?? false),
			queue: {
				max: Math.max(1, Number((edge?.data as any)?.queue?.max ?? 1000)),
				overflow: (
					overflow === 'spill' || overflow === 'error' ? overflow : 'block'
				) as 'block' | 'spill' | 'error',
				policy: (
					queuePolicy === 'round_robin' ? 'round_robin' : 'fifo'
				) as 'fifo' | 'round_robin'
			},
			work: {
				item_mode: (
					itemMode === 'json_items' || itemMode === 'table_rows' ? itemMode : 'artifact'
				) as 'artifact' | 'json_items' | 'table_rows',
				max_items: Math.max(1, Number((edge?.data as any)?.work?.max_items ?? (edge?.data as any)?.work?.maxItems ?? 256))
			},
		};
	}

	function queueEdgeCounterpartyName(
		edgeId: string,
		direction: string,
		selectedId: string,
		nodes: Array<{ id: string; label: string }>
	): string {
		const edge = ($graphStore?.edges ?? []).find((candidate) => String(candidate.id ?? '') === String(edgeId));
		if (!edge) return '(unknown node)';
		const sourceId = String((edge as any)?.source ?? '').trim();
		const targetId = String((edge as any)?.target ?? '').trim();
		const pickId =
			String(direction).trim().toLowerCase() === 'in'
				? sourceId
				: String(direction).trim().toLowerCase() === 'out'
					? targetId
					: sourceId === selectedId
						? targetId
						: sourceId;
		const node = (nodes ?? []).find((item) => String(item.id) === String(pickId));
		const label = String(node?.label ?? '').trim();
		return label || pickId || '(unknown node)';
	}

	function patchEdgeRuntimeConfig(
		edgeId: string,
		patch: {
			mode?: 'work' | 'param' | 'control';
			fatal?: boolean;
			queue?: { max?: number; overflow?: 'block' | 'spill' | 'error'; policy?: 'fifo' | 'round_robin' };
			work?: { item_mode?: 'artifact' | 'json_items' | 'table_rows'; max_items?: number };
		}
	): void {
		const result = graphStore.updateEdgeConfig(edgeId, patch);
		if ((result as any)?.ok) {
			const { [edgeId]: _drop, ...rest } = edgeConfigErrors;
			edgeConfigErrors = rest;
			return;
		}
		edgeConfigErrors = {
			...edgeConfigErrors,
			[edgeId]: String((result as any)?.error ?? 'Failed to update edge config')
		};
	}

	function normalizeExpectedSchemaDraft(node: any): string {
		const typed =
			node?.data?.schema?.expectedSchema?.typedSchema ??
			node?.data?.schema?.inferredSchema?.typedSchema ??
			{ type: 'unknown', fields: [] };
		return JSON.stringify(typed, null, 2);
	}

	$: if (selectedNode?.id && selectedNode.id !== expectedSchemaNodeId) {
		expectedSchemaNodeId = selectedNode.id;
		expectedSchemaDraft = normalizeExpectedSchemaDraft(selectedNode);
		expectedSchemaError = '';
	}

	$: if (selectedNode?.id && selectedNode.id !== expectedInputSchemaNodeId) {
		expectedInputSchemaNodeId = selectedNode.id;
		const schema = (selectedNode.data as any)?.schema ?? {};
		const expectedInputSchemas =
			schema?.expectedInputSchemas && typeof schema.expectedInputSchemas === 'object'
				? (schema.expectedInputSchemas as Record<string, any>)
				: {};
		const incomingByHandle = new Map<string, Record<string, unknown>>();
		for (const edge of (schemaContract?.edges ?? []) as NodeSchemaContractEdge[]) {
			if (String(edge?.direction ?? '').trim().toLowerCase() !== 'incoming') continue;
			const handle = String(edge?.targetHandle ?? 'in').trim() || 'in';
			if (!incomingByHandle.has(handle) && edge?.requiredSchema && typeof edge.requiredSchema === 'object') {
				incomingByHandle.set(handle, edge.requiredSchema as Record<string, unknown>);
			}
		}
		const nextDrafts: Record<string, string> = {};
		for (const handleSummary of expectedInputHandles) {
			const handle = handleSummary.handle;
			const explicit = expectedInputSchemas?.[handle]?.typedSchema ?? null;
			const suggested = incomingByHandle.get(handle) ?? { type: handleSummary.classDefaultType };
			const typed = explicit ?? suggested;
			nextDrafts[handle] = JSON.stringify(typed, null, 2);
		}
		expectedInputSchemaDraftByHandle = nextDrafts;
		expectedInputSchemaErrorByHandle = {};
	}

	$: if (selectedNode?.id && selectedNode.id === expectedInputSchemaNodeId) {
		let changed = false;
		const nextDrafts = { ...expectedInputSchemaDraftByHandle };
		for (const handleSummary of expectedInputHandles) {
			if (nextDrafts[handleSummary.handle] == null) {
				nextDrafts[handleSummary.handle] = JSON.stringify({ type: handleSummary.classDefaultType }, null, 2);
				changed = true;
			}
		}
		if (changed) expectedInputSchemaDraftByHandle = nextDrafts;
	}

	$: if (isLlm && selectedNode?.id && selectedNode.id !== modelEditorNodeId) {
		modelEditorNodeId = selectedNode.id;
		modelGuidedMode = false;
		modelAdvancedOpen = false;
	}

	function useInferredExpectedSchema(): void {
		if (!selectedNode) return;
		const typed = (selectedNode.data as any)?.schema?.inferredSchema?.typedSchema ?? { type: 'unknown', fields: [] };
		expectedSchemaDraft = JSON.stringify(typed, null, 2);
		expectedSchemaError = '';
	}

	async function useHeaderForExpectedSchema(): Promise<void> {
		if (!selectedNode?.id) return;
		const graphId = String($graphStore?.graphId ?? '').trim();
		if (!graphId) {
			expectedSchemaError = 'Graph id is missing.';
			return;
		}
		const nodeBinding = ($graphStore?.nodeBindings ?? {})[selectedNode.id];
		const artifactId = artifactIdFromBinding(nodeBinding);
		if (!artifactId) {
			expectedSchemaError = 'No source artifact found. Run this node first.';
			return;
		}

		headerSchemaLoading = true;
		expectedSchemaError = '';
		try {
			const res = await fetch(getArtifactPreviewUrl(artifactId, graphId, 0, 1));
			if (!res.ok) throw new Error(`Failed to load artifact preview: ${res.status}`);
			const preview = await res.json();
			const inferredExpectedSchema =
				preview?.inferredExpectedSchema && typeof preview.inferredExpectedSchema === 'object'
					? (preview.inferredExpectedSchema as Record<string, unknown>)
					: null;
			const typedType = String(inferredExpectedSchema?.type ?? '').trim().toLowerCase();
			const fields = Array.isArray(inferredExpectedSchema?.fields)
				? (inferredExpectedSchema?.fields as unknown[])
				: [];
			if (typedType !== 'table' || fields.length === 0) {
				expectedSchemaError = 'Backend did not return inferred table schema from header.';
				return;
			}
			expectedSchemaDraft = JSON.stringify(
				inferredExpectedSchema,
				null,
				2
			);
		} catch (error) {
			expectedSchemaError = String((error as Error)?.message ?? 'Failed to infer schema from header.');
		} finally {
			headerSchemaLoading = false;
		}
	}

	function clearExpectedSchema(): void {
		if (!selectedNode?.id) return;
		const result = graphStore.setNodeExpectedSchema(selectedNode.id, null);
		if (!(result as any)?.ok) {
			expectedSchemaError = String((result as any)?.error ?? 'Failed to clear expected schema');
			return;
		}
		expectedSchemaDraft = JSON.stringify({ type: 'unknown', fields: [] }, null, 2);
		expectedSchemaError = '';
	}

	function saveExpectedSchema(): void {
		if (!selectedNode?.id) return;
		try {
			const parsed = JSON.parse(expectedSchemaDraft || '{}');
			const result = graphStore.setNodeExpectedSchema(selectedNode.id, parsed);
			if (!(result as any)?.ok) {
				expectedSchemaError = String((result as any)?.error ?? 'Failed to save expected schema');
				return;
			}
			expectedSchemaError = '';
		} catch (error) {
			expectedSchemaError = String((error as Error)?.message ?? 'Expected schema must be valid JSON.');
		}
	}

	function suggestedInputSchemaForHandle(handle: string): Record<string, unknown> {
		const incoming = (schemaContract.edges ?? []) as NodeSchemaContractEdge[];
		const match = incoming.find(
			(edge) =>
				String(edge?.direction ?? '').trim().toLowerCase() === 'incoming' &&
				(String(edge?.targetHandle ?? 'in').trim() || 'in') === handle &&
				edge.requiredSchema &&
				typeof edge.requiredSchema === 'object'
		);
		if (match?.requiredSchema && typeof match.requiredSchema === 'object') {
			return match.requiredSchema as Record<string, unknown>;
		}
		const summary = expectedInputHandles.find((item) => item.handle === handle);
		return { type: summary?.classDefaultType ?? 'none' };
	}

	function setExpectedInputDraft(handle: string, draft: string): void {
		expectedInputSchemaDraftByHandle = {
			...expectedInputSchemaDraftByHandle,
			[handle]: draft
		};
	}

	function clearExpectedInputError(handle: string): void {
		if (expectedInputSchemaErrorByHandle[handle] == null) return;
		const { [handle]: _drop, ...rest } = expectedInputSchemaErrorByHandle;
		expectedInputSchemaErrorByHandle = rest;
	}

	function setExpectedInputError(handle: string, message: string): void {
		expectedInputSchemaErrorByHandle = {
			...expectedInputSchemaErrorByHandle,
			[handle]: message
		};
	}

	function useSuggestedInputSchema(handle: string): void {
		setExpectedInputDraft(handle, JSON.stringify(suggestedInputSchemaForHandle(handle), null, 2));
		clearExpectedInputError(handle);
	}

	function inferredInputSchemaForHandle(handle: string): Record<string, unknown> {
		const incoming = (schemaContract.edges ?? []) as NodeSchemaContractEdge[];
		const match = incoming.find(
			(edge) =>
				String(edge?.direction ?? '').trim().toLowerCase() === 'incoming' &&
				(String(edge?.targetHandle ?? 'in').trim() || 'in') === handle &&
				edge?.providedSchema &&
				typeof edge.providedSchema === 'object'
		);
		if (match?.providedSchema && typeof match.providedSchema === 'object') {
			const provided = match.providedSchema as Record<string, unknown>;
			const inferredType = String(provided.type ?? '').trim().toLowerCase();
			const fields = Array.isArray(provided.fields) ? (provided.fields as unknown[]) : [];
			const columns = Array.isArray(provided.columns) ? (provided.columns as unknown[]) : [];
			if (inferredType.length > 0 && inferredType !== 'unknown') {
				if (fields.length > 0) {
					return { type: inferredType, fields };
				}
				if (columns.length > 0) {
					const normalizedFields = columns
						.map((col) => {
							const rawName =
								typeof col === 'string'
									? col
									: typeof (col as Record<string, unknown>)?.name === 'string'
										? String((col as Record<string, unknown>).name)
										: '';
							const name = String(rawName).trim();
							if (!name) return null;
							const rawType =
								typeof col === 'object' && col != null
									? String((col as Record<string, unknown>).type ?? 'unknown')
									: 'unknown';
							return { name, type: rawType || 'unknown' };
						})
						.filter((f): f is { name: string; type: string } => Boolean(f));
					if (normalizedFields.length > 0) {
						return { type: inferredType, fields: normalizedFields };
					}
				}
				return { type: inferredType };
			}
		}
		return suggestedInputSchemaForHandle(handle);
	}

	function useInferredInputSchema(handle: string): void {
		const typed = inferredInputSchemaForHandle(handle);
		setExpectedInputDraft(handle, JSON.stringify(typed, null, 2));
		clearExpectedInputError(handle);
	}

	function clearExpectedInputSchema(handle: string): void {
		if (!selectedNode?.id) return;
		const result = graphStore.setNodeExpectedInputSchemaForHandle(selectedNode.id, handle, null);
		if (!(result as any)?.ok) {
			setExpectedInputError(handle, String((result as any)?.error ?? 'Failed to clear expected input schema'));
			return;
		}
		setExpectedInputDraft(handle, JSON.stringify(suggestedInputSchemaForHandle(handle), null, 2));
		clearExpectedInputError(handle);
	}

	function saveExpectedInputSchema(handle: string): void {
		if (!selectedNode?.id) return;
		try {
			const parsed = JSON.parse(expectedInputSchemaDraftByHandle[handle] || '{}');
			const parsedType = String((parsed as any)?.type ?? '').trim().toLowerCase();
			if (parsedType === 'multi_input') {
				setExpectedInputError(
					handle,
					'Use a typed schema object (type + optional fields). multi_input envelopes are display-only.'
				);
				return;
			}
			const result = graphStore.setNodeExpectedInputSchemaForHandle(selectedNode.id, handle, parsed);
			if (!(result as any)?.ok) {
				setExpectedInputError(handle, String((result as any)?.error ?? 'Failed to save expected input schema'));
				return;
			}
			clearExpectedInputError(handle);
		} catch (error) {
			setExpectedInputError(handle, String((error as Error)?.message ?? 'Expected input schema must be valid JSON.'));
		}
	}

	$: if (selectedNode?.id && (isTransform || isLlm)) {
		const nodeId = selectedNode.id;
		const edges = $graphStore?.edges ?? [];
		const nodeBindings = $graphStore?.nodeBindings ?? {};
		const incoming = edges
			.filter((e) => e.target === nodeId)
			.map((e) => {
				const sourceBinding = nodeBindings[e.source];
				const artifactId = artifactIdFromBinding(sourceBinding);
				return `${String(e.id ?? '')}:${e.source}:${String(e.targetHandle ?? 'in')}:${artifactId}`;
			})
			.sort();
		const signature = `${String($graphStore?.graphId ?? '')}|${nodeId}|${incoming.join('|')}`;
		if (signature !== lastInputSignature) {
			lastInputSignature = signature;
			void refreshInputSchemas();
		}
	} else {
		lastInputSignature = '';
		inputSchemas = [];
		inputPreviewRows = [];
		inputPreviewColumns = [];
	}

	async function refreshInputSchemas(): Promise<void> {
		const nodeId = selectedNode?.id;
		if (!nodeId) {
			inputSchemas = [];
			inputPreviewRows = [];
			inputPreviewColumns = [];
			return;
		}
		const reqId = ++inputSchemaReqSeq;
		try {
			const edges = $graphStore?.edges ?? [];
			const nodeBindings = $graphStore?.nodeBindings ?? {};
			const nodesById = new Map(($graphStore?.nodes ?? []).map((n) => [n.id, n]));
			const graphId = String($graphStore?.graphId ?? '').trim();
			if (!graphId) {
				inputSchemas = [];
				inputPreviewRows = [];
				inputPreviewColumns = [];
				return;
			}
			const incoming = edges
				.filter((e) => e.target === nodeId)
				.map((e) => {
					const sourceBinding = nodeBindings[e.source];
					const artifactId = artifactIdFromBinding(sourceBinding);
					const sourceNode = nodesById.get(e.source) as Record<string, any> | undefined;
					return {
						sourceNodeId: e.source,
						inputHandle: String(e.targetHandle ?? 'in'),
						label: `${String(sourceNode?.data?.label ?? e.source)}.${String(e.targetHandle ?? 'in')}`,
						artifactId,
						sourceNode
					};
				});
			if (incoming.length === 0) {
				inputSchemas = [];
				inputPreviewRows = [];
				inputPreviewColumns = [];
				return;
			}
			const responses = await Promise.all(
				incoming.map(async (entry) => {
					const context = {
						sourceNodeId: entry.sourceNodeId,
						inputHandle: entry.inputHandle
					};
					if (entry.artifactId.length > 0) {
						try {
							const res = await fetch(getArtifactMetaUrl(entry.artifactId, graphId));
							if (!res.ok) throw new Error(`Failed to load schema for ${entry.artifactId}: ${res.status}`);
							const meta = await res.json();
							return parseInputSchemaView(
								entry.artifactId,
								entry.label,
								(meta?.schema ?? meta?.payloadSchema) as Record<string, unknown> | undefined,
								context
							);
						} catch {
							// Fallback to authoring-time inferred/observed schema below.
						}
					}
					const fallbackPayload = typedSchemaPayloadFromNode(entry.sourceNode);
					if (!fallbackPayload) return null;
					return parseInputSchemaView(
						entry.artifactId || `schema:${entry.sourceNodeId}`,
						entry.label,
						fallbackPayload,
						context
					);
				})
			);
			if (reqId !== inputSchemaReqSeq) return;
			inputSchemas = responses.filter(Boolean) as InputSchemaView[];
			const previewSource = incoming.find((entry) => entry.artifactId.length > 0);
			if (previewSource?.artifactId) {
				try {
					const previewRes = await fetch(getArtifactPreviewUrl(previewSource.artifactId, graphId, 0, 5));
					if (previewRes.ok) {
						const preview = await previewRes.json();
						const rows = Array.isArray(preview?.rows) ? (preview.rows as Array<Record<string, unknown>>) : [];
						const columns = Array.isArray(preview?.columns)
							? preview.columns
									.map((col: Record<string, unknown>) => String(col?.name ?? '').trim())
									.filter((col: string) => col.length > 0)
							: [];
						if (reqId !== inputSchemaReqSeq) return;
						inputPreviewRows = rows.slice(0, 5);
						inputPreviewColumns = columns;
					} else {
						inputPreviewRows = [];
						inputPreviewColumns = [];
					}
				} catch {
					inputPreviewRows = [];
					inputPreviewColumns = [];
				}
			} else {
				inputPreviewRows = [];
				inputPreviewColumns = [];
			}
		} catch {
			if (reqId !== inputSchemaReqSeq) return;
			inputSchemas = [];
			inputPreviewRows = [];
			inputPreviewColumns = [];
		}
	}

	function onDraft(
		patch: Record<string, any>,
		opts?: { intent?: 'user_edit' | 'system_canonicalize'; notice?: string | null }
	) {
		graphStore.patchInspectorDraft(patch, opts);
	}

	function onCommit(patch: Record<string, any>) {
		graphStore.commitInspectorImmediate(patch);
	}

	function toJoinPatch(patch: Record<string, any>): Record<string, any> {
		const next = patch && typeof patch === 'object' ? patch : {};
		if ('join' in next || 'op' in next) {
			return { op: 'join', ...next };
		}
		return { op: 'join', join: next };
	}

	function onJoinDraft(patch: Record<string, any>) {
		onDraft(toJoinPatch(patch));
	}

	function onJoinCommit(patch: Record<string, any>) {
		onCommit(toJoinPatch(patch));
	}

	function applyTransformAutoFix(patch: Record<string, unknown>): void {
		if (!isTransform) return;
		graphStore.commitInspectorImmediate(patch as Record<string, any>);
	}

	function sourceGuidedControlsForKind(kind: string): Array<{ id: string; label: string; description: string }> {
		const k = String(kind ?? 'file').trim().toLowerCase();
		if (k === 'database' || k === 'warehouse') {
			return [
				{ id: 'conn', label: 'Connection', description: 'Set connection_ref or connection_string first.' },
				{ id: 'query', label: 'Query/Table', description: 'Provide query (or table_name for database).' },
				{ id: 'output', label: 'Output mode', description: 'Default table output for downstream transforms.' },
				{ id: 'limits', label: 'Limit/Incremental', description: 'Add limit and incremental cursor for safe runs.' }
			];
		}
		if (k === 'api') {
			return [
				{ id: 'url', label: 'Endpoint', description: 'Set method + URL first.' },
				{ id: 'auth', label: 'Auth', description: 'Provide auth token ref only if auth is enabled.' },
				{ id: 'retry', label: 'Retry/Rate', description: 'Set retry and rate limits for resilient ingestion.' },
				{ id: 'output', label: 'Output mode', description: 'Use json/table mode to match downstream schema.' }
			];
		}
		if (k === 'object_store') {
			return [
				{ id: 'provider', label: 'Provider', description: 'Choose s3/azure_blob/gcs.' },
				{ id: 'path', label: 'Bucket + key', description: 'Set object path before run.' },
				{ id: 'format', label: 'File format', description: 'Declare file format for deterministic parsing.' },
				{ id: 'output', label: 'Output mode', description: 'Use table/text/json/binary to fit downstream node.' }
			];
		}
		return [
			{ id: 'file', label: 'File selection', description: 'Pick snapshot/file first.' },
			{ id: 'format', label: 'Format', description: 'Use detected file format or override explicitly.' },
			{ id: 'output', label: 'Output mode', description: 'Set output mode to match downstream contract.' },
			{ id: 'cache', label: 'Cache', description: 'Keep cache on for repeatable local workflows.' }
		];
	}

	$: sourceGuidedControls = isSource ? sourceGuidedControlsForKind(sourceKind) : [];

	function computeSourceAutoFixes(
		kind: string,
		error: Record<string, any> | null,
		currentParams: Record<string, unknown>
	): Array<{ id: string; label: string; patch: Record<string, unknown> }> {
		if (!error) return [];
		const fixes: Array<{ id: string; label: string; patch: Record<string, unknown> }> = [];
		const code = String(error.errorCode ?? '').trim().toUpperCase();
		const expectedInputType = String((error as any)?.expected?.inputType ?? '').trim().toLowerCase();
		if (code === 'CONTRACT_EDGE_PAYLOAD_TYPE_MISMATCH' && expectedInputType) {
			fixes.push({
				id: 'set_output_mode',
				label: `Set output mode to ${expectedInputType}`,
				patch: {
					output: {
						...((currentParams?.output as Record<string, unknown> | undefined) ?? {}),
						mode: expectedInputType
					}
				}
			});
		}
		if (code === 'MISSING_SECRET') {
			if (String(kind).toLowerCase() === 'api') {
				fixes.push({
					id: 'disable_auth',
					label: 'Disable auth for now',
					patch: { auth_type: 'none', auth_token_ref: undefined }
				});
			}
			if (String(kind).toLowerCase() === 'database' || String(kind).toLowerCase() === 'warehouse') {
				fixes.push({
					id: 'clear_conn_ref',
					label: 'Clear invalid connection_ref',
					patch: { connection_ref: undefined }
				});
			}
		}
		if (code === 'INVALID_IDENTIFIER') {
			fixes.push({
				id: 'clear_table_name',
				label: 'Clear unsafe table_name',
				patch: { table_name: undefined }
			});
		}
		return fixes;
	}

	$: sourceAutoFixes = isSource
		? computeSourceAutoFixes(
				sourceKind,
				nodeError as Record<string, any> | null,
				((params ?? {}) as Record<string, unknown>)
			)
		: [];

	function applySourceAutoFix(patch: Record<string, unknown>): void {
		if (!isSource) return;
		graphStore.commitInspectorImmediate(patch as Record<string, any>);
	}

	function switchTransformOp(nextOp: TransformKind): void {
		if (!selectedNode?.id) return;
		graphStore.setTransformKind(selectedNode.id, nextOp);
	}

	function setModelProvider(nextKind: LlmKind): void {
		if (!selectedNode?.id) return;
		graphStore.setLlmKind(selectedNode.id, nextKind);
	}

	function setModelKind(nextKind: ModelKind): void {
		if (!selectedNode?.id) return;
		const allowedTasks = taskKindsForModelKind(nextKind);
		const nextTask = allowedTasks.includes(taskKind) ? taskKind : allowedTasks[0];
		graphStore.updateNodeConfig(selectedNode.id, {
			modelKind: nextKind,
			taskKind: nextTask
		} as Record<string, any>);
	}

	function setTaskKind(nextTask: ModelTaskKind): void {
		if (!selectedNode?.id) return;
		graphStore.updateNodeConfig(selectedNode.id, { taskKind: nextTask } as Record<string, any>);
	}

	function setOutputModeQuick(mode: 'text' | 'json' | 'embeddings'): void {
		const nextOutput: Record<string, unknown> = {
			mode,
			strict: true
		};
		if (mode === 'json') {
			nextOutput.jsonSchema = (params as any)?.output?.jsonSchema ?? { type: 'object', properties: {} };
		}
		if (mode === 'embeddings') {
			nextOutput.embedding = (params as any)?.output?.embedding ?? { dims: 1536, dtype: 'float32', layout: '1d' };
		}
		onDraft({ output: nextOutput });
		onCommit({ output: nextOutput });
	}

	function applyModelAutoFix(fix: { patch?: Record<string, unknown>; edgeId?: string }): void {
		if (!isLlm) return;
		if (fix.patch) {
			graphStore.commitInspectorImmediate(fix.patch as Record<string, any>);
			return;
		}
		if (fix.edgeId) {
			const edge = (schemaContract.edges ?? []).find((entry) => entry.edgeId === fix.edgeId);
			if (edge) applySchemaSuggestion(edge);
		}
	}

	function schemaTypeLabel(schema: Record<string, any> | undefined): string {
		return String(schema?.type ?? 'unknown');
	}

	function schemaFieldSummary(schema: Record<string, any> | undefined, key: 'fields' | 'required_fields'): string {
		const fields = Array.isArray(schema?.[key]) ? (schema?.[key] as Array<Record<string, unknown>>) : [];
		if (fields.length === 0) return '-';
		return fields
			.map((field) => {
				const name = String(field?.name ?? '').trim();
				const type = String(field?.type ?? 'unknown').trim();
				return name.length > 0 ? `${name}:${type}` : '';
			})
			.filter((value) => value.length > 0)
			.join(', ');
	}

	function applySchemaSuggestion(edge: NodeSchemaContractEdge): void {
		if (!edge?.adapterKind) return;
		graphStore.deleteEdge(edge.edgeId);
		graphStore.insertSchemaAdapterForEdgeConnection({
			source: edge.sourceNodeId,
			target: edge.targetNodeId,
			sourceHandle: edge.sourceHandle,
			targetHandle: edge.targetHandle,
			adapterKind: edge.adapterKind
		});
	}

	function updateNodeProcessingPolicy(
		patch: {
			consume_mode?: 'once' | 'single_item' | 'batch';
			batch_size?: number;
			max_inflight?: number;
			read_once?: boolean;
		}
	): void {
		if (!selectedNode?.id) return;
		graphStore.updateNodeProcessingPolicy(selectedNode.id, patch);
	}

	function updateNodeProcessingPolicyForHandle(
		inputHandle: string,
		patch: {
			consume_mode?: 'once' | 'single_item' | 'batch';
			batch_size?: number;
			max_inflight?: number;
			read_once?: boolean;
		}
	): void {
		if (!selectedNode?.id) return;
		graphStore.updateNodeInputHandleProcessingPolicy(selectedNode.id, inputHandle, patch);
	}

	function updatePortDeclaration(
		direction: 'in' | 'out',
		handle: string,
		patch: {
			plane?: 'work' | 'param' | 'control';
			required?: boolean;
			cardinality?: 'one' | 'many';
			behavior?: 'once' | 'single_item' | 'batch';
		}
	): void {
		if (!selectedNode?.id) return;
		graphStore.updateNodePortDeclaration(selectedNode.id, direction, handle, patch);
	}

	function removePortDeclaration(direction: 'in' | 'out', handle: string): void {
		if (!selectedNode?.id) return;
		graphStore.removeNodePortDeclaration(selectedNode.id, direction, handle);
	}

	function addPortDeclaration(): void {
		const handle = String(newPortHandle ?? '').trim();
		if (!selectedNode?.id || !handle) return;
		updatePortDeclaration(newPortDirection, handle, {
			plane: newPortPlane,
			required: false,
			cardinality: 'many',
			behavior: newPortDirection === 'in' ? 'single_item' : undefined
		});
		newPortHandle = '';
	}

	function openSourceFullEditor(): void {
		sourceGuidedMode = false;
	}
</script>

{#if selectedNode}
	<div class="nodeInspectorTheme">
	{#if sourceObservability}
		<div class="guidedAssistCard">
			<div class="guidedAssistHead">Source Observability</div>
			<div class="schemaSuggestions">
				kind: {String(sourceObservability.source_kind ?? '-')} | mode: {String(sourceObservability.output_mode ?? '-')}
			</div>
			<div class="schemaSuggestions">
				input bytes: {String(sourceObservability.input_bytes ?? '-')} | output rows: {String(sourceObservability.output_rows ?? '-')}
			</div>
			<div class="schemaSuggestions">
				null ratio: {String(sourceObservability.null_ratio ?? '-')} | retries: {String(sourceObservability.retry_count ?? 0)} | partitions: {String(sourceObservability.partition_count ?? 1)}
			</div>
			{#if sourceObsWarnings.length > 0}
				<div class="guidedAssistDesc">{sourceObsWarnings.join(' | ')}</div>
			{/if}
			{#if sourceObsDetailsText}
				<pre class="jsonBox compact">{sourceObsDetailsText}</pre>
			{/if}
		</div>
	{/if}
	{#if sourcePrimingDrift && Boolean(sourcePrimingDrift.has_drift)}
		<div class="guidedAssistCard guidedAssistError">
			<div class="guidedAssistHead">Priming Drift</div>
			<div class="guidedAssistDesc">
				type mismatch: {String(sourcePrimingDrift.type_mismatch ?? false)} | missing: {String(sourcePrimingDrift.missing_columns ?? [])}
				| new: {String(sourcePrimingDrift.new_columns ?? [])} | mime mismatch: {String(sourcePrimingDrift.mime_mismatch ?? false)}
			</div>
		</div>
	{/if}
	{#if isSource}
		<div class="guidedAssistCard">
			<div class="guidedAssistHead">
				Source Capability: {String(sourceCapability?.supportLevel ?? 'production')}
			</div>
			{#if sourceCapabilityNotices.length > 0}
				<div class="guidedAssistList">
					{#each sourceCapabilityNotices as notice (notice)}
						<div class="guidedAssistItem">
							<div class="guidedAssistDesc">{notice}</div>
						</div>
					{/each}
				</div>
			{/if}
		</div>
		<div class="guidedModeRow">
			<label class="guidedToggle">
				<input type="checkbox" bind:checked={sourceGuidedMode} />
				<span>Guided source mode</span>
			</label>
			<span class="guidedHint">Start with high-value setup controls, then tune advanced fields.</span>
		</div>
		{#if sourceGuidedMode}
			<div class="guidedAssistCard">
				<div class="guidedAssistHead">Source Setup Checklist</div>
				<div class="guidedAssistList">
					{#each sourceGuidedControls as control (control.id)}
						<div class="guidedAssistItem">
							<div class="guidedAssistLabel">{control.label}</div>
							<div class="guidedAssistDesc">{control.description}</div>
						</div>
					{/each}
				</div>
				<div class="assistActionRow">
					<button type="button" class="small" on:click={openSourceFullEditor}>
						Open full editor
					</button>
				</div>
			</div>
		{/if}
		{#if nodeError}
			<div class="guidedAssistCard guidedAssistError">
				<div class="guidedAssistHead">Why This Failed</div>
				<div class="guidedAssistDesc">{nodeError.message || 'Source failed. Review diagnostics.'}</div>
				{#if sourceAutoFixes.length > 0}
					<div class="assistActionRow">
						{#each sourceAutoFixes as fix (fix.id)}
							<button type="button" class="small" on:click={() => applySourceAutoFix(fix.patch)}>
								{fix.label}
							</button>
						{/each}
					</div>
				{/if}
			</div>
		{/if}
		{#if sourceGuidedMode}
			<SourceGuidedQuickEditor
				{selectedNode}
				sourceKind={sourceKind}
				params={params as Record<string, unknown>}
				{nodeError}
				{onDraft}
				{onCommit}
			/>
		{:else}
			<svelte:component
				this={SourceEditorByKind[sourceKind] ?? SourceEditorByKind.file}
				{selectedNode}
				{params}
				{nodeError}
				{onDraft}
				{onCommit}
			/>
		{/if}
	{:else if isLlm}
		<div class="guidedModeRow">
			<label class="guidedToggle">
				<input type="checkbox" bind:checked={modelGuidedMode} />
				<span>Guided model mode</span>
			</label>
			<span class="guidedHint">Use only high-value controls first, then open advanced settings.</span>
		</div>
		{#if modelGuidedMode}
			<div class="guidedAssistCard">
				<div class="guidedAssistHead">Model Setup Checklist</div>
				<div class="guidedAssistList">
					{#each modelGuidedControls as control (control.id)}
						<div class="guidedAssistItem">
							<div class="guidedAssistLabel">{control.label}</div>
							<div class="guidedAssistDesc">{control.description}</div>
						</div>
					{/each}
				</div>
			</div>
			<div class="guidedAssistCard">
				<div class="guidedAssistHead">Quick Controls</div>
				<div class="assistActionRow">
					<label class="guidedToggle">
						<span>Provider</span>
						<select value={llmKind} on:change={(e) => setModelProvider((e.currentTarget as HTMLSelectElement).value as LlmKind)}>
							<option value="ollama">ollama</option>
							<option value="openai_compat">openai_compat</option>
						</select>
					</label>
					<label class="guidedToggle">
						<span>Model kind</span>
						<select value={modelKind} on:change={(e) => setModelKind((e.currentTarget as HTMLSelectElement).value as ModelKind)}>
							<option value="llm">llm</option>
							<option value="vision">vision</option>
							<option value="audio">audio</option>
							<option value="embedding">embedding</option>
							<option value="reranker">reranker</option>
							<option value="multimodal">multimodal</option>
						</select>
					</label>
					<label class="guidedToggle">
						<span>Task</span>
						<select value={taskKind} on:change={(e) => setTaskKind((e.currentTarget as HTMLSelectElement).value as ModelTaskKind)}>
							{#each modelTaskOptions as option}
								<option value={option}>{option}</option>
							{/each}
						</select>
					</label>
				</div>
				<div class="assistActionRow">
					<label class="guidedToggle">
						<span>Model</span>
						<input
							type="text"
							value={String((params as any)?.model ?? '')}
							on:input={(e) => onDraft({ model: (e.currentTarget as HTMLInputElement).value })}
						/>
					</label>
					<label class="guidedToggle">
						<span>Output</span>
						<select value={String((params as any)?.output?.mode ?? 'text')} on:change={(e) => setOutputModeQuick((e.currentTarget as HTMLSelectElement).value as 'text' | 'json' | 'embeddings')}>
							<option value="text">text</option>
							<option value="json">json</option>
							<option value="embeddings">embeddings</option>
						</select>
					</label>
				</div>
				<button type="button" class="small" on:click={() => (modelAdvancedOpen = !modelAdvancedOpen)}>
					{modelAdvancedOpen ? 'Hide Advanced Editor' : 'Show Advanced Editor'}
				</button>
			</div>
			{#if nodeError}
				<div class="guidedAssistCard guidedAssistError">
					<div class="guidedAssistHead">Why This Failed</div>
					<div class="guidedAssistDesc">{nodeError.message || 'Model execution failed. Review diagnostics.'}</div>
					{#if modelAutoFixes.length > 0}
						<div class="assistActionRow">
							{#each modelAutoFixes as fix (fix.id)}
								<button type="button" class="small" on:click={() => applyModelAutoFix(fix)}>
									{fix.label}
								</button>
							{/each}
						</div>
					{/if}
				</div>
			{/if}
			<div class="guidedAssistCard">
				<div class="guidedAssistHead">Live Preview Diff</div>
				<div class="schemaSuggestions">
					Input: {modelPreviewDiff.inputType} | Output: {modelPreviewDiff.outputType}
				</div>
				<div class="previewDiffCols">
					<div>
						<div class="guidedAssistLabel">Input columns ({modelPreviewDiff.inputColumns.length})</div>
						<div class="schemaSuggestions">{modelPreviewDiff.inputColumns.join(', ') || '-'}</div>
					</div>
					<div>
						<div class="guidedAssistLabel">Output columns ({modelPreviewDiff.outputColumns.length})</div>
						<div class="schemaSuggestions">{modelPreviewDiff.outputColumns.join(', ') || '-'}</div>
					</div>
				</div>
				<div class="previewDiffRows">
					<div>
						<div class="guidedAssistLabel">Sample input</div>
						<pre>{JSON.stringify(modelPreviewDiff.sampleInput, null, 2)}</pre>
					</div>
					<div>
						<div class="guidedAssistLabel">Sample output</div>
						<pre>{JSON.stringify(modelPreviewDiff.sampleOutput, null, 2)}</pre>
					</div>
				</div>
				{#if modelPreviewDiff.notes.length > 0}
					<div class="schemaSuggestions">{modelPreviewDiff.notes.join(' ')}</div>
				{/if}
			</div>
		{/if}
		{#if !modelGuidedMode || modelAdvancedOpen}
			<div class="advancedEditor">
				<div class="advancedEditorTitle">Model Editor</div>
				<svelte:component
					this={LlmEditorByKind[llmKind] ?? LlmEditorByKind.ollama}
					{selectedNode}
					{params}
					{onDraft}
					{onCommit}
				/>
			</div>
		{/if}
	{:else if isTool}
		<ToolEditor {selectedNode} {params} {onDraft} {onCommit} />
		<svelte:component
			this={ToolEditorByProvider[toolProvider] ?? ToolEditorByProvider.mcp}
			{params}
			{onDraft}
			{onCommit}
		/>
		{:else if isComponent}
		<ComponentEditor {selectedNode} {params} {onDraft} />
		{:else if isTransform}
			<div class={`schemaAssist schemaAssist-${schemaAssist.state}`}>
				<div class="schemaAssistHead">
					<span>Schema Assist</span>
					<span class="schemaAssistBadge">{schemaAssist.source}/{schemaAssist.state}</span>
				</div>
				{#if !schemaAssist.hasSchema}
					<div class="schemaAssistHint">
						No inferred input schema yet. Run upstream once or declare expected schema to unlock field-aware controls.
					</div>
				{/if}
			</div>
			<div class="guidedModeRow">
				<label class="guidedToggle">
					<input type="checkbox" bind:checked={transformGuidedMode} />
					<span>Guided mode</span>
				</label>
				<span class="guidedHint">Start with 3-5 core controls, then expand advanced editor.</span>
			</div>
			{#if transformGuidedMode}
				<div class="guidedAssistCard">
					<div class="guidedAssistHead">High-Value Controls</div>
					<div class="guidedAssistList">
						{#each guidedControls as control (control.id)}
							<div class="guidedAssistItem">
								<div class="guidedAssistLabel">{control.label}</div>
								<div class="guidedAssistDesc">{control.description}</div>
							</div>
						{/each}
					</div>
				</div>
			{/if}
			{#if nodeError}
				<div class="guidedAssistCard guidedAssistError">
					<div class="guidedAssistHead">Why This Failed</div>
					<div class="guidedAssistDesc">
						{nodeError.message || 'Transform failed. Review diagnostics and apply a suggested fix.'}
					</div>
					{#if transformAutoFixes.length > 0}
						<div class="assistActionRow">
							{#each transformAutoFixes as fix (fix.id)}
								<button type="button" class="small" on:click={() => applyTransformAutoFix(fix.patch)}>
									{fix.label}
								</button>
							{/each}
						</div>
					{/if}
				</div>
			{/if}
			{#if nextTransformOps.length > 0}
				<div class="guidedAssistCard">
					<div class="guidedAssistHead">Suggested Next Transform</div>
					<div class="guidedAssistList">
						{#each nextTransformOps as suggestion, index (`${suggestion.op}-${index}`)}
							<div class="guidedAssistItem">
								<div class="guidedAssistLabel">{suggestion.op}</div>
								<div class="guidedAssistDesc">{suggestion.reason}</div>
								<button type="button" class="small" on:click={() => switchTransformOp(suggestion.op)}>
									Switch to {suggestion.op}
								</button>
							</div>
						{/each}
					</div>
				</div>
			{/if}
			<div class="guidedAssistCard">
				<div class="guidedAssistHead">Live Preview Diff</div>
				<div class="previewDiffCols">
					<div>
						<div class="guidedAssistLabel">Before columns ({transformPreviewDiff.beforeColumns.length})</div>
						<div class="schemaSuggestions">{transformPreviewDiff.beforeColumns.join(', ') || '-'}</div>
					</div>
					<div>
						<div class="guidedAssistLabel">After columns ({transformPreviewDiff.afterColumns.length})</div>
						<div class="schemaSuggestions">{transformPreviewDiff.afterColumns.join(', ') || '-'}</div>
					</div>
				</div>
				<div class="previewDiffRows">
					<div>
						<div class="guidedAssistLabel">Sample input rows</div>
						<pre>{JSON.stringify(transformPreviewDiff.beforeRows, null, 2)}</pre>
					</div>
					<div>
						<div class="guidedAssistLabel">Sample output rows</div>
						<pre>{JSON.stringify(transformPreviewDiff.afterRows, null, 2)}</pre>
					</div>
				</div>
				{#if transformPreviewDiff.notes.length > 0}
					<div class="schemaSuggestions">{transformPreviewDiff.notes.join(' ')}</div>
				{/if}
			</div>
			{#if transformGuidedMode}
				<div class="advancedEditor">
					<div class="advancedEditorTitle">Editor</div>
					{#if transformKind === 'join'}
						<svelte:component
							this={TransformEditorByKind[transformKind] ?? TransformEditorByKind.filter}
							{selectedNode}
							{params}
							{nodeError}
							{inputSchemas}
							onDraft={onJoinDraft}
							onCommit={onJoinCommit}
						/>
					{:else}
						<svelte:component
							this={TransformEditorByKind[transformKind] ?? TransformEditorByKind.filter}
							{selectedNode}
							{params}
							{nodeError}
							inputColumns={Array.from(new Set([...(schemaProps.inputColumns ?? []), ...inputPreviewColumns]))}
							inputSchemaColumns={schemaProps.inputSchemaColumns}
							inputSchemas={schemaProps.inputSchemas}
							{onDraft}
							{onCommit}
						/>
					{/if}
				</div>
			{:else if transformKind === 'join'}
				<svelte:component
					this={TransformEditorByKind[transformKind] ?? TransformEditorByKind.filter}
					{selectedNode}
					{params}
					{nodeError}
					{inputSchemas}
					onDraft={onJoinDraft}
					onCommit={onJoinCommit}
				/>
			{:else}
				<svelte:component
					this={TransformEditorByKind[transformKind] ?? TransformEditorByKind.filter}
					{selectedNode}
					{params}
					{nodeError}
					inputColumns={Array.from(new Set([...(schemaProps.inputColumns ?? []), ...inputPreviewColumns]))}
					inputSchemaColumns={schemaProps.inputSchemaColumns}
					inputSchemas={schemaProps.inputSchemas}
					{onDraft}
					{onCommit}
				/>
			{/if}
		{/if}
		{#if !isLlm}
			<div class="guidedAssistCard">
				<div class="guidedAssistHead">Debug</div>
				<div class="assistActionRow">
					<label class="guidedToggle">
						<input
							type="checkbox"
							checked={nodeDebugEnabled}
							on:change={(event) => {
								const checked = (event.currentTarget as HTMLInputElement).checked;
								const patch = {
									debug: {
										enabled: checked,
										log_input_preview: checked ? nodeDebugLogInputPreview : false,
										log_raw_output: checked ? nodeDebugLogRawOutput : false
									}
								};
								onDraft(patch);
								onCommit(patch);
							}}
						/>
						<span>debug.enabled</span>
					</label>
					<label class="guidedToggle">
						<input
							type="checkbox"
							disabled={!nodeDebugEnabled}
							checked={nodeDebugLogInputPreview}
							on:change={(event) => {
								const checked = (event.currentTarget as HTMLInputElement).checked;
								const patch = {
									debug: {
										enabled: nodeDebugEnabled,
										log_input_preview: checked,
										log_raw_output: nodeDebugLogRawOutput
									}
								};
								onDraft(patch);
								onCommit(patch);
							}}
						/>
						<span>debug.log_input_preview</span>
					</label>
					<label class="guidedToggle">
						<input
							type="checkbox"
							disabled={!nodeDebugEnabled}
							checked={nodeDebugLogRawOutput}
							on:change={(event) => {
								const checked = (event.currentTarget as HTMLInputElement).checked;
								const patch = {
									debug: {
										enabled: nodeDebugEnabled,
										log_input_preview: nodeDebugLogInputPreview,
										log_raw_output: checked
									}
								};
								onDraft(patch);
								onCommit(patch);
							}}
						/>
						<span>debug.log_raw_output</span>
					</label>
				</div>
			</div>
		{/if}
		<div class="guidedAssistCard">
			<div class="guidedAssistHead">Processing Policy</div>
			<div class="assistActionRow">
				<label class="guidedToggle">
					<span>consume</span>
					<ThemedSelect
						value={nodeProcessingPolicy.consume_mode}
						options={consumeModeOptions}
						ariaLabel="Node consume mode"
						onValueChange={(next) =>
							updateNodeProcessingPolicy({
								consume_mode: next as 'once' | 'single_item' | 'batch'
							})}
					/>
				</label>
				{#if nodeProcessingPolicy.consume_mode === 'batch'}
					<label class="guidedToggle">
						<span>batch size</span>
						<input
							type="number"
							min="1"
							step="1"
							value={String(nodeProcessingPolicy.batch_size)}
							on:change={(event) =>
								updateNodeProcessingPolicy({
									batch_size: Math.max(1, Number((event.currentTarget as HTMLInputElement).value || '1'))
								})}
						/>
					</label>
				{/if}
				<label class="guidedToggle">
					<span>max inflight</span>
					<input
						type="number"
						min="1"
						step="1"
						value={String(nodeProcessingPolicy.max_inflight)}
						on:change={(event) =>
							updateNodeProcessingPolicy({
								max_inflight: Math.max(1, Number((event.currentTarget as HTMLInputElement).value || '1'))
							})}
					/>
				</label>
			</div>
			{#if expectedInputHandles.length > 0}
				<div class="guidedAssistList">
					{#each expectedInputHandles as handleSummary (handleSummary.handle)}
						<div class="guidedAssistItem">
							<div class="guidedAssistLabel">input {handleSummary.handle}</div>
							<div class="assistActionRow">
								<label class="guidedToggle">
									<span>consume</span>
									<ThemedSelect
										value={nodeProcessingPolicyByHandle[handleSummary.handle]?.consume_mode ?? nodeProcessingPolicy.consume_mode}
										options={consumeModeOptions}
										ariaLabel={`Consume mode for handle ${handleSummary.handle}`}
										onValueChange={(next) =>
											updateNodeProcessingPolicyForHandle(handleSummary.handle, {
												consume_mode: next as 'once' | 'single_item' | 'batch'
											})}
									/>
								</label>
								{#if (nodeProcessingPolicyByHandle[handleSummary.handle]?.consume_mode ?? nodeProcessingPolicy.consume_mode) === 'batch'}
									<label class="guidedToggle">
										<span>batch</span>
										<input
											type="number"
											min="1"
											step="1"
											value={String(nodeProcessingPolicyByHandle[handleSummary.handle]?.batch_size ?? nodeProcessingPolicy.batch_size)}
											on:change={(event) =>
												updateNodeProcessingPolicyForHandle(handleSummary.handle, {
													batch_size: Math.max(
														1,
														Number((event.currentTarget as HTMLInputElement).value || '1')
													)
												})}
										/>
									</label>
								{/if}
								<label class="guidedToggle">
									<span>inflight</span>
									<input
										type="number"
										min="1"
										step="1"
										value={String(nodeProcessingPolicyByHandle[handleSummary.handle]?.max_inflight ?? nodeProcessingPolicy.max_inflight)}
										on:change={(event) =>
											updateNodeProcessingPolicyForHandle(handleSummary.handle, {
												max_inflight: Math.max(
													1,
													Number((event.currentTarget as HTMLInputElement).value || '1')
												)
											})}
									/>
								</label>
							</div>
						</div>
					{/each}
				</div>
			{/if}
		</div>
		<div class="guidedAssistCard">
			<div class="guidedAssistHead">Port Declarations</div>
			<div class="assistActionRow">
				<ThemedSelect
					value={newPortDirection}
					options={directionOptions}
					ariaLabel="New port direction"
					onValueChange={(next) => (newPortDirection = next as 'in' | 'out')}
				/>
				<input type="text" placeholder="handle (e.g. param_filters)" bind:value={newPortHandle} />
				<ThemedSelect
					value={newPortPlane}
					options={planeOptions}
					ariaLabel="New port plane"
					onValueChange={(next) => (newPortPlane = next as 'work' | 'param' | 'control')}
				/>
				<button type="button" class="small" on:click={addPortDeclaration}>Add</button>
			</div>
			<div class="guidedAssistList">
				{#each Object.entries(nodePortDeclarations.in) as [handle, decl] (`in-${handle}`)}
					<div class="guidedAssistItem">
						<div class="guidedAssistLabel">in.{handle}</div>
						<div class="assistActionRow">
							<ThemedSelect
								value={String((decl as any)?.plane ?? 'work')}
								options={planeOptions}
								ariaLabel={`Input port plane ${handle}`}
								onValueChange={(next) =>
									updatePortDeclaration('in', handle, {
										plane: next as 'work' | 'param' | 'control'
									})}
							/>
							<ThemedSelect
								value={String((decl as any)?.cardinality ?? 'many')}
								options={cardinalityOptions}
								ariaLabel={`Input port cardinality ${handle}`}
								onValueChange={(next) =>
									updatePortDeclaration('in', handle, {
										cardinality: next as 'one' | 'many'
									})}
							/>
							<label class="guidedToggle">
								<input
									type="checkbox"
									checked={Boolean((decl as any)?.required)}
									on:change={(event) =>
										updatePortDeclaration('in', handle, {
											required: (event.currentTarget as HTMLInputElement).checked
										})}
								/>
								<span>required</span>
							</label>
							<button type="button" class="small danger" on:click={() => removePortDeclaration('in', handle)}>
								Remove
							</button>
						</div>
					</div>
				{/each}
				{#each Object.entries(nodePortDeclarations.out) as [handle, decl] (`out-${handle}`)}
					<div class="guidedAssistItem">
						<div class="guidedAssistLabel">out.{handle}</div>
						<div class="assistActionRow">
							<ThemedSelect
								value={String((decl as any)?.plane ?? 'work')}
								options={planeOptions}
								ariaLabel={`Output port plane ${handle}`}
								onValueChange={(next) =>
									updatePortDeclaration('out', handle, {
										plane: next as 'work' | 'param' | 'control'
									})}
							/>
							<ThemedSelect
								value={String((decl as any)?.cardinality ?? 'many')}
								options={cardinalityOptions}
								ariaLabel={`Output port cardinality ${handle}`}
								onValueChange={(next) =>
									updatePortDeclaration('out', handle, {
										cardinality: next as 'one' | 'many'
									})}
							/>
							<button type="button" class="small danger" on:click={() => removePortDeclaration('out', handle)}>
								Remove
							</button>
						</div>
					</div>
				{/each}
			</div>
		</div>
		<div class="guidedAssistCard">
			<div class="guidedAssistHead">Why Not Running</div>
			<div class="guidedAssistList">
				{#if nodeBlockedStatus}
					<div class="guidedAssistItem">
						<div class="guidedAssistLabel">
							{String((nodeBlockedStatus as any)?.reasonCode ?? 'NO_READY_WORK')}
						</div>
						<div class="guidedAssistDesc">
							handle {String((nodeBlockedStatus as any)?.handle ?? '-')} | plane {String((nodeBlockedStatus as any)?.plane ?? '-')}
						</div>
						{#if Array.isArray((nodeBlockedStatus as any)?.missingEdgeIds) && ((nodeBlockedStatus as any)?.missingEdgeIds as any[]).length > 0}
							<div class="guidedAssistDesc">
								missing edges {(((nodeBlockedStatus as any)?.missingEdgeIds as any[]) ?? []).map((item) => String(item)).join(', ')}
							</div>
						{/if}
						{#if Array.isArray((nodeBlockedStatus as any)?.waitingOnNodeIds) && ((nodeBlockedStatus as any)?.waitingOnNodeIds as any[]).length > 0}
							<div class="guidedAssistDesc">
								waiting nodes {(((nodeBlockedStatus as any)?.waitingOnNodeIds as any[]) ?? []).map((item) => String(item)).join(', ')}
							</div>
						{/if}
					</div>
				{:else}
					<div class="guidedAssistItem">
						<div class="guidedAssistDesc">No blocked reason recorded for this node in the current run.</div>
					</div>
				{/if}
			</div>
		</div>
		<div class="guidedAssistCard">
			<div class="guidedAssistHead">Scheduler Snapshot</div>
			<div class="guidedAssistList">
				{#if schedulerSnapshot}
					<div class="guidedAssistItem">
						<div class="guidedAssistLabel">
							stalled {String(Boolean((schedulerSnapshot as any)?.stalled))}
						</div>
						<div class="guidedAssistDesc">
							ready {String((schedulerSnapshot as any)?.readyCount ?? 0)} | inflight {String((schedulerSnapshot as any)?.inflightCount ?? 0)} | pending {String((schedulerSnapshot as any)?.pendingQueueDepth ?? 0)} | runnable {String((schedulerSnapshot as any)?.runnableNodeCount ?? 0)}
						</div>
					</div>
				{:else}
					<div class="guidedAssistItem">
						<div class="guidedAssistDesc">No scheduler snapshot received yet.</div>
					</div>
				{/if}
			</div>
		</div>
		<div class="guidedAssistCard">
			<div class="guidedAssistHead">LLM Lease</div>
			<div class="guidedAssistList">
				{#if llmLeaseStatus}
					<div class="guidedAssistItem">
						<div class="guidedAssistLabel">state {String((llmLeaseStatus as any)?.state ?? 'released')}</div>
						<div class="guidedAssistDesc">
							holder {String((llmLeaseStatus as any)?.holderNodeId ?? '(none)')} | queue {String((llmLeaseStatus as any)?.waitQueueLength ?? 0)} | actor {String((llmLeaseStatus as any)?.nodeId ?? '-')}
						</div>
					</div>
				{:else}
					<div class="guidedAssistItem">
						<div class="guidedAssistDesc">No LLM lease telemetry received yet.</div>
					</div>
				{/if}
			</div>
		</div>
		{#if nodeQueuePortStats.length > 0}
			<div class="guidedAssistCard">
				<div class="guidedAssistHead">Queue Port Status</div>
				<div class="guidedAssistList">
					{#each nodeQueuePortStats as row, idx (`${row.direction}:${row.edgeId}:${row.handle}:${idx}`)}
						<div class="guidedAssistItem">
							<div class="guidedAssistLabel">
								{String(row.direction)} {String(row.edgeId)}:{String(row.handle)}
							</div>
							<div class="queueEdgeCounterparty">
								{queueEdgeCounterpartyName(
									String(row.edgeId),
									String(row.direction),
									String(selectedNode?.id ?? ''),
									graphNodesForInspector
								)}
							</div>
							<div class="guidedAssistDesc">
								depth {String((row.metric as any)?.depth ?? 0)} | blocked {String((row.metric as any)?.blocked ?? false)} | full {String((row.metric as any)?.full ?? false)} | age {String((row.metric as any)?.oldestAgeSec ?? '-')}
							</div>
						</div>
					{/each}
				</div>
			</div>
		{/if}
		{#if runScopedQueueSummary}
			<div class="guidedAssistCard">
				<div class="guidedAssistHead">Run-Scoped Queue Metrics</div>
				<div class="guidedAssistList">
					<div class="guidedAssistItem">
						<div class="guidedAssistLabel">run {runScopedQueueSummary.runId || '-'}</div>
						<div class="guidedAssistDesc">
							scope {runScopedQueueSummary.scope} | enq {runScopedQueueSummary.enq} | deq {runScopedQueueSummary.deq} | accepted {runScopedQueueSummary.accepted} | rejected {runScopedQueueSummary.rejected}
						</div>
					</div>
				</div>
			</div>
		{/if}
		{#if aggregateQueueSummary}
			<div class="guidedAssistCard">
				<div class="guidedAssistHead">Aggregate Queue Diagnostics</div>
				<div class="guidedAssistList">
					<div class="guidedAssistItem">
						<div class="guidedAssistLabel">events {aggregateQueueSummary.events}</div>
						<div class="guidedAssistDesc">
							enq {aggregateQueueSummary.enq} | deq {aggregateQueueSummary.deq} | accepted {aggregateQueueSummary.accepted} | rejected {aggregateQueueSummary.rejected}
						</div>
					</div>
				</div>
			</div>
		{/if}
		{#if nodeHandleStates.length > 0}
			<div class="guidedAssistCard">
				<div class="guidedAssistHead">Handle States</div>
				<div class="guidedAssistList">
					{#each nodeHandleStates as row (`${row.handle}:${row.state}:${row.updatedAt}`)}
						<div class="guidedAssistItem">
							<div class="guidedAssistLabel">{row.handle}</div>
							<div class="guidedAssistDesc">state {row.state} | at {formatUserLocalTime(row.updatedAt)}</div>
						</div>
					{/each}
				</div>
			</div>
		{/if}
		{#if nodeBranchCascade.length > 0}
			<div class="guidedAssistCard">
				<div class="guidedAssistHead">Branch Cascade</div>
				<div class="guidedAssistList">
					{#each nodeBranchCascade as row, idx (`${row.originNodeId}:${row.at}:${idx}`)}
						<div class="guidedAssistItem">
							<div class="guidedAssistLabel">origin {row.originNodeId || '-'}</div>
							<div class="guidedAssistDesc">
								blocked {row.blockedNodeIds.join(', ') || '-'} | reason {row.reasonCode || '-'} | at {formatUserLocalTime(row.at)}
							</div>
						</div>
					{/each}
				</div>
			</div>
		{/if}
		{#if nodeHandleSatisfaction.length > 0}
			<div class="guidedAssistCard">
				<div class="guidedAssistHead">Handle Satisfaction</div>
				<div class="guidedAssistList">
					{#each nodeHandleSatisfaction as row (`${row.handle}:${row.status}:${row.updatedAt}`)}
						<div class="guidedAssistItem">
							<div class="guidedAssistLabel">{row.handle}</div>
							<div class="guidedAssistDesc">
								status {row.status} | provided {row.providedEdges}/{row.connectedEdges} | at {formatUserLocalTime(row.updatedAt)}
							</div>
						</div>
					{/each}
				</div>
			</div>
		{/if}
		{#if nodeParamControlWarnings.length > 0}
			<div class="guidedAssistCard">
				<div class="guidedAssistHead">Param/Control Input Warnings</div>
				<div class="guidedAssistList">
					{#each nodeParamControlWarnings as row (`${row.handle}:${row.edgeId}:${row.code}:${row.updatedAt}`)}
						<div class="guidedAssistItem">
							<div class="guidedAssistLabel">{row.plane} {row.handle}</div>
							<div class="guidedAssistDesc">
								edge {row.edgeId} | code {row.code} | reason {row.reasonCode || '-'} | upstream {row.upstreamNodeId || '-'} | at {formatUserLocalTime(row.updatedAt)}
							</div>
						</div>
					{/each}
				</div>
			</div>
		{/if}
		{#if !isComponent}
			<div class="expectedSchemaEditor">
				<div class="expectedSchemaHead">Expected Input Schemas</div>
				{#each expectedInputHandles as handleSummary (handleSummary.handle)}
					<div class="expectedInputHandleEditor">
						<div class="expectedInputHandleHead">
							<span>{handleSummary.handle}</span>
							<span class="schemaEdgeCounterparty"
								>{handleSummary.affinity} default:{handleSummary.classDefaultType}</span
							>
						</div>
						<textarea
							class="expectedSchemaTextarea"
							rows="6"
							value={expectedInputSchemaDraftByHandle[handleSummary.handle] ?? ''}
							spellcheck="false"
							on:input={(event) =>
								setExpectedInputDraft(handleSummary.handle, (event.currentTarget as HTMLTextAreaElement).value)}
						></textarea>
						<div class="expectedSchemaActions">
							<button type="button" on:click={() => saveExpectedInputSchema(handleSummary.handle)}
								>Save {handleSummary.handle}</button
							>
							<button type="button" on:click={() => useSuggestedInputSchema(handleSummary.handle)}
								>Use contract</button
							>
							<button type="button" on:click={() => useInferredInputSchema(handleSummary.handle)}
								>Use inferred</button
							>
							<button type="button" on:click={() => clearExpectedInputSchema(handleSummary.handle)}
								>Clear</button
							>
						</div>
						{#if expectedInputSchemaErrorByHandle[handleSummary.handle]}
							<div class="expectedSchemaError">
								{expectedInputSchemaErrorByHandle[handleSummary.handle]}
							</div>
						{/if}
					</div>
				{/each}
			</div>
			<div class="expectedSchemaEditor">
				<div class="expectedSchemaHead">Expected Output Schema</div>
				<textarea
					class="expectedSchemaTextarea"
					rows="7"
					bind:value={expectedSchemaDraft}
					spellcheck="false"
				></textarea>
				<div class="expectedSchemaActions">
					<button type="button" on:click={saveExpectedSchema}>Save expected</button>
					<button type="button" on:click={useInferredExpectedSchema}>Use inferred</button>
					{#if isSource && sourceKind === 'database'}
						<button type="button" on:click={useHeaderForExpectedSchema} disabled={headerSchemaLoading}>
							{headerSchemaLoading ? 'Reading header...' : 'Use header'}
						</button>
					{/if}
					<button type="button" on:click={clearExpectedSchema}>Clear</button>
				</div>
				{#if schemaDriftSummary}
					<div class="expectedSchemaDrift">Drift: {schemaDriftSummary}</div>
				{/if}
				{#if expectedSchemaError}
					<div class="expectedSchemaError">{expectedSchemaError}</div>
				{/if}
			</div>
		{/if}
		<div class={`schemaContract schemaContract-${schemaContract.status}`}>
			<div class="schemaHead">Schema Contract</div>
			<div class="schemaStatus">Status: {schemaContract.status}</div>
			{#if schemaDeprecationNotices.length > 0}
				<div class="schemaDeprecationBox">
					<div class="schemaDeprecationHead">Deprecation Notices</div>
					{#each schemaDeprecationNotices as notice (`dep-${notice}`)}
						<div class="schemaDeprecationItem">{notice}</div>
					{/each}
				</div>
			{/if}
			{#if schemaContract.edges.length === 0}
				<div class="schemaEmpty">No connected edges.</div>
			{:else}
				{#each schemaContractGroups as group (group.mode)}
					<div class="schemaModeGroup">
						<div class="schemaModeHead">{group.label} edges ({group.edges.length})</div>
						{#each group.edges as edge (edge.edgeId)}
							{@const edgeRuntimeConfig = readEdgeRuntimeConfig(edge.edgeId)}
							{@const counterpartyName = schemaEdgeCounterpartyName(edge, selectedNode?.id ?? '', $graphStore.nodes ?? [])}
							<div class={`schemaEdge schemaEdge-${edge.severity}`}>
								<div class="schemaEdgeHead">
									<div class="schemaEdgeHeadMain">
										<span>{edge.direction === 'incoming' ? 'in' : 'out'}: {edge.edgeId}</span>
										<span class="schemaEdgeCounterparty">{counterpartyName}</span>
									</div>
									<span>{edge.severity}</span>
								</div>
								<div class="schemaEdgeConfig">
									<label>
										<span>mode</span>
										<ThemedSelect
											value={edgeRuntimeConfig.mode}
											options={planeOptions}
											ariaLabel={`Edge mode ${edge.edgeId}`}
											onValueChange={(next) =>
												patchEdgeRuntimeConfig(edge.edgeId, {
													mode: next as 'work' | 'param' | 'control'
												})}
										/>
									</label>
									<label>
										<span>queue.max</span>
										<input
											type="number"
											min="1"
											step="1"
											value={String(edgeRuntimeConfig.queue.max)}
											on:change={(event) =>
												patchEdgeRuntimeConfig(edge.edgeId, {
													queue: {
														max: Math.max(
															1,
															Math.trunc(Number((event.currentTarget as HTMLInputElement).value || '1'))
														)
													}
												})}
										/>
									</label>
									<label>
										<span>overflow</span>
										<ThemedSelect
											value={edgeRuntimeConfig.queue.overflow}
											options={overflowOptions}
											ariaLabel={`Edge overflow ${edge.edgeId}`}
											onValueChange={(next) =>
												patchEdgeRuntimeConfig(edge.edgeId, {
													queue: {
														overflow: next as 'block' | 'spill' | 'error'
													}
												})}
										/>
									</label>
									<label>
										<span>arbitration</span>
										<ThemedSelect
											value={edgeRuntimeConfig.queue.policy}
											options={arbitrationOptions}
											ariaLabel={`Edge arbitration ${edge.edgeId}`}
											onValueChange={(next) =>
												patchEdgeRuntimeConfig(edge.edgeId, {
													queue: {
														policy: next as 'fifo' | 'round_robin'
													}
												})}
										/>
									</label>
									<label>
										<span>item mode</span>
										<ThemedSelect
											value={edgeRuntimeConfig.work.item_mode}
											options={itemModeOptions}
											ariaLabel={`Edge item mode ${edge.edgeId}`}
											onValueChange={(next) =>
												patchEdgeRuntimeConfig(edge.edgeId, {
													work: {
														item_mode: next as 'artifact' | 'json_items' | 'table_rows'
													}
												})}
										/>
									</label>
									<label>
										<span>max items</span>
										<input
											type="number"
											min="1"
											step="1"
											value={String(edgeRuntimeConfig.work.max_items)}
											on:change={(event) =>
												patchEdgeRuntimeConfig(edge.edgeId, {
													work: {
														max_items: Math.max(
															1,
															Math.trunc(Number((event.currentTarget as HTMLInputElement).value || '1'))
														)
													}
												})}
										/>
									</label>
									<label class="schemaEdgeFatal">
										<input
											type="checkbox"
											checked={edgeRuntimeConfig.fatal}
											on:change={(event) =>
												patchEdgeRuntimeConfig(edge.edgeId, {
													fatal: (event.currentTarget as HTMLInputElement).checked
												})}
										/>
										<span>fatal</span>
									</label>
								</div>
								{#if edgeConfigErrors[edge.edgeId]}
									<div class="expectedSchemaError">{edgeConfigErrors[edge.edgeId]}</div>
								{/if}
								<div class="schemaRow">
									<span class="schemaLabel">provided</span>
									<span>{schemaTypeLabel(edge.providedSchema)} [{schemaFieldSummary(edge.providedSchema, 'fields')}]</span>
								</div>
								<div class="schemaRow">
									<span class="schemaLabel">required</span>
									<span>{schemaTypeLabel(edge.requiredSchema)} [{schemaFieldSummary(edge.requiredSchema, 'required_fields')}]</span>
								</div>
								<div class="schemaContractDiffPanel">
									<div class="schemaContractDiffHead">
										<span>contract diff</span>
										{#each schemaEdgeContractBadges(edge) as badge (`${edge.edgeId}-${badge}`)}
											<span class="schemaContractBadge">{badge}</span>
										{/each}
									</div>
									<div class="schemaRow">
										<span class="schemaLabel">source (current)</span>
										<span>{schemaTypeLabel(edge.providedSchema)} [{schemaFieldSummary(edge.providedSchema, 'fields')}]</span>
									</div>
									<div class="schemaRow">
										<span class="schemaLabel">snapshot</span>
										<span>
											src={(edge.snapshotSourceSchemaFingerprint ?? '').slice(0, 12) || '(none)'} /
											tgt={(edge.snapshotTargetSchemaFingerprint ?? '').slice(0, 12) || '(none)'}
										</span>
									</div>
									<div class="schemaRow">
										<span class="schemaLabel">target (current)</span>
										<span>{schemaTypeLabel(edge.requiredSchema)} [{schemaFieldSummary(edge.requiredSchema, 'required_fields')}]</span>
									</div>
								</div>
								{#if edge.snapshotDrift}
									<div class="schemaSuggestions">{schemaEdgeDriftGuidance(edge)}</div>
								{/if}
								{#if edge.suggestions.length > 0}
									<div class="schemaSuggestions">{edge.suggestions.join(' ')}</div>
								{/if}
								{#if edge.adapterKind}
									<button type="button" class="schemaApplyBtn" on:click={() => applySchemaSuggestion(edge)}>
										Apply {edge.adapterKind} adapter
									</button>
								{/if}
							</div>
						{/each}
					</div>
				{/each}
			{/if}
		</div>
	</div>
{/if}

<style>
	.nodeInspectorTheme {
		--ni-bg: #f7f9fc;
		--ni-card: #ffffff;
		--ni-border: #d7deea;
		--ni-text: #1f2937;
		--ni-muted: #5b6677;
		--ni-control-bg: #ffffff;
		--ni-control-text: #1f2937;
		--ni-control-border: #b9c5da;
		--ni-error-bg: #fee2e2;
		--ni-error-border: #fca5a5;
		--ni-error-text: #7f1d1d;
		color: var(--ni-text);
	}

	@media (prefers-color-scheme: dark) {
		.nodeInspectorTheme {
			--ni-bg: #0b0f17;
			--ni-card: #0f1724;
			--ni-border: #253049;
			--ni-text: #e5e7eb;
			--ni-muted: #9aa3b2;
			--ni-control-bg: #0b1220;
			--ni-control-text: #e5e7eb;
			--ni-control-border: #2c3b59;
			--ni-error-bg: rgba(239, 68, 68, 0.12);
			--ni-error-border: rgba(239, 68, 68, 0.45);
			--ni-error-text: #fecaca;
		}
	}

	:global(.nodeInspectorTheme .section) {
		background: var(--ni-card);
		border: 1px solid var(--ni-border);
		border-radius: 10px;
		padding: 8px 10px;
	}

	:global(.nodeInspectorTheme .sectionTitle) {
		color: var(--ni-text);
	}

	:global(.nodeInspectorTheme .k) {
		color: var(--ni-muted);
	}

	:global(.nodeInspectorTheme .v input),
	:global(.nodeInspectorTheme .v select),
	:global(.nodeInspectorTheme .v textarea) {
		background: var(--ni-control-bg);
		color: var(--ni-control-text);
		border: 1px solid var(--ni-control-border);
	}

	:global(.nodeInspectorTheme .v select option) {
		background: Canvas !important;
		color: CanvasText !important;
	}

	:global(.nodeInspectorTheme .v select option:disabled) {
		background: Canvas !important;
		color: GrayText !important;
	}

	.schemaAssist {
		margin-bottom: 8px;
		border: 1px solid var(--ni-border);
		border-radius: 10px;
		padding: 8px;
		background: var(--ni-card);
		display: grid;
		gap: 4px;
	}

	.schemaAssist-fresh {
		border-color: #22c55e;
	}

	.schemaAssist-partial {
		border-color: #f59e0b;
	}

	.schemaAssist-stale {
		border-color: #ef4444;
	}

	.schemaAssistHead {
		display: flex;
		align-items: center;
		justify-content: space-between;
		font-size: 12px;
		font-weight: 600;
	}

	.schemaAssistBadge {
		font-size: 11px;
		padding: 2px 7px;
		border: 1px solid var(--ni-control-border);
		border-radius: 999px;
		color: var(--ni-muted);
	}

	.schemaAssistHint {
		font-size: 11px;
		color: var(--ni-muted);
	}

	.guidedModeRow {
		display: flex;
		align-items: center;
		justify-content: space-between;
		gap: 8px;
		margin-bottom: 8px;
		font-size: 11px;
	}

	.guidedToggle {
		display: inline-flex;
		align-items: center;
		gap: 6px;
		font-weight: 600;
	}

	.guidedHint {
		color: var(--ni-muted);
	}

	.guidedAssistCard {
		margin-bottom: 8px;
		border: 1px solid var(--ni-border);
		border-radius: 10px;
		padding: 8px;
		background: var(--ni-card);
		display: grid;
		gap: 6px;
	}

	.guidedAssistError {
		border-color: var(--ni-error-border);
		background: var(--ni-error-bg);
	}

	.guidedAssistHead {
		font-size: 12px;
		font-weight: 700;
	}

	.guidedAssistList {
		display: grid;
		gap: 6px;
	}

	.guidedAssistItem {
		border: 1px solid var(--ni-border);
		border-radius: 8px;
		padding: 6px;
		display: grid;
		gap: 4px;
	}

	.guidedAssistLabel {
		font-size: 11px;
		font-weight: 600;
	}

	.guidedAssistDesc {
		font-size: 11px;
		opacity: 0.85;
	}

	.assistActionRow {
		display: flex;
		flex-wrap: wrap;
		gap: 6px;
	}

	.previewDiffCols,
	.previewDiffRows {
		display: grid;
		grid-template-columns: repeat(2, minmax(0, 1fr));
		gap: 8px;
	}

	.previewDiffRows pre {
		margin: 0;
		max-height: 140px;
		overflow: auto;
		border: 1px solid var(--ni-border);
		border-radius: 8px;
		padding: 6px;
		font-size: 10px;
		background: var(--ni-control-bg);
		color: var(--ni-control-text);
	}

	.advancedEditor {
		margin-bottom: 8px;
		border: 1px solid var(--ni-border);
		border-radius: 10px;
		padding: 8px;
		background: var(--ni-card);
	}

	.advancedEditorTitle {
		font-size: 12px;
		font-weight: 600;
		margin-bottom: 8px;
	}

	.expectedSchemaEditor {
		margin-top: 8px;
		border: 1px solid var(--ni-border);
		border-radius: 10px;
		padding: 8px;
		background: var(--ni-card);
		display: grid;
		gap: 6px;
	}

	.expectedSchemaHead {
		font-size: 12px;
		font-weight: 600;
	}

	.expectedSchemaTextarea {
		width: 100%;
		min-height: 112px;
		font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New',
			monospace;
	}

	.expectedSchemaActions {
		display: flex;
		gap: 8px;
	}

	.expectedSchemaActions button {
		font-size: 11px;
		padding: 4px 8px;
	}

	.expectedInputHandleEditor {
		border: 1px solid var(--ni-border);
		border-radius: 8px;
		padding: 6px;
		display: grid;
		gap: 6px;
	}

	.expectedInputHandleHead {
		display: flex;
		justify-content: space-between;
		align-items: baseline;
		font-size: 11px;
		font-weight: 600;
	}

	.expectedSchemaError {
		font-size: 11px;
		color: var(--ni-error-text);
	}

	.expectedSchemaDrift {
		font-size: 11px;
		color: #f59e0b;
	}

	.schemaContract {
		margin-top: 10px;
		border: 1px solid var(--ni-border);
		border-radius: 10px;
		padding: 8px;
		background: var(--ni-card);
		display: grid;
		gap: 6px;
	}

	.schemaContract-warning {
		border-color: #f59e0b;
	}

	.schemaContract-error {
		border-color: #ef4444;
	}

	.schemaHead {
		font-size: 12px;
		font-weight: 700;
	}

	.schemaStatus,
	.schemaEmpty,
	.schemaSuggestions {
		font-size: 11px;
		opacity: 0.86;
	}

	.schemaDeprecationBox {
		border: 1px solid #f59e0b;
		background: color-mix(in srgb, #f59e0b 12%, transparent);
		border-radius: 8px;
		padding: 6px;
		display: grid;
		gap: 4px;
	}

	.schemaDeprecationHead {
		font-size: 11px;
		font-weight: 700;
		letter-spacing: 0.02em;
		text-transform: uppercase;
		color: #f59e0b;
	}

	.schemaDeprecationItem {
		font-size: 11px;
		line-height: 1.35;
	}

	.schemaModeGroup {
		display: grid;
		gap: 6px;
		padding-top: 4px;
	}

	.schemaModeHead {
		font-size: 11px;
		font-weight: 700;
		letter-spacing: 0.02em;
		text-transform: uppercase;
		color: var(--ni-muted);
	}

	.schemaEdge {
		border: 1px solid var(--ni-border);
		border-radius: 8px;
		padding: 6px;
		display: grid;
		gap: 4px;
	}

	.schemaEdge-warning {
		border-color: #f59e0b;
	}

	.schemaEdge-error {
		border-color: #ef4444;
	}

	.schemaEdgeHead {
		display: flex;
		justify-content: space-between;
		align-items: flex-start;
		font-size: 11px;
		font-weight: 600;
	}

	.schemaEdgeHeadMain {
		display: grid;
		gap: 2px;
	}

	.schemaEdgeCounterparty {
		padding-left: 4ch;
		font-weight: 500;
		opacity: 0.9;
	}

	.queueEdgeCounterparty {
		padding-left: 0;
		font-size: 11px;
		font-weight: 500;
		opacity: 0.9;
		text-align: left;
		justify-self: start;
	}

	.schemaEdgeConfig {
		display: grid;
		grid-template-columns: repeat(2, minmax(0, 1fr));
		gap: 6px;
	}

	.schemaEdgeConfig label {
		display: grid;
		gap: 4px;
		font-size: 11px;
	}

	.schemaEdgeConfig :global(.themedSelect),
	.schemaEdgeConfig input[type='number'] {
		width: 100%;
	}

	.schemaEdgeFatal {
		display: inline-flex !important;
		align-items: center;
		gap: 6px;
	}

	.schemaRow {
		display: grid;
		grid-template-columns: auto 1fr;
		gap: 6px;
		font-size: 11px;
	}

	.schemaLabel {
		color: var(--ni-muted);
	}

	.schemaApplyBtn {
		justify-self: start;
		font-size: 11px;
		padding: 4px 8px;
	}

	.schemaContractDiffPanel {
		display: grid;
		gap: 4px;
		border: 1px dashed var(--ni-border);
		border-radius: 8px;
		padding: 6px;
	}

	.schemaContractDiffHead {
		display: flex;
		align-items: center;
		gap: 6px;
		font-size: 11px;
		font-weight: 600;
		text-transform: uppercase;
		letter-spacing: 0.02em;
		color: var(--ni-muted);
	}

	.schemaContractBadge {
		display: inline-flex;
		align-items: center;
		padding: 1px 6px;
		border-radius: 999px;
		border: 1px solid var(--ni-border);
		font-size: 10px;
		font-weight: 600;
		text-transform: none;
		color: var(--ni-text);
	}
</style>
