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
	import { getArtifactMetaUrl, getArtifactPreviewUrl } from '$lib/flow/client/runs';
	import { parseInputSchemaView, type InputSchemaView } from '$lib/flow/components/editors/TransformEditor/inputSchema';
	import { buildTransformSchemaProps } from '$lib/flow/components/editors/TransformEditor/schemaPropagation';
	import {
		buildTransformAutoFixes,
		buildTransformPreviewDiff,
		guidedControlsForTransform,
		suggestNextTransformOps
	} from '$lib/flow/components/editors/TransformEditor/transformAssist';

	import type { PipelineNodeData } from '$lib/flow/types';
	import {
		graphStore,
		__buildNodeSchemaContractSnapshotForTest,
		type NodeSchemaContractEdge
	} from '$lib/flow/store/graphStore';

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
	$: schemaContract = selectedNode
		? __buildNodeSchemaContractSnapshotForTest($graphStore as any, selectedNode.id)
		: { nodeId: '', status: 'clean', edges: [] as NodeSchemaContractEdge[] };
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

	let inputSchemas: InputSchemaView[] = [];
	let inputSchemaReqSeq = 0;
	let lastInputSignature = '';
	let transformGuidedMode = true;
	let sourceGuidedMode = true;
	let modelGuidedMode = true;
	let modelAdvancedOpen = false;
	let modelEditorNodeId = '';
	let inputPreviewRows: Array<Record<string, unknown>> = [];
	let inputPreviewColumns: string[] = [];
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
	$: expectedInputSchemaDraft = (() => {
		if (!selectedNode || !schemaContract?.edges?.length) return '';
		const incoming = (schemaContract.edges as NodeSchemaContractEdge[])
			.filter((edge) => edge.direction === 'incoming' && edge.requiredSchema)
			.map((edge) => ({
				edgeId: edge.edgeId,
				sourceNodeId: edge.sourceNodeId,
				sourceHandle: edge.sourceHandle,
				requiredSchema: edge.requiredSchema
			}));
		if (incoming.length === 0) return '';
		if (incoming.length === 1) {
			return JSON.stringify(incoming[0].requiredSchema ?? { type: 'unknown', fields: [] }, null, 2);
		}
		return JSON.stringify(
			{
				type: 'multi_input',
				inputs: incoming
			},
			null,
			2
		);
	})();
	let expectedSchemaDraft = '';
	let expectedSchemaError = '';
	let expectedSchemaNodeId = '';
	let headerSchemaLoading = false;

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

	$: if (isLlm && selectedNode?.id && selectedNode.id !== modelEditorNodeId) {
		modelEditorNodeId = selectedNode.id;
		modelGuidedMode = true;
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
				{onDraft}
				{onCommit}
			/>
		{:else}
			<svelte:component
				this={SourceEditorByKind[sourceKind] ?? SourceEditorByKind.file}
				{selectedNode}
				{params}
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
							on:blur={(e) => onCommit({ model: (e.currentTarget as HTMLInputElement).value })}
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
							inputColumns={schemaProps.inputColumns}
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
					inputColumns={schemaProps.inputColumns}
					inputSchemaColumns={schemaProps.inputSchemaColumns}
					inputSchemas={schemaProps.inputSchemas}
					{onDraft}
					{onCommit}
				/>
			{/if}
		{/if}
		{#if !isComponent}
			<div class="expectedSchemaEditor">
				<div class="expectedSchemaHead">Expected Input Schema</div>
				<textarea
					class="expectedSchemaTextarea"
					rows="7"
					value={expectedInputSchemaDraft || JSON.stringify({ type: 'none' }, null, 2)}
					readonly
					spellcheck="false"
				/>
			</div>
			<div class="expectedSchemaEditor">
				<div class="expectedSchemaHead">Expected Output Schema</div>
				<textarea
					class="expectedSchemaTextarea"
					rows="7"
					bind:value={expectedSchemaDraft}
					spellcheck="false"
				/>
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
			{#if schemaContract.edges.length === 0}
				<div class="schemaEmpty">No connected edges.</div>
			{:else}
				{#each schemaContract.edges as edge (edge.edgeId)}
					<div class={`schemaEdge schemaEdge-${edge.severity}`}>
						<div class="schemaEdgeHead">
							<span>{edge.direction === 'incoming' ? 'in' : 'out'}: {edge.edgeId}</span>
							<span>{edge.severity}</span>
						</div>
						<div class="schemaRow">
							<span class="schemaLabel">provided</span>
							<span>{schemaTypeLabel(edge.providedSchema)} [{schemaFieldSummary(edge.providedSchema, 'fields')}]</span>
						</div>
						<div class="schemaRow">
							<span class="schemaLabel">required</span>
							<span>{schemaTypeLabel(edge.requiredSchema)} [{schemaFieldSummary(edge.requiredSchema, 'required_fields')}]</span>
						</div>
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
		background: var(--ni-control-bg);
		color: var(--ni-control-text);
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
		font-size: 11px;
		font-weight: 600;
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
</style>
