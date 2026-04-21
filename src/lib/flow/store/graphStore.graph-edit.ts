// src/lib/flow/store/graphStore.graph-edit.ts
// Step 7 of graphStore refactor: graph editing operations

import type { Node, Edge } from '@xyflow/svelte';
import type {
	NodeKind,
	PipelineNodeData,
	PipelineEdgeData,
	PipelineGraphDTO,
	PayloadType
} from '$lib/flow/types';
import type { CheckpointRecord, CheckpointRegistry } from '$lib/flow/types/checkpoint';
import { defaultNodeData } from '$lib/flow/schema/defaults';
import { defaultSourceMetaByKind, defaultSourceParamsByKind } from '$lib/flow/schema/sourceDefaults';
import { defaultLlmParamsByKind } from '$lib/flow/schema/llmDefaults';
import { defaultTransformParamsByKind } from '$lib/flow/schema/transformDefaults';
import { defaultToolParamsByProvider, type ToolProvider } from '$lib/flow/schema/toolDefaults';
import { evaluateSchemaCoercion } from '$lib/flow/schema/coercionPolicy';
import {
	findNodeIdByName,
	normalizeNodeName,
	resolveUniqueNodeName
} from './nodeNameUniqueness';
import { runInHistoryTransaction, createHistoryManager } from './graphStore.history';
import {
	withGraphMeta,
	logPush,
	DEV_MODE,
	ensureNormalizedBindingsForNodes,
	_normalizeBinding
} from './graphStore.audit';
import {
	canonicalizeNodeSchemas,
	isEdgeStillValid,
	normalizeEdgeMode,
	normalizeEdgeLinkKind,
	buildProvidedSchema,
	buildRequiredSchema,
	isSchemaCompatible,
	edgeContractSnapshotFromSchemas,
	normalizeComponentPayloadType,
	inferEdgeModeFromHandles,
	adapterKindForTypes,
	adapterSuggestionForTypes,
	normalizeHintType,
	sourcePayloadHint,
	targetPayloadHint,
	nodePortAffinity,
	portCardinality,
	edgeModeCompatible,
	hasPortHandle,
	declaredPortHandles,
	sameHandleProvidedSchemaConflict,
	payloadHintToTypedSchema,
	hasSchemaEnvelopeContent
} from './graphStore.node-schema';
import {
	canonicalComponentSourceHandleForEdge
} from './graphStore.inspector';
import type {
	GraphState,
	AuditContext,
	SavePreflightDiagnostic,
	SavePreflightResult,
	SaveConsistencyEntity,
	SaveConsistencyMismatch,
	AdapterTransformKind,
	SchemaCompatibility,
	InputResolution,
	ComponentEditSessionSnapshot
} from './graphStore.types';
import {
	RUN_IDLE,
	NODE_STATUS_IDLE,
	NODE_STATUS_SUCCEEDED,
	INITIAL_INSPECTOR
} from './graphStore.types';
import { emptySchemaPlaneState } from './graphStore.schemaPlane';
import type { SourceKind, LlmKind, TransformKind } from '$lib/flow/types/paramsMap';

// ── PART A: Top-level pure function exports ──────────────────────────────────

export function mintGraphId(): string {
	try {
		return `graph_${crypto.randomUUID()}`;
	} catch {
		return `graph_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 10)}`;
	}
}

export function buildHardResetState(freshGraphId: string): GraphState {
	return {
		graphId: freshGraphId,
		nodeDocExplanationMode: 'default',
		nodeDocTrainingMode: 'off',
		nodeDocTooltipEnabled: true,
		nodeDocTooltipOpenDelayMs: 500,
		nodeDocPlanesExpansionEnabled: true,
		nodeDocPlanesExpansionDelayMs: 1200,
		nodeDocExplainModel: 'glm-4.7-flash:latest',
		nodeDocExplainTemperature: 0.2,
		nodeDocExplainTopP: 1.0,
		nodeDocExplainMaxTokens: 512,
		nodes: [],
		edges: [],
		selectedNodeId: null,
		inspector: INITIAL_INSPECTOR,
		logs: [],
		runStatus: RUN_IDLE,
		lastRunStatus: 'never_run',
		freshness: 'never_run',
		staleNodeCount: 0,
		activeRunMode: 'from_start',
		activeRunFrom: null,
		activeRunNodeSet: new Set<string>(),
		runBlockedReason: null,
		viewMode: 'execution',
		schemaWarningDismissCount: 0,
		nodeOutputs: {},
		nodeBindings: {},
		activeRunId: null,
		editingContext: 'graph',
		componentEditSession: null,
		componentContractDraftCache: {},
		checkpointRegistry: {},
		schemaPlane: emptySchemaPlaneState()
	};
}

export function captureComponentEditSnapshot(state: GraphState): ComponentEditSessionSnapshot {
	return {
		graphId: state.graphId,
		nodes: structuredClone(state.nodes),
		edges: structuredClone(state.edges),
		checkpointRegistry: structuredClone(state.checkpointRegistry ?? {}),
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
		runBlockedReason: state.runBlockedReason ? structuredClone(state.runBlockedReason) : null,
		nodeOutputs: structuredClone(state.nodeOutputs),
		nodeBindings: structuredClone(state.nodeBindings),
		activeRunId: state.activeRunId
	};
}

export function __hardResetGraphForTest(_state: GraphState, freshGraphId = 'graph_test_reset'): GraphState {
	return buildHardResetState(freshGraphId);
}

export function stripToDTO(
	nodes: Node<PipelineNodeData>[],
	edges: Edge<PipelineEdgeData>[],
	graphId?: string,
	checkpointRegistry?: CheckpointRegistry
): PipelineGraphDTO {
	const persistedNodes = nodes.map((node) => {
		const data = (node as any)?.data;
		if (!data || typeof data !== 'object') return node;
		const nextData = { ...data } as Record<string, unknown>;
		return {
			...node,
			data: nextData as PipelineNodeData & Record<string, unknown>
		};
	});
	const dto: PipelineGraphDTO = {
		version: 1,
		nodes: persistedNodes as any,
		edges: recomputeEdgeContractsBestEffort(nodes, edges),
		checkpointRegistry: structuredClone(checkpointRegistry ?? {})
	};
	if (graphId) {
		dto.meta = { ...(dto.meta ?? {}), graphId } as any;
	}
	return dto;
}

export function __stripToDTOForTest(
	nodes: Node<PipelineNodeData>[],
	edges: Edge<PipelineEdgeData>[],
	graphId?: string,
	checkpointRegistry?: CheckpointRegistry
): PipelineGraphDTO {
	return stripToDTO(nodes, edges, graphId, checkpointRegistry);
}

export function edgeStructuralSignature(edge: Edge<PipelineEdgeData>): string {
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

export function shouldPreserveStoreEdgesOnCanvasSync(
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

export function normalizeComponentPayloadTypeOrDefault(value: unknown, fallback: PayloadType = 'json'): PayloadType {
	const normalized = normalizeComponentPayloadType(value);
	return normalized ?? fallback;
}

export function normalizeComponentNodeForMigration(
	node: Node<PipelineNodeData>
): { node: Node<PipelineNodeData>; outputNames: string[]; outputByName: Map<string, PayloadType>; bindingNames: string[] } {
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
			const outputType = normalizeComponentPayloadTypeOrDefault((out as any)?.typedSchema?.type, 'json');
			const typedSchemaRaw =
				(out as any)?.typedSchema && typeof (out as any).typedSchema === 'object'
					? ((out as any).typedSchema as Record<string, any>)
					: {};
			const fieldsRaw = Array.isArray(typedSchemaRaw.fields) ? (typedSchemaRaw.fields as any[]) : [];
			const normalizedFields =
				outputType === 'table' || outputType === 'json'
					? fieldsRaw
					: [];
			return {
				...(out as any),
				name: outName,
				typedSchema: {
					type: outputType,
					fields: normalizedFields
				}
			};
		})
		.filter((out) => String((out as any)?.name ?? '').trim().length > 0);

	const outputNames = normalizedOutputs.map((out) => String((out as any)?.name ?? '').trim());
	const outputSet = new Set(outputNames);
	const outputByName = new Map<string, PayloadType>();
	for (const out of normalizedOutputs) {
		const name = String((out as any)?.name ?? '').trim();
		const outputType = normalizeComponentPayloadTypeOrDefault((out as any)?.typedSchema?.type, 'json');
		outputByName.set(name, outputType);
	}

	const exposureRegistryRaw = Array.isArray(params.exposureRegistry) ? (params.exposureRegistry as any[]) : [];
	const exposureRegistry = exposureRegistryRaw.filter((rec) => {
		if (!rec || typeof rec !== 'object') return false;
		if (String((rec as any).kind ?? '').trim().toLowerCase() !== 'data_output') return true;
		const alias = String((rec as any).alias ?? '').trim();
		const handleId = String((rec as any).handle_id ?? '').trim();
		if (alias && outputSet.has(alias)) return true;
		if (handleId.startsWith('data_out::')) {
			const outName = handleId.slice('data_out::'.length).trim();
			return outName.length > 0 && outputSet.has(outName);
		}
		return false;
	});

		const nextNode: Node<PipelineNodeData> = {
			...node,
			data: {
				...node.data,
				params: {
					...params,
				api: {
					...(api as Record<string, any>),
					outputs: normalizedOutputs
				},
				exposureRegistry
			}
		}
	};
	const bindingNames = outputNames.filter((name) =>
		exposureRegistry.some(
			(rec: any) =>
				String(rec?.kind ?? '').trim().toLowerCase() === 'data_output' &&
				(
					String(rec?.alias ?? '').trim() === name ||
					String(rec?.handle_id ?? '').trim() === `data_out::${name}`
				)
		)
	);
	return { node: nextNode, outputNames, outputByName, bindingNames };
}

export function normalizeGraphForComponentMigration(
	nodes: Node<PipelineNodeData>[],
	edges: Edge<PipelineEdgeData>[]
): { nodes: Node<PipelineNodeData>[]; edges: Edge<PipelineEdgeData>[] } {
	const nodeInfo = new Map<
		string,
		{ outputNames: string[]; outputByName: Map<string, PayloadType>; bindingNames: string[] }
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
		const contractOut = normalizeComponentPayloadType(edgeDataContract?.out ?? null);
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
	const canonicalNodes = canonicalizeNodeSchemas(normalizedNodes);

	return {
		nodes: canonicalNodes,
		edges: recomputeEdgeContractsBestEffort(canonicalNodes, normalizedEdges)
	};
}

function mintCheckpointId(): string {
	try {
		return crypto.randomUUID();
	} catch {
		return `ck_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 10)}`;
	}
}

function normalizeMigratedOutputs(
	raw: unknown
): Record<string, { artifactId: string; execKey?: string }> | undefined {
	if (!raw || typeof raw !== 'object') return undefined;
	const out: Record<string, { artifactId: string; execKey?: string }> = {};
	for (const [rawHandle, rawPair] of Object.entries(raw as Record<string, unknown>)) {
		const handle = String(rawHandle ?? '').trim();
		if (!handle || !rawPair || typeof rawPair !== 'object') continue;
		const artifactId = String((rawPair as any).artifactId ?? '').trim();
		const execKey = String((rawPair as any).execKey ?? '').trim();
		if (!artifactId) continue;
		out[handle] = execKey ? { artifactId, execKey } : { artifactId };
	}
	return Object.keys(out).length > 0 ? out : undefined;
}

function migrateFreezePinsToCheckpoints(
	nodes: Node<PipelineNodeData>[],
	existingRegistry: CheckpointRegistry,
	graphId: string
): { nodes: Node<PipelineNodeData>[]; checkpointRegistry: CheckpointRegistry } {
	const registry: CheckpointRegistry = structuredClone(existingRegistry ?? {});
	const migratedNodes = nodes.map((node) => {
		const nodeData = (node as any)?.data;
		if (!nodeData || typeof nodeData !== 'object') return node;
		const meta = (nodeData as any).meta;
		if (!meta || typeof meta !== 'object') return node;

		const freeze = (meta as any).freeze;
		const freezeLineage = (meta as any).freezeLineage;
		const hasLegacyFields =
			Object.prototype.hasOwnProperty.call(meta, 'freeze') ||
			Object.prototype.hasOwnProperty.call(meta, 'freezeLineage');

		if (
			freeze &&
			typeof freeze === 'object' &&
			(freeze as any).enabled === true &&
			freezeLineage &&
			typeof freezeLineage === 'object' &&
			!registry[node.id]
		) {
			const artifactId = String((freezeLineage as any).artifactId ?? '').trim();
			const execKey = String((freezeLineage as any).execKey ?? '').trim();
			if (artifactId && execKey) {
				const mode = String((freeze as any).mode ?? 'sticky').trim() || 'sticky';
				const createdAtRaw = String((meta as any).updatedAt ?? '').trim();
				const createdAt = createdAtRaw || new Date().toISOString();
				registry[node.id] = {
					id: mintCheckpointId(),
					name: `Migrated pin (${mode})`,
					description: 'Automatically migrated from legacy pin system.',
					nodeId: node.id,
					graphId,
					runId: 'legacy_migration',
					artifactId,
					execKey,
					fingerprintAtCreation: '0'.repeat(64),
					createdAt,
					staleness: 'unknown',
					outputs: normalizeMigratedOutputs((freezeLineage as any).outputs)
				};
			}
		}

		if (!hasLegacyFields) return node;
		const nextMeta: Record<string, unknown> = { ...(meta as Record<string, unknown>) };
		delete (nextMeta as any).freeze;
		delete (nextMeta as any).freezeLineage;
		return {
			...node,
			data: {
				...(node.data as any),
				meta: nextMeta
			}
		} as Node<PipelineNodeData>;
	});
	return { nodes: migratedNodes, checkpointRegistry: registry };
}

export function setEdgeExec(
	edges: Edge<PipelineEdgeData>[],
	edgeId: string,
	exec: 'idle' | 'active' | 'done'
) {
	return edges.map((e) => (e.id === edgeId ? { ...e, data: { ...e.data, exec: exec } } : e));
}

export function downstreamIds(startId: string, edges: Edge<PipelineEdgeData>[]) {
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

export function pruneAndRecontractEdgesStrict(
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
				error: `Edge ${e.id} has unresolved schema compatibility (source=${e.source}:${e.sourceHandle ?? 'out'} target=${e.target}:${e.targetHandle ?? 'in'})`
			};
		}

		next.push({
			...e,
			data: {
				...(e.data ?? {}),
				exec: e.data?.exec ?? 'idle',
				mode: normalizeEdgeMode(e),
				contract: (() => {
					const sourceHandle = String((e as any).sourceHandle ?? 'out').trim() || 'out';
					const targetHandle = String((e as any).targetHandle ?? 'in').trim() || 'in';
					const sourceNode = nodes.find((n) => n.id === e.source)!;
					const targetNode = nodes.find((n) => n.id === e.target)!;
					const payloadSource = buildProvidedSchema(sourceNode as any, sourceHandle) as Record<string, any>;
					const payloadTarget = buildRequiredSchema(targetNode as any, targetHandle) as Record<string, any>;
					const compatibility = isSchemaCompatible(
						payloadSource ?? { type: 'unknown' },
						payloadTarget ?? { type: 'unknown' },
						normalizeEdgeMode(e)
					);
					return {
						out: chk.out,
						in: chk.in,
						payload: {
							source: payloadSource,
							target: payloadTarget
						},
						snapshot: edgeContractSnapshotFromSchemas(
							payloadSource,
							payloadTarget,
							compatibility,
							normalizeEdgeMode(e)
						)
					};
				})()
			}
		});
	}

	return { ok: true, edges: next, prunedIds };
}

export function canonicalizeComponentEdgeSourceHandles(
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

export function recomputeEdgeContractsBestEffort(
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
		const sourceHandle = String((edge as any).sourceHandle ?? 'out').trim() || 'out';
		const targetHandle = String((edge as any).targetHandle ?? 'in').trim() || 'in';
		const payload = {
			source: buildProvidedSchema(sourceNode as any, sourceHandle),
			target: buildRequiredSchema(targetNode as any, targetHandle)
		};
		const edgeMode = normalizeEdgeMode(edge);
		const snapshotCompatibility: SchemaCompatibility = chk.ok
			? { ok: true }
			: {
					ok: false,
					reason:
						chk.reason === 'schema_mismatch'
							? 'missing_required_columns'
							: chk.reason === 'typed_schema_missing'
								? 'missing_typed_schema'
								: 'type_mismatch',
					missingColumns: chk.missingColumns,
					suggestion: chk.suggestion ?? null,
					adapterKind: chk.adapterKind ?? null
				};
		const snapshot = edgeContractSnapshotFromSchemas(
			payload.source as any,
			payload.target as any,
			snapshotCompatibility,
			edgeMode
		);
		if (chk.ok) {
			return {
				...edge,
				data: {
					...(edge.data ?? {}),
					contract: {
						out: chk.out,
						in: chk.in,
						payload,
						snapshot
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
					payload,
					snapshot
				}
			}
		};
	});
}

export function topoFrom(
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

// ── PART B: Factory function ─────────────────────────────────────────────────

type GraphEditDeps = {
	update: (fn: (s: GraphState) => GraphState, ctx?: AuditContext) => void;
	set: (s: GraphState) => void;
	getState: () => GraphState;
	history: ReturnType<typeof createHistoryManager>;
	persist: (state: GraphState) => void;
	applyLocalStaleInvalidation: (nodeId: string, reason?: string) => void;
	updateNodeConfig: (nodeId: string, config: any, opts?: any) => { ok: boolean; error?: string };
};

export function createGraphEditManager(deps: GraphEditDeps) {
	const { update, set, getState, history, persist, applyLocalStaleInvalidation, updateNodeConfig } = deps;

	const COMPONENT_DRAFT_GRAPH_KEY = '__graphDraft';
	const COMPONENT_DRAFT_LAST_COMMITTED_CHECKPOINTS_KEY = '__lastCommittedCheckpointRegistry';

	/** When in component edit context, sync the committed checkpoint baseline
	 *  so that checkpoint create/remove actions are treated as committed state
	 *  rather than "unsaved changes" that would block a parent graph run. */
	function syncComponentDraftCommittedCheckpoints(): void {
		const state = getState();
		const session = state.componentEditSession;
		if (!session || state.editingContext !== 'component') return;
		const cacheKey = `${String(session.componentId ?? '').trim()}@${String(session.revisionId ?? '').trim()}`;
		if (!cacheKey) return;
		// Use the current store checkpointRegistry (which includes the just-created/removed
		// checkpoint) rather than the stale draft cache, since inside component edit the
		// active registry IS the component's registry.
		const currentCheckpoints = structuredClone(state.checkpointRegistry ?? {});
		update((s) => {
			const nextCache = { ...(s.componentContractDraftCache ?? {}) };
			const existingEntry = (nextCache[cacheKey] as Record<string, unknown>) ?? {};
			// Also update the draft graph's checkpointRegistry to stay in sync.
			const existingGraphDraft = (existingEntry as Record<string, unknown>)[COMPONENT_DRAFT_GRAPH_KEY];
			const nextDraftGraph = existingGraphDraft && typeof existingGraphDraft === 'object'
				? { ...(existingGraphDraft as Record<string, unknown>), checkpointRegistry: currentCheckpoints }
				: { nodes: [], edges: [], checkpointRegistry: currentCheckpoints };
			nextCache[cacheKey] = {
				...existingEntry,
				[COMPONENT_DRAFT_GRAPH_KEY]: nextDraftGraph,
				[COMPONENT_DRAFT_LAST_COMMITTED_CHECKPOINTS_KEY]: currentCheckpoints
			};
			const next = withGraphMeta({ ...s, componentContractDraftCache: nextCache });
			persist(next);
			return next;
		});
	}

	function applyGraphDocument(
		graph: { nodes: unknown[]; edges: unknown[]; checkpointRegistry?: CheckpointRegistry },
		graphIdOverride?: string | null
	): { ok: boolean; reason?: string } {
		const nextNodes = Array.isArray(graph?.nodes) ? (graph.nodes as Node<PipelineNodeData>[]) : null;
		const nextEdges = Array.isArray(graph?.edges) ? (graph.edges as Edge<PipelineEdgeData>[]) : null;
		const nextCheckpointRegistry =
			graph?.checkpointRegistry && typeof graph.checkpointRegistry === 'object'
				? structuredClone(graph.checkpointRegistry)
				: {};
		if (!nextNodes || !nextEdges) return { ok: false, reason: 'invalid_payload' };
		const normalized = normalizeGraphForComponentMigration(nextNodes, nextEdges);
		const targetGraphId = String(graphIdOverride || getState().graphId);
		const migrated = migrateFreezePinsToCheckpoints(
			normalized.nodes,
			nextCheckpointRegistry,
			targetGraphId
		);
		const canonicalized = canonicalizeComponentEdgeSourceHandles(migrated.nodes, normalized.edges, 'strict');
		if (!canonicalized.ok) return { ok: false, reason: canonicalized.error };
		const rechecked = pruneAndRecontractEdgesStrict(migrated.nodes, canonicalized.edges);
		if (!rechecked.ok) return { ok: false, reason: rechecked.error };
		update((s) => {
			const nextState = withGraphMeta({
				...s,
				graphId: String(graphIdOverride || s.graphId),
				nodes: migrated.nodes,
				edges: rechecked.edges,
				selectedNodeId: null,
				inspector: { ...INITIAL_INSPECTOR, uiByNodeId: s.inspector.uiByNodeId },
				logs: [],
				runStatus: RUN_IDLE,
				lastRunStatus: 'never_run',
				freshness: 'never_run',
				staleNodeCount: 0,
				activeRunMode: 'from_start',
				activeRunFrom: null,
				activeRunNodeSet: new Set<string>(),
				nodeOutputs: {},
				nodeBindings: ensureNormalizedBindingsForNodes(migrated.nodes as any, {}),
				activeRunId: null,
				editingContext: 'graph',
				componentEditSession: null,
				checkpointRegistry: migrated.checkpointRegistry
			});
			persist(nextState);
			return nextState;
		}, { source: 'graph_edit' });
		if (!history.isApplying()) {
			const st = getState();
			history.resetToSnapshot(stripToDTO(
				st.nodes as any,
				st.edges as any,
				st.graphId,
				st.checkpointRegistry ?? {}
			));
		}
		return { ok: true };
	}

	function applySemanticSubtypeReset(
		nodeId: string,
		payload: Record<string, unknown>
	): void {
		applyLocalStaleInvalidation(nodeId, 'KIND_CHANGED');
		if (DEV_MODE) {
			const st = getState();
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

	function setSourceKind(nodeId: string, nextKind: SourceKind) {
		return runInHistoryTransaction(history, () => {
			const nextParams = structuredClone(defaultSourceParamsByKind[nextKind]);
			const nextMetaDefaults = defaultSourceMetaByKind[nextKind] ?? {};

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
								sourceKind: nextKind, // ✅ structural
								meta: (() => {
									const nextMeta = { ...(n.data.meta ?? {}), updatedAt: new Date().toISOString() } as Record<string, unknown>;
									if (nextMetaDefaults.memoizable === false) {
										nextMeta.memoizable = false;
									} else {
										delete nextMeta.memoizable;
									}
									return nextMeta;
								})()
							}
						}
						: n
				);

				const next = { ...s, nodes };
				persist(next);
				return next;
			});

			// 2) replace params via your validated path (schema stripping happens here)
			const r = updateNodeConfig(nodeId, { params: nextParams });
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
		});
	}

	// graphStore.ts (inside your graphStore object)
	function setLlmKind(nodeId: string, nextKind: LlmKind) {
		return runInHistoryTransaction(history, () => {
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
								llmKind: nextKind, // ✅ structural
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
			const r = updateNodeConfig(nodeId, { params: nextParams });
			if (r.ok) {
				const node = getState().nodes.find((n: any) => n.id === nodeId);
				applySemanticSubtypeReset(nodeId, { kind: (node as any)?.data?.kind ?? 'model', llmKind: nextKind });
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
		});
	}

	// graphStore.ts (inside your graphStore object)
	function setTransformKind(nodeId: string, nextKind: TransformKind) {
		return runInHistoryTransaction(history, () => {
			const nextParams = structuredClone(defaultTransformParamsByKind[nextKind]);

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
								transformKind: nextKind, // ✅ structural
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
			const r = updateNodeConfig(nodeId, { params: nextParams });
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
		});
	}

	function setToolProvider(nodeId: string, nextProvider: ToolProvider) {
		return runInHistoryTransaction(history, () => {
			const nextParams = structuredClone(defaultToolParamsByProvider[nextProvider]);
			const r = updateNodeConfig(nodeId, { params: nextParams });
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
		});
	}

	function setToolKind(nodeId: string, nextProvider: ToolProvider) {
		return setToolProvider(nodeId, nextProvider);
	}

	// ----- sync entrypoints (because SvelteFlow uses bind:nodes/bind:edges) -----
	function syncFromCanvas(nodes: Node<PipelineNodeData>[], edges: Edge<PipelineEdgeData>[]) {
		update((s) => {
			const nextEdges = shouldPreserveStoreEdgesOnCanvasSync(s.edges, edges) ? s.edges : edges;
			const normalized = normalizeGraphForComponentMigration(nodes, nextEdges);
			// avoid needless churn if same references
			if (s.nodes === normalized.nodes && s.edges === normalized.edges) return s;
			const next = {
				...s,
				nodes: normalized.nodes,
				edges: normalized.edges,
				nodeBindings: ensureNormalizedBindingsForNodes(normalized.nodes, s.nodeBindings ?? {})
			};
			persist(next);
			return next;
		});
	}

	// ----- node CRUD -----
	function addNode(kind: NodeKind, position: { x: number; y: number }, opts?: { label?: string }) {
		const id = `n_${crypto.randomUUID()}`;
		const baseNode: Node<PipelineNodeData> = {
			id,
			type: kind,
			position,
			data: defaultNodeData(kind)
		};
		const node = canonicalizeNodeSchemas([baseNode])[0] as Node<PipelineNodeData>;
		if ((node.data as any)?.schema) {
			delete (node.data as any).schema;
		}

		update((s) => {
			const requestedLabel =
				typeof opts?.label === 'string' && String(opts.label).trim().length > 0
					? String(opts.label).trim()
					: String((node.data as any)?.label ?? '').trim();
			const uniqueLabel = resolveUniqueNodeName(s.nodes as Node<PipelineNodeData>[], requestedLabel);
			if (uniqueLabel) {
				(node.data as any).label = uniqueLabel;
			}
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
	}

	function deleteNode(nodeId: string) {
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
	}

	// ----- edge CRUD -----
	function deleteEdge(edgeId: string) {
		update((s) => {
			const edges = s.edges.filter((e) => e.id !== edgeId);
			const next = logPush({ ...s, edges }, 'info', `Deleted edge ${edgeId}`);
			persist(next);
			return next;
		});
	}

	function updateEdgeConfig(
		edgeId: string,
		patch: {
			mode?: 'work' | 'param' | 'control';
			fatal?: boolean;
			queue?: { max?: number; overflow?: 'block' | 'spill' | 'error'; policy?: 'fifo' | 'round_robin' };
			work?: { item_mode?: 'artifact' | 'json_items' | 'table_rows'; max_items?: number };
		}
	) {
		let out: { ok: boolean; error?: string } = { ok: true };
		update((s) => {
			const idx = s.edges.findIndex((e) => e.id === edgeId);
			if (idx < 0) {
				out = { ok: false, error: 'Edge not found' };
				return s;
			}
			const edge = s.edges[idx];
			const nextMode = String(patch.mode ?? normalizeEdgeMode(edge)).trim().toLowerCase();
			if (!['work', 'param', 'control'].includes(nextMode)) {
				out = { ok: false, error: 'Invalid edge mode' };
				return s;
			}
			const nextQueue = {
				max: Math.max(1, Number(patch.queue?.max ?? (edge.data as any)?.queue?.max ?? 1000)),
				overflow: String(
					patch.queue?.overflow ?? (edge.data as any)?.queue?.overflow ?? 'block'
				).toLowerCase() as 'block' | 'spill' | 'error',
				policy: String(
					patch.queue?.policy ?? (edge.data as any)?.queue?.policy ?? 'fifo'
				).toLowerCase() as 'fifo' | 'round_robin'
			};
			if (!['block', 'spill', 'error'].includes(nextQueue.overflow)) {
				out = { ok: false, error: 'Invalid queue overflow policy' };
				return s;
			}
			if (!['fifo', 'round_robin'].includes(nextQueue.policy)) {
				out = { ok: false, error: 'Invalid queue arbitration policy' };
				return s;
			}
			const nextWork = {
				item_mode: String(
					patch.work?.item_mode ??
						(edge.data as any)?.work?.item_mode ??
						(edge.data as any)?.work?.itemMode ??
						'artifact'
				).toLowerCase() as 'artifact' | 'json_items' | 'table_rows',
				max_items: Math.max(
					1,
					Number(patch.work?.max_items ?? (edge.data as any)?.work?.max_items ?? (edge.data as any)?.work?.maxItems ?? 256)
				)
			};
			if (!['artifact', 'json_items', 'table_rows'].includes(nextWork.item_mode)) {
				out = { ok: false, error: 'Invalid work item mode' };
				return s;
			}
			const nextEdge: Edge<PipelineEdgeData> = {
				...edge,
				data: {
					...(edge.data ?? { exec: 'idle' as const }),
					exec: edge.data?.exec ?? 'idle',
					linkKind: normalizeEdgeLinkKind(edge),
					mode: nextMode as any,
					fatal: Boolean(patch.fatal ?? (edge.data as any)?.fatal ?? false),
					queue: nextQueue,
					work: nextWork
				}
			};
			const chk = isEdgeStillValid(s.nodes, nextEdge);
			if (!chk.ok) {
				out = { ok: false, error: `Edge incompatible after config update (${chk.reason})` };
				return s;
			}
			const edges = [...s.edges];
			edges[idx] = nextEdge;
			const next = logPush({ ...s, edges }, 'info', `Updated edge ${edgeId} config`);
			persist(next);
			return next;
		});
		return out;
	}

	function preflightConnection(input: {
		source: string;
		target: string;
		sourceHandle?: string | null;
		targetHandle?: string | null;
		mode?: 'work' | 'param' | 'control' | null;
	}) {
		const source = String(input?.source ?? '').trim();
		const target = String(input?.target ?? '').trim();
		if (!source || !target) {
			return { ok: false as const, error: 'Missing source or target' };
		}
		if (source === target) {
			return { ok: false as const, error: 'Cannot connect node to itself' };
		}
		const state = getState();
		const sourceNode = state.nodes.find((node) => node.id === source);
		const targetNode = state.nodes.find((node) => node.id === target);
		if (!sourceNode || !targetNode) {
			return { ok: false as const, error: 'Source or target node not found' };
		}
		const sourceHandle = String(input?.sourceHandle ?? 'out').trim() || 'out';
		const targetHandleRaw = String(input?.targetHandle ?? '').trim();
		const modeRaw = String(input?.mode ?? '').trim().toLowerCase();
		const inferredMode = inferEdgeModeFromHandles({
			sourceHandle,
			targetHandle: targetHandleRaw || undefined
		} as any);
		const mode =
			modeRaw === 'work' || modeRaw === 'param' || modeRaw === 'control'
				? (modeRaw as 'work' | 'param' | 'control')
				: inferredMode;
		const sourceAffinity = nodePortAffinity(sourceNode, 'out', sourceHandle);
		const detailsBase = {
			mode,
			sourceHandle,
			sourceAffinity,
			targetHandle: targetHandleRaw || null
		};

		if (!targetHandleRaw) {
			const declared = declaredPortHandles(targetNode, 'in');
			const candidateHandles = declared.length > 0 ? [...declared] : ['in'];
			if (hasPortHandle(targetNode, 'in', 'in') && !candidateHandles.includes('in')) {
				candidateHandles.unshift('in');
			}
			const compatible = candidateHandles.some((candidate) =>
				edgeModeCompatible(mode, sourceAffinity, nodePortAffinity(targetNode, 'in', candidate))
			);
			if (!compatible) {
				return {
					ok: false as const,
					error: 'No compatible target input handle for this edge mode',
					details: {
						...detailsBase,
						candidateHandles
					}
				};
			}
			return {
				ok: true as const,
				deferred: true as const,
				details: {
					...detailsBase,
					candidateHandles
				}
			};
		}

		if (!hasPortHandle(targetNode, 'in', targetHandleRaw)) {
			return {
				ok: false as const,
				error: `Target handle '${targetHandleRaw}' is not declared for this node`,
				details: detailsBase
			};
		}
		if (portCardinality(targetNode, 'in', targetHandleRaw) === 'one') {
			const existingInbound = state.edges.filter(
				(edge) =>
					String((edge as any).target ?? '') === target &&
					(String((edge as any).targetHandle ?? 'in').trim() || 'in') === targetHandleRaw
			);
			if (existingInbound.length >= 1) {
				return {
					ok: false as const,
					error: `Target handle '${targetHandleRaw}' allows only one inbound edge`,
					details: detailsBase
				};
			}
		}
		const targetAffinity = nodePortAffinity(targetNode, 'in', targetHandleRaw);
		if (!edgeModeCompatible(mode, sourceAffinity, targetAffinity)) {
			return {
				ok: false as const,
				error: 'Edge mode is incompatible with source/target port affinities',
				details: {
					...detailsBase,
					targetHandle: targetHandleRaw,
					targetAffinity
				}
			};
		}
		const edgeCandidate = {
			id: '__preflight__',
			source,
			target,
			sourceHandle,
			targetHandle: targetHandleRaw,
			data: { exec: 'idle', mode }
		} as any;
		const schemaCheck = isEdgeStillValid(state.nodes, edgeCandidate);
		if (!schemaCheck.ok) {
			return {
				ok: false as const,
				error:
					schemaCheck.reason === 'mode_mismatch'
						? 'Edge mode is incompatible with source/target port affinities'
						: schemaCheck.reason === 'type_mismatch'
							? `Incompatible schemas${schemaCheck.suggestion ? `. ${schemaCheck.suggestion}` : ''}`
							: schemaCheck.reason === 'typed_schema_missing'
								? `Missing required typed schema coverage: ${(schemaCheck.missingColumns ?? []).join(', ') || '(unknown)'}`
								: schemaCheck.reason === 'schema_mismatch'
									? `Missing required columns: ${(schemaCheck.missingColumns ?? []).join(', ') || '(unknown)'}`
									: 'Cannot resolve schema compatibility for this connection',
				suggestion: schemaCheck.suggestion ?? null,
				adapterKind: schemaCheck.adapterKind ?? null,
				details: {
					...detailsBase,
					targetHandle: targetHandleRaw,
					targetAffinity
				}
			};
		}
		if (schemaCheck.warning === 'lossy_coercion' || schemaCheck.adapterKind || schemaCheck.suggestion) {
			return {
				ok: true as const,
				deferred: false as const,
				warning: schemaCheck.warning ?? null,
				suggestion: schemaCheck.suggestion ?? null,
				adapterKind: schemaCheck.adapterKind ?? null
			};
		}
		const preflightEdgeId = '__preflight__';
		const sameHandleConflict = sameHandleProvidedSchemaConflict(
			state.nodes as any,
			state.edges as any,
			{
				id: preflightEdgeId,
				source,
				target,
				sourceHandle,
				targetHandle: targetHandleRaw,
				data: { exec: 'idle', mode }
			} as any
		);
		if (sameHandleConflict.conflict) {
			return {
				ok: false as const,
				error:
					'Multiple inbound work edges on the same target handle must provide identical schemas',
				details: {
					...detailsBase,
					targetHandle: targetHandleRaw,
					targetAffinity
				}
			};
		}
		return {
			ok: true as const,
			deferred: false as const,
			details: {
				...detailsBase,
				targetHandle: targetHandleRaw,
				targetAffinity
			}
		};
	}

	function addEdge(edge: Edge<PipelineEdgeData>) {
		let out: {
			ok: boolean;
			id?: string;
			error?: string;
			suggestion?: string | null;
			adapterKind?: AdapterTransformKind | null;
		} = { ok: true };
		update((s) => {
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
			const targetNodeForCardinality = s.nodes.find((n) => n.id === edgeForValidation.target);
			const targetHandle = String((edgeForValidation as any).targetHandle ?? 'in').trim() || 'in';
			if (targetNodeForCardinality && portCardinality(targetNodeForCardinality as any, 'in', targetHandle) === 'one') {
				const existingInbound = s.edges.filter(
					(existing) =>
						String((existing as any).target ?? '') === String(edgeForValidation.target ?? '') &&
						(String((existing as any).targetHandle ?? 'in').trim() || 'in') === targetHandle
				);
				if (existingInbound.length >= 1) {
					out = {
						ok: false,
						error: `Target handle '${targetHandle}' allows only one inbound edge`
					};
					return s;
				}
			}

			// Validate schema compatibility and refresh edge contract metadata.
			const chk = isEdgeStillValid(s.nodes, edgeForValidation);
			if (chk.ok === false) {
				out = {
					ok: false,
					suggestion: chk.suggestion,
					adapterKind: chk.adapterKind,
					error:
						chk.reason === 'mode_mismatch'
							? 'Edge mode is incompatible with source/target port affinities'
						: chk.reason === 'type_mismatch'
							? `Incompatible schemas${chk.suggestion ? `. ${chk.suggestion}` : ''}`
							: chk.reason === 'typed_schema_missing'
								? `Missing required typed schema coverage: ${(chk.missingColumns ?? []).join(', ') || '(unknown)'}`
							: chk.reason === 'schema_mismatch'
								? `Missing required columns: ${(chk.missingColumns ?? []).join(', ') || '(unknown)'}`
							: 'Cannot resolve schema compatibility for this connection'
				};
				return logPush(
					s,
					'info',
					`[schema-edge-checks-v2] decision=block edge=${id} reason=${chk.reason}`,
					edge.source
				);
			}
			const sameHandleConflict = sameHandleProvidedSchemaConflict(
				s.nodes as any,
				s.edges as any,
				edgeForValidation
			);
			if (sameHandleConflict.conflict) {
				out = {
					ok: false,
					error:
						'Multiple inbound work edges on the same target handle must provide identical schemas'
				};
				return logPush(
					s,
					'info',
					`[schema-edge-checks-v2] decision=block edge=${id} reason=multi_edge_same_handle_schema_mismatch target=${sameHandleConflict.targetNodeId}:${sameHandleConflict.targetHandle}`,
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
			const constraintProvidedSchema = buildProvidedSchema(
				sourceNode as any,
				String((edgeForValidation as any).sourceHandle ?? 'out')
			);
			const constraintRequiredSchema = buildRequiredSchema(
				targetNode as any,
				String((edgeForValidation as any).targetHandle ?? 'in')
			);
			const providedType = normalizeHintType(sourceHint?.type ?? chk.out ?? 'unknown');
			const requiredType = normalizeHintType(targetHint?.type ?? chk.in ?? 'unknown');
			const coercion = evaluateSchemaCoercion(providedType, requiredType);
			const adapterKind = adapterKindForTypes(providedType, requiredType);
			if (adapterKind) {
				out.adapterKind = adapterKind;
				out.suggestion = adapterSuggestionForTypes(providedType, requiredType);
			}
			const edgeMode = normalizeEdgeMode(edgeForValidation as any);
			const explicitItemModeRaw = String(
				(edge.data as any)?.work?.item_mode ?? (edge.data as any)?.work?.itemMode ?? ''
			)
				.trim()
				.toLowerCase();
			const explicitItemMode =
				explicitItemModeRaw === 'artifact' ||
				explicitItemModeRaw === 'json_items' ||
				explicitItemModeRaw === 'table_rows'
					? (explicitItemModeRaw as 'artifact' | 'json_items' | 'table_rows')
					: null;
			const inferredDefaultItemMode: 'artifact' | 'json_items' | 'table_rows' =
				providedType === 'table'
					? 'table_rows'
					: providedType === 'json'
						? 'json_items'
						: 'artifact';
			const nextItemMode: 'artifact' | 'json_items' | 'table_rows' =
				explicitItemMode ?? (edgeMode === 'work' ? inferredDefaultItemMode : 'artifact');

			const nextEdge: Edge<PipelineEdgeData> = {
				...edgeForValidation,
				id,
				data: {
					...(edge.data ?? {}),
					exec: edge.data?.exec ?? 'idle',
					linkKind: normalizeEdgeLinkKind(edgeForValidation as any),
					mode: edgeMode,
					fatal: Boolean((edge.data as any)?.fatal ?? false),
					queue: {
						max: Math.max(1, Number((edge.data as any)?.queue?.max ?? 1000)),
						overflow: String((edge.data as any)?.queue?.overflow ?? 'block').trim().toLowerCase() as
							| 'block'
							| 'spill'
							| 'error',
						policy: (
							(() => {
								const raw = String((edge.data as any)?.queue?.policy ?? 'fifo').trim().toLowerCase();
								return raw === 'round_robin' ? 'round_robin' : 'fifo';
							})()
						) as 'fifo' | 'round_robin'
					},
					work: {
						item_mode: nextItemMode,
						max_items: Math.max(1, Number((edge.data as any)?.work?.max_items ?? (edge.data as any)?.work?.maxItems ?? 256))
					},
					contract: {
						out: chk.out,
						in: chk.in,
						payload: {
							source: constraintProvidedSchema,
							target: constraintRequiredSchema
						},
						snapshot: edgeContractSnapshotFromSchemas(
							constraintProvidedSchema as Record<string, any>,
							constraintRequiredSchema as Record<string, any>,
							{ ok: true },
							normalizeEdgeMode(edgeForValidation as any)
						)
					}
				}
			};

			const decision = adapterKind ? 'adapter' : coercion.mode === 'native' ? 'native' : 'coerced';
			let nextState: GraphState = { ...s, edges: [...s.edges, nextEdge] };
			nextState = logPush(
				nextState,
				'info',
				`[schema-edge-checks-v2] decision=${decision} edge=${id} source=${providedType} target=${requiredType}`
			);
			const next = logPush(nextState, 'info', `Added edge ${id}`);
			persist(next);
			out.id = id;
			return next;
		});

		return out;
	}

	function insertSchemaAdapterForEdgeConnection(input: {
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

		const state = getState();
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
		const providedType = normalizeHintType(sourceHint?.type ?? 'unknown');
		const requiredType = normalizeHintType(targetHint?.type ?? 'unknown');
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
		const adapterNodeId = addNode('transform', { x: midX, y: midY });
		const subtypeRes = setTransformKind(adapterNodeId, adapterKind);
		if (!subtypeRes.ok) {
			deleteNode(adapterNodeId);
			return {
				ok: false as const,
				error: String(subtypeRes.error ?? 'Failed to configure adapter node')
			};
		}

		const incomingRes = addEdge({
			id: `e_${crypto.randomUUID()}`,
			source,
			target: adapterNodeId,
			sourceHandle,
			targetHandle: 'in',
			data: { exec: 'idle', linkKind: 'data_link', mode: 'work' as any }
		} as Edge<PipelineEdgeData>);
		if (!incomingRes.ok) {
			deleteNode(adapterNodeId);
			return {
				ok: false as const,
				error: String(incomingRes.error ?? 'Failed to connect source to adapter')
			};
		}

		const outgoingRes = addEdge({
			id: `e_${crypto.randomUUID()}`,
			source: adapterNodeId,
			target,
			sourceHandle: 'out',
			targetHandle,
			data: { exec: 'idle', linkKind: 'data_link', mode: 'work' as any }
		} as Edge<PipelineEdgeData>);
		if (!outgoingRes.ok) {
			if (incomingRes.id) deleteEdge(incomingRes.id);
			deleteNode(adapterNodeId);
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
	}

	function updateNodeTitle(nodeId: string, label: string) {
		const state = getState();
		const cleaned = String(label ?? '').trim();
		const normalized = normalizeNodeName(cleaned);
		if (!normalized) {
			return { ok: false as const, error: 'Node name cannot be empty.' };
		}
		const duplicateNodeId = findNodeIdByName(state.nodes as Node<PipelineNodeData>[], cleaned, {
			excludeNodeId: nodeId
		});
		if (duplicateNodeId) {
			return {
				ok: false as const,
				error: `Node name "${cleaned}" already exists in this graph.`,
				reason: 'duplicate_name_in_scope' as const,
				existingNodeId: duplicateNodeId
			};
		}
		update((s) => {
			const nodes = s.nodes.map((n) =>
				n.id === nodeId ? { ...n, data: { ...n.data, label: cleaned } } : n
			);
			const next = { ...s, nodes };
			persist(next);
			return next;
		});
		return { ok: true as const };
	}

	function validateNodeName(name: string, opts?: { excludeNodeId?: string | null }) {
		const state = getState();
		const cleaned = String(name ?? '').trim();
		const normalized = normalizeNodeName(cleaned);
		if (!normalized) {
			return { ok: false as const, error: 'Node name cannot be empty.' };
		}
		const duplicateNodeId = findNodeIdByName(state.nodes as Node<PipelineNodeData>[], cleaned, {
			excludeNodeId: String(opts?.excludeNodeId ?? '').trim() || null
		});
		if (duplicateNodeId) {
			return {
				ok: false as const,
				error: `Node name "${cleaned}" already exists in this graph.`,
				reason: 'duplicate_name_in_scope' as const,
				existingNodeId: duplicateNodeId
			};
		}
		return { ok: true as const, cleanedName: cleaned };
	}

	function createCheckpoint(
		nodeId: string,
		name: string,
		description?: string
	): { ok: true; checkpoint: CheckpointRecord } | { ok: false; error: string } {
		const state = getState();
		const node = state.nodes.find((n) => n.id === nodeId) as
			| Node<PipelineNodeData & Record<string, unknown>>
			| undefined;
		if (!node) return { ok: false, error: 'Node not found.' };

		const checkpointName = String(name ?? '').trim();
		if (!checkpointName) return { ok: false, error: 'Checkpoint name is required.' };

		const normalizedBinding = _normalizeBinding(state.nodeBindings?.[nodeId], nodeId);
		const displayStatus = String(normalizedBinding?.status ?? '').trim().toLowerCase();
		if (!displayStatus.startsWith('succeeded')) {
			return { ok: false, error: 'Checkpoint is only allowed when node status is succeeded.' };
		}
		if (
			!String(normalizedBinding?.current?.artifactId ?? '').trim() ||
			!String(normalizedBinding?.current?.execKey ?? '').trim()
		) {
			return { ok: false, error: 'Checkpoint requires a current bound artifact. Run the node first.' };
		}

		const lineage =
			normalizedBinding && (normalizedBinding.last?.artifactId || normalizedBinding.last?.execKey)
				? normalizedBinding.last
				: normalizedBinding.current;
		const artifactId = String(lineage?.artifactId ?? '').trim();
		const execKey = String(lineage?.execKey ?? '').trim();
		if (!artifactId || !execKey) return { ok: false, error: 'No artifact available.' };

		const memoKey = String(normalizedBinding?.memoState?.memoKey ?? '').trim();
		if (!memoKey || memoKey.length !== 64) {
			return { ok: false, error: 'No fingerprint available. Run the node first.' };
		}

		const outputs: Record<string, { artifactId: string; execKey?: string }> = {};
		const outputLineage = (normalizedBinding?.outputLineage ?? {}) as Record<string, any>;
		for (const [rawHandle, rawPair] of Object.entries(outputLineage)) {
			const handle = String(rawHandle ?? '').trim();
			if (!handle || !rawPair || typeof rawPair !== 'object') continue;
			const outputArtifactId = String((rawPair as any).artifactId ?? '').trim();
			const outputExecKey = String((rawPair as any).execKey ?? '').trim();
			if (!outputArtifactId) continue;
			outputs[handle] = outputExecKey
				? { artifactId: outputArtifactId, execKey: outputExecKey }
				: { artifactId: outputArtifactId };
		}

		const checkpoint: CheckpointRecord = {
			id:
				typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function'
					? crypto.randomUUID()
					: `ck_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 10)}`,
			name: checkpointName,
			...(String(description ?? '').trim() ? { description: String(description).trim() } : {}),
			nodeId,
			graphId: state.graphId,
			runId: String(state.lastRunId ?? '').trim(),
			artifactId,
			execKey,
			fingerprintAtCreation: memoKey,
			createdAt: new Date().toISOString(),
			staleness: 'valid',
			...(Object.keys(outputs).length > 0 ? { outputs } : {})
		};

		update((s) => {
			const nodeBindings = {
				...s.nodeBindings,
				[nodeId]: {
					..._normalizeBinding(s.nodeBindings?.[nodeId], nodeId),
					checkpointable: false
				}
			};
			const next = withGraphMeta({
				...s,
				nodeBindings,
				checkpointRegistry: {
					...(s.checkpointRegistry ?? {}),
					[nodeId]: checkpoint
				}
			});
			persist(next);
			return next;
		});

		return { ok: true, checkpoint };
	}

	syncComponentDraftCommittedCheckpoints();

	function removeCheckpoint(nodeId: string): { ok: true } {
		update((s) => {
			const checkpointRegistry = { ...(s.checkpointRegistry ?? {}) };
			delete checkpointRegistry[nodeId];
			const next = withGraphMeta({
				...s,
				checkpointRegistry
			});
			persist(next);
			return next;
		});
		syncComponentDraftCommittedCheckpoints();
		return { ok: true };
	}

	function renameCheckpoint(nodeId: string, name: string): { ok: boolean; error?: string } {
		const nextName = String(name ?? '').trim();
		if (!nextName) return { ok: false, error: 'Checkpoint name is required.' };
		let exists = false;
		update((s) => {
			const current = (s.checkpointRegistry ?? {})[nodeId];
			if (!current) return s;
			exists = true;
			const next = withGraphMeta({
				...s,
				checkpointRegistry: {
					...(s.checkpointRegistry ?? {}),
					[nodeId]: {
						...current,
						name: nextName
					}
				}
			});
			persist(next);
			return next;
		});
		return exists ? { ok: true } : { ok: false, error: 'Checkpoint not found.' };
	}

	function removeAllStaleCheckpoints(): { ok: true; removed: number } {
		let removed = 0;
		update((s) => {
			const checkpointRegistry = { ...(s.checkpointRegistry ?? {}) };
			for (const [nodeId, checkpoint] of Object.entries(checkpointRegistry)) {
				const staleness = String((checkpoint as any)?.staleness ?? '').trim().toLowerCase();
				if (staleness === 'stale' || staleness === 'artifact_missing') {
					delete checkpointRegistry[nodeId];
					removed += 1;
				}
			}
			if (removed <= 0) return s;
			const next = withGraphMeta({ ...s, checkpointRegistry });
			persist(next);
			return next;
		});
		return { ok: true, removed };
	}

	function clearAllCheckpoints(): { ok: true; removed: number } {
		let removed = 0;
		update((s) => {
			removed = Object.keys(s.checkpointRegistry ?? {}).length;
			if (removed <= 0) return s;
			const next = withGraphMeta({ ...s, checkpointRegistry: {} });
			persist(next);
			return next;
		});
		return { ok: true, removed };
	}

	function updateNodeProcessingPolicy(
		nodeId: string,
		patch: {
			consume_mode?: 'once' | 'single_item' | 'batch';
			batch_size?: number;
			max_inflight?: number;
			read_once?: boolean;
			on_error?: 'fail_fast' | 'skip_failed';
		}
	) {
		let out: { ok: boolean; error?: string } = { ok: true };
		update((s) => {
			const node = s.nodes.find((n) => n.id === nodeId);
			if (!node) {
				out = { ok: false, error: 'Node not found' };
				return s;
			}
			const existing = ((node.data as any)?.processingPolicy ?? {}) as Record<string, any>;
			const nextMode = String(patch.consume_mode ?? existing.consume_mode ?? 'once').trim().toLowerCase();
			if (!['once', 'single_item', 'batch'].includes(nextMode)) {
				out = { ok: false, error: 'Invalid consume mode' };
				return s;
			}
			const nextPolicy = {
				...(existing as Record<string, any>),
				consume_mode: nextMode as 'once' | 'single_item' | 'batch',
				batch_size: Math.max(1, Number(patch.batch_size ?? existing.batch_size ?? 1)),
				max_inflight: Math.max(1, Number(patch.max_inflight ?? existing.max_inflight ?? 1)),
				read_once: nextMode === 'once',
				...(patch.on_error ? { on_error: patch.on_error } : {})
			};
			const nodes = s.nodes.map((n) =>
				n.id === nodeId
					? {
							...n,
							data: {
								...n.data,
								processingPolicy: nextPolicy
							}
						}
					: n
			);
			const next = logPush({ ...s, nodes }, 'info', `Updated node ${nodeId} processing policy`);
			persist(next);
			return next;
		});
		return out;
	}

	function updateNodeInputHandleProcessingPolicy(
		nodeId: string,
		inputHandle: string,
		patch: {
			consume_mode?: 'once' | 'single_item' | 'batch';
			batch_size?: number;
			max_inflight?: number;
			read_once?: boolean;
		}
	) {
		let out: { ok: boolean; error?: string } = { ok: true };
		update((s) => {
			const node = s.nodes.find((n) => n.id === nodeId);
			if (!node) {
				out = { ok: false, error: 'Node not found' };
				return s;
			}
			const handle = String(inputHandle ?? '').trim() || 'in';
			const existing = ((node.data as any)?.processingPolicy ?? {}) as Record<string, any>;
			const existingByHandle =
				existing.input_handles && typeof existing.input_handles === 'object'
					? (existing.input_handles as Record<string, any>)
					: {};
			const existingHandle = (existingByHandle[handle] ?? {}) as Record<string, any>;
			const nextMode = String(patch.consume_mode ?? existingHandle.consume_mode ?? 'once').trim().toLowerCase();
			if (!['once', 'single_item', 'batch'].includes(nextMode)) {
				out = { ok: false, error: 'Invalid consume mode' };
				return s;
			}
			const nextHandlePolicy = {
				consume_mode: nextMode as 'once' | 'single_item' | 'batch',
				batch_size: Math.max(1, Number(patch.batch_size ?? existingHandle.batch_size ?? existing.batch_size ?? 1)),
				max_inflight: Math.max(
					1,
					Number(patch.max_inflight ?? existingHandle.max_inflight ?? existing.max_inflight ?? 1)
				),
				read_once: nextMode === 'once'
			};
			const nextPolicy = {
				...(existing as Record<string, any>),
				input_handles: {
					...existingByHandle,
					[handle]: nextHandlePolicy
				}
			};
			const nodes = s.nodes.map((n) =>
				n.id === nodeId
					? {
							...n,
							data: {
								...n.data,
								processingPolicy: nextPolicy
							}
						}
					: n
			);
			const next = logPush(
				{ ...s, nodes },
				'info',
				`Updated node ${nodeId} processing policy for input handle ${handle}`
			);
			persist(next);
			return next;
		});
		return out;
	}

	function updateNodePortDeclaration(
		nodeId: string,
		direction: 'in' | 'out',
		handle: string,
		patch: {
			plane?: 'work' | 'param' | 'control';
			required?: boolean;
			cardinality?: 'one' | 'many';
			behavior?: 'once' | 'single_item' | 'batch';
		}
	) {
		let out: { ok: boolean; error?: string } = { ok: true };
		update((s) => {
			const node = s.nodes.find((n) => n.id === nodeId);
			if (!node) {
				out = { ok: false, error: 'Node not found' };
				return s;
			}
			const dir = direction === 'out' ? 'out' : 'in';
			const key = String(handle ?? '').trim();
			if (!key) {
				out = { ok: false, error: 'Handle is required' };
				return s;
			}
			const data = (node.data ?? {}) as Record<string, any>;
			const existingDecls =
				data.portDeclarations && typeof data.portDeclarations === 'object'
					? (data.portDeclarations as Record<string, any>)
					: {};
			const byDir =
				existingDecls[dir] && typeof existingDecls[dir] === 'object'
					? (existingDecls[dir] as Record<string, any>)
					: {};
			const existing = (byDir[key] ?? {}) as Record<string, any>;
			const nextPlane = String(patch.plane ?? existing.plane ?? existing.affinity ?? 'work')
				.trim()
				.toLowerCase();
			if (!['work', 'param', 'control'].includes(nextPlane)) {
				out = { ok: false, error: 'Invalid plane' };
				return s;
			}
			const nextCardinality = String(patch.cardinality ?? existing.cardinality ?? 'many')
				.trim()
				.toLowerCase();
			if (!['one', 'many'].includes(nextCardinality)) {
				out = { ok: false, error: 'Invalid cardinality' };
				return s;
			}
			const nextDecl: Record<string, any> = {
				plane: nextPlane,
				affinity: nextPlane,
				required: Boolean(patch.required ?? existing.required ?? false),
				cardinality: nextCardinality
			};
			if (dir === 'in') {
				const nextBehavior = String(patch.behavior ?? existing.behavior ?? 'single_item')
					.trim()
					.toLowerCase();
				if (!['once', 'single_item', 'batch'].includes(nextBehavior)) {
					out = { ok: false, error: 'Invalid behavior' };
					return s;
				}
				nextDecl.behavior = nextBehavior;
			}
			const nextPortDeclarations: Record<string, any> = {
				...existingDecls,
				[dir]: {
					...byDir,
					[key]: nextDecl
				}
			};
			const nextPortContractsByDir: Record<string, any> = {
				...((data.portContracts && typeof data.portContracts === 'object'
					? data.portContracts
					: {}) as Record<string, any>),
				[dir]: {
					...(((data.portContracts as any)?.[dir] ?? {}) as Record<string, any>),
					[key]: {
						affinity: nextPlane,
						...(dir === 'in' ? { behavior: String(nextDecl.behavior ?? 'single_item') } : {})
					}
				}
			};
			const nodes = s.nodes.map((n) =>
				n.id === nodeId
					? {
							...n,
							data: {
								...n.data,
								portDeclarations: nextPortDeclarations,
								portContracts: nextPortContractsByDir
							}
						}
					: n
			);
			const next = logPush(
				{ ...s, nodes },
				'info',
				`Updated node ${nodeId} ${dir} port declaration ${key}`
			);
			persist(next);
			return next;
		});
		return out;
	}

	function removeNodePortDeclaration(nodeId: string, direction: 'in' | 'out', handle: string) {
		let out: { ok: boolean; error?: string } = { ok: true };
		update((s) => {
			const node = s.nodes.find((n) => n.id === nodeId);
			if (!node) {
				out = { ok: false, error: 'Node not found' };
				return s;
			}
			const dir = direction === 'out' ? 'out' : 'in';
			const key = String(handle ?? '').trim();
			if (!key) {
				out = { ok: false, error: 'Handle is required' };
				return s;
			}
			const data = (node.data ?? {}) as Record<string, any>;
			const existingDecls =
				data.portDeclarations && typeof data.portDeclarations === 'object'
					? (data.portDeclarations as Record<string, any>)
					: {};
			const byDir =
				existingDecls[dir] && typeof existingDecls[dir] === 'object'
					? ({ ...(existingDecls[dir] as Record<string, any>) } as Record<string, any>)
					: {};
			if (!Object.prototype.hasOwnProperty.call(byDir, key)) {
				return s;
			}
			const previousHandles = declaredPortHandles(node as Node<PipelineNodeData>, dir);
			const previousIndex = previousHandles.indexOf(key);
			delete byDir[key];
			const nextPortDeclarations = {
				...existingDecls,
				[dir]: byDir
			};
			const existingContracts =
				data.portContracts && typeof data.portContracts === 'object'
					? ({ ...(data.portContracts as Record<string, any>) } as Record<string, any>)
					: {};
			const nextContractsByDir =
				existingContracts[dir] && typeof existingContracts[dir] === 'object'
					? ({ ...(existingContracts[dir] as Record<string, any>) } as Record<string, any>)
					: {};
			delete nextContractsByDir[key];
			existingContracts[dir] = nextContractsByDir;
			const nodes = s.nodes.map((n) =>
				n.id === nodeId
					? {
							...n,
							data: {
								...n.data,
								portDeclarations: nextPortDeclarations,
								portContracts: existingContracts
							}
						}
					: n
			);
			const updatedNode = nodes.find((n) => n.id === nodeId) as Node<PipelineNodeData> | undefined;
			const remainingHandles = updatedNode ? declaredPortHandles(updatedNode, dir) : [];
			const nextHandleForRemoved = (() => {
				if (remainingHandles.length === 0) return null;
				if (previousIndex < 0) return remainingHandles[0];
				const idx = Math.max(0, Math.min(previousIndex, remainingHandles.length - 1));
				return remainingHandles[idx];
			})();
			let droppedEdges = 0;
			const edges = s.edges.flatMap((edge) => {
				const touchesNode =
					dir === 'in' ? String(edge.target ?? '') === nodeId : String(edge.source ?? '') === nodeId;
				if (!touchesNode) return [edge];
				const edgeHandle =
					dir === 'in'
						? String((edge as any).targetHandle ?? 'in').trim() || 'in'
						: String((edge as any).sourceHandle ?? 'out').trim() || 'out';
				if (edgeHandle !== key) {
					// Re-clone to force edge anchor refresh when handle layout changes.
					return [{ ...edge }];
				}
				if (!nextHandleForRemoved) {
					droppedEdges += 1;
					return [];
				}
				if (dir === 'in') {
					return [{ ...edge, targetHandle: nextHandleForRemoved }];
				}
				return [{ ...edge, sourceHandle: nextHandleForRemoved }];
			});
			const next = logPush(
				{ ...s, nodes, edges },
				'info',
				`Removed node ${nodeId} ${dir} port declaration ${key}${droppedEdges > 0 ? ` (dropped ${droppedEdges} edge${droppedEdges === 1 ? '' : 's'})` : ''}`
			);
			persist(next);
			return next;
		});
		return out;
	}

	function setNodeMeta(nodeId: string, patch: Record<string, unknown>) {
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
	}

	function hardResetGraph() {
		const freshGraphId = mintGraphId();
		const next = buildHardResetState(freshGraphId);
		persist(next);
		set(next);
		history.resetToSnapshot(stripToDTO(next.nodes as any, next.edges as any, next.graphId, next.checkpointRegistry ?? {}));
	}

	function loadGraphDocument(
		graph: { nodes: unknown[]; edges: unknown[]; checkpointRegistry?: CheckpointRegistry },
		graphIdOverride?: string | null
	) {
		const applied = applyGraphDocument(graph, graphIdOverride);
		if (!applied.ok) return { ok: false, reason: 'invalid_payload' as const };
		return { ok: true };
	}

	return {
		actions: {
			setSourceKind,
			setLlmKind,
			setTransformKind,
			setToolProvider,
			setToolKind,
			syncFromCanvas,
			addNode,
			deleteNode,
			deleteEdge,
			addEdge,
			updateEdgeConfig,
			preflightConnection,
			insertSchemaAdapterForEdgeConnection,
			updateNodeTitle,
			validateNodeName,
			createCheckpoint,
			removeCheckpoint,
			renameCheckpoint,
			removeAllStaleCheckpoints,
			clearAllCheckpoints,
			updateNodeProcessingPolicy,
			updateNodeInputHandleProcessingPolicy,
			updateNodePortDeclaration,
			removeNodePortDeclaration,
			setNodeMeta,
			hardResetGraph,
			loadGraphDocument,
			applyGraphDocument
		}
	};
}
