import type { GraphState } from './graphStore.types';
import type { SchemaError, SchemaPlaneOutput, SchemaPlaneResult, SchemaPlaneState } from '$lib/flow/types/schemaPlane';
import { propagateSchemas } from '$lib/flow/schema/schemaPropagator';

export function emptySchemaPlaneState(): SchemaPlaneState {
	return {
		nodeSchemas: {},
		edgeSchemas: {}
	};
}

export function recomputeSchemaPlane(state: GraphState): SchemaPlaneState {
	return propagateSchemas(state.nodes as any, state.edges as any, {
		resolveComponentGraph: (componentNodeId: string) => {
			const node = (state.nodes ?? []).find((candidate) => String(candidate?.id ?? '') === componentNodeId);
			if (!node) return null;
			const params = ((node.data as any)?.params ?? {}) as Record<string, unknown>;
			const directDraft = (params?.__graphDraft ?? null) as Record<string, unknown> | null;
			if (directDraft && Array.isArray((directDraft as any).nodes) && Array.isArray((directDraft as any).edges)) {
				return {
					nodes: ((directDraft as any).nodes ?? []) as any[],
					edges: ((directDraft as any).edges ?? []) as any[]
				};
			}
			const ref = ((params?.componentRef ?? {}) as Record<string, unknown>) ?? {};
			const componentId = String(ref?.componentId ?? '').trim();
			const revisionId = String(ref?.revisionId ?? '').trim();
			if (!componentId || !revisionId) return null;
			const cacheKey = `${componentId}@${revisionId}`;
			const cacheEntry = ((state.componentContractDraftCache ?? {}) as Record<string, unknown>)[cacheKey];
			if (!cacheEntry || typeof cacheEntry !== 'object') return null;
			const draftGraph = (cacheEntry as Record<string, unknown>)['__graphDraft'];
			if (!draftGraph || typeof draftGraph !== 'object') return null;
			const nodes = Array.isArray((draftGraph as any).nodes) ? ((draftGraph as any).nodes as any[]) : null;
			const edges = Array.isArray((draftGraph as any).edges) ? ((draftGraph as any).edges as any[]) : null;
			if (!nodes || !edges) return null;
			return { nodes, edges };
		}
	});
}

type Deps = {
	getState: () => GraphState;
};

export function createSchemaPlaneManager(deps: Deps) {
	const resolveOpaqueUpstreamPolicy = (state: GraphState): 'warn' | 'none' => {
		const statePolicy = String((state as any)?.schemaOpaqueUpstreamPolicy ?? '')
			.trim()
			.toLowerCase();
		if (statePolicy === 'none' || statePolicy === 'off' || statePolicy === 'ignore') return 'none';
		if (statePolicy === 'warn' || statePolicy === 'warning') return 'warn';
		const envPolicy = String(import.meta.env.VITE_SCHEMA_OPAQUE_UPSTREAM_POLICY ?? 'warn')
			.trim()
			.toLowerCase();
		if (envPolicy === 'none' || envPolicy === 'off' || envPolicy === 'ignore') return 'none';
		return 'warn';
	};

	const getNodeSchemaResult = (nodeId: string): SchemaPlaneResult | null => {
		const state = deps.getState();
		return state.schemaPlane?.nodeSchemas?.[nodeId] ?? null;
	};

	const getEdgeSchema = (edgeId: string): SchemaPlaneOutput | null => {
		const state = deps.getState();
		return state.schemaPlane?.edgeSchemas?.[edgeId] ?? null;
	};

	const getSchemaErrors = (): Array<{ nodeId: string; error: SchemaError }> => {
		const state = deps.getState();
		const out: Array<{ nodeId: string; error: SchemaError }> = [];
		for (const [nodeId, result] of Object.entries(state.schemaPlane?.nodeSchemas ?? {})) {
			if (result && result.ok === false) out.push({ nodeId, error: result.error });
		}
		return out;
	};

	const hasSchemaErrors = (): boolean => getSchemaErrors().length > 0;

	const getEdgeValidationState = (
		edgeId: string
	): { state: 'valid' | 'error' | 'warning' | 'neutral'; message?: string; code?: string } => {
		const state = deps.getState();
		const opaquePolicy = resolveOpaqueUpstreamPolicy(state);
		const edge = (state.edges ?? []).find((candidate) => String(candidate?.id ?? '') === edgeId);
		if (!edge) return { state: 'neutral' };
		const edgeSchema = state.schemaPlane?.edgeSchemas?.[edgeId];
		const sourceResult = state.schemaPlane?.nodeSchemas?.[String(edge.source ?? '')];
		if (sourceResult && sourceResult.ok === false) {
			return {
				state: 'error',
				message: sourceResult.error.message,
				code: sourceResult.error.code
			};
		}
		const targetResult = state.schemaPlane?.nodeSchemas?.[String(edge.target ?? '')];
		if (targetResult && targetResult.ok === false) {
			const targetHandle = String(edge.targetHandle ?? 'in').trim();
			const handles = Array.isArray(targetResult.error.handles) ? targetResult.error.handles : [];
			if (handles.length === 0 || handles.includes(targetHandle)) {
				const targetErrorCode = String(targetResult.error.code ?? '').trim();
				const upstreamAllowsAdditionalProperties =
					Boolean((edgeSchema as any)?.properties?.additional_properties) === true;
				// If upstream is opaque, a missing-column mismatch is uncertain rather than definitive.
				// Keep it as a warning/info-grade diagnostic at edge level.
				if (targetErrorCode === 'SHAPE_MISMATCH' && (edgeSchema?.mode === 'opaque' || upstreamAllowsAdditionalProperties)) {
					if (opaquePolicy === 'none') return { state: 'valid' };
					if (upstreamAllowsAdditionalProperties) {
						return {
							state: 'warning',
							message: `${String(targetResult.error.message ?? 'Required field is missing in input schema')}. Upstream output allows additional properties, so this remains a warning until schema is made explicit.`,
							code: 'SHAPE_MISMATCH_ADDITIONAL_PROPERTIES'
						};
					}
					return {
						state: 'warning',
						message: `${String(targetResult.error.message ?? 'Required field is missing in input schema')}. Upstream is opaque, so this remains a warning until schema is made explicit.`,
						code: 'SHAPE_MISMATCH_OPAQUE'
					};
				}
				return {
					state: 'error',
					message: targetResult.error.message,
					code: targetResult.error.code
				};
			}
		}
		if (!edgeSchema) return { state: 'neutral' };
		if (edgeSchema.mode === 'opaque') {
			if (opaquePolicy === 'none') return { state: 'valid' };
			return {
				state: 'warning',
				message: 'Schema unverified: upstream output is opaque. Run source to infer.',
				code: 'OPAQUE_DEPENDENCY'
			};
		}
		return { state: 'valid' };
	};

	const getConfigurationHints = (nodeId: string): Record<string, unknown> => {
		const state = deps.getState();
		const node = (state.nodes ?? []).find((candidate) => String(candidate?.id ?? '') === nodeId);
		if (!node) return {};
		const incomingEdges = (state.edges ?? [])
			.filter((edge) => String(edge.target ?? '') === nodeId)
			.sort((a, b) => String(a.targetHandle ?? '').localeCompare(String(b.targetHandle ?? '')));
		const incomingSchemas = incomingEdges
			.map((edge) => state.schemaPlane?.edgeSchemas?.[String(edge.id ?? '')] ?? null)
			.filter((schema): schema is SchemaPlaneOutput => Boolean(schema));
		const firstTable = incomingSchemas.find((schema) => schema.mode === 'table');
		const firstTensor = incomingSchemas.find((schema) => schema.mode === 'tensor');
		const first = incomingSchemas[0] ?? null;
		const columns = (firstTable?.columns ?? []).map((column) => String(column.name ?? '')).filter(Boolean);
		const suggestions: Record<string, unknown> = {};
		const kind = String((node.data as any)?.kind ?? '').trim().toLowerCase();
		const transformKind = String((node.data as any)?.transformKind ?? '').trim().toLowerCase();
		if (transformKind === 'spectrogram') {
			const sampleRate = Number(first?.properties?.sample_rate ?? NaN);
			if (Number.isFinite(sampleRate)) suggestions['params.sample_rate'] = sampleRate;
		}
		if (transformKind === 'join' && incomingSchemas.length >= 2) {
			const schemaBySourceNodeId = new Map<string, SchemaPlaneOutput>();
			for (const edge of incomingEdges) {
				const edgeId = String(edge.id ?? '');
				if (!edgeId) continue;
				const schema = state.schemaPlane?.edgeSchemas?.[edgeId];
				if (!schema) continue;
				const sourceNodeId = String(edge.source ?? '').trim();
				if (!sourceNodeId || schemaBySourceNodeId.has(sourceNodeId)) continue;
				schemaBySourceNodeId.set(sourceNodeId, schema);
			}
			const joinParams =
				(node.data as any)?.params?.join && typeof (node.data as any)?.params?.join === 'object'
					? ((node.data as any).params.join as Record<string, unknown>)
					: {};
			const clauses = Array.isArray(joinParams?.clauses) ? (joinParams.clauses as Array<Record<string, unknown>>) : [];
			const availableKeys = new Set<string>();
			for (const clause of clauses) {
				const leftNodeId = String(clause?.leftNodeId ?? '').trim();
				const rightNodeId = String(clause?.rightNodeId ?? '').trim();
				const leftSchema = leftNodeId ? schemaBySourceNodeId.get(leftNodeId) : null;
				const rightSchema = rightNodeId ? schemaBySourceNodeId.get(rightNodeId) : null;
				if (!leftSchema || !rightSchema) continue;
				const left = new Set((leftSchema.columns ?? []).map((column) => String(column.name ?? '')));
				const right = new Set((rightSchema.columns ?? []).map((column) => String(column.name ?? '')));
				for (const key of [...left].filter((name) => right.has(name))) {
					if (!key) continue;
					availableKeys.add(key);
				}
			}
			if (availableKeys.size === 0) {
				const left = new Set((incomingSchemas[0].columns ?? []).map((column) => String(column.name ?? '')));
				const right = new Set((incomingSchemas[1].columns ?? []).map((column) => String(column.name ?? '')));
				for (const key of [...left].filter((name) => right.has(name))) {
					if (!key) continue;
					availableKeys.add(key);
				}
			}
			suggestions['join.availableKeys'] = Array.from(availableKeys).sort((a, b) => a.localeCompare(b));
		}
		if (transformKind === 'aggregate') {
			const numeric = (firstTable?.columns ?? [])
				.filter((column) => column.type === 'number')
				.map((column) => String(column.name ?? ''))
				.filter(Boolean);
			suggestions['aggregate.numericColumns'] = numeric;
		}
		if (kind === 'training_job' || transformKind === 'training_job') {
			const shape = firstTensor?.shape ?? [];
			const lastDim = shape.length > 0 ? shape[shape.length - 1] : null;
			if (typeof lastDim === 'number') suggestions['params.architecture_config.input_dim'] = lastDim;
			const classSet = first?.properties?.class_set;
			if (Array.isArray(classSet) && classSet.length > 0) suggestions['params.num_classes'] = classSet.length;
		}
		return {
			suggestions,
			availableColumns: columns,
			upstreamShape: firstTensor?.shape ?? first?.shape ?? null,
			upstreamDtype: firstTensor?.dtype ?? first?.dtype ?? first?.properties?.dtype ?? null,
			upstreamClassSet: first?.properties?.class_set ?? null,
			sampleRate: first?.properties?.sample_rate ?? null,
			cardinality: first?.properties?.cardinality ?? null
		};
	};

	return {
		getNodeSchemaResult,
		getEdgeSchema,
		getEdgeValidationState,
		getSchemaErrors,
		hasSchemaErrors,
		getConfigurationHints
	};
}
