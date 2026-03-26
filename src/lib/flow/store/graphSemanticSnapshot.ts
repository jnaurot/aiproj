type SemanticNodeInput = {
	id?: string;
	type?: string;
	data?: Record<string, unknown>;
};

type SemanticEdgeInput = {
	id?: string;
	source?: string;
	target?: string;
	sourceHandle?: string | null;
	targetHandle?: string | null;
};

export type CanonicalGraphSemanticSnapshot = {
	graphId: string;
	nodes: Array<{
		id: string;
		type: string;
		data: {
			kind?: string;
			label?: string;
			sourceKind?: string;
			transformKind?: string;
			llmKind?: string;
			modelKind?: string;
			taskKind?: string;
			componentKind?: string;
			params?: unknown;
		};
	}>;
	edges: Array<{
		id: string;
		source: string;
		target: string;
		sourceHandle: string | null;
		targetHandle: string | null;
	}>;
};

function stableCanonicalValue(value: unknown): unknown {
	if (Array.isArray(value)) return value.map((v) => stableCanonicalValue(v));
	if (value && typeof value === 'object') {
		const obj = value as Record<string, unknown>;
		const out: Record<string, unknown> = {};
		for (const key of Object.keys(obj).sort()) {
			out[key] = stableCanonicalValue(obj[key]);
		}
		return out;
	}
	return value;
}

export function canonicalGraphSemanticSnapshot(
	graphId: string | null | undefined,
	nodeList: SemanticNodeInput[],
	edgeList: SemanticEdgeInput[]
): CanonicalGraphSemanticSnapshot {
	const nodesCanonical = [...(nodeList ?? [])]
		.map((node) => {
			const data = (node?.data ?? {}) as Record<string, unknown>;
			return {
				id: String(node?.id ?? ''),
				type: String(node?.type ?? ''),
				data: {
					kind: typeof data.kind === 'string' ? data.kind : undefined,
					label: typeof data.label === 'string' ? data.label : undefined,
					sourceKind: typeof data.sourceKind === 'string' ? data.sourceKind : undefined,
					transformKind: typeof data.transformKind === 'string' ? data.transformKind : undefined,
					llmKind: typeof data.llmKind === 'string' ? data.llmKind : undefined,
					modelKind: typeof data.modelKind === 'string' ? data.modelKind : undefined,
					taskKind: typeof data.taskKind === 'string' ? data.taskKind : undefined,
					componentKind: typeof data.componentKind === 'string' ? data.componentKind : undefined,
					params: stableCanonicalValue(data.params ?? {})
				}
			};
		})
		.sort((a, b) => a.id.localeCompare(b.id));

	const edgesCanonical = [...(edgeList ?? [])]
		.map((edge) => ({
			id: String(edge?.id ?? ''),
			source: String(edge?.source ?? ''),
			target: String(edge?.target ?? ''),
			sourceHandle: edge?.sourceHandle ? String(edge.sourceHandle) : null,
			targetHandle: edge?.targetHandle ? String(edge.targetHandle) : null
		}))
		.sort((a, b) => a.id.localeCompare(b.id));

	return {
		graphId: String(graphId ?? ''),
		nodes: nodesCanonical,
		edges: edgesCanonical
	};
}

export function graphSemanticSnapshotKey(
	graphId: string | null | undefined,
	nodeList: SemanticNodeInput[],
	edgeList: SemanticEdgeInput[]
): string {
	return JSON.stringify(canonicalGraphSemanticSnapshot(graphId, nodeList, edgeList));
}

export function isGraphSemanticDirty(
	savedSnapshotKey: string | null | undefined,
	currentSnapshotKey: string | null | undefined
): boolean {
	if (typeof savedSnapshotKey !== 'string') return false;
	if (typeof currentSnapshotKey !== 'string') return false;
	return savedSnapshotKey !== currentSnapshotKey;
}
