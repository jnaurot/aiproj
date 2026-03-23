import type { NodeSchemaContractEdge } from '$lib/flow/store/graphStore';

type MinimalNode = {
	id: string;
	data?: {
		label?: unknown;
	};
};

export function schemaEdgeCounterpartyName(
	edge: NodeSchemaContractEdge,
	selectedNodeId: string,
	nodes: MinimalNode[]
): string {
	const selectedId = String(selectedNodeId ?? '').trim();
	const sourceId = String(edge?.sourceNodeId ?? '').trim();
	const targetId = String(edge?.targetNodeId ?? '').trim();
	const direction = String(edge?.direction ?? '').trim().toLowerCase();

	let otherId = '';
	if (selectedId.length > 0) {
		if (selectedId === sourceId) otherId = targetId;
		else if (selectedId === targetId) otherId = sourceId;
	}
	if (!otherId) {
		otherId = direction === 'incoming' ? sourceId : targetId;
	}
	if (!otherId) return '(unknown node)';

	const match = nodes.find((node) => String(node?.id ?? '').trim() === otherId);
	const rawLabel = String(match?.data?.label ?? '').trim();
	return rawLabel.length > 0 ? rawLabel : otherId;
}

export type SchemaModeGroup = {
	mode: 'work' | 'param' | 'control';
	label: 'Work' | 'Param' | 'Control';
	edges: NodeSchemaContractEdge[];
};

export function schemaEdgeDriftGuidance(edge: NodeSchemaContractEdge): string | null {
	if (!edge?.snapshotDrift) return null;
	const snapSource = String(edge.snapshotSourceSchemaFingerprint ?? '').slice(0, 12);
	const snapTarget = String(edge.snapshotTargetSchemaFingerprint ?? '').slice(0, 12);
	const curSource = String(edge.currentSourceSchemaFingerprint ?? '').slice(0, 12);
	const curTarget = String(edge.currentTargetSchemaFingerprint ?? '').slice(0, 12);
	return `Contract drift detected: snapshot (${snapSource} / ${snapTarget}) != current (${curSource} / ${curTarget}).`;
}

export function schemaEdgeContractBadges(edge: NodeSchemaContractEdge): string[] {
	const badges: string[] = [];
	if (edge?.snapshotDrift) badges.push('drift');
	const suggestions = Array.isArray(edge?.suggestions) ? edge.suggestions.join(' ').toLowerCase() : '';
	if (suggestions.includes('coercion') || edge?.severity === 'warning') badges.push('coercion');
	if (edge?.adapterKind) badges.push(`adapter:${String(edge.adapterKind)}`);
	return badges;
}

export type ExpectedInputHandleSummary = {
	handle: string;
	affinity: 'work' | 'param' | 'control';
	classDefaultType: 'text' | 'json' | 'none';
};

function inferAffinityFromHandle(handle: string): 'work' | 'param' | 'control' {
	const normalized = String(handle ?? '').trim().toLowerCase();
	if (normalized.startsWith('param')) return 'param';
	if (normalized.startsWith('control') || normalized.startsWith('ctl')) return 'control';
	return 'work';
}

function readHandleAffinity(node: MinimalNode | undefined, handle: string): 'work' | 'param' | 'control' {
	const inferred = inferAffinityFromHandle(handle);
	if (inferred !== 'work') return inferred;
	const portDeclarations = (node?.data as any)?.portDeclarations;
	const declarationInPorts =
		portDeclarations && typeof portDeclarations === 'object' ? portDeclarations.in : undefined;
	const portContracts = (node?.data as any)?.portContracts;
	const inPorts =
		declarationInPorts && typeof declarationInPorts === 'object'
			? declarationInPorts
			: portContracts && typeof portContracts === 'object'
				? portContracts.in
				: undefined;
	if (!inPorts || typeof inPorts !== 'object') return 'work';
	const key = String(handle ?? '').trim() || 'in';
	const exact = (inPorts as Record<string, unknown>)[key] as Record<string, unknown> | undefined;
	const fallback = (inPorts as Record<string, unknown>).default as Record<string, unknown> | undefined;
	const affinity = String((exact ?? fallback ?? {}).plane ?? (exact ?? fallback ?? {}).affinity ?? '')
		.trim()
		.toLowerCase();
	if (affinity === 'work' || affinity === 'param' || affinity === 'control') {
		return affinity;
	}
	return 'work';
}

function classDefaultTypeForAffinity(
	affinity: 'work' | 'param' | 'control'
): 'text' | 'json' | 'none' {
	if (affinity === 'param') return 'json';
	if (affinity === 'control') return 'none';
	return 'text';
}

export function collectExpectedInputHandles(
	node: MinimalNode | undefined,
	edges: NodeSchemaContractEdge[]
): ExpectedInputHandleSummary[] {
	const handles = new Set<string>();
	handles.add('in');

	const expectedInputSchemas = (node?.data as any)?.schema?.expectedInputSchemas;
	if (expectedInputSchemas && typeof expectedInputSchemas === 'object') {
		for (const rawHandle of Object.keys(expectedInputSchemas as Record<string, unknown>)) {
			const handle = String(rawHandle ?? '').trim();
			if (handle) handles.add(handle);
		}
	}

	const inPorts = (node?.data as any)?.portDeclarations?.in ?? (node?.data as any)?.portContracts?.in;
	if (inPorts && typeof inPorts === 'object') {
		for (const rawHandle of Object.keys(inPorts as Record<string, unknown>)) {
			const handle = String(rawHandle ?? '').trim();
			if (!handle || handle === 'default') continue;
			handles.add(handle);
		}
	}

	for (const edge of edges ?? []) {
		if (String(edge?.direction ?? '').trim().toLowerCase() !== 'incoming') continue;
		const handle = String(edge?.targetHandle ?? 'in').trim() || 'in';
		handles.add(handle);
	}

	const ordered = Array.from(handles).sort((a, b) => {
		if (a === 'in') return -1;
		if (b === 'in') return 1;
		return a.localeCompare(b);
	});

	return ordered.map((handle) => {
		const affinity = readHandleAffinity(node, handle);
		return {
			handle,
			affinity,
			classDefaultType: classDefaultTypeForAffinity(affinity)
		};
	});
}

export function groupSchemaEdgesByMode(edges: NodeSchemaContractEdge[]): SchemaModeGroup[] {
	const groups: SchemaModeGroup[] = [
		{ mode: 'work', label: 'Work', edges: [] },
		{ mode: 'param', label: 'Param', edges: [] },
		{ mode: 'control', label: 'Control', edges: [] }
	];
	for (const edge of edges ?? []) {
		const modeRaw = String(edge?.mode ?? 'work').trim().toLowerCase();
		const mode = modeRaw === 'param' || modeRaw === 'control' ? modeRaw : 'work';
		const group = groups.find((candidate) => candidate.mode === mode);
		if (group) group.edges.push(edge);
	}
	return groups.filter((group) => group.edges.length > 0);
}

export type NodeHandleRuntimeState = {
	handle: string;
	state: string;
	updatedAt: string;
};

export function collectNodeHandleStates(
	nodeId: string | null | undefined,
	handleStates: Record<string, unknown> | null | undefined
): NodeHandleRuntimeState[] {
	const selectedNodeId = String(nodeId ?? '').trim();
	if (!selectedNodeId || !handleStates || typeof handleStates !== 'object') return [];
	const out: NodeHandleRuntimeState[] = [];
	for (const [key, value] of Object.entries(handleStates)) {
		if (!key.startsWith(`${selectedNodeId}:`)) continue;
		const handle = String(key.slice(selectedNodeId.length + 1) ?? '').trim();
		if (!handle) continue;
		const row = (value ?? {}) as Record<string, unknown>;
		out.push({
			handle,
			state: String(row.state ?? '').trim() || 'unknown',
			updatedAt: String(row.updatedAt ?? '').trim()
		});
	}
	out.sort((a, b) => a.handle.localeCompare(b.handle));
	return out;
}

export function queueMetricScopeSummary(queueRuntime: Record<string, unknown> | null | undefined): {
	runScopedPresent: boolean;
	aggregatePresent: boolean;
} {
	const runtime = (queueRuntime ?? {}) as Record<string, unknown>;
	return {
		runScopedPresent: Boolean(runtime.runScoped && typeof runtime.runScoped === 'object'),
		aggregatePresent: Boolean(
			runtime.aggregateDiagnostics && typeof runtime.aggregateDiagnostics === 'object'
		),
	};
}
