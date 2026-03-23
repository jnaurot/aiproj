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
