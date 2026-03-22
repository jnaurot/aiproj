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
