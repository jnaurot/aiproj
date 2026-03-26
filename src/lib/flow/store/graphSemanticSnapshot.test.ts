import { describe, expect, it } from 'vitest';

import { graphSemanticSnapshotKey, isGraphSemanticDirty } from './graphSemanticSnapshot';

type TestNode = {
	id: string;
	type: string;
	position?: { x: number; y: number };
	data: Record<string, unknown>;
};

type TestEdge = {
	id: string;
	source: string;
	target: string;
	sourceHandle?: string | null;
	targetHandle?: string | null;
};

function baseNodes(): TestNode[] {
	return [
		{
			id: 'n1',
			type: 'default',
			position: { x: 0, y: 0 },
			data: {
				kind: 'transform',
				label: 'Transform_HardFilter',
				transformKind: 'filter',
				params: {
					op: 'filter',
					expr: 'score >= 70'
				}
			}
		}
	];
}

function baseEdges(): TestEdge[] {
	return [
		{
			id: 'e1',
			source: 'n1',
			sourceHandle: 'out',
			target: 'n1',
			targetHandle: 'in'
		}
	];
}

describe('graphSemanticSnapshot', () => {
	it('ignores layout-only position changes', () => {
		const nodes = baseNodes();
		const edges = baseEdges();
		const before = graphSemanticSnapshotKey('g1', nodes, edges);
		const moved = nodes.map((n) => ({ ...n, position: { x: 420, y: 120 } }));
		const after = graphSemanticSnapshotKey('g1', moved, edges);
		expect(after).toBe(before);
	});

	it('changes when semantic node params change', () => {
		const nodes = baseNodes();
		const edges = baseEdges();
		const before = graphSemanticSnapshotKey('g1', nodes, edges);
		const edited = nodes.map((n) => ({
			...n,
			data: {
				...n.data,
				params: {
					op: 'filter',
					expr: 'score >= 80'
				}
			}
		}));
		const after = graphSemanticSnapshotKey('g1', edited, edges);
		expect(after).not.toBe(before);
	});

	it('dirty check toggles off when state returns to saved baseline', () => {
		const nodes = baseNodes();
		const edges = baseEdges();
		const baseline = graphSemanticSnapshotKey('g1', nodes, edges);
		const edited = nodes.map((n) => ({
			...n,
			data: {
				...n.data,
				params: {
					op: 'filter',
					expr: 'score >= 90'
				}
			}
		}));
		const editedKey = graphSemanticSnapshotKey('g1', edited, edges);
		expect(isGraphSemanticDirty(baseline, editedKey)).toBe(true);
		const revertedKey = graphSemanticSnapshotKey('g1', nodes, edges);
		expect(isGraphSemanticDirty(baseline, revertedKey)).toBe(false);
	});
});
