import { describe, expect, it } from 'vitest';

import type { Node } from '@xyflow/svelte';
import type { PipelineNodeData } from '$lib/flow/types';
import {
	annotateCanonicalNodeNames,
	canonicalNodeName,
	findDuplicateNodeNames,
	nodeScopeKey,
	normalizeNodeName,
	parsePromotedNodeId,
	resolveUniqueNodeName
} from './nodeNameUniqueness';

function makeNode(id: string, label: string): Node<PipelineNodeData> {
	return {
		id,
		type: 'tool',
		position: { x: 0, y: 0 },
		data: {
			kind: 'tool',
			label
		} as any
	} as Node<PipelineNodeData>;
}

describe('nodeNameUniqueness helpers', () => {
	it('normalizes names by trim + case-insensitive fold', () => {
		expect(normalizeNodeName('  Alpha  ')).toBe('alpha');
		expect(normalizeNodeName('ALPHA')).toBe('alpha');
	});

	it('finds duplicate names after normalization', () => {
		const duplicates = findDuplicateNodeNames([
			makeNode('n1', 'Alpha'),
			makeNode('n2', ' alpha '),
			makeNode('n3', 'Beta')
		]);
		expect(duplicates).toHaveLength(1);
		expect(duplicates[0].scopeKey).toBe('root');
		expect(duplicates[0].normalizedName).toBe('alpha');
		expect(duplicates[0].nodeIds.sort()).toEqual(['n1', 'n2']);
	});

	it('resolves a unique suffix when base name already exists', () => {
		const nodes = [makeNode('n1', 'Source'), makeNode('n2', 'Source (2)')];
		expect(resolveUniqueNodeName(nodes, 'Source')).toBe('Source (3)');
	});

	it('allows same local name across different component scopes', () => {
		const nodes = [
			makeNode('n_top', 'Transform_A'),
			makeNode('comp_1', 'Comp1'),
			makeNode('cmp:comp_1:n_inner', 'Transform_A')
		];
		const duplicates = findDuplicateNodeNames(nodes);
		expect(duplicates).toHaveLength(0);
	});

	it('derives canonical names for promoted component nodes', () => {
		const nodes = [makeNode('comp_1', 'Comp1'), makeNode('cmp:comp_1:n_inner', 'Transform_A')];
		expect(canonicalNodeName(nodes, nodes[1])).toBe('Comp1.Transform_A');
	});

	it('annotates canonical names into node meta', () => {
		const nodes = [makeNode('comp_1', 'Comp1'), makeNode('cmp:comp_1:n_inner', 'Transform_A')];
		const annotated = annotateCanonicalNodeNames(nodes);
		expect(String((annotated[0].data as any)?.meta?.canonicalName ?? '')).toBe('Comp1');
		expect(String((annotated[1].data as any)?.meta?.canonicalName ?? '')).toBe(
			'Comp1.Transform_A'
		);
	});

	it('parses promoted node IDs and scope keys deterministically', () => {
		const parsed = parsePromotedNodeId('cmp:comp_1:n_inner');
		expect(parsed?.componentInstanceIds).toEqual(['comp_1']);
		expect(parsed?.leafNodeId).toBe('n_inner');
		expect(nodeScopeKey('cmp:comp_1:n_inner')).toBe('cmp:comp_1');
	});
});
