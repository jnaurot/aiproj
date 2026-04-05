import { describe, expect, it } from 'vitest';

import type { Node } from '@xyflow/svelte';
import type { PipelineNodeData } from '$lib/flow/types';
import {
	findDuplicateNodeNames,
	normalizeNodeName,
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
		expect(duplicates[0].normalizedName).toBe('alpha');
		expect(duplicates[0].nodeIds.sort()).toEqual(['n1', 'n2']);
	});

	it('resolves a unique suffix when base name already exists', () => {
		const nodes = [makeNode('n1', 'Source'), makeNode('n2', 'Source (2)')];
		expect(resolveUniqueNodeName(nodes, 'Source')).toBe('Source (3)');
	});
});
