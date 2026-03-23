import { describe, expect, it } from 'vitest';

import { graphStore } from './graphStore';

describe('graphStore port authoring connectability', () => {
	it('requires declared target handle and allows connection after authoring', () => {
		graphStore.hardResetGraph();
		const src = graphStore.addNode('tool', { x: 0, y: 0 });
		const dst = graphStore.addNode('tool', { x: 250, y: 0 });

		// no declaration yet
		const first = graphStore.preflightConnection({
			source: src,
			target: dst,
			sourceHandle: 'out',
			targetHandle: 'param_filters',
			mode: 'param'
		});
		expect(first.ok).toBe(false);

		const authored = graphStore.updateNodePortDeclaration(dst, 'in', 'param_filters', {
			plane: 'param',
			required: false,
			cardinality: 'many',
			behavior: 'once'
		});
		expect(authored.ok).toBe(true);

		const second = graphStore.preflightConnection({
			source: src,
			target: dst,
			sourceHandle: 'out',
			targetHandle: 'param_filters',
			mode: 'param'
		});
		expect(second.ok).toBe(true);
	});
});

