import { describe, expect, it } from 'vitest';

import { graphStore } from './graphStore';

describe('graphStore ports deprecation preflight', () => {
	it('warns when authored node.data.ports is present', () => {
		graphStore.hardResetGraph();
		const applied = graphStore.loadGraphDocument(
			{
				nodes: [
					{
						id: 'src_with_ports',
						type: 'default',
						position: { x: 0, y: 0 },
						data: {
							kind: 'source',
							sourceKind: 'api',
							params: {},
							ports: { in: null, out: 'table' },
							status: 'idle'
						}
					}
				],
				edges: []
			},
			'graph_ports_deprecation'
		);
		expect(applied.ok).toBe(true);

		const preflight = graphStore.getSavePreflight();
		const warning = preflight.diagnostics.find((d) => d.code === 'PORTS_AUTHORED_DEPRECATED');
		expect(warning).toBeTruthy();
		expect(warning?.severity).toBe('warning');
	});
});
