import { describe, expect, it } from 'vitest';

import { __setStrictSchemaFeatureFlagsForTest } from '$lib/flow/portCapabilities';
import { graphStore } from './graphStore';

describe('graphStore strict schema v2 rollout flag', () => {
	it('uses schema-first compatibility when STRICT_SCHEMA_EDGE_CHECKS_V2 is disabled', () => {
		__setStrictSchemaFeatureFlagsForTest({ STRICT_SCHEMA_EDGE_CHECKS_V2: false });
		graphStore.hardResetGraph();
		const sourceId = graphStore.addNode('source', { x: 0, y: 0 });
		const transformId = graphStore.addNode('transform', { x: 220, y: 0 });
		graphStore.updateNodeConfig(sourceId, {
			params: { file_format: 'txt', output: { mode: 'text' } }
		});
		graphStore.updateNodeConfig(transformId, {
			params: { op: 'filter', filter: { expr: '' } }
		});
		const added = graphStore.addEdge({
			id: 'e_flag_legacy',
			source: sourceId,
			target: transformId,
			data: { exec: 'idle' }
		} as any);
		expect(added.ok).toBe(true);
		__setStrictSchemaFeatureFlagsForTest({ STRICT_SCHEMA_EDGE_CHECKS_V2: true });
	});

	it('uses schema-first compatibility when STRICT_SCHEMA_EDGE_CHECKS_V2 is enabled', () => {
		__setStrictSchemaFeatureFlagsForTest({ STRICT_SCHEMA_EDGE_CHECKS_V2: true });
		graphStore.hardResetGraph();
		const sourceId = graphStore.addNode('source', { x: 0, y: 0 });
		const transformId = graphStore.addNode('transform', { x: 220, y: 0 });
		graphStore.updateNodeConfig(sourceId, {
			params: { file_format: 'txt', output: { mode: 'text' } }
		});
		graphStore.updateNodeConfig(transformId, {
			params: { op: 'filter', filter: { expr: '' } }
		});
		const added = graphStore.addEdge({
			id: 'e_flag_v2',
			source: sourceId,
			target: transformId,
			data: { exec: 'idle' }
		} as any);
		expect(added.ok).toBe(true);
	});
});
