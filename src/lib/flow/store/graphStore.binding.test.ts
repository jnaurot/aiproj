import { describe, expect, it } from 'vitest';

import {
	__assertBindingPairForTest,
	__hydrateFromRunSnapshotForTest,
	__normalizeBindingForTest,
	type GraphState
} from './graphStore';

describe('graphStore binding normalization', () => {
	it('normalized binding always has required fields', () => {
		const b = __normalizeBindingForTest(undefined, 'n_test');
		expect(b.status).toBeDefined();
		expect(b.current).toEqual({ execKey: null, artifactId: null });
		expect(b.last).toEqual({ execKey: null, artifactId: null });
		expect(b.isUpToDate).toBe(false);
		expect(b.cacheValid).toBe(false);
		expect(b.currentRunId).toBeNull();
		expect(b.staleReason).toBeNull();
	});

	it('binding pair invariant rejects partial pairs', () => {
		expect(() =>
			__assertBindingPairForTest({ current: { execKey: 'ek', artifactId: null } }, 'n_bad', 'test')
		).toThrow(/INVALID_BINDING_PAIR/);
	});

	it('prunes orphaned bindings and outputs when graph nodes no longer exist', () => {
		const state: GraphState = {
			graphId: 'graph-test',
			nodes: [{ id: 'live', data: { status: 'succeeded' } } as any],
			edges: [],
			selectedNodeId: null,
			inspector: { nodeId: null, draftParams: {}, dirty: false, uiByNodeId: {} },
			logs: [],
			runStatus: 'succeeded',
			lastRunStatus: 'succeeded',
			freshness: 'stale',
			staleNodeCount: 1,
			activeRunMode: 'from_start',
			activeRunFrom: null,
			activeRunNodeSet: new Set<string>(),
			nodeOutputs: {
				live: { payloadType: 'json' },
				ghost: { payloadType: 'json' }
			},
			nodeBindings: {
				live: __normalizeBindingForTest({ status: 'succeeded_up_to_date', isUpToDate: true }, 'live'),
				ghost: __normalizeBindingForTest({ status: 'stale', isUpToDate: false }, 'ghost')
			},
			activeRunId: null
		};

		const next = __hydrateFromRunSnapshotForTest(state, { status: 'succeeded' });
		expect(next.nodeBindings.ghost).toBeUndefined();
		expect(next.nodeOutputs.ghost).toBeUndefined();
		expect(next.staleNodeCount).toBe(0);
		expect(next.freshness).toBe('never_run');
	});
});

