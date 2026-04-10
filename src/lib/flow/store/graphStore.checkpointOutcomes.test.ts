import { describe, expect, it } from 'vitest';
import { get } from 'svelte/store';

import { graphStore } from './graphStore';
import { __hydrateFromRunSnapshotForTest } from './graphStore.run';

function checkpoint(nodeId: string, staleness: 'valid' | 'stale' | 'artifact_missing' | 'unknown') {
	return {
		id: `5a29644e-7ce4-4be8-955d-22f91f5b0${nodeId === 'n1' ? '11' : '22'}`,
		name: `cp-${nodeId}`,
		nodeId,
		graphId: 'graph-checkpoint-outcomes',
		runId: 'run-initial',
		artifactId: `artifact-${nodeId}`,
		execKey: `exec-${nodeId}`,
		fingerprintAtCreation: 'a'.repeat(64),
		createdAt: '2026-04-10T00:00:00.000Z',
		staleness
	} as const;
}

describe('graphStore hydrate checkpoint outcomes', () => {
	it('updates checkpoint staleness when outcome is present', () => {
		graphStore.hardResetGraph();
		const initial = get(graphStore);
		const state: any = {
			...initial,
			checkpointRegistry: {
				n1: checkpoint('n1', 'unknown')
			}
		};
		const next = __hydrateFromRunSnapshotForTest(state, {
			status: 'succeeded',
			checkpoint_outcomes: { n1: 'valid' }
		} as any);
		expect(next.checkpointRegistry.n1?.staleness).toBe('valid');
	});

	it('keeps checkpoint staleness unchanged for nodes not in outcomes', () => {
		graphStore.hardResetGraph();
		const initial = get(graphStore);
		const state: any = {
			...initial,
			checkpointRegistry: {
				n1: checkpoint('n1', 'unknown'),
				n2: checkpoint('n2', 'stale')
			}
		};
		const next = __hydrateFromRunSnapshotForTest(state, {
			status: 'succeeded',
			checkpoint_outcomes: { n1: 'valid' }
		} as any);
		expect(next.checkpointRegistry.n1?.staleness).toBe('valid');
		expect(next.checkpointRegistry.n2?.staleness).toBe('stale');
	});

	it('ignores invalid checkpoint outcome statuses', () => {
		graphStore.hardResetGraph();
		const initial = get(graphStore);
		const state: any = {
			...initial,
			checkpointRegistry: {
				n1: checkpoint('n1', 'unknown')
			}
		};
		const next = __hydrateFromRunSnapshotForTest(state, {
			status: 'succeeded',
			checkpoint_outcomes: { n1: 'not_a_status' }
		} as any);
		expect(next.checkpointRegistry.n1?.staleness).toBe('unknown');
	});
});

