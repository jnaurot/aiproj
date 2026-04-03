import { describe, expect, it } from 'vitest';

import {
	__normalizeBindingForTest,
	__resetRunUiStateForTest,
	type GraphState
} from './graphStore';

describe('graphStore resetRunUiState', () => {
	it('resets all node bindings to idle baseline like freshly loaded graph', () => {
		const state = {
			graphId: 'g-reset',
			nodes: [
				{
					id: 'n_source',
					type: 'source',
					position: { x: 0, y: 0 },
					data: {
						kind: 'source',
						label: 'Source',
						params: {},
						meta: { freeze: { enabled: true, mode: 'sticky' } }
					}
				},
				{
					id: 'n_xform',
					type: 'transform',
					position: { x: 200, y: 0 },
					data: {
						kind: 'transform',
						label: 'Transform',
						params: {}
					}
				}
			] as any,
			edges: [{ id: 'e1', source: 'n_source', target: 'n_xform', data: { exec: 'active', mode: 'work' } }] as any,
			selectedNodeId: null,
			inspector: { nodeId: null, draftParams: {}, dirty: false },
			logs: [{ ts: 'now', level: 'info', msg: 'something' }] as any,
			runStatus: 'running',
			lastRunStatus: 'succeeded',
			freshness: 'up_to_date',
			staleNodeCount: 0,
			activeRunMode: 'from_start',
			activeRunFrom: null,
			activeRunNodeSet: new Set<string>(['n_source', 'n_xform']),
			nodeOutputs: {},
			nodeBindings: {
				n_source: __normalizeBindingForTest(
					{
						status: 'succeeded_up_to_date',
						isUpToDate: true,
						cacheValid: true,
						current: { execKey: 'exec-source', artifactId: 'art-source' },
						last: { execKey: 'exec-source', artifactId: 'art-source' },
						currentRunId: 'run-1',
						lastRunId: 'run-1'
					},
					'n_source'
				),
				n_xform: __normalizeBindingForTest(
					{
						status: 'failed',
						isUpToDate: false,
						cacheValid: false,
						current: { execKey: 'exec-xform', artifactId: 'art-xform' },
						last: { execKey: 'exec-xform', artifactId: 'art-xform' },
						currentRunId: 'run-1',
						lastRunId: 'run-1',
						staleReason: 'UPSTREAM_CHANGED'
					},
					'n_xform'
				)
			},
			activeRunId: 'run-1'
		} as unknown as GraphState;

		const next = __resetRunUiStateForTest(state);
		expect(next.runStatus).toBe('idle');
		expect(next.activeRunId).toBeNull();
		expect(next.logs).toEqual([]);
		for (const nodeId of ['n_source', 'n_xform']) {
			const b = next.nodeBindings[nodeId];
			expect(String(b.status ?? '')).toBe('idle');
			expect(Boolean(b.isUpToDate)).toBe(false);
			expect(Boolean(b.cacheValid)).toBe(false);
			expect(b.current?.execKey ?? null).toBeNull();
			expect(b.current?.artifactId ?? null).toBeNull();
			expect(b.last?.execKey ?? null).toBeNull();
			expect(b.last?.artifactId ?? null).toBeNull();
			expect((b as any).currentExecKey ?? null).toBeNull();
			expect((b as any).currentArtifactId ?? null).toBeNull();
			expect((b as any).lastExecKey ?? null).toBeNull();
			expect((b as any).lastArtifactId ?? null).toBeNull();
		}
	});
});

