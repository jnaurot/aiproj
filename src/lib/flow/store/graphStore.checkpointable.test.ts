import { describe, expect, it } from 'vitest';

import { __applyRunEventForTest, __hardResetGraphForTest, __normalizeBindingForTest } from './graphStore';
import type { KnownRunEvent } from '$lib/flow/types/run';

describe('graphStore checkpointable binding flags', () => {
	it('run_started clears checkpointable and memoState', () => {
		const runId = 'run-1';
		const base = __hardResetGraphForTest({} as any, 'graph-checkpointable');
		const state = {
			...base,
			runStatus: 'running' as const,
			activeRunId: runId,
			nodes: [{ id: 'n1', data: { kind: 'source', params: {} } }] as any,
			nodeBindings: {
				n1: __normalizeBindingForTest(
					{
						status: 'succeeded_up_to_date',
						isUpToDate: true,
						current: { execKey: 'exec-1', artifactId: 'art-1' },
						last: { execKey: 'exec-1', artifactId: 'art-1' },
						checkpointable: true,
						memoState: {
							decision: 'compute',
							memoKey: 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
						}
					},
					'n1'
				)
			}
		};
		const next = __applyRunEventForTest(
			state as any,
			{
				type: 'run_started',
				runId,
				at: '2026-04-10T00:00:00.000Z',
				plannedNodeIds: ['n1']
			} as KnownRunEvent,
			runId
		);
		expect((next.nodeBindings as any).n1.checkpointable).toBe(false);
		expect((next.nodeBindings as any).n1.memoState).toBeUndefined();
	});

	it('node_finished sets checkpointable only when node has no checkpoint', () => {
		const runId = 'run-2';
		const base = __hardResetGraphForTest({} as any, 'graph-checkpointable');
		const binding = __normalizeBindingForTest(
			{
				status: 'running',
				isUpToDate: false,
				currentRunId: runId,
				current: { execKey: 'exec-1', artifactId: 'art-1' },
				last: { execKey: 'exec-1', artifactId: 'art-1' }
			},
			'n1'
		);
		const event = {
			type: 'node_finished',
			nodeId: 'n1',
			runId,
			at: '2026-04-10T00:00:00.000Z',
			status: 'succeeded'
		} as KnownRunEvent;
		const stateWithoutCheckpoint = {
			...base,
			runStatus: 'running' as const,
			activeRunId: runId,
			nodes: [{ id: 'n1', data: { kind: 'source', params: {} } }] as any,
			nodeBindings: { n1: binding },
			checkpointRegistry: {}
		};
		const successNoCheckpoint = __applyRunEventForTest(stateWithoutCheckpoint as any, event, runId);
		expect((successNoCheckpoint.nodeBindings as any).n1.checkpointable).toBe(true);

		const stateWithCheckpoint = {
			...stateWithoutCheckpoint,
			checkpointRegistry: {
				n1: {
					id: '00000000-0000-4000-8000-000000000001',
					name: 'ck-1',
					nodeId: 'n1',
					graphId: 'graph-checkpointable',
					runId: 'old-run',
					artifactId: 'art-1',
					execKey: 'exec-1',
					fingerprintAtCreation: 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
					createdAt: '2026-04-10T00:00:00.000Z',
					staleness: 'valid'
				}
			}
		};
		const successWithCheckpoint = __applyRunEventForTest(stateWithCheckpoint as any, event, runId);
		expect((successWithCheckpoint.nodeBindings as any).n1.checkpointable).toBe(false);
	});
});

