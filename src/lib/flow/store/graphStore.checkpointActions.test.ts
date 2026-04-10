import { get } from 'svelte/store';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import type { KnownRunEvent } from '$lib/flow/types/run';

const createRunMock = vi.fn();
const getRunMock = vi.fn();
const streamRunEventsMock = vi.fn();

vi.mock('$lib/flow/client/runs', async () => {
	const actual = await vi.importActual<typeof import('$lib/flow/client/runs')>('$lib/flow/client/runs');
	return {
		...actual,
		createRun: (...args: any[]) => createRunMock(...args),
		getRun: (...args: any[]) => getRunMock(...args),
		streamRunEvents: (...args: any[]) => streamRunEventsMock(...args)
	};
});

import { graphStore } from './graphStore';

function installSingleNodeGraph(nodeId: string): void {
	graphStore.loadGraphDocument({
		nodes: [
			{
				id: nodeId,
				type: 'source',
				position: { x: 0, y: 0 },
				data: {
					kind: 'source',
					label: 'Source',
					sourceKind: 'text',
					params: {}
				}
			}
		],
		edges: []
	});
}

function makeSnapshot(graphId: string, nodeId: string, runId: string, withMemo = true) {
	return {
		graphId,
		status: 'succeeded',
		runId,
		runMode: 'from_start',
		plannedNodeIds: [nodeId],
		nodeBindings: {
			[nodeId]: {
				status: 'succeeded_up_to_date',
				isUpToDate: true,
				cacheValid: true,
				currentRunId: runId,
				lastRunId: runId,
				current: { execKey: 'exec-1', artifactId: 'art-1' },
				last: { execKey: 'exec-1', artifactId: 'art-1' },
				outputLineage: {
					out: { execKey: 'exec-1', artifactId: 'art-1' }
				},
				...(withMemo
					? {
							memoState: {
								decision: 'compute',
								memoKey: 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
							}
						}
					: {})
			}
		}
	};
}

describe('graphStore checkpoint actions', () => {
	beforeEach(() => {
		createRunMock.mockReset();
		getRunMock.mockReset();
		streamRunEventsMock.mockReset();
		graphStore.hardResetGraph();
		graphStore.clearHistory();
	});

	it('createCheckpoint requires succeeded node binding', () => {
		installSingleNodeGraph('n1');
		const result = graphStore.createCheckpoint('n1', 'checkpoint 1');
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.toLowerCase()).toContain('status is succeeded');
		}
	});

	it('createCheckpoint requires memo fingerprint', async () => {
		const nodeId = 'n1';
		installSingleNodeGraph(nodeId);
		const graphId = String((get(graphStore as any)?.graphId ?? '').trim());
		const runId = 'run-no-memo';
		createRunMock.mockResolvedValueOnce({ runId, graphId });
		getRunMock.mockResolvedValue(makeSnapshot(graphId, nodeId, runId, false));
		streamRunEventsMock.mockImplementation((rid: string, onEvent: (evt: KnownRunEvent) => void) => {
			queueMicrotask(() =>
				onEvent({
					type: 'run_finished',
					runId: rid,
					at: '2026-04-10T00:00:00Z',
					status: 'succeeded'
				} as KnownRunEvent)
			);
			return { close: vi.fn() };
		});

		await graphStore.runRemote(null, 'from_start');
		const result = graphStore.createCheckpoint(nodeId, 'checkpoint 1');
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error).toContain('fingerprint');
		}
	});

	it('createCheckpoint stores registry entry and removeCheckpoint deletes it', async () => {
		const nodeId = 'n1';
		installSingleNodeGraph(nodeId);
		const graphId = String((get(graphStore as any)?.graphId ?? '').trim());
		const runId = 'run-with-memo';
		createRunMock.mockResolvedValueOnce({ runId, graphId });
		getRunMock.mockResolvedValue(makeSnapshot(graphId, nodeId, runId, true));
		streamRunEventsMock.mockImplementation((rid: string, onEvent: (evt: KnownRunEvent) => void) => {
			queueMicrotask(() =>
				onEvent({
					type: 'run_finished',
					runId: rid,
					at: '2026-04-10T00:00:00Z',
					status: 'succeeded'
				} as KnownRunEvent)
			);
			return { close: vi.fn() };
		});

		await graphStore.runRemote(null, 'from_start');
		const created = graphStore.createCheckpoint(nodeId, 'checkpoint 1', 'desc');
		expect(created.ok).toBe(true);
		if (created.ok) {
			expect(created.checkpoint.nodeId).toBe(nodeId);
		}
		expect((get(graphStore).checkpointRegistry as any)?.[nodeId]?.name).toBe('checkpoint 1');
		expect((get(graphStore).nodeBindings as any)?.[nodeId]?.checkpointable).toBe(false);

		graphStore.removeCheckpoint(nodeId);
		expect((get(graphStore).checkpointRegistry as any)?.[nodeId]).toBeUndefined();
	});

	it('createCheckpoint replaces existing checkpoint for the same node', async () => {
		const nodeId = 'n1';
		installSingleNodeGraph(nodeId);
		const graphId = String((get(graphStore as any)?.graphId ?? '').trim());
		const runId = 'run-replace';
		createRunMock.mockResolvedValueOnce({ runId, graphId });
		getRunMock.mockResolvedValue(makeSnapshot(graphId, nodeId, runId, true));
		streamRunEventsMock.mockImplementation((rid: string, onEvent: (evt: KnownRunEvent) => void) => {
			queueMicrotask(() =>
				onEvent({
					type: 'run_finished',
					runId: rid,
					at: '2026-04-10T00:00:00Z',
					status: 'succeeded'
				} as KnownRunEvent)
			);
			return { close: vi.fn() };
		});

		await graphStore.runRemote(null, 'from_start');
		const first = graphStore.createCheckpoint(nodeId, 'first');
		expect(first.ok).toBe(true);
		const second = graphStore.createCheckpoint(nodeId, 'second');
		expect(second.ok).toBe(true);
		expect((get(graphStore).checkpointRegistry as any)?.[nodeId]?.name).toBe('second');
	});

	it('renameCheckpoint and bulk checkpoint removals update registry', async () => {
		const nodeId = 'n1';
		installSingleNodeGraph(nodeId);
		const graphId = String((get(graphStore as any)?.graphId ?? '').trim());
		const runId = 'run-bulk';
		createRunMock.mockResolvedValueOnce({ runId, graphId });
		getRunMock.mockResolvedValue(makeSnapshot(graphId, nodeId, runId, true));
		streamRunEventsMock.mockImplementation((rid: string, onEvent: (evt: KnownRunEvent) => void) => {
			queueMicrotask(() =>
				onEvent({
					type: 'run_finished',
					runId: rid,
					at: '2026-04-10T00:00:00Z',
					status: 'succeeded'
				} as KnownRunEvent)
			);
			return { close: vi.fn() };
		});

		await graphStore.runRemote(null, 'from_start');
		graphStore.createCheckpoint(nodeId, 'first');
		const renamed = graphStore.renameCheckpoint(nodeId, 'renamed');
		expect(renamed.ok).toBe(true);
		expect((get(graphStore).checkpointRegistry as any)?.[nodeId]?.name).toBe('renamed');

		graphStore.loadGraphDocument({
			nodes: [
				{
					id: 'n1',
					type: 'source',
					position: { x: 0, y: 0 },
					data: { kind: 'source', label: 'Source', sourceKind: 'text', params: {} }
				},
				{
					id: 'n2',
					type: 'source',
					position: { x: 20, y: 0 },
					data: { kind: 'source', label: 'Source 2', sourceKind: 'text', params: {} }
				}
			],
			edges: [],
			checkpointRegistry: {
				n1: {
					id: '00000000-0000-4000-8000-000000000001',
					name: 'valid',
					nodeId: 'n1',
					graphId,
					runId: 'r1',
					artifactId: 'a1',
					execKey: 'e1',
					fingerprintAtCreation: 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
					createdAt: '2026-04-10T00:00:00.000Z',
					staleness: 'valid'
				},
				n2: {
					id: '00000000-0000-4000-8000-000000000002',
					name: 'stale',
					nodeId: 'n2',
					graphId,
					runId: 'r2',
					artifactId: 'a2',
					execKey: 'e2',
					fingerprintAtCreation: 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
					createdAt: '2026-04-10T00:00:00.000Z',
					staleness: 'stale'
				}
			} as any
		});
		const removedStale = graphStore.removeAllStaleCheckpoints();
		expect(removedStale.removed).toBe(1);
		expect((get(graphStore).checkpointRegistry as any)?.n2).toBeUndefined();
		expect((get(graphStore).checkpointRegistry as any)?.n1).toBeTruthy();

		const cleared = graphStore.clearAllCheckpoints();
		expect(cleared.removed).toBe(1);
		expect(get(graphStore).checkpointRegistry).toEqual({});
	});
});
