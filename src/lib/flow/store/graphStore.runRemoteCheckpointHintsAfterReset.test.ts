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

function makeSnapshot(graphId: string, status: string, nodeId: string, runId: string) {
	return {
		graphId,
		status,
		runId,
		runMode: 'from_start',
		plannedNodeIds: [nodeId],
		nodeBindings: {
			[nodeId]: {
				status: 'idle',
				isUpToDate: false,
				cacheValid: false,
				currentRunId: runId,
				lastRunId: runId,
				current: { execKey: 'exec-ckpt', artifactId: 'art-ckpt' },
				last: { execKey: 'exec-ckpt', artifactId: 'art-ckpt' }
			}
		}
	};
}

describe('graphStore runRemote checkpoint hints after reset', () => {
	beforeEach(() => {
		createRunMock.mockReset();
		getRunMock.mockReset();
		streamRunEventsMock.mockReset();
		graphStore.hardResetGraph();
		graphStore.clearHistory();
	});

	it('emits checkpoint execution hints from preserved registry after reset', async () => {
		graphStore.loadGraphDocument({
			nodes: [
				{
					id: 'n1',
					type: 'component',
					position: { x: 20, y: 20 },
					data: {
						kind: 'component',
						label: 'Checkpointed Component',
						params: {
							componentRef: {
								componentId: 'cmp_test',
								revisionId: 'crev_test',
								apiVersion: 'v1'
							},
							api: {
								outputs: [{ name: 'out', required: true }]
							}
						}
					}
				}
			],
			edges: [],
			checkpointRegistry: {
				n1: {
					id: '00000000-0000-4000-8000-000000000123',
					name: 'ckpt n1',
					nodeId: 'n1',
					graphId: 'graph-test',
					runId: 'run-old',
					artifactId: 'art-ckpt',
					execKey: 'exec-ckpt',
					fingerprintAtCreation: 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
					createdAt: '2026-04-10T00:00:00.000Z',
					staleness: 'valid',
					outputs: {
						out: { artifactId: 'art-ckpt', execKey: 'exec-ckpt' }
					}
				}
			}
		} as any);
		const nodeId = 'n1';
		const graphId = String(get(graphStore as any)?.graphId ?? '').trim();
		const run1 = 'run-seed';
		const run2 = 'run-after-reset';

		createRunMock
			.mockResolvedValueOnce({ runId: run1, graphId })
			.mockResolvedValueOnce({ runId: run2, graphId });
		getRunMock.mockImplementation(async (runId: string) =>
			makeSnapshot(graphId, 'succeeded', nodeId, String(runId || run2))
		);
		streamRunEventsMock.mockImplementation(
			(streamRunId: string, onEvent: (evt: KnownRunEvent) => void) => {
				queueMicrotask(() =>
					onEvent({
						type: 'run_finished',
						runId: streamRunId,
						at: '2026-04-10T20:00:00Z',
						status: 'succeeded'
					} as KnownRunEvent)
				);
				return { close: vi.fn() };
			}
		);

		await graphStore.runRemote(null, 'from_start');
		graphStore.resetRunUi();
		await graphStore.runRemote(null, 'from_start');
		expect(createRunMock).toHaveBeenCalledTimes(2);

		const secondPayload = createRunMock.mock.calls[1]?.[0] as any;
		const hints = secondPayload?.graph?.__executionHints ?? {};
		expect(hints.checkpoints).toEqual({
			n1: {
				artifactId: 'art-ckpt',
				execKey: 'exec-ckpt',
				fingerprintAtCreation: 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
				outputs: {
					out: { artifactId: 'art-ckpt', execKey: 'exec-ckpt' }
				}
			}
		});
	});
});

