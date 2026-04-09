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
				current: { execKey: 'exec-pinned', artifactId: 'art-pinned' },
				last: { execKey: 'exec-pinned', artifactId: 'art-pinned' },
				outputLineage: {
					summary: { execKey: 'exec-summary', artifactId: 'art-summary' },
					full: { execKey: 'exec-full', artifactId: 'art-full' }
				}
			}
		}
	};
}

describe('graphStore runRemote pinned hints after reset', () => {
	beforeEach(() => {
		createRunMock.mockReset();
		getRunMock.mockReset();
		streamRunEventsMock.mockReset();
		graphStore.hardResetGraph();
		graphStore.clearHistory();
		graphStore.setPinHintTraceLoggingEnabled(false);
	});

	it('emits pinned execution hints from preserved lineage after reset', async () => {
		graphStore.loadGraphDocument({
			nodes: [
				{
					id: 'n1',
					type: 'component',
					position: { x: 20, y: 20 },
					data: {
						kind: 'component',
						label: 'Pinned Component',
						params: {
							componentRef: {
								componentId: 'cmp_test',
								revisionId: 'crev_test',
								apiVersion: 'v1'
							},
							api: {
								outputs: [{ name: 'out', required: true }]
							}
						},
						meta: { freeze: { enabled: true, mode: 'sticky' } }
					}
				}
			],
			edges: []
		});
		const nodeId = 'n1';
		const graphId = String((get(graphStore as any)?.graphId ?? '').trim());
		const run1 = 'run-pin-seed';
		const run2 = 'run-pin-after-reset';

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
						at: '2026-04-08T20:00:00Z',
						status: 'succeeded'
					} as KnownRunEvent)
				);
				return { close: vi.fn() };
			}
		);

		await graphStore.runRemote(null, 'from_start');
		await graphStore.runRemote(null, 'from_start');
		expect(createRunMock).toHaveBeenCalledTimes(2);

		const secondPayload = createRunMock.mock.calls[1]?.[0] as any;
		const hints = secondPayload?.graph?.__executionHints ?? {};
		expect(hints.pinnedNodeIds).toEqual([nodeId]);
		expect(hints.pinnedArtifacts).toEqual({
			[nodeId]: {
				artifactId: 'art-pinned',
				execKey: 'exec-pinned',
				outputs: {
					summary: { artifactId: 'art-summary', execKey: 'exec-summary' },
					full: { artifactId: 'art-full', execKey: 'exec-full' }
				}
			}
		});
	});

	it('emits pin trace logs from collect through submit when enabled', async () => {
		graphStore.loadGraphDocument({
			nodes: [
				{
					id: 'n1',
					type: 'component',
					position: { x: 0, y: 0 },
					data: {
						kind: 'component',
						label: 'Pinned Component',
						params: {
							componentRef: {
								componentId: 'cmp_test',
								revisionId: 'crev_test',
								apiVersion: 'v1'
							},
							api: { outputs: [{ name: 'out', required: true }] }
						},
						meta: { freeze: { enabled: true, mode: 'sticky' } }
					}
				}
			],
			edges: []
		});
		const graphId = String((get(graphStore as any)?.graphId ?? '').trim());
		const nodeId = 'n1';
		const runId = 'run-trace-pin';

		createRunMock.mockResolvedValueOnce({ runId, graphId });
		getRunMock.mockImplementation(async (rid: string) =>
			makeSnapshot(graphId, 'succeeded', nodeId, String(rid || runId))
		);
		streamRunEventsMock.mockImplementation((streamRunId: string, onEvent: (evt: KnownRunEvent) => void) => {
			queueMicrotask(() =>
				onEvent({
					type: 'run_finished',
					runId: streamRunId,
					at: '2026-04-09T00:00:00Z',
					status: 'succeeded'
				} as KnownRunEvent)
			);
			return { close: vi.fn() };
		});

		graphStore.setPinHintTraceLoggingEnabled(true);
		await graphStore.runRemote(null, 'from_start');
		graphStore.setPinHintTraceLoggingEnabled(false);

		const logs = (get(graphStore as any)?.logs ?? []).map((entry: any) => String(entry?.message ?? ''));
		expect(logs.some((message: string) => message.includes('[trace][pin.collect]'))).toBe(true);
		expect(logs.some((message: string) => message.includes('[trace][pin.request_build]'))).toBe(true);
		expect(logs.some((message: string) => message.includes('[trace][pin.submit]'))).toBe(true);
	});
});
