import { get } from 'svelte/store';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const createRunMock = vi.fn();
const getRunMock = vi.fn();
const resumeRunMock = vi.fn();
const streamRunEventsMock = vi.fn();

vi.mock('$lib/flow/client/runs', async () => {
	const actual = await vi.importActual<typeof import('$lib/flow/client/runs')>('$lib/flow/client/runs');
	return {
		...actual,
		createRun: (...args: any[]) => createRunMock(...args),
		getRun: (...args: any[]) => getRunMock(...args),
		resumeRun: (...args: any[]) => resumeRunMock(...args),
		streamRunEvents: (...args: any[]) => streamRunEventsMock(...args)
	};
});

import { graphStore } from './graphStore';
import type { KnownRunEvent } from '$lib/flow/types/run';

describe('graphStore resume stream disconnect regression', () => {
	beforeEach(() => {
		createRunMock.mockReset();
		getRunMock.mockReset();
		resumeRunMock.mockReset();
		streamRunEventsMock.mockReset();
		graphStore.hardResetGraph();
		graphStore.clearHistory();
	});

	afterEach(() => {
		vi.useRealTimers();
	});

	it('reconciles to terminal status when resume stream disconnects near boundary', async () => {
		graphStore.addNode('source', { x: 10, y: 10 });
		const graphId = String((get(graphStore as any)?.graphId ?? '').trim());
		const runId = 'run-resume-regression';

		createRunMock.mockResolvedValue({ runId, graphId });
		resumeRunMock.mockResolvedValue({ ok: true });

		const snapshots = [
			{ graphId, status: 'running', nodeBindings: {} },
			{ graphId, status: 'paused', nodeBindings: {} },
			{ graphId, status: 'resuming', nodeBindings: {} },
			{ graphId, status: 'running', nodeBindings: {} },
			{ graphId, status: 'succeeded', nodeBindings: {} }
		];
		getRunMock.mockImplementation(async () => snapshots.shift() ?? { graphId, status: 'succeeded', nodeBindings: {} });

		streamRunEventsMock.mockImplementation(
			(
				_streamRunId: string,
				onEvent: (evt: KnownRunEvent) => void,
				onError: () => void
			) => {
				const callIndex = streamRunEventsMock.mock.calls.length;
				if (callIndex === 1) {
					queueMicrotask(() =>
						onEvent({
							type: 'run_paused',
							runId,
							at: '2026-04-01T12:00:00Z'
						} as KnownRunEvent)
					);
				} else {
					queueMicrotask(() => onError());
				}
				return { close: vi.fn() };
			}
		);

		await graphStore.runRemote(null, 'from_start');
		expect(get(graphStore as any).runStatus).toBe('paused');

		vi.useFakeTimers();
		const resumed = await graphStore.resumeActiveRun();
		expect(resumed.ok).toBe(true);
		expect(get(graphStore as any).runStatus).toBe('resuming');

		await vi.advanceTimersByTimeAsync(0);
		await Promise.resolve();
		await vi.advanceTimersByTimeAsync(1500);
		await Promise.resolve();
		await vi.advanceTimersByTimeAsync(2000);
		await Promise.resolve();

		const state = get(graphStore as any);
		expect(state.runStatus).toBe('succeeded');
		expect(streamRunEventsMock).toHaveBeenCalledTimes(2);
		expect(getRunMock).toHaveBeenCalled();
		expect(
			(state.logs as Array<{ message: string }>).some((log) =>
				String(log.message).includes('Event stream error; reconciling run status')
			)
		).toBe(true);
		expect(
			(state.logs as Array<{ message: string }>).some((log) =>
				String(log.message).includes('Run finished via polling (succeeded)')
			)
		).toBe(true);
	});
});
