import { get } from 'svelte/store';
import { beforeEach, describe, expect, it, vi } from 'vitest';

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

function hasLog(state: any, text: string): boolean {
	return (state.logs as Array<{ message: string }>).some((log) => String(log.message).includes(text));
}

describe('stream-close classification + pause/resume matrix', () => {
	beforeEach(() => {
		createRunMock.mockReset();
		getRunMock.mockReset();
		resumeRunMock.mockReset();
		streamRunEventsMock.mockReset();
		graphStore.hardResetGraph();
		graphStore.clearHistory();
		graphStore.addNode('source', { x: 10, y: 10 });
	});

	it('classifies stream close as expected during pausing and reconciles to paused', async () => {
		const graphId = String((get(graphStore as any)?.graphId ?? '').trim());
		const runId = 'run-close-expected-pausing';
		createRunMock.mockResolvedValue({ runId, graphId });
		getRunMock
			.mockResolvedValueOnce({ graphId, status: 'running', nodeBindings: {} })
			.mockResolvedValueOnce({ graphId, status: 'paused', nodeBindings: {} });

		streamRunEventsMock.mockImplementation(
			(_id: string, onEvent: (evt: KnownRunEvent) => void, onError: () => void) => {
				queueMicrotask(() => {
					onEvent({ type: 'run_pausing', runId, at: '2026-05-02T01:00:00Z' } as KnownRunEvent);
					onError();
				});
				return { close: vi.fn() };
			}
		);

		await graphStore.runRemote(null, 'from_start');
		const state = get(graphStore as any);
		expect(state.runStatus).toBe('paused');
		expect(hasLog(state, 'Event stream closed; reconciling run status')).toBe(true);
		expect(hasLog(state, 'Event stream closed unexpectedly; reconciling run status')).toBe(false);
		expect(hasLog(state, 'Run reconciled via immediate poll (paused)')).toBe(true);
	});

	it('classifies close as unexpected during active run and still reconciles via immediate poll', async () => {
		const graphId = String((get(graphStore as any)?.graphId ?? '').trim());
		const runId = 'run-close-unexpected-running';
		createRunMock.mockResolvedValue({ runId, graphId });
		getRunMock
			.mockResolvedValueOnce({ graphId, status: 'running', nodeBindings: {} })
			.mockResolvedValueOnce({ graphId, status: 'succeeded', nodeBindings: {} });

		streamRunEventsMock.mockImplementation(
			(_id: string, _onEvent: (evt: KnownRunEvent) => void, onError: () => void) => {
				queueMicrotask(() => onError());
				return { close: vi.fn() };
			}
		);

		await graphStore.runRemote(null, 'from_start');
		const state = get(graphStore as any);
		expect(state.runStatus).toBe('succeeded');
		expect(hasLog(state, 'Event stream closed unexpectedly; reconciling run status')).toBe(true);
		expect(hasLog(state, 'Run reconciled via immediate poll (succeeded)')).toBe(true);
	});

	it('handles pause->resume sequence with expected close classification on resume reconnect', async () => {
		const graphId = String((get(graphStore as any)?.graphId ?? '').trim());
		const runId = 'run-pause-resume-close-matrix';
		createRunMock.mockResolvedValue({ runId, graphId });
		resumeRunMock.mockResolvedValue({ ok: true });
		getRunMock.mockImplementation(async () => {
			const idx = getRunMock.mock.calls.length;
			if (idx <= 1) return { graphId, status: 'running', nodeBindings: {} };
			if (idx === 2) return { graphId, status: 'paused', nodeBindings: {} };
			return { graphId, status: 'succeeded', nodeBindings: {} };
		});

		streamRunEventsMock.mockImplementation(
			(_id: string, onEvent: (evt: KnownRunEvent) => void, onError: () => void) => {
				const callIndex = streamRunEventsMock.mock.calls.length;
				if (callIndex === 1) {
					queueMicrotask(() =>
						onEvent({ type: 'run_paused', runId, at: '2026-05-02T01:05:00Z' } as KnownRunEvent)
					);
				} else {
					queueMicrotask(() => {
						onEvent({ type: 'run_resuming', runId, at: '2026-05-02T01:05:10Z' } as KnownRunEvent);
						onError();
					});
				}
				return { close: vi.fn() };
			}
		);

		await graphStore.runRemote(null, 'from_start');
		expect(get(graphStore as any).runStatus).toBe('paused');
		const resumed = await graphStore.resumeActiveRun();
		expect(resumed.ok).toBe(true);
		for (let i = 0; i < 5; i += 1) {
			if (String((get(graphStore as any)?.runStatus ?? '')).toLowerCase() === 'succeeded') break;
			await Promise.resolve();
			await new Promise((resolve) => setTimeout(resolve, 0));
		}

		const state = get(graphStore as any);
		expect(state.runStatus).toBe('succeeded');
		expect(hasLog(state, 'Event stream closed; reconciling run status')).toBe(true);
		expect(hasLog(state, 'Event stream closed unexpectedly; reconciling run status')).toBe(false);
	});
});
