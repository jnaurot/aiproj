import { beforeEach, describe, expect, it, vi } from 'vitest';
import { get } from 'svelte/store';

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

function installSuccessfulRunMocks(graphId: string, runId = 'run-schema-guard') {
	createRunMock.mockResolvedValue({ runId, graphId });
	getRunMock.mockResolvedValue({
		graphId,
		runId,
		status: 'succeeded',
		runMode: 'from_start',
		plannedNodeIds: [],
		nodeBindings: {}
	});
	streamRunEventsMock.mockImplementation((_streamRunId: string, onEvent: (evt: KnownRunEvent) => void) => {
		queueMicrotask(() =>
			onEvent({
				type: 'run_finished',
				runId,
				at: new Date().toISOString(),
				status: 'succeeded'
			} as KnownRunEvent)
		);
		return { close: vi.fn() };
	});
}

describe('graphStore schema-aware pre-run guard', () => {
	beforeEach(() => {
		createRunMock.mockReset();
		getRunMock.mockReset();
		streamRunEventsMock.mockReset();
		graphStore.hardResetGraph();
		graphStore.clearHistory();
	});

	it('dispatches immediately when there are no schema errors', async () => {
		const graphId = String(get(graphStore as any)?.graphId ?? 'graph-schema-run-ok');
		installSuccessfulRunMocks(graphId, 'run-no-errors');
		const sourceId = graphStore.addNode('source', { x: 0, y: 0 });
		const result = await graphStore.runRemote(sourceId, 'from_selected_onward');
		expect(result.ok).toBe(true);
		expect(createRunMock).toHaveBeenCalledTimes(1);
	});

	it('blocks run when schema errors exist in run path', async () => {
		const graphId = String(get(graphStore as any)?.graphId ?? 'graph-schema-run-blocked');
		installSuccessfulRunMocks(graphId, 'run-blocked');
		graphStore.loadGraphDocument({
			nodes: [
				{
					id: 'a',
					position: { x: 0, y: 0 },
					data: { kind: 'transform', transformKind: 'filter', status: 'idle', label: 'A', params: {} }
				},
				{
					id: 'b',
					position: { x: 120, y: 0 },
					data: { kind: 'transform', transformKind: 'filter', status: 'idle', label: 'B', params: {} }
				}
			],
			edges: [
				{ id: 'e1', source: 'a', target: 'b', targetHandle: 'in', data: { exec: 'idle' } },
				{ id: 'e2', source: 'b', target: 'a', targetHandle: 'in', data: { exec: 'idle' } }
			]
		} as any);

		const result = await graphStore.runRemote(null, 'from_start');
		expect(result.ok).toBe(false);
		expect((result as any).reason).toBe('schema_errors_in_run_path');
		expect(createRunMock).toHaveBeenCalledTimes(0);
		expect((get(graphStore as any) as any).runBlockedReason?.type).toBe('schema_errors_in_run_path');
	});

	it('proceeds when schema errors are outside selected run path', async () => {
		const graphId = String(get(graphStore as any)?.graphId ?? 'graph-schema-run-outside');
		installSuccessfulRunMocks(graphId, 'run-outside-path');
		graphStore.loadGraphDocument({
			nodes: [
				{
					id: 'safe',
					position: { x: 0, y: 0 },
					data: { kind: 'source', sourceKind: 'file', status: 'idle', label: 'Safe', params: {} }
				},
				{
					id: 'a',
					position: { x: 250, y: 0 },
					data: { kind: 'transform', transformKind: 'filter', status: 'idle', label: 'A', params: {} }
				},
				{
					id: 'b',
					position: { x: 370, y: 0 },
					data: { kind: 'transform', transformKind: 'filter', status: 'idle', label: 'B', params: {} }
				}
			],
			edges: [
				{ id: 'e1', source: 'a', target: 'b', targetHandle: 'in', data: { exec: 'idle' } },
				{ id: 'e2', source: 'b', target: 'a', targetHandle: 'in', data: { exec: 'idle' } }
			]
		} as any);

		const result = await graphStore.runRemote('safe', 'from_selected_onward');
		expect(result.ok).toBe(true);
		expect(createRunMock).toHaveBeenCalledTimes(1);
		expect((get(graphStore as any) as any).runBlockedReason).toBeNull();
	});

	it('allowSchemaErrors bypasses block and increments dismissal counter', async () => {
		const graphId = String(get(graphStore as any)?.graphId ?? 'graph-schema-run-proceed');
		installSuccessfulRunMocks(graphId, 'run-proceed-errors');
		graphStore.loadGraphDocument({
			nodes: [
				{
					id: 'a',
					position: { x: 0, y: 0 },
					data: { kind: 'transform', transformKind: 'filter', status: 'idle', label: 'A', params: {} }
				},
				{
					id: 'b',
					position: { x: 120, y: 0 },
					data: { kind: 'transform', transformKind: 'filter', status: 'idle', label: 'B', params: {} }
				}
			],
			edges: [
				{ id: 'e1', source: 'a', target: 'b', targetHandle: 'in', data: { exec: 'idle' } },
				{ id: 'e2', source: 'b', target: 'a', targetHandle: 'in', data: { exec: 'idle' } }
			]
		} as any);

		const blocked = await graphStore.runRemote(null, 'from_start');
		expect(blocked.ok).toBe(false);

		const proceeded = await graphStore.runRemote(null, 'from_start', 'default_on', null, {
			allowSchemaErrors: true
		});
		expect(proceeded.ok).toBe(true);
		expect(createRunMock).toHaveBeenCalledTimes(1);
		expect((get(graphStore as any) as any).schemaWarningDismissCount).toBe(1);
	});
});

