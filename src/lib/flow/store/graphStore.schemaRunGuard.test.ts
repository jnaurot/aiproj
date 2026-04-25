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

	it('proceeds when schema errors are upstream of checkpoint boundary for selected run', async () => {
		const graphId = String(get(graphStore as any)?.graphId ?? 'graph-schema-run-checkpoint-cut');
		installSuccessfulRunMocks(graphId, 'run-checkpoint-cut');
		graphStore.loadGraphDocument({
			nodes: [
				{
					id: 'up_a',
					position: { x: 0, y: 0 },
					data: { kind: 'transform', transformKind: 'filter', status: 'idle', label: 'Up A', params: {} }
				},
				{
					id: 'up_b',
					position: { x: 100, y: 0 },
					data: { kind: 'transform', transformKind: 'filter', status: 'idle', label: 'Up B', params: {} }
				},
				{
					id: 'cp',
					position: { x: 220, y: 0 },
					data: { kind: 'transform', transformKind: 'filter', status: 'idle', label: 'Checkpointed', params: {} }
				},
				{
					id: 'target',
					position: { x: 360, y: 0 },
					data: { kind: 'source', sourceKind: 'file', status: 'idle', label: 'Target', params: {} }
				}
			],
			edges: [
				{ id: 'e1', source: 'up_a', target: 'up_b', targetHandle: 'in', data: { exec: 'idle' } },
				{ id: 'e2', source: 'up_b', target: 'up_a', targetHandle: 'in', data: { exec: 'idle' } },
				{ id: 'e3', source: 'up_a', target: 'cp', targetHandle: 'in', data: { exec: 'idle' } },
				{ id: 'e4', source: 'cp', target: 'target', targetHandle: 'in', data: { exec: 'idle' } }
			],
			checkpointRegistry: {
				cp: {
					id: '00000000-0000-4000-8000-000000000201',
					name: 'ck-cp',
					nodeId: 'cp',
					graphId,
					runId: 'run-ck',
					artifactId: 'art-cp',
					execKey: 'exec-cp',
					fingerprintAtCreation: 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
					createdAt: '2026-04-01T00:00:00.000Z',
					staleness: 'valid'
				}
			}
		} as any);

		const result = await graphStore.runRemote('target', 'from_selected_onward');
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

	it('does not block run for additional-properties uncertainty warnings in run path', async () => {
		const graphId = String(get(graphStore as any)?.graphId ?? 'graph-schema-run-additional-props-warning');
		installSuccessfulRunMocks(graphId, 'run-additional-props-warning');
		graphStore.loadGraphDocument({
			nodes: [
				{
					id: 'llm_json',
					position: { x: 0, y: 0 },
					data: {
						kind: 'llm',
						label: 'LLM',
						status: 'idle',
						params: {
							output: {
								mode: 'json',
								jsonSchema: {
									type: 'object',
									properties: { title: { type: 'string' } },
									additionalProperties: true
								}
							}
						}
					}
				},
				{
					id: 'sel',
					position: { x: 120, y: 0 },
					data: {
						kind: 'transform',
						transformKind: 'select',
						status: 'idle',
						label: 'Select',
						params: { op: 'select', select: { columns: ['candidate_required_location'] } }
					}
				}
			],
			edges: [{ id: 'e1', source: 'llm_json', target: 'sel', targetHandle: 'in', data: { exec: 'idle' } }]
		} as any);
		const result = await graphStore.runRemote('sel', 'from_selected_onward');
		expect(result.ok).toBe(true);
		expect(createRunMock).toHaveBeenCalledTimes(1);
	});
});

