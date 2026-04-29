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
import type { GraphState } from './graphStore.types';

function state(): GraphState {
	return get(graphStore as any) as GraphState;
}

function sourceNodeDoc(
	id: string,
	params: Record<string, unknown>,
	x = 0
): Record<string, unknown> {
	return {
		id,
		type: 'node',
		position: { x, y: 0 },
		data: {
			kind: 'source',
			sourceKind: 'file',
			label: id,
			params,
			status: 'idle'
		}
	};
}

function transformNodeDoc(
	id: string,
	op: string,
	extra: Record<string, unknown> = {},
	x = 120
): Record<string, unknown> {
	return {
		id,
		type: 'node',
		position: { x, y: 0 },
		data: {
			kind: 'transform',
			transformKind: op,
			label: id,
			params: { op, ...extra },
			status: 'idle'
		}
	};
}

function edgeDoc(id: string, source: string, target: string, targetHandle = 'in'): Record<string, unknown> {
	return { id, source, target, targetHandle, data: { exec: 'idle' } };
}

function installSuccessfulRunMocks(graphId: string, runId = 'run-baseline') {
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

beforeEach(() => {
	createRunMock.mockReset();
	getRunMock.mockReset();
	streamRunEventsMock.mockReset();
	graphStore.hardResetGraph();
	graphStore.clearHistory();
});

describe('INT-BASE-01: mixed source precedence baseline', () => {
	it('uses declared > artifact > sample > opaque ordering', () => {
		const loaded = graphStore.loadGraphDocument({
			nodes: [
				sourceNodeDoc('src_declared', {
					sourceKind: 'database',
					declared_schema: {
						fields: [{ name: 'declared_col', type: 'string', nullable: true }]
					},
					introspected_schema: {
						fields: [{ name: 'artifact_col', type: 'string', nullable: true }]
					},
					priming: {
						sample_schema: {
							fields: [{ name: 'sample_col', type: 'string', nullable: true }]
						}
					}
				}),
				sourceNodeDoc(
					'src_artifact',
					{
						sourceKind: 'database',
						introspected_schema: {
							fields: [{ name: 'artifact_only', type: 'string', nullable: true }]
						}
					},
					0
				),
				sourceNodeDoc(
					'src_sample',
					{
						sourceKind: 'file',
						priming: {
							sample_schema: {
								fields: [{ name: 'sample_only', type: 'string', nullable: true }]
							}
						}
					},
					0
				),
				sourceNodeDoc('src_opaque', { sourceKind: 'api' }, 0)
			],
			edges: []
		} as any);
		expect(loaded.ok).toBe(true);
		const s = state();
		const declared = s.schemaPlane.nodeSchemas['src_declared'];
		const artifact = s.schemaPlane.nodeSchemas['src_artifact'];
		const sample = s.schemaPlane.nodeSchemas['src_sample'];
		const opaque = s.schemaPlane.nodeSchemas['src_opaque'];
		expect(declared?.ok).toBe(true);
		expect(artifact?.ok).toBe(true);
		expect(sample?.ok).toBe(true);
		expect(opaque?.ok).toBe(true);
		if (declared?.ok) {
			expect(declared.output.columns.map((c) => c.name)).toEqual(['declared_col']);
			expect(String((declared.output.properties as any)?.sourceProvenance ?? '')).toBe('declared');
		}
		if (artifact?.ok) {
			expect(artifact.output.columns.map((c) => c.name)).toEqual(['artifact_only']);
			expect(String((artifact.output.properties as any)?.sourceProvenance ?? '')).toBe('artifact');
		}
		if (sample?.ok) {
			expect(sample.output.columns.map((c) => c.name)).toEqual(['sample_only']);
			expect(String((sample.output.properties as any)?.sourceProvenance ?? '')).toBe('sample');
		}
		if (opaque?.ok) {
			expect(opaque.output.mode).toBe('opaque');
			expect(String((opaque.output.properties as any)?.sourceProvenance ?? '')).toBe('opaque');
		}
	});
});

describe('INT-BASE-02: run guard structured finding baseline', () => {
	it('returns structured node/code/message fields when blocking active path', async () => {
		const graphId = String((state() as any)?.graphId ?? 'graph-baseline-run-guard');
		installSuccessfulRunMocks(graphId, 'run-baseline-guard');
		graphStore.loadGraphDocument({
			nodes: [
				transformNodeDoc('a', 'filter', {}, 0),
				transformNodeDoc('b', 'filter', {}, 120)
			],
			edges: [
				edgeDoc('e1', 'a', 'b', 'in'),
				edgeDoc('e2', 'b', 'a', 'in')
			]
		} as any);
		const result = await graphStore.runRemote(null, 'from_start');
		expect(result.ok).toBe(false);
		expect((result as any).reason).toBe('schema_errors_in_run_path');
		const errors = Array.isArray((result as any).errors) ? ((result as any).errors as Array<Record<string, unknown>>) : [];
		expect(errors.length).toBeGreaterThan(0);
		for (const entry of errors) {
			expect(String(entry.nodeId ?? '').trim().length).toBeGreaterThan(0);
			expect(String(entry.message ?? '').trim().length).toBeGreaterThan(0);
			if (entry.code !== undefined) {
				expect(String(entry.code).trim().length).toBeGreaterThan(0);
			}
		}
	});
});

describe('REG-BASE-01: legacy graph compatibility baseline', () => {
	it('loads/executes a legacy-style source graph without provenance metadata', async () => {
		const graphId = String((state() as any)?.graphId ?? 'graph-baseline-legacy');
		installSuccessfulRunMocks(graphId, 'run-baseline-legacy');
		const loaded = graphStore.loadGraphDocument({
			nodes: [
				{
					id: 'src_legacy',
					type: 'node',
					position: { x: 0, y: 0 },
					data: {
						kind: 'source',
						sourceKind: 'file',
						label: 'Legacy Source',
						params: {},
						status: 'idle'
					}
				}
			],
			edges: []
		} as any);
		expect(loaded.ok).toBe(true);
		const result = await graphStore.runRemote(null, 'from_start');
		expect(result.ok).toBe(true);
	});
});

