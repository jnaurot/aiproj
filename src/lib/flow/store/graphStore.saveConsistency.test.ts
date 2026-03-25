import { describe, expect, it } from 'vitest';

import { __computeSaveConsistencyMismatchForTest } from './graphStore';

describe('graphStore save consistency mismatch detection', () => {
	it('returns null when canvas and persisted graphs match', () => {
		const graph = {
			version: 1,
			nodes: [
				{
					id: 'n1',
					type: 'default',
					position: { x: 0, y: 0 },
					data: { kind: 'transform', label: 'TransformSelect', transformKind: 'select', params: { op: 'select' } }
				}
			],
			edges: []
		} as any;
		const mismatch = __computeSaveConsistencyMismatchForTest(graph, graph);
		expect(mismatch).toBeNull();
	});

	it('detects missing nodes and changed edges', () => {
		const canvas = {
			version: 1,
			nodes: [
				{
					id: 'n_select',
					type: 'default',
					position: { x: 0, y: 0 },
					data: { kind: 'transform', label: 'TransformSelect', transformKind: 'select', params: { op: 'select' } }
				},
				{
					id: 'n_model',
					type: 'default',
					position: { x: 220, y: 0 },
					data: { kind: 'model', label: 'Model_ScoreJob', llmKind: 'ollama', params: {} }
				}
			],
			edges: [
				{
					id: 'e1',
					source: 'n_select',
					sourceHandle: 'out',
					target: 'n_model',
					targetHandle: 'in',
					data: { mode: 'work' }
				}
			]
		} as any;
		const persisted = {
			version: 1,
			nodes: [
				{
					id: 'n_model',
					type: 'default',
					position: { x: 220, y: 0 },
					data: { kind: 'model', label: 'Model_ScoreJob', llmKind: 'ollama', params: {} }
				}
			],
			edges: [
				{
					id: 'e1',
					source: 'n_model',
					sourceHandle: 'out',
					target: 'n_model',
					targetHandle: 'in',
					data: { mode: 'work' }
				}
			]
		} as any;
		const mismatch = __computeSaveConsistencyMismatchForTest(canvas, persisted);
		expect(mismatch).not.toBeNull();
		expect(mismatch?.missingNodes.map((entry) => entry.id)).toContain('n_select');
		expect(mismatch?.changedEdges.map((entry) => entry.id)).toContain('e1');
	});

	it('does not block on changed-only deltas when structure is unchanged', () => {
		const canvas = {
			version: 1,
			nodes: [
				{
					id: 'n1',
					type: 'default',
					position: { x: 0, y: 0 },
					data: { kind: 'transform', label: 'Transform', transformKind: 'filter', params: { op: 'filter', expr: '1=1' } }
				}
			],
			edges: [
				{
					id: 'e1',
					source: 'n1',
					sourceHandle: 'out',
					target: 'n1',
					targetHandle: 'in',
					data: { mode: 'work', contract: { out: 'table', in: 'table' } }
				}
			]
		} as any;
		const persisted = {
			version: 1,
			nodes: [
				{
					id: 'n1',
					type: 'default',
					position: { x: 0, y: 0 },
					data: { kind: 'transform', label: 'Transform', transformKind: 'filter', params: { op: 'filter', expr: '2=2' } }
				}
			],
			edges: [
				{
					id: 'e1',
					source: 'n1',
					sourceHandle: 'out',
					target: 'n1',
					targetHandle: 'in',
					data: { mode: 'work', contract: { out: 'json', in: 'json' } }
				}
			]
		} as any;
		const mismatch = __computeSaveConsistencyMismatchForTest(canvas, persisted);
		expect(mismatch).toBeNull();
	});
});
