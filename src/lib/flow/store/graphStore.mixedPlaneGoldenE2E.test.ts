import { describe, expect, it } from 'vitest';
import { get } from 'svelte/store';

import {
	__applyRunEventForTest,
	__computeEdgeSchemaConstraintsForTest,
	__computeEdgeSchemaDiagnosticsForTest,
	graphStore
} from './graphStore';

describe('graphStore mixed-plane golden contract e2e', () => {
	it('keeps work/param/control edges compatible in one graph', () => {
		const nodes: any[] = [
			{
				id: 'work_src',
				data: {
					kind: 'source',
					sourceKind: 'api',
					params: {},
					schema: { expectedSchema: { source: 'declared', typedSchema: { type: 'json', fields: [] } } }
				}
			},
			{
				id: 'param_src',
				data: {
					kind: 'source',
					sourceKind: 'api',
					params: {},
					schema: { expectedSchema: { source: 'declared', typedSchema: { type: 'json', fields: [] } } }
				}
			},
			{
				id: 'control_src',
				data: {
					kind: 'source',
					sourceKind: 'api',
					params: {},
					schema: { expectedSchema: { source: 'declared', typedSchema: { type: 'json', fields: [] } } }
				}
			},
			{
				id: 'sink',
				data: {
					kind: 'tool',
					params: { provider: 'builtin', builtin: { toolId: 'noop' } },
					schema: {
						expectedInputSchemas: {
							in: { source: 'declared', typedSchema: { type: 'json', fields: [] } },
							param_filters: { source: 'declared', typedSchema: { type: 'json', fields: [] } },
							control_in: { source: 'declared', typedSchema: { type: 'text', fields: [] } }
						}
					}
				}
			}
		];
		const edges: any[] = [
			{ id: 'e_work', source: 'work_src', sourceHandle: 'out', target: 'sink', targetHandle: 'in', data: { mode: 'work' } },
			{
				id: 'e_param',
				source: 'param_src',
				sourceHandle: 'out',
				target: 'sink',
				targetHandle: 'param_filters',
				data: {
					mode: 'param',
					contract: {
						payload: {
							source: { keys: ['location', 'salary'] },
							target: { requiredKeys: ['location'] }
						}
					}
				}
			},
			{
				id: 'e_control',
				source: 'control_src',
				sourceHandle: 'control_out',
				target: 'sink',
				targetHandle: 'control_in',
				data: { mode: 'control' }
			}
		];

		const constraints = __computeEdgeSchemaConstraintsForTest(nodes as any, edges as any);
		expect(constraints.e_work?.compatible).toBe(true);
		expect(constraints.e_param?.compatible).toBe(true);
		expect(constraints.e_control?.compatible).toBe(true);

		const diagnostics = __computeEdgeSchemaDiagnosticsForTest(constraints as any);
		expect(diagnostics.e_work).toBeNull();
		expect(diagnostics.e_param).toBeNull();
		expect(diagnostics.e_control).toBeNull();
	});

	it('captures mixed-plane runtime/reject telemetry into queueRuntime and logs', () => {
		graphStore.hardResetGraph();
		const sink = graphStore.addNode('tool', { x: 0, y: 0 });
		const base = get(graphStore as any);
		let next = __applyRunEventForTest(
			base as any,
			{
				type: 'queue_metrics',
				runId: 'run_mixed_plane',
				at: new Date().toISOString(),
				metrics: { globalDepth: 1, globalMax: 10, perEdgeMax: 5, edges: {} },
				nodeMetrics: {},
				runtimeItemMetrics: {
					itemsEnqueued: 1,
					itemsDequeued: 1,
					itemsRejected: 0,
					itemsAccepted: 1,
					byPlane: {
						work: { itemsEnqueued: 1, itemsDequeued: 1, itemsAccepted: 1, itemsRejected: 0 },
						param: { itemsEnqueued: 0, itemsDequeued: 0, itemsAccepted: 0, itemsRejected: 0 },
						control: { itemsEnqueued: 0, itemsDequeued: 0, itemsAccepted: 0, itemsRejected: 0 }
					},
					byHandle: {
						[`${sink}:in`]: {
							nodeId: sink,
							handle: 'in',
							plane: 'work',
							itemsEnqueued: 1,
							itemsDequeued: 1,
							itemsAccepted: 1,
							itemsRejected: 0
						}
					}
				}
			} as any,
			'run_mixed_plane'
		);
		next = __applyRunEventForTest(
			next as any,
			{
				type: 'node_reject',
				runId: 'run_mixed_plane',
				at: new Date().toISOString(),
				nodeId: sink,
				plane: 'work',
				reasonCode: 'FILTERED_OUT',
				count: 1
			} as any,
			'run_mixed_plane'
		);
		expect((next as any)?.queueRuntime?.runtimeItemMetrics?.byPlane?.work?.itemsAccepted).toBe(1);
		const lastLog = (next as any)?.logs?.[(next as any)?.logs?.length - 1];
		expect(String(lastLog?.message ?? '')).toContain('[reject]');
	});
});
