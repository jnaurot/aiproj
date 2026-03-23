import { describe, expect, it } from 'vitest';

import {
	__computeEdgeSchemaConstraintsForTest,
	__computeEdgeSchemaDiagnosticsForTest
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
});
