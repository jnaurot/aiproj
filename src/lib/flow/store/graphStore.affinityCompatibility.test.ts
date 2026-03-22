import { describe, expect, it } from 'vitest';

import { __computeEdgeSchemaConstraintsForTest } from './graphStore';

describe('graphStore affinity-specific compatibility', () => {
	it('enforces payload type only for work edges', () => {
		const nodes: any[] = [
			{
				id: 'src_json',
				data: {
					kind: 'source',
					sourceKind: 'api',
					params: {},
					schema: {
						expectedSchema: { source: 'declared', typedSchema: { type: 'json', fields: [] } }
					}
				}
			},
			{
				id: 'dst',
				data: {
					kind: 'transform',
					transformKind: 'select',
					params: { op: 'select', select: { mode: 'include', columns: [] } },
					schema: {
						expectedInputSchemas: {
							in: { source: 'declared', typedSchema: { type: 'text', fields: [] } },
							param_config: { source: 'declared', typedSchema: { type: 'text', fields: [] } },
							control_in: { source: 'declared', typedSchema: { type: 'text', fields: [] } }
						}
					}
				}
			}
		];
		const edges: any[] = [
			{ id: 'e_work', source: 'src_json', target: 'dst', targetHandle: 'in', data: { mode: 'work' } },
			{
				id: 'e_param',
				source: 'src_json',
				target: 'dst',
				targetHandle: 'param_config',
				data: { mode: 'param' }
			},
			{
				id: 'e_control',
				source: 'src_json',
				target: 'dst',
				targetHandle: 'control_in',
				data: { mode: 'control' }
			}
		];

		const constraints = __computeEdgeSchemaConstraintsForTest(nodes as any, edges as any);
		expect(constraints.e_work?.compatible).toBe(false);
		expect(constraints.e_param?.compatible).toBe(true);
		expect(constraints.e_control?.compatible).toBe(true);
	});
});
