import { describe, expect, it } from 'vitest';

import { __computeEdgeSchemaConstraintsForTest } from './graphStore';

describe('graphStore handle-aware required schema resolution', () => {
	it('resolves required schema from expectedInputSchemas[targetHandle] with in fallback', () => {
		const nodes: any[] = [
			{
				id: 'src_text',
				data: {
					kind: 'source',
					sourceKind: 'file',
					params: { file_format: 'txt', output: { mode: 'text' } },
					schema: {
						expectedSchema: { source: 'declared', typedSchema: { type: 'text', fields: [] } }
					}
				}
			},
			{
				id: 'target',
				data: {
					kind: 'model',
					params: {},
					schema: {
						expectedInputSchemas: {
							in: { source: 'declared', typedSchema: { type: 'text', fields: [] } },
							param_config: { source: 'declared', typedSchema: { type: 'json', fields: [] } }
						}
					}
				}
			}
		];

		const constraints = __computeEdgeSchemaConstraintsForTest(nodes as any, [
			{ id: 'e_in', source: 'src_text', target: 'target', targetHandle: 'in', data: { mode: 'work' } } as any,
			{
				id: 'e_unknown',
				source: 'src_text',
				target: 'target',
				targetHandle: 'unknown',
				data: { mode: 'work' }
			} as any,
			{
				id: 'e_param_handle',
				source: 'src_text',
				target: 'target',
				targetHandle: 'param_config',
				data: { mode: 'work' }
			} as any
		] as any);

		expect(constraints.e_in?.compatible).toBe(true);
		expect(constraints.e_unknown?.compatible).toBe(true);
		expect(constraints.e_param_handle?.compatible).toBe(false);
		expect(constraints.e_param_handle?.reason).toBe('type_mismatch');
	});
});
