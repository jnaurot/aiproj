import { describe, expect, it } from 'vitest';

import {
	__computeEdgeSchemaConstraintsForTest,
	__computeEdgeSchemaDiagnosticsForTest
} from './graphStore';

describe('graphStore coercion warning diagnostics', () => {
	it('emits warning diagnostics for allowed lossy coercion', () => {
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
				id: 'dst_text',
				data: {
					kind: 'model',
					modelKind: 'llm',
					params: { coercion_policy: 'allow_lossy' },
					schema: {
						expectedInputSchemas: {
							in: { source: 'declared', typedSchema: { type: 'text', fields: [] } }
						}
					}
				}
			}
		];
		const edges: any[] = [{ id: 'e1', source: 'src_json', target: 'dst_text', targetHandle: 'in', data: { mode: 'work' } }];

		const constraints = __computeEdgeSchemaConstraintsForTest(nodes as any, edges as any);
		expect(constraints.e1?.compatible).toBe(true);
		expect(constraints.e1?.warning).toBe('lossy_coercion');

		const diagnostics = __computeEdgeSchemaDiagnosticsForTest(constraints as any);
		expect(diagnostics.e1?.severity).toBe('warning');
		expect(String(diagnostics.e1?.message ?? '')).toContain('lossy coercion');
	});
});
