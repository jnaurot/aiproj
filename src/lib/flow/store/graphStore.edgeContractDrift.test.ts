import { describe, expect, it } from 'vitest';

import {
	__computeEdgeSchemaConstraintsForTest,
	__computeEdgeSchemaDiagnosticsForTest
} from './graphStore';

describe('graphStore edge contract drift diagnostics', () => {
	it('marks edge snapshot drift and emits warning guidance', () => {
		const nodes: any[] = [
			{ id: 'src', data: { kind: 'source', params: { output: { mode: 'text' } } } },
			{
				id: 'dst',
				data: {
					kind: 'model',
					schema: {
						expectedInputSchemas: {
							in: { typedSchema: { type: 'text', fields: [] } }
						}
					}
				}
			}
		];
		const edges: any[] = [
			{
				id: 'e_drift',
				source: 'src',
				target: 'dst',
				sourceHandle: 'out',
				targetHandle: 'in',
				data: {
					mode: 'work',
					contract: {
						snapshot: {
							sourceSchemaFingerprint: '{"type":"json"}',
							targetSchemaFingerprint: '{"type":"json"}',
							compatible: true,
							decision: 'native'
						}
					}
				}
			}
		];
		const constraints = __computeEdgeSchemaConstraintsForTest(nodes as any, edges as any);
		expect(constraints.e_drift?.snapshotDrift).toBe(true);
		const diagnostics = __computeEdgeSchemaDiagnosticsForTest(constraints as any);
		expect(diagnostics.e_drift?.severity).toBe('warning');
		expect(String(diagnostics.e_drift?.message ?? '')).toContain('snapshot drift detected');
		expect(diagnostics.e_drift?.suggestions?.length ?? 0).toBeGreaterThan(0);
	});
});

