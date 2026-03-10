import { describe, expect, it } from 'vitest';

import {
	__computeEdgeSchemaConstraintsForTest,
	__computeEdgeSchemaDiagnosticsForTest
} from './graphStore';

describe('graphStore edge schema diagnostics', () => {
	it('emits structured diagnostics for incompatible edges', () => {
		const nodes: any[] = [
			{
				id: 'n_source',
				data: {
					kind: 'source',
					sourceKind: 'file',
					ports: { in: null, out: 'text' },
					params: { file_format: 'txt' }
				}
			},
			{
				id: 'n_transform',
				data: {
					kind: 'transform',
					transformKind: 'filter',
					ports: { in: 'binary', out: 'table' },
					params: {
						op: 'filter',
						filter: { expr: '' }
					}
				}
			}
		];
		const edges: any[] = [{ id: 'e1', source: 'n_source', target: 'n_transform' }];
		const constraints = __computeEdgeSchemaConstraintsForTest(nodes as any, edges as any);
		const diagnostics = __computeEdgeSchemaDiagnosticsForTest(constraints);
		expect(diagnostics.e1).toBeTruthy();
		expect(diagnostics.e1?.code).toBe('EDGE_SCHEMA_TYPE_MISMATCH');
		expect(diagnostics.e1?.severity).toBe('error');
		expect(diagnostics.e1?.details?.providedSchema?.type).toBe('string');
		expect(diagnostics.e1?.details?.requiredSchema?.type).toBe('binary');
	});
});
