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
					params: { file_format: 'txt' },
					schema: {
						inferredSchema: {
							source: 'sample',
							state: 'fresh',
							typedSchema: { type: 'text', fields: [] }
						}
					}
				}
			},
			{
				id: 'n_transform',
				data: {
					kind: 'transform',
					transformKind: 'filter',
					params: {
						op: 'filter',
						filter: { expr: '' }
					},
					schema: {
						expectedSchema: {
							source: 'declared',
							typedSchema: { type: 'binary', fields: [] }
						}
					}
				}
			}
		];
		const edges: any[] = [{ id: 'e1', source: 'n_source', target: 'n_transform' }];
		const constraints = __computeEdgeSchemaConstraintsForTest(nodes as any, edges as any);
		const diagnostics = __computeEdgeSchemaDiagnosticsForTest(constraints);
		expect(diagnostics.e1).toBeTruthy();
		expect(diagnostics.e1?.code).toBe('TYPE_MISMATCH');
		expect(diagnostics.e1?.severity).toBe('error');
		expect(diagnostics.e1?.details?.providedSchema?.type).toBe('string');
		expect(diagnostics.e1?.details?.requiredSchema?.type).toBe('binary');
	});

	it('emits warning diagnostics for lossy coercions', () => {
		const nodes: any[] = [
			{
				id: 'n_source',
				data: {
					kind: 'source',
					sourceKind: 'api',
					params: {},
					schema: {
						inferredSchema: {
							source: 'sample',
							state: 'fresh',
							typedSchema: { type: 'json', fields: [] }
						}
					}
				}
			},
			{
				id: 'n_llm',
				data: {
					kind: 'llm',
					params: { coercion_policy: 'allow_lossy' },
					schema: {
						expectedSchema: {
							source: 'declared',
							typedSchema: { type: 'text', fields: [] }
						}
					}
				}
			}
		];
		const edges: any[] = [{ id: 'e2', source: 'n_source', target: 'n_llm' }];
		const constraints = __computeEdgeSchemaConstraintsForTest(nodes as any, edges as any);
		const diagnostics = __computeEdgeSchemaDiagnosticsForTest(constraints);
		expect(diagnostics.e2).toBeTruthy();
		expect(diagnostics.e2?.severity).toBe('warning');
	});

	it('emits payload diagnostics when required typed coverage is missing', () => {
		const nodes: any[] = [
			{
				id: 'n_source',
				data: {
					kind: 'source',
					sourceKind: 'file',
					params: { file_format: 'csv' },
					schema: {
						inferredSchema: {
							source: 'sample',
							state: 'fresh',
							typedSchema: { type: 'table', fields: [] }
						}
					}
				}
			},
			{
				id: 'n_transform',
				data: {
					kind: 'transform',
					transformKind: 'select',
					params: { op: 'select', select: { mode: 'include', columns: ['id'] } }
				}
			}
		];
		const edges: any[] = [{ id: 'e3', source: 'n_source', target: 'n_transform' }];
		const constraints = __computeEdgeSchemaConstraintsForTest(nodes as any, edges as any);
		const diagnostics = __computeEdgeSchemaDiagnosticsForTest(constraints);
		expect(diagnostics.e3).toBeTruthy();
		expect(diagnostics.e3?.code).toBe('PAYLOAD_SCHEMA_MISMATCH');
		expect(String(diagnostics.e3?.message ?? '')).toContain('Required typed schema coverage is missing');
	});
});
