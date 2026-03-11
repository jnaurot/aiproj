import { describe, expect, it } from 'vitest';

import { __computeEdgeSchemaConstraintsForTest } from './graphStore';

describe('graphStore schema solver', () => {
	it('computes deterministic provided/required schemas per edge', () => {
		const nodes: any[] = [
			{
				id: 'n_source',
				data: {
					kind: 'source',
					sourceKind: 'file',
					ports: { in: null, out: 'table' },
					params: { file_format: 'csv' }
				}
			},
			{
				id: 'n_transform',
				data: {
					kind: 'transform',
					transformKind: 'select',
					ports: { in: 'table', out: 'table' },
					params: {
						op: 'select',
						select: { mode: 'include', columns: ['id'], strict: true }
					}
				}
			}
		];
		const edges: any[] = [{ id: 'e1', source: 'n_source', target: 'n_transform' }];
		const constraints = __computeEdgeSchemaConstraintsForTest(nodes as any, edges as any);
		expect(constraints.e1).toBeTruthy();
		expect(constraints.e1.providedSchema.type).toBe('table');
		expect(constraints.e1.requiredSchema.type).toBe('table');
		expect(constraints.e1.requiredSchema.required_columns).toEqual(['id']);
		expect(constraints.e1.compatible).toBe(false);
		expect(constraints.e1.reason).toBe('missing_typed_schema');
	});

	it('infers adapter suggestion for coercible type mismatch', () => {
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
					ports: { in: 'table', out: 'table' },
					params: {
						op: 'filter',
						filter: { expr: '' }
					}
				}
			}
		];
		const edges: any[] = [{ id: 'e1', source: 'n_source', target: 'n_transform' }];
		const constraints = __computeEdgeSchemaConstraintsForTest(nodes as any, edges as any);
		expect(constraints.e1.compatible).toBe(true);
		expect(constraints.e1.suggestions).toEqual([]);
	});

	it('fails when required typed schema exists but source cannot emit typed coverage', () => {
		const nodes: any[] = [
			{
				id: 'n_source',
				data: {
					kind: 'source',
					sourceKind: 'file',
					ports: { in: null, out: 'table' },
					params: { file_format: 'csv' }
				}
			},
			{
				id: 'n_transform',
				data: {
					kind: 'transform',
					transformKind: 'select',
					ports: { in: 'table', out: 'table' },
					params: {
						op: 'select',
						select: { mode: 'include', columns: ['id'] }
					}
				}
			}
		];
		const edges: any[] = [{ id: 'e1', source: 'n_source', target: 'n_transform' }];
		const constraints = __computeEdgeSchemaConstraintsForTest(nodes as any, edges as any);
		expect(constraints.e1.compatible).toBe(false);
		expect(constraints.e1.reason).toBe('missing_typed_schema');
		expect(constraints.e1.missingColumns).toEqual(['id']);
	});
});
