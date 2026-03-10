import { describe, expect, it } from 'vitest';

import {
	__computeEdgeSchemaConstraintsForTest,
	__normalizeSchemaFieldsForTest
} from './graphStore';

describe('graphStore typed schema field propagation', () => {
	it('prefers non-unknown field definitions while deduping by name', () => {
		const normalized = __normalizeSchemaFieldsForTest([
			{ name: 'score', type: 'unknown', nullable: true },
			{ name: 'score', type: 'float', nullable: false },
			{ name: 'label', type: 'string', nullable: true }
		]);
		expect(normalized).toEqual([
			{ name: 'score', type: 'float', nullable: false, constraints: undefined },
			{ name: 'label', type: 'string', nullable: true, constraints: undefined }
		]);
	});

	it('uses typed required_fields for compatibility checks with legacy fallback intact', () => {
		const nodes: any[] = [
			{
				id: 'n_source',
				data: {
					kind: 'source',
					sourceKind: 'file',
					ports: { in: null, out: 'table' },
					params: { file_format: 'txt' }
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
		expect(constraints.e1.reason).toBe('missing_required_columns');
		expect(constraints.e1.requiredSchema.required_fields).toEqual([
			{ name: 'id', type: 'unknown', nullable: true, constraints: undefined }
		]);
		expect(constraints.e1.requiredSchema.required_columns).toEqual(['id']);
		expect(constraints.e1.providedSchema.fields).toEqual([
			{ name: 'text', type: 'string', nullable: true, constraints: undefined }
		]);
		expect(constraints.e1.providedSchema.columns).toEqual(['text']);
	});
});
