import { describe, expect, it } from 'vitest';

import type { NodeSchemaContractEdge } from '$lib/flow/store/graphStore';
import {
	buildTransformAutoFixes,
	buildTransformPreviewDiff,
	guidedControlsForTransform,
	suggestNextTransformOps
} from './transformAssist';

describe('transformAssist', () => {
	it('returns guided controls for transform kinds', () => {
		const controls = guidedControlsForTransform('select');
		expect(controls.length).toBeGreaterThan(2);
		expect(controls[0].label.length).toBeGreaterThan(0);
	});

	it('builds auto-fix patches for missing select columns', () => {
		const fixes = buildTransformAutoFixes({
			kind: 'select',
			params: { op: 'select', select: { mode: 'include', strict: true, keepOrder: 'custom', columns: ['a', 'b'] } },
			nodeError: {
				errorCode: 'MISSING_COLUMN',
				paramPath: 'select.columns',
				missingColumns: ['b'],
				availableColumns: ['a', 'c']
			},
			availableColumns: ['a', 'c']
		});
		expect(fixes.some((f) => f.id === 'select_drop_missing')).toBe(true);
		expect(fixes.some((f) => f.id === 'select_disable_strict')).toBe(true);
	});

	it('projects preview diff for rename transform', () => {
		const diff = buildTransformPreviewDiff({
			kind: 'rename',
			params: { op: 'rename', rename: { map: { text: 'body' } } },
			inputColumns: ['id', 'text'],
			sampleRows: [{ id: 1, text: 'a' }]
		});
		expect(diff.beforeColumns).toEqual(['id', 'text']);
		expect(diff.afterColumns).toEqual(['id', 'body']);
		expect(diff.afterRows[0]).toEqual({ id: 1, body: 'a' });
	});

	it('suggests adapter ops from schema mismatch', () => {
		const edges: NodeSchemaContractEdge[] = [
			{
				edgeId: 'e1',
				direction: 'incoming',
				severity: 'error',
				sourceNodeId: 'src',
				targetNodeId: 'tr',
				sourceHandle: 'out',
				targetHandle: 'in',
				providedSchema: { type: 'string', fields: [] },
				requiredSchema: { type: 'table', fields: [] },
				suggestions: [],
				adapterKind: null
			}
		];
		const suggestions = suggestNextTransformOps({
			kind: 'filter',
			nodeError: null,
			schemaEdges: edges
		});
		expect(suggestions.some((s) => s.op === 'text_to_table')).toBe(true);
	});
});
