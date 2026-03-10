import { get } from 'svelte/store';
import { describe, expect, it } from 'vitest';

import { graphStore } from './graphStore';

describe('graphStore expected schema authoring', () => {
	it('persists and clears node expected schema envelope', () => {
		graphStore.hardResetGraph();
		const nodeId = graphStore.addNode('source', { x: 16, y: 16 });
		const saveResult = graphStore.setNodeExpectedSchema(nodeId, {
			type: 'table',
			fields: [{ name: 'id', type: 'json', nullable: false }]
		});
		expect((saveResult as any)?.ok).toBe(true);

		const afterSave = get(graphStore);
		const nodeAfterSave = afterSave.nodes.find((n) => n.id === nodeId);
		expect((nodeAfterSave?.data as any)?.schema?.expectedSchema?.source).toBe('declared');
		expect((nodeAfterSave?.data as any)?.schema?.expectedSchema?.typedSchema?.type).toBe('table');

		const clearResult = graphStore.setNodeExpectedSchema(nodeId, null);
		expect((clearResult as any)?.ok).toBe(true);
		const afterClear = get(graphStore);
		const nodeAfterClear = afterClear.nodes.find((n) => n.id === nodeId);
		expect((nodeAfterClear?.data as any)?.schema?.expectedSchema).toBeUndefined();
	});

	it('save preflight fails when expected schema type mismatches derived output type', () => {
		graphStore.hardResetGraph();
		const applied = graphStore.loadGraphDocument(
			{
				nodes: [
					{
						id: 'src_text',
						type: 'default',
						position: { x: 0, y: 0 },
						data: {
							kind: 'source',
							label: 'Source',
							sourceKind: 'file',
							params: { file_format: 'txt', output_mode: 'text' },
							schema: {
								expectedSchema: {
									source: 'declared',
									typedSchema: { type: 'table', fields: [] }
								}
							},
							status: 'idle'
						}
					}
				],
				edges: []
			},
			'graph_expected_schema_preflight'
		);
		expect(applied.ok).toBe(true);

		const preflight = graphStore.getSavePreflight();
		expect(preflight.ok).toBe(false);
		expect(preflight.diagnostics.some((d) => d.code === 'EXPECTED_SCHEMA_PORT_MISMATCH')).toBe(true);
	});
});
