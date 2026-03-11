import { describe, expect, it } from 'vitest';
import { get } from 'svelte/store';

import {
	__buildNodeSchemaContractSnapshotForTest,
	__stripToDTOForTest,
	graphStore
} from './graphStore';

describe('graphStore schema lifecycle golden', () => {
	it('preserves declared expected schema while persisting schema-only payloads', () => {
		graphStore.hardResetGraph();
		const sourceId = graphStore.addNode('source', { x: 0, y: 0 });
		const transformId = graphStore.addNode('transform', { x: 260, y: 0 });
		graphStore.updateNodeConfig(sourceId, {
			params: { file_format: 'csv', output: { mode: 'table' } }
		});
		graphStore.updateNodeConfig(transformId, {
			params: { op: 'filter', filter: { expr: '' } }
		});
		const edgeAdded = graphStore.addEdge({
			id: 'e_schema_lifecycle',
			source: sourceId,
			target: transformId,
			data: { exec: 'idle' }
		} as any);
		expect(edgeAdded.ok).toBe(true);

		const saveResult = graphStore.setNodeExpectedSchema(sourceId, {
			type: 'table',
			fields: [
				{ name: 'id', type: 'unknown', nullable: false },
				{ name: 'price', type: 'unknown', nullable: true }
			]
		});
		expect((saveResult as any)?.ok).toBe(true);

		const state = get(graphStore);
		const sourceNode = state.nodes.find((n) => n.id === sourceId) as any;
		expect(sourceNode?.data?.schema?.expectedSchema?.source).toBe('declared');

		const dto = __stripToDTOForTest(state.nodes as any, state.edges as any, 'graph_schema_lifecycle');
		const persistedSourceNode = (dto.nodes as any[]).find((n) => String(n?.id ?? '') === sourceId) as any;
		expect(persistedSourceNode?.data?.schema?.expectedSchema?.typedSchema?.type).toBe('table');

		const snapshot = __buildNodeSchemaContractSnapshotForTest(state as any, transformId);
		const summary = {
			status: snapshot.status,
			incoming: (snapshot.edges ?? [])
				.filter((e: any) => String(e?.direction ?? '') === 'incoming')
				.map((e: any) => ({
					severity: String(e?.severity ?? ''),
					providedType: String((e?.providedSchema ?? {}).type ?? ''),
					requiredType: String((e?.requiredSchema ?? {}).type ?? '')
				}))
		};
		expect(summary).toMatchInlineSnapshot(`
			{
			  "incoming": [
			    {
			      "providedType": "table",
			      "requiredType": "table",
			      "severity": "clean",
			    },
			  ],
			  "status": "clean",
			}
		`);
	});
});
