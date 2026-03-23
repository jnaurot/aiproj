import { describe, expect, it } from 'vitest';

import {
	collectExpectedInputHandles,
	groupSchemaEdgesByMode,
	type NodeSchemaContractEdge
} from './nodeInspectorSchema';

describe('nodeInspector plane contract editor helpers', () => {
	it('collects handle defaults with plane-specific class schema types', () => {
		const node = {
			id: 'n_target',
			data: {
				kind: 'model',
				schema: {
					expectedInputSchemas: {
						in: { typedSchema: { type: 'text', fields: [] } },
						param_filters: { typedSchema: { type: 'json', fields: [] } },
						control_in: { typedSchema: { type: 'none', fields: [] } }
					}
				},
				portDeclarations: {
					in: {
						in: { plane: 'work' },
						param_filters: { plane: 'param' },
						control_in: { plane: 'control' }
					}
				}
			}
		} as any;

		const handles = collectExpectedInputHandles(node, []);
		const byHandle = Object.fromEntries(handles.map((entry) => [entry.handle, entry]));
		expect(byHandle.in?.affinity).toBe('work');
		expect(byHandle.in?.classDefaultType).toBe('text');
		expect(byHandle.param_filters?.affinity).toBe('param');
		expect(byHandle.param_filters?.classDefaultType).toBe('json');
		expect(byHandle.control_in?.affinity).toBe('control');
		expect(byHandle.control_in?.classDefaultType).toBe('none');
	});

	it('groups schema contract edges by explicit plane mode', () => {
		const edges: NodeSchemaContractEdge[] = [
			{
				edgeId: 'e_work',
				direction: 'incoming',
				mode: 'work',
				sourceNodeId: 'n1',
				targetNodeId: 'n2',
				targetHandle: 'in'
			} as any,
			{
				edgeId: 'e_param',
				direction: 'incoming',
				mode: 'param',
				sourceNodeId: 'n3',
				targetNodeId: 'n2',
				targetHandle: 'param_filters'
			} as any,
			{
				edgeId: 'e_control',
				direction: 'incoming',
				mode: 'control',
				sourceNodeId: 'n4',
				targetNodeId: 'n2',
				targetHandle: 'control_in'
			} as any
		];

		const groups = groupSchemaEdgesByMode(edges);
		expect(groups.map((group) => group.mode)).toEqual(['work', 'param', 'control']);
		expect(groups.find((group) => group.mode === 'work')?.edges.map((edge) => edge.edgeId)).toEqual([
			'e_work'
		]);
		expect(groups.find((group) => group.mode === 'param')?.edges.map((edge) => edge.edgeId)).toEqual([
			'e_param'
		]);
		expect(groups.find((group) => group.mode === 'control')?.edges.map((edge) => edge.edgeId)).toEqual([
			'e_control'
		]);
	});
});
