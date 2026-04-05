import { describe, expect, it } from 'vitest';

import { collectExpectedInputHandles, groupSchemaEdgesByMode, type NodeSchemaContractEdge } from './nodeInspectorSchema';

describe('nodeInspectorSchema legacy config plane migration', () => {
	it('maps legacy config affinity to param in expected input handle summaries', () => {
		const handles = collectExpectedInputHandles(
			{
				id: 'n1',
				data: {
					portDeclarations: {
						in: {
							config_in: { plane: 'config' }
						}
					}
				}
			} as any,
			[]
		);
		const row = handles.find((entry) => entry.handle === 'config_in');
		expect(row?.affinity).toBe('param');
	});

	it('groups legacy config edge mode under Param bucket', () => {
		const edges: NodeSchemaContractEdge[] = [
			{
				edgeId: 'e1',
				mode: 'config' as any,
				direction: 'incoming',
				sourceNodeId: 'a',
				targetNodeId: 'b',
				sourceHandle: 'out',
				targetHandle: 'in',
				providedSchema: {},
				requiredSchema: {},
				severity: 'clean',
				suggestions: [],
				adapterKind: null
			}
		];
		const groups = groupSchemaEdgesByMode(edges);
		expect(groups).toHaveLength(1);
		expect(groups[0].mode).toBe('param');
	});
});
