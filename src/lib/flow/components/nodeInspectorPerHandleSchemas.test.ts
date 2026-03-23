import { describe, expect, it } from 'vitest';

import { collectExpectedInputHandles } from '$lib/flow/components/nodeInspectorSchema';

describe('nodeInspector per-handle input schema routing', () => {
	it('collects target handles as independent schema slots', () => {
		const node = {
			id: 'n_sink',
			data: {
				portContracts: {
					in: {
						default: { affinity: 'work' },
						param_profile: { affinity: 'param' },
						param_filters: { affinity: 'param' },
						control_gate: { affinity: 'control' }
					}
				},
				schema: {
					expectedInputSchemas: {
						in: { typedSchema: { type: 'text' } },
						param_profile: { typedSchema: { type: 'json' } },
						param_filters: { typedSchema: { type: 'json' } }
					}
				}
			}
		};
		const edges = [
			{
				edgeId: 'e_work',
				direction: 'incoming',
				targetHandle: 'in'
			},
			{
				edgeId: 'e_param_1',
				direction: 'incoming',
				targetHandle: 'param_profile'
			},
			{
				edgeId: 'e_param_2',
				direction: 'incoming',
				targetHandle: 'param_filters'
			},
			{
				edgeId: 'e_ctl',
				direction: 'incoming',
				targetHandle: 'control_gate'
			}
		] as any;

		const handles = collectExpectedInputHandles(node as any, edges as any);
		expect(handles.map((h) => h.handle)).toEqual(['in', 'control_gate', 'param_filters', 'param_profile']);
	});
});
