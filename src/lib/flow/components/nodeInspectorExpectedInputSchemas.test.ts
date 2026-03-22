import { describe, expect, it } from 'vitest';

import {
	collectExpectedInputHandles,
	type ExpectedInputHandleSummary
} from '$lib/flow/components/nodeInspectorSchema';
import type { NodeSchemaContractEdge } from '$lib/flow/store/graphStore';

describe('nodeInspector expected input schemas', () => {
	it('collects per-handle entries from edges, port contracts, and expected schema map', () => {
		const node = {
			id: 'n_model',
			data: {
				label: 'Model',
				portContracts: {
					in: {
						default: { affinity: 'work' },
						param_profile: { affinity: 'param' },
						control_gate: { affinity: 'control' }
					}
				},
				schema: {
					expectedInputSchemas: {
						param_history: { typedSchema: { type: 'json' } }
					}
				}
			}
		};
		const edges: NodeSchemaContractEdge[] = [
			{
				edgeId: 'e_work',
				direction: 'incoming',
				severity: 'clean',
				message: 'ok',
				suggestions: [],
				sourceNodeId: 'n_src',
				targetNodeId: 'n_model',
				targetHandle: 'in'
			},
			{
				edgeId: 'e_param',
				direction: 'incoming',
				severity: 'clean',
				message: 'ok',
				suggestions: [],
				sourceNodeId: 'n_src_param',
				targetNodeId: 'n_model',
				targetHandle: 'param_runtime'
			}
		] as any;

		const handles = collectExpectedInputHandles(node as any, edges as any);
		const summary = handles.map((item: ExpectedInputHandleSummary) => ({
			handle: item.handle,
			affinity: item.affinity,
			classDefaultType: item.classDefaultType
		}));
		expect(summary).toEqual([
			{ handle: 'in', affinity: 'work', classDefaultType: 'text' },
			{ handle: 'control_gate', affinity: 'control', classDefaultType: 'none' },
			{ handle: 'param_history', affinity: 'param', classDefaultType: 'json' },
			{ handle: 'param_profile', affinity: 'param', classDefaultType: 'json' },
			{ handle: 'param_runtime', affinity: 'param', classDefaultType: 'json' }
		]);
	});
});
