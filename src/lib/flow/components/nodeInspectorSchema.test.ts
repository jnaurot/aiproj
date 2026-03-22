import { describe, expect, it } from 'vitest';

import { groupSchemaEdgesByMode, schemaEdgeCounterpartyName } from './nodeInspectorSchema';

describe('schemaEdgeCounterpartyName', () => {
	it('returns label for incoming counterparty', () => {
		const result = schemaEdgeCounterpartyName(
			{
				edgeId: 'e1',
				mode: 'work',
				direction: 'incoming',
				sourceNodeId: 'n_src',
				targetNodeId: 'n_target',
				sourceHandle: 'out',
				targetHandle: 'in',
				severity: 'clean',
				status: 'compatible',
				providedSchema: { type: 'text', fields: [] },
				requiredSchema: { type: 'text', required_fields: [] }
			},
			'n_target',
			[
				{ id: 'n_src', data: { label: 'Resume Source' } },
				{ id: 'n_target', data: { label: 'Select Jobs' } }
			]
		);

		expect(result).toBe('Resume Source');
	});

	it('falls back to node id when label is missing', () => {
		const result = schemaEdgeCounterpartyName(
			{
				edgeId: 'e2',
				mode: 'work',
				direction: 'outgoing',
				sourceNodeId: 'n_src',
				targetNodeId: 'n_target',
				sourceHandle: 'out',
				targetHandle: 'in',
				severity: 'warning',
				status: 'incompatible',
				providedSchema: { type: 'json', fields: [] },
				requiredSchema: { type: 'text', required_fields: [] }
			},
			'n_src',
			[{ id: 'n_target', data: {} }]
		);

		expect(result).toBe('n_target');
	});

	it('uses direction-based fallback when selected node is not an endpoint', () => {
		const result = schemaEdgeCounterpartyName(
			{
				edgeId: 'e3',
				mode: 'work',
				direction: 'incoming',
				sourceNodeId: 'n_a',
				targetNodeId: 'n_b',
				sourceHandle: 'out',
				targetHandle: 'in',
				severity: 'clean',
				status: 'compatible',
				providedSchema: { type: 'table', fields: [] },
				requiredSchema: { type: 'table', required_fields: [] }
			},
			'n_unrelated',
			[{ id: 'n_a', data: { label: 'Jobs API' } }]
		);

		expect(result).toBe('Jobs API');
	});

	it('groups schema edges by mode', () => {
		const groups = groupSchemaEdgesByMode([
			{
				edgeId: 'e1',
				mode: 'param',
				direction: 'incoming',
				sourceNodeId: 'n_a',
				targetNodeId: 'n_b',
				sourceHandle: 'out',
				targetHandle: 'param_filters',
				severity: 'clean',
				status: 'compatible',
				providedSchema: { type: 'json', fields: [] },
				requiredSchema: { type: 'json', required_fields: [] }
			},
			{
				edgeId: 'e2',
				mode: 'work',
				direction: 'incoming',
				sourceNodeId: 'n_c',
				targetNodeId: 'n_b',
				sourceHandle: 'out',
				targetHandle: 'in',
				severity: 'warning',
				status: 'coercible',
				providedSchema: { type: 'json', fields: [] },
				requiredSchema: { type: 'text', required_fields: [] }
			}
		] as any);

		expect(groups).toHaveLength(2);
		expect(groups[0]?.mode).toBe('work');
		expect(groups[0]?.edges[0]?.edgeId).toBe('e2');
		expect(groups[1]?.mode).toBe('param');
		expect(groups[1]?.edges[0]?.edgeId).toBe('e1');
	});
});
