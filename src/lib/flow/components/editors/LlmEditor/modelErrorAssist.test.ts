import { describe, expect, it } from 'vitest';
import { buildModelAutoFixes } from './modelErrorAssist';

describe('modelErrorAssist', () => {
	it('returns adapter insertion fix for contract edge mismatch', () => {
		const fixes = buildModelAutoFixes({
			nodeError: {
				errorCode: 'CONTRACT_EDGE_PAYLOAD_TYPE_MISMATCH',
				message: 'Input edge contract mismatch'
			},
			params: {},
			schemaEdges: [
				{
					edgeId: 'e1',
					sourceNodeId: 'n1',
					targetNodeId: 'n2',
					direction: 'incoming',
					severity: 'error',
					adapterKind: 'json_to_table',
					sourceHandle: 'out',
					targetHandle: 'in',
					providedSchema: { type: 'json', fields: [] },
					requiredSchema: { type: 'table', required_fields: [] },
					suggestions: []
				}
			] as any
		});
		expect(fixes).toEqual([
			{ id: 'insert_adapter', label: 'Insert json_to_table adapter', edgeId: 'e1' }
		]);
	});

	it('returns secret and unsupported mode fixes when relevant', () => {
		const fixes = buildModelAutoFixes({
			nodeError: {
				errorCode: 'MISSING_SECRET',
				message: "LLM output contract mismatch: unsupported output type 'TABLE_V1'",
				paramPath: 'params.output.mode'
			},
			params: { output: { mode: 'json', strict: true } },
			schemaEdges: []
		});
		expect(fixes).toContainEqual({
			id: 'clear_connection_ref',
			label: 'Clear missing connectionRef',
			patch: { connectionRef: undefined }
		});
		expect(fixes).toContainEqual({
			id: 'set_output_text',
			label: 'Set output mode to text',
			patch: { output: { mode: 'text', strict: true } }
		});
	});
});
