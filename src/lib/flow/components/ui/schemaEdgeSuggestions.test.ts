import { describe, expect, it } from 'vitest';

import { buildDownstreamSchemaSuggestion } from './schemaEdgeSuggestions';

describe('buildDownstreamSchemaSuggestion', () => {
	it('returns null when message is empty', () => {
		const out = buildDownstreamSchemaSuggestion({
			mismatchMessage: '',
			sourceSchema: {},
			targetSchema: {},
			targetLabel: 'Transform_Select'
		});
		expect(out).toBeNull();
	});

	it('builds valid typed table schema and adds missing column', () => {
		const out = buildDownstreamSchemaSuggestion({
			mismatchMessage: "Column 'candidate_required_location' not found in input schema",
			sourceSchema: {
				mode: 'opaque',
				columns: [{ name: 'length_text', type: 'number', nullable: true }]
			},
			targetSchema: {
				mode: 'opaque',
				columns: [{ name: 'length_text', type: 'number', nullable: true }]
			},
			targetLabel: 'Transform_Select'
		});
		expect(out).not.toBeNull();
		const schema = out!.targetSchema as Record<string, unknown>;
		expect(schema.type).toBe('table');
		const fields = Array.isArray(schema.fields) ? (schema.fields as Array<Record<string, unknown>>) : [];
		expect(fields.some((f) => String(f.name ?? '') === 'candidate_required_location')).toBe(true);
	});

	it('does not duplicate missing column when already present', () => {
		const out = buildDownstreamSchemaSuggestion({
			mismatchMessage: "Column 'candidate_required_location' not found in input schema",
			sourceSchema: {
				mode: 'opaque',
				columns: [{ name: 'candidate_required_location', type: 'string', nullable: true }]
			},
			targetSchema: {
				type: 'table',
				fields: [{ name: 'candidate_required_location', type: 'string', nullable: true }]
			},
			targetLabel: 'Transform_Select'
		});
		const schema = out!.targetSchema as Record<string, unknown>;
		const fields = Array.isArray(schema.fields) ? (schema.fields as Array<Record<string, unknown>>) : [];
		expect(fields.filter((f) => String(f.name ?? '') === 'candidate_required_location')).toHaveLength(1);
	});
});
