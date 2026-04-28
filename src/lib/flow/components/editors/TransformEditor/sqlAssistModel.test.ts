import { describe, expect, it } from 'vitest';
import {
	extractQuotedIdentifiers,
	insertQuotedColumnReference,
	sqlAvailableColumns,
	summarizeSchemaAssist,
	unknownSqlReferences
} from './sqlAssistModel';

describe('sqlAssistModel', () => {
	it('merges and sorts available columns from input columns and schemas', () => {
		const out = sqlAvailableColumns(
			['title', 'id'],
			[
				{
					artifactId: 'a1',
					label: 'in',
					columns: [{ name: 'summary', type: 'string' }, { name: 'id', type: 'number' }],
					rowCount: null,
					provenance: null,
					coercion: null,
					schemaSource: 'sample',
					schemaState: 'fresh'
				}
			] as any
		);
		expect(out).toEqual(['id', 'summary', 'title']);
	});

	it('inserts quoted column reference into sql', () => {
		expect(insertQuotedColumnReference('', 'title')).toBe('"title"');
		expect(insertQuotedColumnReference('SELECT * FROM input', 'title')).toBe('SELECT * FROM input "title"');
	});

	it('finds unknown quoted references', () => {
		const unknown = unknownSqlReferences('SELECT "id", "missing" FROM input', ['id', 'title']);
		expect(unknown).toEqual(['missing']);
	});

	it('extracts quoted identifiers from SQL and deduplicates', () => {
		expect(extractQuotedIdentifiers('SELECT "id", "id", `title` FROM input')).toEqual(['id', 'title']);
	});

	it('summarizes schema assist from input schemas', () => {
		const summary = summarizeSchemaAssist([
			{
				artifactId: 'a1',
				label: 'in',
				columns: [],
				rowCount: null,
				provenance: null,
				coercion: null,
				schemaSource: 'sample',
				schemaState: 'fresh'
			},
			{
				artifactId: 'a2',
				label: 'in2',
				columns: [],
				rowCount: null,
				provenance: null,
				coercion: null,
				schemaSource: 'artifact',
				schemaState: 'stale'
			}
		] as any);
		expect(summary.source).toBe('sample');
		expect(summary.state).toBe('stale');
		expect(summary.hasSchema).toBe(true);
	});
});

