import { describe, expect, it } from 'vitest';
import { computeColumnUniverse, isByParamPath, missingColumnsFromError, normalizeColumnNames } from './columnSelectionModel';

describe('normalizeColumnNames', () => {
	it('trims, dedupes, and drops empties', () => {
		expect(normalizeColumnNames([' text ', 'text', '', 'other'])).toEqual(['text', 'other']);
	});
});

describe('computeColumnUniverse', () => {
	it('uses schema as source when available', () => {
		const out = computeColumnUniverse({
			stickyColumns: [],
			schemaColumns: ['b', 'a'],
			selectedColumns: ['a']
		});
		expect(out.hasKnownSchema).toBe(true);
		expect(out.knownColumns).toEqual(['a', 'b']);
		expect(out.availableColumns).toEqual(['b']);
	});

	it('falls back to sticky columns when schema unavailable', () => {
		const out = computeColumnUniverse({
			stickyColumns: ['text', 'other'],
			schemaColumns: [],
			selectedColumns: ['text']
		});
		expect(out.hasKnownSchema).toBe(false);
		expect(out.availableColumns).toEqual(['other']);
	});
});

describe('missingColumnsFromError', () => {
	it('extracts by-path missing columns', () => {
		const missing = missingColumnsFromError(
			{
				errorCode: 'MISSING_COLUMN',
				paramPath: 'params.sort.by',
				missingColumns: ['x']
			},
			['MISSING_COLUMN'],
			(path) => path === 'params.sort.by' || isByParamPath(path)
		);
		expect(missing).toEqual(['x']);
	});
});
