import { describe, expect, it } from 'vitest';
import {
	canCommitDedupeDraft,
	dedupePreviewSql,
	missingDedupeColumnsFromError,
	normalizeDedupeParams,
	resolveDedupeAvailableColumns
} from './dedupeModel';

describe('normalizeDedupeParams', () => {
	it('normalizes by as the canonical state and migrates legacy allColumns', () => {
		const all = normalizeDedupeParams({ allColumns: true, by: ['text'] });
		expect(all.by).toEqual([]);
		expect(all.allColumns).toBe(true);

		const byCols = normalizeDedupeParams({ by: ['text', ' text ', 'id'] });
		expect(byCols.by).toEqual(['text', 'id']);
		expect(byCols.allColumns).toBe(false);
	});
});

describe('dedupePreviewSql', () => {
	it('includes stable order by __rowid', () => {
		const sqlAll = dedupePreviewSql({ by: [] });
		expect(sqlAll).toContain('ORDER BY "__rowid"');
		expect(sqlAll).toContain('PARTITION BY <all columns>');

		const sqlBy = dedupePreviewSql({ by: ['text'] });
		expect(sqlBy).toContain('PARTITION BY "text"');
		expect(sqlBy).toContain('ORDER BY "__rowid"');

		const sqlByTwo = dedupePreviewSql({ by: ['text', 'other'] });
		expect(sqlByTwo).toContain('PARTITION BY "text","other"');
	});
});

describe('canCommitDedupeDraft', () => {
	it('requires at least one by column when all-columns is unchecked in UI', () => {
		expect(canCommitDedupeDraft(false, [])).toBe(true);
		expect(canCommitDedupeDraft(true, [])).toBe(false);
		expect(canCommitDedupeDraft(true, ['text'])).toBe(true);
	});
});

describe('error column helpers', () => {
	it('uses runtime error availableColumns first, falls back to input schema columns', () => {
		expect(resolveDedupeAvailableColumns(['text', 'other'], ['x'])).toEqual(['x']);
		expect(resolveDedupeAvailableColumns([], ['text', 'other'])).toEqual(['text', 'other']);
	});
	it('extracts missing dedupe columns only for by-path missing-column errors', () => {
		expect(
			missingDedupeColumnsFromError({
				errorCode: 'MISSING_COLUMN',
				paramPath: 'params.dedupe.by',
				missingColumns: ['missing'],
			})
		).toEqual(['missing']);
		expect(
			missingDedupeColumnsFromError({
				errorCode: 'COLUMN_SELECTION_REQUIRED',
				paramPath: 'params.dedupe.by',
				missingColumns: ['__none__'],
			})
		).toEqual(['__none__']);
		expect(
			missingDedupeColumnsFromError({
				errorCode: 'MISSING_COLUMN',
				paramPath: 'split.sourceColumn',
				missingColumns: ['missing'],
			})
		).toEqual([]);
	});
});
