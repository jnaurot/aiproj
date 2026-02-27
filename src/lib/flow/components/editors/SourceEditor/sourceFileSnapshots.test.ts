import { describe, expect, it } from 'vitest';

import {
	mergeRecentSnapshotOnUpload,
	optionLabel,
	sortRecentSnapshotsForDisplay,
	updateRecentSnapshotInPlace,
	type RecentSnapshot
} from './sourceFileSnapshots';

function sid(ch: string): string {
	return ch.repeat(64);
}

describe('source file snapshot list behavior', () => {
	it('selecting_previous_upload_does_not_reorder_dropdown', () => {
		const entries: RecentSnapshot[] = [
			{ id: sid('a'), filename: 'zeta.txt' },
			{ id: sid('b'), filename: 'alpha.txt' },
			{ id: sid('c'), filename: 'beta.txt' }
		];

		const selectedId = sid('b');
		const updated = updateRecentSnapshotInPlace(entries, selectedId, {});

		expect(updated.map((e) => e.id)).toEqual(entries.map((e) => e.id));
	});

	it('dropdown_is_sorted_alphabetically_by_filename', () => {
		const entries: RecentSnapshot[] = [
			{ id: sid('a'), filename: 'zeta.txt', importedAt: '2026-02-27T00:00:00Z' },
			{ id: sid('b'), filename: 'Alpha.txt', importedAt: '2026-02-27T00:00:00Z' },
			{ id: sid('c'), filename: 'beta.txt', importedAt: '2026-02-27T00:00:00Z' },
			{ id: sid('d') }
		];

		const sorted = sortRecentSnapshotsForDisplay(entries);
		expect(sorted.map((e) => e.filename ?? '')).toEqual(['Alpha.txt', 'beta.txt', 'zeta.txt', '']);
	});

	it('selecting_previous_upload_populates_missing_filename_and_updates_label', () => {
		const entries: RecentSnapshot[] = [{ id: sid('a') }, { id: sid('b'), filename: 'notes.md' }];
		const backfilled = updateRecentSnapshotInPlace(entries, sid('a'), { filename: 'README.md' });

		expect(backfilled[0].filename).toBe('README.md');
		expect(backfilled.map((e) => e.id)).toEqual(entries.map((e) => e.id));
		const label = optionLabel(backfilled[0], false, (id) => `${id.slice(0, 8)}...`);
		expect(label).toContain('README.md');
	});

	it('upload_adds_or_refreshes_entry_with_dedupe_and_cap', () => {
		const current: RecentSnapshot[] = [{ id: sid('a'), filename: 'a.txt' }, { id: sid('b'), filename: 'b.txt' }];
		const merged = mergeRecentSnapshotOnUpload(
			{ id: sid('b'), filename: 'b2.txt' },
			current,
			2
		);
		expect(merged[0].id).toBe(sid('b'));
		expect(merged[0].filename).toBe('b2.txt');
		expect(merged).toHaveLength(2);
	});
});
