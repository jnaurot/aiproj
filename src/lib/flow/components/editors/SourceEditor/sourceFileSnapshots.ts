import { asString } from '$lib/flow/components/editors/shared';

export type RecentSnapshot = {
	id: string;
	filename?: string;
	importedAt?: string;
	size?: number;
	mimeType?: string;
};

export function isSnapshotId(value: unknown): value is string {
	const id = asString(value, '').toLowerCase();
	return /^[a-f0-9]{64}$/.test(id);
}

export function normalizeRecentSnapshots(
	rawEntries: unknown,
	rawIds: unknown,
	limit = 10
): RecentSnapshot[] {
	const out: RecentSnapshot[] = [];
	const seen = new Set<string>();
	if (Array.isArray(rawEntries)) {
		for (const item of rawEntries) {
			if (!item || typeof item !== 'object') continue;
			const id = asString((item as any).id, '').toLowerCase();
			if (!isSnapshotId(id) || seen.has(id)) continue;
			seen.add(id);
			out.push({
				id,
				filename: asString((item as any).filename, '') || undefined,
				importedAt: asString((item as any).importedAt, '') || undefined,
				size: Number.isFinite(Number((item as any).size)) ? Number((item as any).size) : undefined,
				mimeType: asString((item as any).mimeType, '') || undefined
			});
		}
	}
	if (Array.isArray(rawIds)) {
		for (const idRaw of rawIds) {
			const id = asString(idRaw, '').toLowerCase();
			if (!isSnapshotId(id) || seen.has(id)) continue;
			seen.add(id);
			out.push({ id });
		}
	}
	return out.slice(0, limit);
}

export function mergeRecentSnapshotOnUpload(
	incoming: RecentSnapshot,
	current: RecentSnapshot[],
	limit = 10
): RecentSnapshot[] {
	const id = asString(incoming.id, '').toLowerCase();
	if (!isSnapshotId(id)) return current;
	const merged: RecentSnapshot = {
		id,
		filename: incoming.filename,
		importedAt: incoming.importedAt,
		size: incoming.size,
		mimeType: incoming.mimeType
	};
	const rest = current.filter((s) => s.id !== id);
	return [merged, ...rest].slice(0, limit);
}

export function updateRecentSnapshotInPlace(
	current: RecentSnapshot[],
	idRaw: string,
	patch: Partial<RecentSnapshot>
): RecentSnapshot[] {
	const id = asString(idRaw, '').toLowerCase();
	if (!isSnapshotId(id)) return current;
	let changed = false;
	const next = current.map((entry) => {
		if (entry.id !== id) return entry;
		const merged: RecentSnapshot = {
			...entry,
			filename: entry.filename ?? patch.filename,
			importedAt: entry.importedAt ?? patch.importedAt,
			size: entry.size ?? patch.size,
			mimeType: entry.mimeType ?? patch.mimeType
		};
		if (JSON.stringify(merged) !== JSON.stringify(entry)) changed = true;
		return merged;
	});
	return changed ? next : current;
}

function sortKeyName(entry: RecentSnapshot): string {
	return asString(entry.filename, '').toLocaleLowerCase();
}

function sortKeyImportedAt(entry: RecentSnapshot): string {
	return asString(entry.importedAt, '');
}

export function sortRecentSnapshotsForDisplay(entries: RecentSnapshot[]): RecentSnapshot[] {
	return [...entries].sort((a, b) => {
		const aName = sortKeyName(a);
		const bName = sortKeyName(b);
		if (aName && bName && aName !== bName) return aName.localeCompare(bName);
		if (!aName && bName) return 1;
		if (aName && !bName) return -1;
		const aImported = sortKeyImportedAt(a);
		const bImported = sortKeyImportedAt(b);
		if (aImported !== bImported) return aImported.localeCompare(bImported);
		return a.id.localeCompare(b.id);
	});
}

export function optionLabel(entry: RecentSnapshot, isLoading: boolean, shortHash: (id: string) => string): string {
	const prefix = shortHash(entry.id);
	if (entry.filename) return `${entry.filename} ( ${prefix})`;
	return isLoading ? `${prefix} (loading...)` : prefix;
}
