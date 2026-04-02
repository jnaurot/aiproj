import type { SourceKind } from '$lib/flow/types/paramsMap';
import { defaultSourceSectionOpenState } from './sourceEditorLayout';

export type SourceDisclosureStateStore = Record<string, Record<string, boolean>>;

export function sourceDisclosureKey(nodeId: string, sourceKind: SourceKind): string {
	return `${sourceKind}:${String(nodeId ?? '').trim()}`;
}

export function ensureSourceDisclosureState(
	store: SourceDisclosureStateStore,
	nodeId: string,
	sourceKind: SourceKind
): Record<string, boolean> {
	const key = sourceDisclosureKey(nodeId, sourceKind);
	const existing = store[key];
	if (existing) return existing;
	const seeded = defaultSourceSectionOpenState(sourceKind);
	store[key] = seeded;
	return seeded;
}

export function patchSourceDisclosureState(
	store: SourceDisclosureStateStore,
	nodeId: string,
	sourceKind: SourceKind,
	patch: Record<string, boolean>
): Record<string, boolean> {
	const current = ensureSourceDisclosureState(store, nodeId, sourceKind);
	const merged = { ...current, ...patch };
	store[sourceDisclosureKey(nodeId, sourceKind)] = merged;
	return merged;
}

