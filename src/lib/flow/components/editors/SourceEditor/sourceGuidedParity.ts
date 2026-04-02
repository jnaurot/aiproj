import type { SourceKind } from '$lib/flow/types/paramsMap';

const GUIDED_WHITELIST: Record<SourceKind, string[]> = {
	file: ['filename', 'file_format', 'output.mode', 'cache_enabled'],
	database: ['connection_ref', 'query', 'limit', 'output.mode'],
	api: ['method', 'url', 'auth_type', 'auth_token_ref', 'output.mode'],
	object_store: ['provider', 'bucket', 'key', 'file_format', 'output.mode'],
	warehouse: ['provider', 'connection_ref', 'query', 'limit', 'output.mode']
};

export function guidedWhitelistForSource(sourceKind: SourceKind): string[] {
	return GUIDED_WHITELIST[sourceKind] ?? [];
}

export function applyGuidedPatchPreservingAdvanced<T extends Record<string, unknown>>(
	current: T,
	patch: Partial<T>
): T {
	return {
		...current,
		...patch
	};
}

export function guidedToFullRoundtrip<T extends Record<string, unknown>>(
	current: T,
	guidedPatch: Partial<T>
): T {
	return applyGuidedPatchPreservingAdvanced(current, guidedPatch);
}

