import type { SourceKind } from '$lib/flow/types/paramsMap';
import { NODE_CAPABILITIES } from '$lib/flow/schemaCapabilities';

export type SourceSupportLevel = 'production' | 'preview' | 'mock_only';

export type SourceCapabilityDescriptor = {
	sourceKind: SourceKind;
	supportLevel: SourceSupportLevel;
	notes: string[];
};

const FALLBACK_SOURCE_CAPABILITIES: Record<SourceKind, SourceCapabilityDescriptor> = {
	file: { sourceKind: 'file', supportLevel: 'production', notes: [] },
	database: { sourceKind: 'database', supportLevel: 'production', notes: [] },
	api: { sourceKind: 'api', supportLevel: 'production', notes: [] },
	object_store: {
		sourceKind: 'object_store',
		supportLevel: 'preview',
		notes: ['Use object_store_mode=provider for production-backed reads.']
	},
	warehouse: { sourceKind: 'warehouse', supportLevel: 'preview', notes: [] }
};

function normalizeSupportLevel(raw: unknown): SourceSupportLevel {
	const value = String(raw ?? '').trim().toLowerCase();
	if (value === 'mock_only') return 'mock_only';
	if (value === 'preview') return 'preview';
	return 'production';
}

type SourceCapsInput = Record<string, any> | null | undefined;

export function resolveSourceCapabilityDescriptor(
	sourceKindRaw: unknown,
	capsOverride?: SourceCapsInput
): SourceCapabilityDescriptor {
	const sourceKind = (String(sourceKindRaw ?? 'file').trim().toLowerCase() as SourceKind) || 'file';
	const fallback = FALLBACK_SOURCE_CAPABILITIES[sourceKind] ?? FALLBACK_SOURCE_CAPABILITIES.file;
	const sourceCaps =
		(capsOverride as any) ??
		((((NODE_CAPABILITIES as any)?.source as Record<string, any> | undefined) ?? null) as Record<string, any> | null);
	const rawMatrix = (sourceCaps as any)?.kindCapabilities;
	const entry =
		rawMatrix && typeof rawMatrix === 'object' && rawMatrix[sourceKind] && typeof rawMatrix[sourceKind] === 'object'
			? (rawMatrix[sourceKind] as Record<string, any>)
			: null;
	if (!entry) return fallback;
	const notes = Array.isArray(entry.notes) ? entry.notes.map((v) => String(v)).filter(Boolean) : [];
	return {
		sourceKind,
		supportLevel: normalizeSupportLevel(entry.supportLevel),
		notes
	};
}

export function buildSourceCapabilityNotices(
	descriptor: SourceCapabilityDescriptor,
	params: Record<string, unknown> | null | undefined
): string[] {
	const notices: string[] = [];
	if (descriptor.supportLevel === 'preview') {
		notices.push('Preview capability: behavior may evolve; validate in your environment.');
	}
	if (descriptor.supportLevel === 'mock_only') {
		notices.push('Mock-only capability: intended for local/dev testing, not production fetches.');
	}
	const objectStoreMode = String((params as any)?.object_store_mode ?? 'provider').trim().toLowerCase();
	if (descriptor.sourceKind === 'object_store' && objectStoreMode === 'mock') {
		notices.push('Object store mock mode is active (local/dev behavior).');
	}
	for (const note of descriptor.notes) {
		if (!notices.includes(note)) notices.push(note);
	}
	return notices;
}
