import { writable, get } from 'svelte/store';
import type { NodeKind, PipelineNodeData, PortType } from '$lib/flow/types';
import type { LlmKind, SourceKind, TransformKind, ToolProvider } from '$lib/flow/types/paramsMap';

const KEY = 'flow:node-presets:v1';

export type NodeSubtype = SourceKind | TransformKind | LlmKind | ToolProvider;

export type NodePreset = {
	id: string;
	name: string;
	kind: NodeKind;
	subtype: NodeSubtype;
	params: Record<string, unknown>;
	ports?: {
		in?: PortType | null;
		out?: PortType | null;
	};
	description?: string;
	tags?: string[];
	createdAt: string;
	updatedAt: string;
	lastUsedAt?: string;
	useCount: number;
};

export type SavePresetResult =
	| { ok: true; preset: NodePreset; mode: 'created' | 'updated' }
	| {
			ok: false;
			error:
				| 'duplicate_name_in_scope'
				| 'preset_not_found'
				| 'invalid_name'
				| 'identical_preset_exists';
			existingPresetId?: string;
	  };

function hasLocalStorage(): boolean {
	return typeof window !== 'undefined' && typeof window.localStorage !== 'undefined';
}

function cloneRecord(value: Record<string, unknown> | undefined | null): Record<string, unknown> {
	return value ? (structuredClone(value) as Record<string, unknown>) : {};
}

function normalizeSubtype(data: PipelineNodeData): NodeSubtype {
	if (data.kind === 'source') return data.sourceKind;
	if (data.kind === 'transform') return data.transformKind;
	if (data.kind === 'llm') return data.llmKind;
	const provider = String((data.params as Record<string, unknown>)?.provider ?? 'mcp') as ToolProvider;
	return provider;
}

function parsePreset(raw: unknown): NodePreset | null {
	if (!raw || typeof raw !== 'object') return null;
	const v = raw as Record<string, unknown>;
	const id = String(v.id ?? '').trim();
	const name = String(v.name ?? '').trim();
	const kind = String(v.kind ?? '').trim() as NodeKind;
	const subtype = String(v.subtype ?? '').trim() as NodeSubtype;
	const params = cloneRecord((v.params ?? {}) as Record<string, unknown>);
	const createdAt = String(v.createdAt ?? '').trim();
	const updatedAt = String(v.updatedAt ?? '').trim();
	if (!id || !name || !kind || !subtype || !createdAt || !updatedAt) return null;
	if (kind !== 'source' && kind !== 'transform' && kind !== 'llm' && kind !== 'tool') return null;
	const portsRaw = v.ports as Record<string, unknown> | undefined;
	const ports =
		portsRaw && typeof portsRaw === 'object'
			? {
					in: (portsRaw.in as PortType | null | undefined) ?? null,
					out: (portsRaw.out as PortType | null | undefined) ?? null
				}
			: undefined;
	return {
		id,
		name,
		kind,
		subtype,
		params,
		ports,
		description: typeof v.description === 'string' ? v.description : undefined,
		tags: Array.isArray(v.tags) ? v.tags.map((x) => String(x)) : undefined,
		createdAt,
		updatedAt,
		lastUsedAt: typeof v.lastUsedAt === 'string' ? v.lastUsedAt : undefined,
		useCount: Number.isFinite(Number(v.useCount)) ? Number(v.useCount) : 0
	};
}

function loadPresets(): NodePreset[] {
	if (!hasLocalStorage()) return [];
	try {
		const raw = window.localStorage.getItem(KEY);
		if (!raw) return [];
		const parsed = JSON.parse(raw) as unknown;
		if (!Array.isArray(parsed)) return [];
		return parsed.map(parsePreset).filter((x): x is NodePreset => Boolean(x));
	} catch (error) {
		console.warn('Failed to load node presets', error);
		return [];
	}
}

function savePresets(presets: NodePreset[]): void {
	if (!hasLocalStorage()) return;
	try {
		window.localStorage.setItem(KEY, JSON.stringify(presets));
	} catch (error) {
		console.warn('Failed to save node presets', error);
	}
}

function sortPresets(presets: NodePreset[]): NodePreset[] {
	return [...presets].sort((a, b) => {
		const aT = Date.parse(a.lastUsedAt ?? a.updatedAt ?? a.createdAt);
		const bT = Date.parse(b.lastUsedAt ?? b.updatedAt ?? b.createdAt);
		return bT - aT;
	});
}

function createPresetFromNodeData(data: PipelineNodeData, name: string): NodePreset {
	const now = new Date().toISOString();
	return {
		id: `preset_${crypto.randomUUID()}`,
		name: name.trim(),
		kind: data.kind,
		subtype: normalizeSubtype(data),
		params: cloneRecord((data.params ?? {}) as Record<string, unknown>),
		ports: {
			in: data.ports?.in ?? null,
			out: data.ports?.out ?? null
		},
		createdAt: now,
		updatedAt: now,
		useCount: 0
	};
}

function normalizeName(name: string): string {
	return name.trim();
}

function sameScopedName(
	preset: NodePreset,
	kind: NodeKind,
	subtype: NodeSubtype,
	name: string
): boolean {
	return (
		preset.kind === kind &&
		preset.subtype === subtype &&
		String(preset.name).trim().toLowerCase() === name.trim().toLowerCase()
	);
}

function stableStringify(value: unknown): string {
	if (value === null || value === undefined) return String(value);
	if (typeof value !== 'object') return JSON.stringify(value);
	if (Array.isArray(value)) return `[${value.map((x) => stableStringify(x)).join(',')}]`;
	const obj = value as Record<string, unknown>;
	const keys = Object.keys(obj).sort();
	return `{${keys.map((k) => `${JSON.stringify(k)}:${stableStringify(obj[k])}`).join(',')}}`;
}

function samePresetPayload(
	preset: NodePreset,
	kind: NodeKind,
	subtype: NodeSubtype,
	params: Record<string, unknown>,
	ports: { in?: PortType | null; out?: PortType | null }
): boolean {
	if (preset.kind !== kind || preset.subtype !== subtype) return false;
	const a = stableStringify(preset.params ?? {});
	const b = stableStringify(params ?? {});
	if (a !== b) return false;
	const pa = stableStringify({
		in: preset.ports?.in ?? null,
		out: preset.ports?.out ?? null
	});
	const pb = stableStringify({
		in: ports.in ?? null,
		out: ports.out ?? null
	});
	return pa === pb;
}

function createNodePresetStore() {
	const store = writable<NodePreset[]>(sortPresets(loadPresets()));
	const { subscribe, set, update } = store;

	function persist(next: NodePreset[]): NodePreset[] {
		const sorted = sortPresets(next);
		savePresets(sorted);
		return sorted;
	}

	return {
		subscribe,
		reload() {
			set(sortPresets(loadPresets()));
		},
		getById(presetId: string): NodePreset | null {
			const all = get(store);
			const found = all.find((p) => p.id === presetId);
			return found ? structuredClone(found) : null;
		},
		upsertFromNodeData(
			data: PipelineNodeData,
			name: string,
			options?: { overwritePresetId?: string | null }
		): SavePresetResult {
			const cleanName = normalizeName(name);
			if (!cleanName) return { ok: false, error: 'invalid_name' };
			const kind = data.kind;
			const subtype = normalizeSubtype(data);
			const params = cloneRecord((data.params ?? {}) as Record<string, unknown>);
			const ports = {
				in: data.ports?.in ?? null,
				out: data.ports?.out ?? null
			};
			const all = get(store);
			const overwriteId = String(options?.overwritePresetId ?? '').trim();
			const identical = all.find((p) => samePresetPayload(p, kind, subtype, params, ports));
			if (identical && identical.id !== overwriteId) {
				return {
					ok: false,
					error: 'identical_preset_exists',
					existingPresetId: identical.id
				};
			}
			if (overwriteId) {
				const idx = all.findIndex((p) => p.id === overwriteId);
				if (idx < 0) return { ok: false, error: 'preset_not_found' };
				const target = all[idx];
				if (samePresetPayload(target, kind, subtype, params, ports)) {
					return {
						ok: false,
						error: 'identical_preset_exists',
						existingPresetId: target.id
					};
				}
				const duplicate = all.find(
					(p) => p.id !== overwriteId && sameScopedName(p, kind, subtype, cleanName)
				);
				if (duplicate) {
					return {
						ok: false,
						error: 'duplicate_name_in_scope',
						existingPresetId: duplicate.id
					};
				}
				const now = new Date().toISOString();
				const prev = all[idx];
				const updated: NodePreset = {
					...prev,
					name: cleanName,
					kind,
					subtype,
					params,
					ports,
					updatedAt: now
				};
				const next = [...all];
				next[idx] = updated;
				set(persist(next));
				return { ok: true, preset: structuredClone(updated), mode: 'updated' };
			}
			const duplicate = all.find((p) => sameScopedName(p, kind, subtype, cleanName));
			if (duplicate) {
				return {
					ok: false,
					error: 'duplicate_name_in_scope',
					existingPresetId: duplicate.id
				};
			}
			const preset: NodePreset = {
				...createPresetFromNodeData(data, cleanName),
				params,
				ports
			};
			set(persist([preset, ...all]));
			return { ok: true, preset: structuredClone(preset), mode: 'created' };
		},
		saveFromNodeData(data: PipelineNodeData, name: string): NodePreset {
			const preset = createPresetFromNodeData(data, name);
			update((cur) => persist([preset, ...cur]));
			return preset;
		},
		delete(presetId: string): void {
			update((cur) => persist(cur.filter((p) => p.id !== presetId)));
		},
		markUsed(presetId: string): void {
			update((cur) =>
				persist(
					cur.map((p) =>
						p.id === presetId
							? {
									...p,
									lastUsedAt: new Date().toISOString(),
									useCount: (p.useCount ?? 0) + 1
								}
							: p
					)
				)
			);
		}
	};
}

export const nodePresetStore = createNodePresetStore();
