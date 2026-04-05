import type {
	ComponentApiContract,
	ComponentExposureHandle,
	ComponentExposureKind,
	ComponentTypedSchema
} from '$lib/flow/client/components';

const ALLOWED_KINDS = new Set<ComponentExposureKind>([
	'data_input',
	'data_output',
	'param_input',
	'control_input'
]);

function normalizeTypedSchema(raw: unknown, fallback: ComponentTypedSchema['type'] = 'json'): ComponentTypedSchema {
	const value = raw && typeof raw === 'object' ? (raw as Record<string, unknown>) : {};
	const typed = String(value.type ?? fallback).trim().toLowerCase();
	const normalizedType = (typed === 'string' ? 'text' : typed) as ComponentTypedSchema['type'];
	const allowed = new Set(['table', 'json', 'text', 'binary', 'embeddings', 'unknown']);
	const type = allowed.has(normalizedType) ? normalizedType : fallback;
	const fieldsRaw = Array.isArray(value.fields) ? value.fields : [];
	const fields =
		type === 'table'
			? fieldsRaw
					.filter((field) => field && typeof field === 'object')
					.map((field) => {
						const src = field as Record<string, unknown>;
						const rawFieldType = String(src.type ?? 'unknown').trim().toLowerCase();
						const fieldType = (rawFieldType === 'string' ? 'text' : rawFieldType) || 'unknown';
						return {
							name: String(src.name ?? '').trim(),
							type: allowed.has(fieldType) ? (fieldType as ComponentTypedSchema['type']) : 'unknown',
							nativeType:
								src.nativeType != null && String(src.nativeType).trim()
									? String(src.nativeType).trim()
									: undefined,
							nullable: Boolean(src.nullable ?? false)
						};
					})
					.filter((field) => field.name.length > 0)
			: [];
	return { type, fields };
}

function deriveDefaultExposureRegistry(api: ComponentApiContract): ComponentExposureHandle[] {
	const workInputs = Array.isArray((api as any)?.workInputs)
		? ((api as any).workInputs as Array<Record<string, unknown>>)
		: Array.isArray(api?.inputs)
			? (api.inputs as Array<Record<string, unknown>>)
			: [];
	const paramInputs = Array.isArray((api as any)?.paramInputs)
		? ((api as any).paramInputs as Array<Record<string, unknown>>)
		: [];
	const controlInputs = Array.isArray((api as any)?.controlInputs)
		? ((api as any).controlInputs as Array<Record<string, unknown>>)
		: [];
	const outputs = Array.isArray(api?.outputs) ? (api.outputs as Array<Record<string, unknown>>) : [];
	const out: ComponentExposureHandle[] = [];
	for (const port of workInputs) {
		const name = String(port?.name ?? '').trim();
		if (!name) continue;
		out.push({
			handle_id: `work_in::${name}`,
			alias: name,
			internal_source_path: `in:${name}`,
			kind: 'data_input',
			native_contract: normalizeTypedSchema(port?.typedSchema, 'json'),
			exposed: true,
			published: true,
			debug_visible: false
		});
	}
	for (const port of paramInputs) {
		const name = String(port?.name ?? '').trim();
		if (!name) continue;
		out.push({
			handle_id: `param_in::${name}`,
			alias: name,
			internal_source_path: `param:${name}`,
			kind: 'param_input',
			native_contract: normalizeTypedSchema(port?.typedSchema, 'json'),
			exposed: true,
			published: true,
			debug_visible: false
		});
	}
	for (const port of controlInputs) {
		const name = String(port?.name ?? '').trim();
		if (!name) continue;
		out.push({
			handle_id: `control_in::${name}`,
			alias: name,
			internal_source_path: `control:${name}`,
			kind: 'control_input',
			native_contract: normalizeTypedSchema(port?.typedSchema, 'json'),
			exposed: true,
			published: false,
			debug_visible: true
		});
	}
	for (const port of outputs) {
		const name = String(port?.name ?? '').trim();
		if (!name) continue;
		out.push({
			handle_id: `data_out::${name}`,
			alias: name,
			internal_source_path: `out:${name}`,
			kind: 'data_output',
			native_contract: normalizeTypedSchema(port?.typedSchema, 'json'),
			exposed: true,
			published: true,
			debug_visible: false
		});
	}
	return out;
}

export function normalizeExposureRegistry(
	rawRegistry: unknown,
	api: ComponentApiContract
): ComponentExposureHandle[] {
	const value = Array.isArray(rawRegistry) ? rawRegistry : [];
	const out: ComponentExposureHandle[] = [];
	const seen = new Set<string>();
	for (const item of value) {
		if (!item || typeof item !== 'object') continue;
		const raw = item as Record<string, unknown>;
		const kindRaw = String(raw.kind ?? '').trim().toLowerCase() as ComponentExposureKind;
		const kind: ComponentExposureKind = ALLOWED_KINDS.has(kindRaw) ? kindRaw : 'data_output';
		let handleId = String(raw.handle_id ?? raw.handleId ?? '').trim();
		if (!handleId) continue;
		if (seen.has(handleId)) {
			let suffix = 2;
			let next = `${handleId}__${suffix}`;
			while (seen.has(next)) {
				suffix += 1;
				next = `${handleId}__${suffix}`;
			}
			handleId = next;
		}
		seen.add(handleId);
		const exposed = Boolean(raw.exposed ?? true);
		const published = Boolean(raw.published ?? false);
		out.push({
			handle_id: handleId,
			alias: String(raw.alias ?? raw.name ?? '').trim() || handleId,
			internal_source_path: String(raw.internal_source_path ?? raw.internalSourcePath ?? '').trim(),
			kind,
			native_contract: normalizeTypedSchema(raw.native_contract ?? raw.nativeContract, 'json'),
			exposed: published ? true : exposed,
			published,
			debug_visible: Boolean(raw.debug_visible ?? raw.debugVisible ?? false)
		});
	}
	return out.length > 0 ? out : deriveDefaultExposureRegistry(api);
}

export function materializeExposureProfiles(exposureRegistry: ComponentExposureHandle[]): {
	published_profile: ComponentExposureHandle[];
	debug_profile: ComponentExposureHandle[];
} {
	const records = Array.isArray(exposureRegistry) ? exposureRegistry : [];
	const published_profile = records.filter((record) => Boolean(record.published));
	const debug_profile = records.filter(
		(record) => Boolean(record.published) || Boolean(record.debug_visible)
	);
	return { published_profile, debug_profile };
}

export type PublishedProfileDiff = {
	breaking: boolean;
	removed: string[];
	added: string[];
	retyped: Array<{
		handle_id: string;
		before_kind: string;
		after_kind: string;
		before_type: string;
		after_type: string;
	}>;
};

export function comparePublishedProfiles(
	fromProfile: ComponentExposureHandle[],
	toProfile: ComponentExposureHandle[]
): PublishedProfileDiff {
	const before = new Map(fromProfile.map((item) => [String(item.handle_id), item]));
	const after = new Map(toProfile.map((item) => [String(item.handle_id), item]));
	const removed = [...before.keys()].filter((id) => !after.has(id)).sort();
	const added = [...after.keys()].filter((id) => !before.has(id)).sort();
	const retyped: PublishedProfileDiff['retyped'] = [];
	for (const [handleId, left] of before.entries()) {
		const right = after.get(handleId);
		if (!right) continue;
		const beforeKind = String(left.kind ?? '');
		const afterKind = String(right.kind ?? '');
		const beforeType = String(left.native_contract?.type ?? '');
		const afterType = String(right.native_contract?.type ?? '');
		if (beforeKind !== afterKind || beforeType !== afterType) {
			retyped.push({
				handle_id: handleId,
				before_kind: beforeKind,
				after_kind: afterKind,
				before_type: beforeType,
				after_type: afterType
			});
		}
	}
	return { breaking: removed.length > 0 || retyped.length > 0, removed, added, retyped };
}
