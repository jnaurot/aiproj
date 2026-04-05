import type { PipelineNodeData } from '$lib/flow/types';

export type HandlePlane = 'work' | 'param' | 'control';

export type NodeHandleDef = {
	id: string;
	label?: string;
	plane?: HandlePlane;
};

export function portHintText(direction: 'in' | 'out', handle: NodeHandleDef): string {
	const plane = String(handle?.plane ?? inferPlaneFromHandleId(String(handle?.id ?? ''))).trim() || 'work';
	const id = String(handle?.id ?? '').trim() || (direction === 'in' ? 'in' : 'out');
	const label = String(handle?.label ?? '').trim();
	const role = direction === 'in' ? 'Input' : 'Output';
	const shown = label.length > 0 ? `${label} (${id})` : id;
	return `${role}: ${shown} [${plane}]`;
}

function toPlane(value: unknown): HandlePlane | null {
	const v = String(value ?? '')
		.trim()
		.toLowerCase();
	if (v === 'config') return 'param';
	if (v === 'work' || v === 'param' || v === 'control') return v;
	return null;
}

export function inferPlaneFromHandleId(id: string): HandlePlane {
	const raw = String(id ?? '')
		.trim()
		.toLowerCase();
	if (raw.startsWith('param')) return 'param';
	if (raw.startsWith('config')) return 'param';
	if (raw.startsWith('control') || raw.startsWith('ctl')) return 'control';
	return 'work';
}

function titleFromHandle(id: string): string {
	const text = String(id ?? '').trim();
	if (!text) return '';
	return text
		.replace(/[_-]+/g, ' ')
		.replace(/\s+/g, ' ')
		.trim();
}

function declarationByHandle(data: PipelineNodeData, direction: 'in' | 'out', id: string): Record<string, unknown> | null {
	const declarations =
		data && typeof (data as any).portDeclarations === 'object'
			? ((data as any).portDeclarations as Record<string, unknown>)
			: null;
	const byDir =
		declarations && declarations[direction] && typeof declarations[direction] === 'object'
			? (declarations[direction] as Record<string, unknown>)
			: null;
	if (!byDir) return null;
	const direct = byDir[id];
	if (direct && typeof direct === 'object') return direct as Record<string, unknown>;
	const fallback = byDir.default;
	if (fallback && typeof fallback === 'object') return fallback as Record<string, unknown>;
	return null;
}

function declaredHandleIds(data: PipelineNodeData, direction: 'in' | 'out'): string[] {
	const declarations =
		data && typeof (data as any).portDeclarations === 'object'
			? ((data as any).portDeclarations as Record<string, unknown>)
			: null;
	const byDir =
		declarations && declarations[direction] && typeof declarations[direction] === 'object'
			? (declarations[direction] as Record<string, unknown>)
			: null;
	if (!byDir) return [];
	const ids = Object.keys(byDir)
		.map((key) => String(key ?? '').trim())
		.filter((key) => key.length > 0 && key !== 'default');
	if (ids.length > 0) return ids;
	if (Object.prototype.hasOwnProperty.call(byDir, 'default')) {
		return [direction === 'in' ? 'in' : 'out'];
	}
	return [];
}

function normalizeHandle(
	data: PipelineNodeData,
	direction: 'in' | 'out',
	handle: NodeHandleDef
): NodeHandleDef {
	const id = String(handle?.id ?? '').trim();
	if (!id) return { id: direction === 'in' ? 'in' : 'out', label: direction === 'in' ? 'in' : 'out', plane: 'work' };
	const decl = declarationByHandle(data, direction, id);
	const declaredPlane = toPlane((decl as any)?.plane) ?? toPlane((decl as any)?.affinity);
	const plane = handle?.plane ?? declaredPlane ?? inferPlaneFromHandleId(id);
	const label = String(handle?.label ?? (decl as any)?.label ?? titleFromHandle(id)).trim() || id;
	return { id, label, plane };
}

export function resolveNodeHandles(
	data: PipelineNodeData,
	direction: 'in' | 'out',
	provided: NodeHandleDef[] | null,
	ioType: unknown
): NodeHandleDef[] {
	const fromProvided =
		Array.isArray(provided) && provided.length > 0
			? provided
					.map((h) => ({ id: String(h?.id ?? '').trim(), label: h?.label, plane: h?.plane }))
					.filter((h) => h.id.length > 0)
			: [];
	const fromDeclared = declaredHandleIds(data, direction).map((id) => ({ id }));
	const hasAny = fromProvided.length > 0 || fromDeclared.length > 0;
	const fallback =
		!hasAny && ioType !== null && ioType !== undefined ? [{ id: direction === 'in' ? 'in' : 'out' }] : [];
	const merged = [...fromProvided, ...fromDeclared, ...fallback];
	const deduped = new Map<string, NodeHandleDef>();
	for (const handle of merged) {
		const id = String(handle?.id ?? '').trim();
		if (!id) continue;
		if (!deduped.has(id)) deduped.set(id, handle);
	}
	return Array.from(deduped.values()).map((h) => normalizeHandle(data, direction, h));
}
