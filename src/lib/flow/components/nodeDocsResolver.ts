import { NodeDocV1Schema, type NodeDocV1 } from '$lib/flow/schema/nodeDocs';
import { NODE_DOCS_REGISTRY, type NodeDocRegistry } from './nodeDocsRegistry';

export type NodeDocLookupKind = 'source' | 'transform' | 'model' | 'llm' | 'tool' | 'component';

const ORDERED_KINDS: NodeDocLookupKind[] = ['source', 'transform', 'model', 'llm', 'tool', 'component'];

function canonicalDocKind(kindRaw: unknown): NodeDocV1['node_kind'] | null {
	const kind = String(kindRaw ?? '')
		.trim()
		.toLowerCase();
	if (!kind) return null;
	if (kind === 'llm') return 'model';
	if (kind === 'source' || kind === 'transform' || kind === 'model' || kind === 'tool' || kind === 'component') {
		return kind;
	}
	return null;
}

function normalizeSubtype(raw: unknown): string {
	const subtype = String(raw ?? '').trim();
	return subtype.length > 0 ? subtype : '*';
}

export function validateNodeDocRegistryEntry(entry: unknown): NodeDocV1 {
	return NodeDocV1Schema.parse(entry);
}

function parseRegistryEntrySafe(entry: unknown): NodeDocV1 | null {
	const parsed = NodeDocV1Schema.safeParse(entry);
	if (!parsed.success) return null;
	return parsed.data;
}

export function validateNodeDocRegistry(registry: NodeDocRegistry): NodeDocRegistry {
	for (const kind of Object.keys(registry) as Array<keyof NodeDocRegistry>) {
		const bySubtype = registry[kind] ?? {};
		for (const [subtype, doc] of Object.entries(bySubtype)) {
			try {
				validateNodeDocRegistryEntry(doc);
			} catch (error) {
				throw new Error(`Invalid node doc registry entry for ${String(kind)}:${String(subtype)} (${String(error)})`);
			}
		}
	}
	return registry;
}

export function hasNodeDocWildcardForKind(
	registry: NodeDocRegistry,
	kind: NodeDocLookupKind
): { ok: true } | { ok: false; reason: string } {
	const canonicalKind = canonicalDocKind(kind);
	if (!canonicalKind) {
		return { ok: false, reason: `Unknown node kind: ${String(kind)}` };
	}
	const kindEntries = registry[canonicalKind];
	if (!kindEntries) {
		return { ok: false, reason: `Missing registry bucket for kind: ${canonicalKind}` };
	}
	if (!kindEntries['*']) {
		return { ok: false, reason: `Missing wildcard registry entry for kind: ${canonicalKind}` };
	}
	return { ok: true };
}

export function supportedNodeDocKinds(): NodeDocLookupKind[] {
	return [...ORDERED_KINDS];
}

export function resolveNodeDocBase(
	kindRaw: unknown,
	subtypeRaw?: unknown,
	registry: NodeDocRegistry = NODE_DOCS_REGISTRY
): NodeDocV1 | null {
	const kind = canonicalDocKind(kindRaw);
	if (!kind) return null;
	const bySubtype = registry[kind];
	if (!bySubtype) return null;
	const subtype = normalizeSubtype(subtypeRaw);
	const exact = bySubtype[subtype];
	if (exact) return parseRegistryEntrySafe(exact);
	const wildcard = bySubtype['*'];
	if (wildcard) return parseRegistryEntrySafe(wildcard);
	return null;
}
