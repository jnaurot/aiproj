import type { NodeDocGeneratedExplanation, NodeDocExplanationMode } from '$lib/flow/schema/nodeDocs';
import type { NodeDocLlmContext } from './nodeDocLlmContext';
import {
	generateNodeDocLlmExplanation,
	type NodeDocLlmServiceOptions,
	type NodeDocLlmServiceResult
} from './nodeDocLlmService';

type CacheEntry = {
	key: string;
	explanation: NodeDocGeneratedExplanation;
	createdAt: string;
};

const cache = new Map<string, CacheEntry>();

function cacheKey(mode: NodeDocExplanationMode, nodeId: string, signature: string): string {
	return `${mode}::${String(nodeId ?? '').trim()}::${String(signature ?? '').trim()}`;
}

export function clearNodeDocLlmCache(): void {
	cache.clear();
}

export function clearNodeDocLlmCacheEntry(
	mode: NodeDocExplanationMode,
	nodeId: string,
	signature: string
): void {
	const key = cacheKey(mode, nodeId, signature);
	cache.delete(key);
}

export function getNodeDocLlmCacheEntry(
	mode: NodeDocExplanationMode,
	nodeId: string,
	signature: string
): CacheEntry | null {
	const key = cacheKey(mode, nodeId, signature);
	return cache.get(key) ?? null;
}

export async function getOrGenerateNodeDocLlmExplanation(
	mode: NodeDocExplanationMode,
	nodeId: string,
	context: NodeDocLlmContext,
	signature: string,
	options: NodeDocLlmServiceOptions = {}
): Promise<NodeDocLlmServiceResult> {
	const key = cacheKey(mode, nodeId, signature);
	const existing = cache.get(key);
	if (existing) {
		return {
			explanation: existing.explanation,
			telemetry: { status: 'success', latencyMs: 0, cacheHit: true }
		};
	}
	const generated = await generateNodeDocLlmExplanation(context, signature, options);
	if (generated.explanation) {
		cache.set(key, {
			key,
			explanation: generated.explanation,
			createdAt: new Date().toISOString()
		});
	}
	return generated;
}
