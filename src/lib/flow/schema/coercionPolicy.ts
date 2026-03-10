export type CoercionMode = 'native' | 'safe' | 'lossy' | 'blocked';

export type CoercionDecision = {
	mode: CoercionMode;
	allowed: boolean;
	lossy: boolean;
};

function normalizeType(raw: unknown): string {
	const value = String(raw ?? '').trim().toLowerCase();
	if (value === 'string') return 'text';
	return value;
}

const SAFE_COERCIONS = new Set([
	'text->table',
	'json->table',
	'table->json'
]);

const LOSSY_COERCIONS = new Set([
	'json->text',
	'text->json'
]);

export function evaluateSchemaCoercion(providedTypeRaw: unknown, requiredTypeRaw: unknown): CoercionDecision {
	const providedType = normalizeType(providedTypeRaw);
	const requiredType = normalizeType(requiredTypeRaw);
	if (!providedType || !requiredType) return { mode: 'blocked', allowed: false, lossy: false };
	if (providedType === requiredType) return { mode: 'native', allowed: true, lossy: false };
	const pair = `${providedType}->${requiredType}`;
	if (SAFE_COERCIONS.has(pair)) return { mode: 'safe', allowed: true, lossy: false };
	if (LOSSY_COERCIONS.has(pair)) return { mode: 'lossy', allowed: true, lossy: true };
	return { mode: 'blocked', allowed: false, lossy: false };
}
