export type CoercionMode = 'native' | 'safe' | 'lossy' | 'blocked';
export type CoercionPolicy = 'strict' | 'safe_widening' | 'allow_lossy';

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

function normalizePolicy(raw: unknown): CoercionPolicy {
	const value = String(raw ?? '').trim().toLowerCase();
	if (value === 'allow_lossy') return 'allow_lossy';
	if (value === 'strict' || value === 'forbid') return 'strict';
	return 'safe_widening';
}

export function evaluateSchemaCoercion(
	providedTypeRaw: unknown,
	requiredTypeRaw: unknown,
	policyRaw: unknown = 'safe_widening'
): CoercionDecision {
	const providedType = normalizeType(providedTypeRaw);
	const requiredType = normalizeType(requiredTypeRaw);
	const policy = normalizePolicy(policyRaw);
	if (!providedType || !requiredType) return { mode: 'blocked', allowed: false, lossy: false };
	if (providedType === requiredType) return { mode: 'native', allowed: true, lossy: false };
	const pair = `${providedType}->${requiredType}`;
	if (SAFE_COERCIONS.has(pair)) {
		if (policy === 'strict') return { mode: 'blocked', allowed: false, lossy: false };
		return { mode: 'safe', allowed: true, lossy: false };
	}
	if (LOSSY_COERCIONS.has(pair)) {
		if (policy !== 'allow_lossy') return { mode: 'blocked', allowed: false, lossy: false };
		return { mode: 'lossy', allowed: true, lossy: true };
	}
	return { mode: 'blocked', allowed: false, lossy: false };
}
