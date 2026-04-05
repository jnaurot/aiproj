import { describe, expect, it } from 'vitest';

import capsRaw from '../../../shared/schema_capabilities.v1.json';

describe('schema capabilities plane contract', () => {
	it('declares exactly three canonical planes and excludes legacy config plane', () => {
		const planes = Array.isArray((capsRaw as any)?.portDeclarations?.planes)
			? (((capsRaw as any).portDeclarations.planes as unknown[]) ?? [])
			: [];
		const normalized = planes.map((v) => String(v).trim().toLowerCase()).filter(Boolean);
		expect(normalized).toEqual(['work', 'param', 'control']);
		expect(normalized.includes('config')).toBe(false);
	});
});
