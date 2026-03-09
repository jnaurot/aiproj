import { describe, expect, it } from 'vitest';

import {
	TOOL_BUILTIN_ALLOWED_PACKAGE_NAMES,
	validateCustomPackageDraft
} from './toolBuiltinCustomPackages';

describe('tool builtin custom package validation', () => {
	it('accepts allowlisted packages and dedupes cleanly', () => {
		const result = validateCustomPackageDraft('numpy\nnumpy\npandas>=2.0');
		expect(result.packages).toEqual(['numpy', 'pandas>=2.0']);
		expect(result.duplicates).toEqual(['numpy']);
		expect(result.errors.some((e) => e.includes('duplicate entries removed'))).toBe(true);
	});

	it('reports invalid and blocked packages inline', () => {
		const result = validateCustomPackageDraft('@@bad@@\nsome-new-lib\nrequests');
		expect(result.packages).toEqual(['requests']);
		expect(result.blocked).toEqual(['some-new-lib']);
		expect(result.errors.some((e) => e.includes('invalid package spec'))).toBe(true);
		expect(result.errors.some((e) => e.includes('blocked package'))).toBe(true);
	});

	it('exports allowlist for known profiles', () => {
		expect(TOOL_BUILTIN_ALLOWED_PACKAGE_NAMES.has('numpy')).toBe(true);
		expect(TOOL_BUILTIN_ALLOWED_PACKAGE_NAMES.has('transformers')).toBe(true);
	});
});

