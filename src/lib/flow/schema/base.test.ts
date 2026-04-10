import { describe, expect, it } from 'vitest';
import { NodeMetaSchema } from '$lib/flow/schema/base';

describe('NodeMetaSchema memoizable', () => {
	it('accepts memoizable false', () => {
		const parsed = NodeMetaSchema.parse({ memoizable: false });
		expect(parsed.memoizable).toBe(false);
	});

	it('accepts memoizable true', () => {
		const parsed = NodeMetaSchema.parse({ memoizable: true });
		expect(parsed.memoizable).toBe(true);
	});

	it('accepts memoizable absent', () => {
		const parsed = NodeMetaSchema.parse({});
		expect(parsed.memoizable).toBeUndefined();
	});

	it('rejects non-boolean memoizable', () => {
		expect(() => NodeMetaSchema.parse({ memoizable: 'yes' })).toThrow();
	});
});
