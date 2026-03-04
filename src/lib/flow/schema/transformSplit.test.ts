import { describe, expect, it } from 'vitest';
import { TransformSplitParamsSchema } from './transform';

describe('TransformSplitParamsSchema', () => {
	it('accepts defaults and valid mode-specific params', () => {
		const parsed = TransformSplitParamsSchema.parse({});
		expect(parsed.mode).toBe('sentences');
		expect(parsed.sourceColumn).toBe('text');
		expect(parsed.outColumn).toBe('part');
		expect(parsed.maxParts).toBe(5000);
		expect(parsed.emitSourceRow).toBe(true);

		expect(() =>
			TransformSplitParamsSchema.parse({
				mode: 'regex',
				pattern: '\\s+',
				flags: 'im'
			})
		).not.toThrow();

		expect(() =>
			TransformSplitParamsSchema.parse({
				mode: 'delimiter',
				delimiter: '\\n'
			})
		).not.toThrow();
	});

	it('rejects invalid mode-specific params and bounds', () => {
		expect(() => TransformSplitParamsSchema.parse({ mode: 'regex' })).toThrow(
			/Pattern is required/
		);
		expect(() => TransformSplitParamsSchema.parse({ mode: 'delimiter' })).toThrow(
			/Delimiter is required/
		);
		expect(() => TransformSplitParamsSchema.parse({ flags: 'x' })).toThrow(
			/Flags must contain only i, m, s/
		);
		expect(() => TransformSplitParamsSchema.parse({ maxParts: 0 })).toThrow();
		expect(() => TransformSplitParamsSchema.parse({ maxParts: 100001 })).toThrow();
	});
});
