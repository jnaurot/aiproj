import { describe, expect, it } from 'vitest';

import { evaluateSchemaCoercion } from './coercionPolicy';

describe('evaluateSchemaCoercion policy behavior', () => {
	it('allows only native coercions in strict policy', () => {
		expect(evaluateSchemaCoercion('text', 'text', 'strict')).toEqual({
			mode: 'native',
			allowed: true,
			lossy: false
		});
		expect(evaluateSchemaCoercion('text', 'table', 'strict').allowed).toBe(false);
	});

	it('allows safe widening but blocks lossy coercions in safe_widening policy', () => {
		expect(evaluateSchemaCoercion('text', 'table', 'safe_widening')).toEqual({
			mode: 'safe',
			allowed: true,
			lossy: false
		});
		expect(evaluateSchemaCoercion('json', 'text', 'safe_widening').allowed).toBe(false);
	});

	it('allows lossy coercions in allow_lossy policy', () => {
		expect(evaluateSchemaCoercion('json', 'text', 'allow_lossy')).toEqual({
			mode: 'lossy',
			allowed: true,
			lossy: true
		});
	});
});
