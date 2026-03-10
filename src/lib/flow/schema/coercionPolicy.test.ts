import { describe, expect, it } from 'vitest';

import { evaluateSchemaCoercion } from './coercionPolicy';

describe('coercionPolicy', () => {
	it('marks native compatibility', () => {
		expect(evaluateSchemaCoercion('table', 'table')).toEqual({
			mode: 'native',
			allowed: true,
			lossy: false
		});
	});

	it('marks safe coercions', () => {
		expect(evaluateSchemaCoercion('text', 'table')).toEqual({
			mode: 'safe',
			allowed: true,
			lossy: false
		});
		expect(evaluateSchemaCoercion('json', 'table')).toEqual({
			mode: 'safe',
			allowed: true,
			lossy: false
		});
	});

	it('marks lossy coercions', () => {
		expect(evaluateSchemaCoercion('json', 'text')).toEqual({
			mode: 'lossy',
			allowed: true,
			lossy: true
		});
	});

	it('blocks unsupported coercions', () => {
		expect(evaluateSchemaCoercion('binary', 'table')).toEqual({
			mode: 'blocked',
			allowed: false,
			lossy: false
		});
	});
});
