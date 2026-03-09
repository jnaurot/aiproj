import { describe, expect, it } from 'vitest';

import { parseComponentExitDecision } from './componentExitGuard';

describe('parseComponentExitDecision', () => {
	it('parses save options', () => {
		expect(parseComponentExitDecision('1')).toBe('save');
		expect(parseComponentExitDecision('save')).toBe('save');
		expect(parseComponentExitDecision('S')).toBe('save');
	});

	it('parses discard options', () => {
		expect(parseComponentExitDecision('2')).toBe('discard');
		expect(parseComponentExitDecision('discard')).toBe('discard');
		expect(parseComponentExitDecision('D')).toBe('discard');
	});

	it('defaults to cancel for unknown or empty values', () => {
		expect(parseComponentExitDecision('3')).toBe('cancel');
		expect(parseComponentExitDecision('')).toBe('cancel');
		expect(parseComponentExitDecision(null)).toBe('cancel');
		expect(parseComponentExitDecision(undefined)).toBe('cancel');
	});
});

