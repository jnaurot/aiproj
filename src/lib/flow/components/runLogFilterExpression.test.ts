import { describe, expect, it } from 'vitest';
import { buildRunLogFilterPredicate } from './runLogFilterExpression';

describe('run log filter expression', () => {
	it('supports simple term matching', () => {
		const match = buildRunLogFilterPredicate('error');
		expect(match('node failed with ERROR code')).toBe(true);
		expect(match('node finished succeeded')).toBe(false);
	});

	it('supports quoted phrases', () => {
		const match = buildRunLogFilterPredicate('"node started" & error');
		expect(match('10:00 node started then ERROR')).toBe(true);
		expect(match('10:00 node started then warn')).toBe(false);
	});

	it('supports OR and AND with precedence', () => {
		const match = buildRunLogFilterPredicate('foo | bar & baz');
		expect(match('foo')).toBe(true);
		expect(match('bar baz')).toBe(true);
		expect(match('bar only')).toBe(false);
	});

	it('supports parenthesis for grouping', () => {
		const match = buildRunLogFilterPredicate('(foo | bar) & baz');
		expect(match('foo baz')).toBe(true);
		expect(match('bar baz')).toBe(true);
		expect(match('foo only')).toBe(false);
	});

	it('supports square brackets as grouping', () => {
		const match = buildRunLogFilterPredicate('[foo | bar] & baz');
		expect(match('foo baz')).toBe(true);
		expect(match('bar baz')).toBe(true);
		expect(match('bar only')).toBe(false);
	});

	it('supports escaping operators as literal text', () => {
		const match = buildRunLogFilterPredicate('error \\| warning');
		expect(match('error | warning')).toBe(true);
		expect(match('error warning')).toBe(false);
	});

	it('supports escaped operators inside quotes', () => {
		const match = buildRunLogFilterPredicate('"a \\& b" | c');
		expect(match('x a & b y')).toBe(true);
		expect(match('x c y')).toBe(true);
		expect(match('x a b y')).toBe(false);
	});

	it('supports unary NOT', () => {
		const match = buildRunLogFilterPredicate('error & !timeout');
		expect(match('error occurred')).toBe(true);
		expect(match('error timeout occurred')).toBe(false);
	});

	it('supports NOT over grouped expressions', () => {
		const match = buildRunLogFilterPredicate('!(pause | resume) & run');
		expect(match('run started')).toBe(true);
		expect(match('run paused')).toBe(false);
		expect(match('resume run')).toBe(false);
	});

	it('supports escaping literal exclamation mark', () => {
		const match = buildRunLogFilterPredicate('\\!important & notice');
		expect(match('!important notice')).toBe(true);
		expect(match('important notice')).toBe(false);
	});

	it('uses implicit AND for adjacent terms', () => {
		const match = buildRunLogFilterPredicate('node failed');
		expect(match('node failed hard')).toBe(true);
		expect(match('node completed')).toBe(false);
	});

	it('falls back safely for malformed expressions', () => {
		const match = buildRunLogFilterPredicate('(error |');
		expect(match('error happened')).toBe(true);
		expect(match('warning happened')).toBe(false);
	});
});
