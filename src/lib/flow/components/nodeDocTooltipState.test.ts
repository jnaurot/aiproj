import { afterEach, describe, expect, it, vi } from 'vitest';
import { createNodeDocTooltipState } from './nodeDocTooltipState';

describe('nodeDocTooltipState', () => {
	afterEach(() => {
		vi.useRealTimers();
	});

	it('opens after hover delay and expands after expand delay', () => {
		vi.useFakeTimers();
		const state = createNodeDocTooltipState();
		state.enter();
		expect(state.snapshot().open).toBe(false);
		vi.advanceTimersByTime(500);
		expect(state.snapshot().open).toBe(true);
		expect(state.snapshot().expanded).toBe(false);
		vi.advanceTimersByTime(700);
		expect(state.snapshot().expanded).toBe(true);
		state.destroy();
	});

	it('supports keyboard expand and escape close', () => {
		vi.useFakeTimers();
		const state = createNodeDocTooltipState();
		state.enter();
		vi.advanceTimersByTime(500);
		expect(state.snapshot().open).toBe(true);
		state.keydown('?');
		expect(state.snapshot().expanded).toBe(true);
		state.keydown('Escape');
		expect(state.snapshot().open).toBe(false);
		expect(state.snapshot().expanded).toBe(false);
		state.destroy();
	});

	it('cleans timers on destroy', () => {
		vi.useFakeTimers();
		const onChange = vi.fn();
		const state = createNodeDocTooltipState({ onChange });
		state.enter();
		state.destroy();
		vi.runAllTimers();
		expect(onChange).not.toHaveBeenCalled();
	});
});
