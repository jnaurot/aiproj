import { describe, expect, it } from 'vitest';

import { resolveEdgeVisualClass } from '$lib/flow/edgeVisualState';

describe('edge visual state resolver', () => {
	it('marks active work edges as running', () => {
		expect(
			resolveEdgeVisualClass({
				edgeMode: 'work',
				edgeExec: 'active',
				sourceLifecycle: 'running',
				targetLifecycle: 'waiting',
				waiting: true,
				blocked: false,
				full: false
			})
		).toBe('edge-state-running');
	});

	it('settles completed endpoints even when stale waiting flags are present', () => {
		expect(
			resolveEdgeVisualClass({
				edgeMode: 'work',
				edgeExec: 'idle',
				sourceLifecycle: 'completed',
				targetLifecycle: 'completed',
				waiting: true,
				blocked: false,
				full: false
			})
		).toBe('edge-state-settled');
	});

	it('treats blocked/full as blocked when not settled/running', () => {
		expect(
			resolveEdgeVisualClass({
				edgeMode: 'work',
				edgeExec: 'idle',
				sourceLifecycle: 'waiting',
				targetLifecycle: 'waiting',
				waiting: false,
				blocked: true,
				full: false
			})
		).toBe('edge-state-blocked');
	});

	it('keeps non-work edges nonwork', () => {
		expect(
			resolveEdgeVisualClass({
				edgeMode: 'control',
				edgeExec: 'active',
				sourceLifecycle: 'running',
				targetLifecycle: 'running',
				waiting: true,
				blocked: true,
				full: true
			})
		).toBe('edge-state-nonwork');
	});
});
