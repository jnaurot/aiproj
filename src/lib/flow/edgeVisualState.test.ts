import { describe, expect, it } from 'vitest';

import { resolveEdgeVisualClass, computeEdgeVisualClass } from '$lib/flow/edgeVisualState';

const execBase = {
	edgeMode: 'work',
	edgeExec: 'idle',
	sourceLifecycle: 'idle',
	targetLifecycle: 'idle',
	waiting: false,
	blocked: false,
	full: false
};

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
				targetFreshness: 'fresh',
				waiting: true,
				blocked: false,
				full: false
			})
		).toBe('edge-state-settled');
	});

	it('does not settle completed endpoints when target is stale', () => {
		expect(
			resolveEdgeVisualClass({
				edgeMode: 'work',
				edgeExec: 'idle',
				sourceLifecycle: 'completed',
				targetLifecycle: 'completed',
				targetFreshness: 'stale',
				waiting: false,
				blocked: false,
				full: false
			})
		).toBe('edge-state-inactive');
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

// ---------------------------------------------------------------------------
// Phase 2a — computeEdgeVisualClass (schema-view-aware)
// ---------------------------------------------------------------------------
describe('computeEdgeVisualClass — schema view suppresses execution signals', () => {
	it('schema error → edge-state-blocked in schema view', () => {
		expect(
			computeEdgeVisualClass({
				...execBase,
				viewMode: 'schema',
				schemaClass: 'edge-schema-error'
			})
		).toBe('edge-state-blocked');
	});

	it('schema warning → edge-state-waiting in schema view', () => {
		expect(
			computeEdgeVisualClass({
				...execBase,
				viewMode: 'schema',
				schemaClass: 'edge-schema-warning'
			})
		).toBe('edge-state-waiting');
	});

	it('no schema issue → edge-state-inactive in schema view even when monitor flags say waiting', () => {
		expect(
			computeEdgeVisualClass({
				...execBase,
				viewMode: 'schema',
				schemaClass: '',
				waiting: true
			})
		).toBe('edge-state-inactive');
	});

	it('no schema issue → edge-state-inactive in schema view even when both nodes completed', () => {
		expect(
			computeEdgeVisualClass({
				...execBase,
				viewMode: 'schema',
				schemaClass: '',
				sourceLifecycle: 'completed',
				targetLifecycle: 'completed'
			})
		).toBe('edge-state-inactive');
	});

	it('no schema issue → edge-state-inactive in schema view even when exec is active', () => {
		expect(
			computeEdgeVisualClass({
				...execBase,
				viewMode: 'schema',
				schemaClass: '',
				edgeExec: 'active'
			})
		).toBe('edge-state-inactive');
	});
});

describe('computeEdgeVisualClass — execution view delegates to resolveEdgeVisualClass', () => {
	it('running edge in execution view → edge-state-running', () => {
		expect(
			computeEdgeVisualClass({
				...execBase,
				viewMode: 'execution',
				schemaClass: '',
				edgeExec: 'active'
			})
		).toBe('edge-state-running');
	});

	it('settled nodes in execution view → edge-state-settled', () => {
		expect(
			computeEdgeVisualClass({
				...execBase,
				viewMode: 'execution',
				schemaClass: '',
				sourceLifecycle: 'completed',
				targetLifecycle: 'completed',
				targetFreshness: 'fresh'
			})
		).toBe('edge-state-settled');
	});

	it('stale completed target in execution view → edge-state-inactive', () => {
		expect(
			computeEdgeVisualClass({
				...execBase,
				viewMode: 'execution',
				schemaClass: '',
				sourceLifecycle: 'completed',
				targetLifecycle: 'completed',
				targetFreshness: 'stale'
			})
		).toBe('edge-state-inactive');
	});

	it('mixed fan-out keeps stale branch inactive while sibling branch is running', () => {
		const runningBranch = computeEdgeVisualClass({
			...execBase,
			viewMode: 'execution',
			schemaClass: '',
			edgeExec: 'active',
			sourceLifecycle: 'completed',
			targetLifecycle: 'running'
		});
		const staleBranch = computeEdgeVisualClass({
			...execBase,
			viewMode: 'execution',
			schemaClass: '',
			sourceLifecycle: 'completed',
			targetLifecycle: 'completed',
			targetFreshness: 'stale'
		});
		expect(runningBranch).toBe('edge-state-running');
		expect(staleBranch).toBe('edge-state-inactive');
	});

	it('schema error class is present but execution view ignores schema → uses exec logic', () => {
		// In execution view the schemaClass is appended to the CSS classes separately;
		// computeEdgeVisualClass only drives the visualClass part.
		expect(
			computeEdgeVisualClass({
				...execBase,
				viewMode: 'execution',
				schemaClass: 'edge-schema-error',
				edgeExec: 'idle',
				waiting: true
			})
		).toBe('edge-state-waiting');
	});
});
