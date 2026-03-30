import { describe, expect, it } from 'vitest';

import { buildScopedStatus, type StatusSnapshot } from './statusScope';

const graphBase: StatusSnapshot = {
	runStatus: 'idle',
	lastRunStatus: 'succeeded',
	freshness: 'fresh',
	staleNodeCount: 0,
	unsaved: false
};

const componentBase: StatusSnapshot = {
	runStatus: 'idle',
	lastRunStatus: 'failed',
	freshness: 'stale',
	staleNodeCount: 2,
	unsaved: true
};

describe('buildScopedStatus', () => {
	it('uses graph status source in graph context', () => {
		const out = buildScopedStatus({
			editingContext: 'graph',
			graph: graphBase,
			component: componentBase
		});
		expect(out).toEqual({
			statusText: 'Succeeded',
			tone: 'succeeded',
			unsaved: false
		});
	});

	it('uses component status source in component context', () => {
		const out = buildScopedStatus({
			editingContext: 'component',
			graph: graphBase,
			component: componentBase
		});
		expect(out).toEqual({
			statusText: 'Failed + Needs rerun (2 stale)',
			tone: 'failed',
			unsaved: true
		});
	});

	it('prioritizes running tone and text in active scope', () => {
		const out = buildScopedStatus({
			editingContext: 'component',
			graph: { ...graphBase, runStatus: 'running', unsaved: true },
			component: { ...componentBase, runStatus: 'running', unsaved: false }
		});
		expect(out).toEqual({
			statusText: 'Running',
			tone: 'running',
			unsaved: false
		});
	});

	it('renders pausing, paused, and resuming as transitional running tone states', () => {
		const pausing = buildScopedStatus({
			editingContext: 'graph',
			graph: { ...graphBase, runStatus: 'pausing' },
			component: componentBase
		});
		const paused = buildScopedStatus({
			editingContext: 'graph',
			graph: { ...graphBase, runStatus: 'paused' },
			component: componentBase
		});
		const resuming = buildScopedStatus({
			editingContext: 'graph',
			graph: { ...graphBase, runStatus: 'resuming' },
			component: componentBase
		});
		expect(pausing).toEqual({ statusText: 'Pausing', tone: 'running', unsaved: false });
		expect(paused).toEqual({ statusText: 'Paused', tone: 'running', unsaved: false });
		expect(resuming).toEqual({ statusText: 'Resuming', tone: 'running', unsaved: false });
	});
});
