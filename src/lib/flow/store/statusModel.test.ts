import { describe, expect, it } from 'vitest';

import {
	normalizeRuntimeStatus,
	projectEdgeStatus,
	projectNodeStatus,
	reconcileLifecycleForActiveRun,
	toDisplayNodeStatus
} from './statusModel';

describe('statusModel node projection', () => {
	it('maps running runtime to running lifecycle/execution/display', () => {
		const projection = projectNodeStatus({ status: 'active' });
		expect(projection.lifecycle).toBe('running');
		expect(projection.execution).toBe('running');
		expect(projection.display).toBe('running');
	});

	it('maps blocked runtime to blocked lifecycle and busy display', () => {
		const projection = projectNodeStatus({ status: 'blocked' });
		expect(projection.lifecycle).toBe('blocked');
		expect(projection.execution).toBe('blocked');
		expect(projection.display).toBe('busy');
	});

	it('maps succeeded runtime to completed lifecycle with fresh/stale freshness', () => {
		const fresh = projectNodeStatus({ status: 'succeeded_up_to_date', isUpToDate: true });
		expect(fresh.lifecycle).toBe('completed');
		expect(fresh.freshness).toBe('fresh');
		expect(fresh.display).toBe('succeeded');

		const stale = projectNodeStatus({ status: 'succeeded_up_to_date', isUpToDate: false });
		expect(stale.lifecycle).toBe('completed');
		expect(stale.freshness).toBe('stale');
		expect(stale.display).toBe('stale');
	});

	it('maps skipped runtime to skipped lifecycle/display', () => {
		const projection = projectNodeStatus({ status: 'skipped' });
		expect(projection.lifecycle).toBe('skipped');
		expect(projection.display).toBe('skipped');
	});

	it('uses artifact fallback as completed lifecycle', () => {
		const projection = projectNodeStatus({ currentArtifactId: 'art-1' });
		expect(projection.lifecycle).toBe('completed');
		expect(projection.execution).toBe('finished');
		expect(projection.display).toBe('succeeded');
	});

	it('normalizes supported runtime statuses only', () => {
		expect(normalizeRuntimeStatus('RUNNING')).toBe('running');
		expect(normalizeRuntimeStatus('skipped')).toBe('skipped');
		expect(normalizeRuntimeStatus('cancelled')).toBe('canceled');
		expect(normalizeRuntimeStatus('unknown')).toBeNull();
	});
});

describe('statusModel edge projection', () => {
	it('treats active non-work edges as waiting, not running', () => {
		const projection = projectEdgeStatus({ exec: 'active', mode: 'param', depth: 1 });
		expect(projection.lifecycle).toBe('waiting');
		expect(projection.exec).toBe('active');
	});

	it('treats active work edges as running', () => {
		const projection = projectEdgeStatus({ exec: 'active', mode: 'work' });
		expect(projection.lifecycle).toBe('running');
	});

	it('marks done exec as done lifecycle', () => {
		const projection = projectEdgeStatus({ exec: 'done', mode: 'work' });
		expect(projection.lifecycle).toBe('done');
	});
});

describe('toDisplayNodeStatus', () => {
	it('maps lifecycle/freshness to legacy display enum', () => {
		expect(toDisplayNodeStatus('completed', 'stale')).toBe('stale');
		expect(toDisplayNodeStatus('completed', 'fresh')).toBe('succeeded');
		expect(toDisplayNodeStatus('waiting')).toBe('busy');
		expect(toDisplayNodeStatus('canceled')).toBe('canceled');
	});
});

describe('reconcileLifecycleForActiveRun', () => {
	it('keeps once nodes completed during active run when no pending signals remain', () => {
		expect(
			reconcileLifecycleForActiveRun({
				lifecycle: 'completed',
				consumeMode: 'once',
				runStatus: 'running',
				inflight: 0,
				pendingInputCount: 0,
				readyWork: false
			})
		).toBe('completed');
	});

	it('downgrades single/batch completed nodes to waiting during active run', () => {
		expect(
			reconcileLifecycleForActiveRun({
				lifecycle: 'completed',
				consumeMode: 'single_item',
				runStatus: 'running',
				inflight: 0,
				pendingInputCount: 0,
				readyWork: false
			})
		).toBe('waiting');
		expect(
			reconcileLifecycleForActiveRun({
				lifecycle: 'completed',
				consumeMode: 'batch',
				runStatus: 'resuming',
				inflight: 0,
				pendingInputCount: 0,
				readyWork: false
			})
		).toBe('waiting');
	});
});
