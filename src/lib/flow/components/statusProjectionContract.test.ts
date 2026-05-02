import { describe, expect, it } from 'vitest';

import backendNodeStates from './backend-node-states.fixture.json';
import {
	normalizeRuntimeStatus,
	projectNodeStatus,
	type NodeExecutionStatus,
	type NodeFreshnessStatus,
	type NodeLifecycleStatus
} from '$lib/flow/store/statusModel';

const LIFECYCLE_SET = new Set<NodeLifecycleStatus>([
	'idle',
	'waiting',
	'running',
	'blocked',
	'completed',
	'failed',
	'canceled',
	'skipped'
]);
const EXECUTION_SET = new Set<NodeExecutionStatus>(['inactive', 'waiting', 'running', 'blocked', 'finished']);
const FRESHNESS_SET = new Set<NodeFreshnessStatus>(['fresh', 'stale', 'unknown']);

describe('status projection contract (backend node states)', () => {
	it('uses JSON fixture as source-of-truth contract list', () => {
		expect(Array.isArray(backendNodeStates)).toBe(true);
		expect(backendNodeStates.length).toBeGreaterThan(0);
	});

	it('normalizes every backend node state and projects valid lifecycle/execution/freshness (binding path)', () => {
		for (const status of backendNodeStates) {
			const normalized = normalizeRuntimeStatus(status);
			expect(normalized, `status=${status}`).not.toBeNull();
			const projection = projectNodeStatus({ status });
			expect(LIFECYCLE_SET.has(projection.lifecycle), `lifecycle status=${status}`).toBe(true);
			expect(EXECUTION_SET.has(projection.execution), `execution status=${status}`).toBe(true);
			expect(FRESHNESS_SET.has(projection.freshness), `freshness status=${status}`).toBe(true);
		}
	});

	it('normalizes every backend node state and projects valid lifecycle/execution/freshness (runtime arg path)', () => {
		for (const status of backendNodeStates) {
			const projection = projectNodeStatus({}, status);
			expect(LIFECYCLE_SET.has(projection.lifecycle), `lifecycle status=${status}`).toBe(true);
			expect(EXECUTION_SET.has(projection.execution), `execution status=${status}`).toBe(true);
			expect(FRESHNESS_SET.has(projection.freshness), `freshness status=${status}`).toBe(true);
		}
	});

	it('uses explicit runtime status when binding and runtime arg are both provided', () => {
		const projection = projectNodeStatus({ status: 'failed' }, 'running');
		expect(projection.runtime).toBe('running');
		expect(projection.lifecycle).toBe('running');
		expect(projection.execution).toBe('running');
	});

	it('asserts compressed mappings for non-obvious backend states', () => {
		const active = projectNodeStatus({ status: 'active' });
		expect(active.lifecycle).toBe('running');

		const succeeded = projectNodeStatus({ status: 'succeeded_up_to_date' });
		expect(succeeded.lifecycle).toBe('completed');

		const paused = projectNodeStatus({ status: 'paused' });
		expect(paused.lifecycle).toBe('waiting');
		expect(paused.execution).toBe('waiting');

		const stale = projectNodeStatus({ status: 'stale' });
		expect(stale.lifecycle).toBe('completed');
		expect(stale.freshness).toBe('stale');
		expect(stale.execution).toBe('inactive');
	});

	it('handles unknown backend status deterministically', () => {
		expect(normalizeRuntimeStatus('new_backend_state')).toBeNull();
		const projection = projectNodeStatus({ status: 'new_backend_state' });
		expect(projection.lifecycle).toBe('idle');
		expect(projection.execution).toBe('inactive');
		expect(projection.freshness).toBe('unknown');
	});
});

