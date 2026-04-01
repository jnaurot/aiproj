import { describe, expect, it } from 'vitest';

import {
	buildRunMonitorTransitionRows,
	filterRunMonitorTransitionRows
} from './runMonitorModel';

describe('runMonitorModel transition rows', () => {
	it('projects transition events into normalized rows sorted newest-first', () => {
		const rows = buildRunMonitorTransitionRows([
			{
				id: 3,
				runId: 'run_1',
				type: 'state_transition',
				at: '2026-03-31T00:00:01Z',
				payload: {
					entity: 'node',
					entityId: 'n_a',
					source: 'running',
					target: 'blocked',
					reason: 'WAITING_REQUIRED_INPUT'
				}
			},
			{
				id: 4,
				runId: 'run_1',
				type: 'state_transition_violation',
				at: '2026-03-31T00:00:02Z',
				payload: {
					entity: 'run',
					entityId: 'run_1',
					source: 'pending',
					target: 'paused',
					code: 'illegal_transition'
				}
			}
		]);
		expect(rows).toHaveLength(2);
		expect(rows[0]?.id).toBe(4);
		expect(rows[0]?.isViolation).toBe(true);
		expect(rows[0]?.reasonCode).toBe('illegal_transition');
		expect(rows[1]?.entity).toBe('node');
		expect(rows[1]?.entityId).toBe('n_a');
	});

	it('filters transition rows by entity and violation mode', () => {
		const rows = buildRunMonitorTransitionRows([
			{
				id: 1,
				runId: 'run_1',
				type: 'state_transition',
				payload: { entity: 'run', entityId: 'run_1' }
			},
			{
				id: 2,
				runId: 'run_1',
				type: 'state_transition',
				payload: { entity: 'node', entityId: 'n_1' }
			},
			{
				id: 3,
				runId: 'run_1',
				type: 'state_transition_violation',
				payload: { entity: 'run', entityId: 'run_1' }
			}
		]);
		expect(filterRunMonitorTransitionRows(rows, 'all')).toHaveLength(3);
		expect(filterRunMonitorTransitionRows(rows, 'run')).toHaveLength(2);
		expect(filterRunMonitorTransitionRows(rows, 'node')).toHaveLength(1);
		expect(filterRunMonitorTransitionRows(rows, 'violations')).toHaveLength(1);
	});
});

