import { describe, expect, it } from 'vitest';

import { buildRunMonitorAdaptiveDecisionRows } from './runMonitorModel';

describe('runMonitorModel adaptive decision rows', () => {
	it('projects and sorts adaptive decision timeline rows', () => {
		const rows = buildRunMonitorAdaptiveDecisionRows({
			adaptiveDecisions: [
				{
					at: '2026-03-31T00:00:01.000Z',
					runId: 'run_1',
					mode: 'observe',
					enforced: false,
					reasons: ['queue_ok'],
					changedCaps: {},
					effectiveCaps: { global: 4, model: 1 }
				},
				{
					at: '2026-03-31T00:00:02.000Z',
					runId: 'run_1',
					mode: 'enforce',
					enforced: true,
					reasons: ['queue_depth_high'],
					changedCaps: { global: { from: 4, to: 3 } },
					effectiveCaps: { global: 3, model: 1 }
				}
			]
		});
		expect(rows).toHaveLength(2);
		expect(rows[0]).toMatchObject({
			at: '2026-03-31T00:00:02.000Z',
			mode: 'enforce',
			enforced: true
		});
		expect(rows[0]?.changedCaps?.global).toEqual({ from: 4, to: 3 });
		expect(rows[1]?.mode).toBe('observe');
	});
});

