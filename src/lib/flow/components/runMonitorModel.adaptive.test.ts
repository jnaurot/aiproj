import { describe, expect, it } from 'vitest';

import {
	buildRunMonitorAdaptiveDecisionRows,
	buildTrendSparkline,
	explainAdaptiveDecision
} from './runMonitorModel';

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
		expect(Number(rows[0]?.explanation?.score ?? 0)).toBeGreaterThan(0);
		expect(['low', 'medium', 'high']).toContain(rows[0]?.explanation?.severity);
		expect(rows[0]?.changedCaps?.global).toEqual({ from: 4, to: 3 });
		expect(rows[1]?.mode).toBe('observe');
	});

	it('scores pressure + enforced decisions higher than low-signal observe events', () => {
		const low = explainAdaptiveDecision({
			enforced: false,
			reasons: ['recovery'],
			changedCaps: {},
			inputs: { queueDepth: 1, failureRate: 0 },
			effectiveCaps: { global: 4 }
		});
		const high = explainAdaptiveDecision({
			enforced: true,
			reasons: ['queue_depth_high', 'failure_rate_high'],
			changedCaps: { global: { from: 4, to: 2 }, tool: { from: 2, to: 1 } },
			inputs: { queueDepth: 25, failureRate: 0.22, leaseWaitMs: 5200 },
			effectiveCaps: { global: 2, tool: 1 }
		});
		expect(high.score).toBeGreaterThan(low.score);
		expect(high.severity).toBe('high');
	});

	it('builds a sparkline projection for trend points', () => {
		const spark = buildTrendSparkline([
			{ createdAt: '2026-03-31T00:00:00Z', value: 100 },
			{ createdAt: '2026-03-31T00:10:00Z', value: 160 },
			{ createdAt: '2026-03-31T00:20:00Z', value: 130 }
		]);
		expect(spark).not.toBeNull();
		expect(String(spark?.path ?? '')).toContain('M ');
		expect(Number(spark?.pointsCount ?? 0)).toBe(3);
	});
});
