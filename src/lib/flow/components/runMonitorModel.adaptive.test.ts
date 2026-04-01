import { describe, expect, it } from 'vitest';

import {
	buildAdaptiveComponentBreakdown,
	buildRunMonitorAdaptiveDecisionRows,
	buildTrendSparkline,
	explainAdaptiveDecision,
	filterRunMonitorAdaptiveDecisionRows,
	pickRunMonitorRegressionPairFromHistory,
	resolveRunMonitorRegressionPair,
	summarizeAdaptiveDecisionRows
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
					reasons: ['queue_depth_high', 'failure_rate_high'],
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
		expect((rows[0]?.explanation?.components ?? []).length).toBeGreaterThan(0);
		expect(rows[0]?.changedCaps?.global).toEqual({ from: 4, to: 3 });
		expect(rows[1]?.mode).toBe('observe');
		expect(rows[0]?.diffFromPrevious?.modeChanged).toBe(true);
		expect(rows[0]?.diffFromPrevious?.scoreDelta).not.toBe(0);
		expect(rows[0]?.diffFromPrevious?.capDelta?.global).toEqual({ from: 4, to: 3 });
		expect(rows[0]?.diffFromPrevious?.reasonsAdded).toContain('failure_rate_high');
		expect(rows[0]?.diffFromPrevious?.reasonsRemoved).not.toContain('queue_depth_high');
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
		expect(high.components.length).toBeGreaterThan(0);
		expect(high.components.some((item) => item.label.includes('reason:failure_rate_high'))).toBe(true);
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
		expect((spark?.points ?? []).length).toBe(3);
		expect(Number(spark?.baselines?.firstValueY ?? 0)).toBeGreaterThanOrEqual(0);
	});

	it('filters adaptive decision rows by mode and severity', () => {
		const rows = buildRunMonitorAdaptiveDecisionRows({
			adaptiveDecisions: [
				{
					at: '2026-03-31T00:00:01.000Z',
					runId: 'run_1',
					mode: 'observe',
					enforced: false,
					reasons: ['recovery'],
					changedCaps: {},
					effectiveCaps: { global: 4, model: 1 }
				},
				{
					at: '2026-03-31T00:00:02.000Z',
					runId: 'run_1',
					mode: 'enforce',
					enforced: true,
					reasons: ['queue_depth_high', 'failure_rate_high'],
					changedCaps: { global: { from: 4, to: 2 } },
					effectiveCaps: { global: 2, model: 1 }
				}
			]
		});
		const enforceOnly = filterRunMonitorAdaptiveDecisionRows(rows, 'enforce', 'all');
		expect(enforceOnly).toHaveLength(1);
		expect(enforceOnly[0]?.mode).toBe('enforce');
		const lowOnly = filterRunMonitorAdaptiveDecisionRows(rows, 'all', 'low');
		expect(lowOnly.length).toBeGreaterThanOrEqual(1);
		expect(lowOnly.every((row) => row.explanation.severity === 'low')).toBe(true);
		const changedOnly = filterRunMonitorAdaptiveDecisionRows(rows, 'all', 'all', true);
		expect(changedOnly.length).toBeGreaterThanOrEqual(1);
		expect(changedOnly.every((row) => Boolean(row.diffFromPrevious))).toBe(true);
		const highScoreOnly = filterRunMonitorAdaptiveDecisionRows(rows, 'all', 'all', false, 60);
		expect(highScoreOnly).toHaveLength(1);
		expect(highScoreOnly[0]?.mode).toBe('enforce');
	});

	it('resolves regression pair using valid override or latest history fallback', () => {
		const historyRows = [
			{ runId: 'run_new' },
			{ runId: 'run_prev' },
			{ runId: 'run_old' }
		] as Array<Record<string, unknown>>;
		const fromOverride = resolveRunMonitorRegressionPair(historyRows, {
			runId: 'run_prev',
			baselineRunId: 'run_old'
		});
		expect(fromOverride).toEqual({ runId: 'run_prev', baselineRunId: 'run_old' });
		const fallback = resolveRunMonitorRegressionPair(historyRows, {
			runId: 'missing',
			baselineRunId: 'run_old'
		});
		expect(fallback).toEqual({ runId: 'run_new', baselineRunId: 'run_prev' });
	});

	it('picks a regression pair from adjacent history rows', () => {
		const historyRows = [
			{ runId: 'run_new' },
			{ runId: 'run_prev' },
			{ runId: 'run_old' }
		] as Array<Record<string, unknown>>;
		expect(pickRunMonitorRegressionPairFromHistory(historyRows, 1)).toEqual({
			runId: 'run_prev',
			baselineRunId: 'run_old'
		});
		expect(pickRunMonitorRegressionPairFromHistory(historyRows, 2)).toEqual({
			runId: '',
			baselineRunId: ''
		});
	});

	it('builds normalized adaptive component breakdown bars', () => {
		const rows = buildAdaptiveComponentBreakdown([
			{ label: 'reason:failure_rate_high', delta: 20 },
			{ label: 'queue_depth', delta: 6 },
			{ label: 'recovery', delta: -4 }
		]);
		expect(rows).toHaveLength(3);
		expect(rows[0]?.label).toBe('reason:failure_rate_high');
		expect(rows[0]?.percentOfMax).toBeCloseTo(100, 4);
		expect(rows[1]?.direction).toBe('up');
		expect(rows[2]?.direction).toBe('down');
		expect(rows[2]?.percentOfMax).toBeLessThan(100);
	});

	it('summarizes adaptive decisions by mode/severity/enforced', () => {
		const rows = buildRunMonitorAdaptiveDecisionRows({
			adaptiveDecisions: [
				{
					at: '2026-03-31T00:00:01.000Z',
					runId: 'run_1',
					mode: 'observe',
					enforced: false,
					reasons: ['recovery'],
					changedCaps: {},
					effectiveCaps: { global: 4 }
				},
				{
					at: '2026-03-31T00:00:02.000Z',
					runId: 'run_1',
					mode: 'enforce',
					enforced: true,
					reasons: ['queue_depth_high', 'failure_rate_high'],
					changedCaps: { global: { from: 4, to: 2 } },
					effectiveCaps: { global: 2 }
				}
			]
		});
		const summary = summarizeAdaptiveDecisionRows(rows);
		expect(summary.total).toBe(2);
		expect(summary.enforced).toBe(1);
		expect(summary.byMode.observe).toBe(1);
		expect(summary.byMode.enforce).toBe(1);
		expect(summary.bySeverity.high + summary.bySeverity.medium + summary.bySeverity.low).toBe(2);
	});
});
