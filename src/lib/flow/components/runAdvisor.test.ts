import { describe, expect, it } from 'vitest';

import { buildRunAdvisory } from './runAdvisor';
import type { RunMonitorNodeRow } from './runMonitorModel';

function row(overrides: Partial<RunMonitorNodeRow>): RunMonitorNodeRow {
	return {
		nodeId: 'n1',
		label: 'Node',
		status: 'busy',
		lifecycle: 'waiting',
		execution: 'finished',
		freshness: 'unknown',
		consumeMode: 'single_item',
		acceptedCount: 0,
		rejectedCount: 0,
		totalProcessed: 0,
		pendingInputCount: 0,
		inflight: 0,
		inboundDepth: 0,
		readyWork: false,
		blockedReasonCode: 'WAITING_REQUIRED_INPUT',
		blockedHandle: 'in',
		blockedPlane: 'work',
		updatedAt: null,
		terminalReasonCode: null,
		isBlocked: true,
		isWaiting: true,
		isLlmHolder: false,
		isLlmWaiting: false,
		phase: 'AWAITING_INPUT',
		phaseSince: null,
		blocker: null,
		lastBlocker: null,
		blockerHistory: [],
		displayReason: 'WAITING_REQUIRED_INPUT',
		statusParityMismatch: false,
		...overrides
	};
}

describe('buildRunAdvisory', () => {
	it('emits component output resolution advisory for component output errors', () => {
		const items = buildRunAdvisory({
			runStatus: 'running',
			rows: [row({ nodeId: 'c1', label: 'Component: Sum_Diet', lifecycle: 'failed' })],
			logs: ['COMPONENT_OUTPUT_NOT_RESOLVED: output summary missing'],
			now: '2026-01-01T00:00:00.000Z'
		});
		const hit = items.find((item) => item.ruleId === 'COMPONENT_OUTPUT_RESOLUTION');
		expect(hit).toBeTruthy();
		expect(hit?.severity).toBe('error');
		expect(hit?.nodeIds).toContain('c1');
	});

	it('emits waiting-without-work advisory for zero pending zero inflight waiting nodes', () => {
		const items = buildRunAdvisory({
			runStatus: 'running',
			rows: [row({ nodeId: 'n_wait' })],
			logs: [],
			now: '2026-01-01T00:00:00.000Z'
		});
		const hit = items.find((item) => item.ruleId === 'WAITING_WITHOUT_WORK');
		expect(hit).toBeTruthy();
		expect(hit?.nodeIds).toEqual(['n_wait']);
	});

	it('produces stable ids for identical inputs', () => {
		const input = {
			runStatus: 'running' as const,
			rows: [row({ nodeId: 'n_wait' })],
			logs: ['Ollama request failed (attempt 1/2)'],
			now: '2026-01-01T00:00:00.000Z'
		};
		const first = buildRunAdvisory(input);
		const second = buildRunAdvisory(input);
		expect(first.map((item) => item.id)).toEqual(second.map((item) => item.id));
	});

	it('does not mutate input rows', () => {
		const rows = [row({ nodeId: 'n_wait' })];
		const before = JSON.stringify(rows);
		buildRunAdvisory({ runStatus: 'running', rows, logs: [] });
		expect(JSON.stringify(rows)).toBe(before);
	});
});
