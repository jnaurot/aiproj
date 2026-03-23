import { describe, expect, it } from 'vitest';
import { get } from 'svelte/store';

import { __applyRunEventForTest, graphStore } from './graphStore';

describe('graphStore warning dedupe timeline parity', () => {
	it('keeps first warning timeline event and updates aggregate summary counts', () => {
		graphStore.hardResetGraph();
		const nodeId = graphStore.addNode('tool', { x: 0, y: 0 });
		const base = get(graphStore as any);
		const warningKey = `run_warn|${nodeId}|param_filters|PARAM_CONTROL_EMPTY_INPUT`;
		let next = __applyRunEventForTest(
			base as any,
			{
				type: 'node_input_warning',
				runId: 'run_warn',
				at: '2026-03-23T19:00:00.000Z',
				nodeId,
				handle: 'param_filters',
				edgeId: 'e_param_a',
				plane: 'param',
				code: 'PARAM_CONTROL_EMPTY_INPUT',
				reasonCode: 'EMPTY_JSON',
				warningKey
			} as any,
			'run_warn'
		);
		next = __applyRunEventForTest(
			next as any,
			{
				type: 'node_warning_summary',
				runId: 'run_warn',
				at: '2026-03-23T19:00:01.000Z',
				warningKey,
				nodeId,
				handle: 'param_filters',
				code: 'PARAM_CONTROL_EMPTY_INPUT',
				plane: 'param',
				edgeId: 'e_param_a',
				reasonCode: 'EMPTY_JSON',
				count: 1
			} as any,
			'run_warn'
		);
		next = __applyRunEventForTest(
			next as any,
			{
				type: 'node_warning_summary',
				runId: 'run_warn',
				at: '2026-03-23T19:00:02.000Z',
				warningKey,
				nodeId,
				handle: 'param_filters',
				code: 'PARAM_CONTROL_EMPTY_INPUT',
				plane: 'param',
				edgeId: 'e_param_b',
				reasonCode: 'EMPTY_JSON',
				count: 2
			} as any,
			'run_warn'
		);

		const summary = ((next as any)?.queueRuntime?.warningSummary ?? {})[warningKey] as any;
		expect(Number(summary?.count ?? 0)).toBe(2);
		expect(String(summary?.code ?? '')).toBe('PARAM_CONTROL_EMPTY_INPUT');

		const logs = (((next as any)?.logs ?? []) as Array<{ message?: string }>).map((row) =>
			String(row?.message ?? '')
		);
		expect(logs.filter((message) => message.includes('[input-warning]')).length).toBe(1);
		expect(logs.filter((message) => message.includes('[warning-summary]')).length).toBe(1);
	});
});
