import { describe, expect, it } from 'vitest';
import { get } from 'svelte/store';

import { __applyRunEventForTest, graphStore } from './graphStore';

describe('graphStore node_blocked projection', () => {
	it('stores node_blocked reason in queueRuntime.blockedByNode', () => {
		graphStore.hardResetGraph();
		const nodeId = graphStore.addNode('transform', { x: 0, y: 0 });
		const base = get(graphStore as any);
		const next = __applyRunEventForTest(
			base as any,
			{
				type: 'node_blocked',
				runId: 'run_blocked_1',
				at: '2026-03-29T03:00:00.000Z',
				nodeId,
				reasonCode: 'WAITING_REQUIRED_INPUT',
				handle: 'in_cold',
				plane: 'work',
				missingEdgeIds: ['e_cold']
			} as any,
			'run_blocked_1'
		);
		const row = ((next as any)?.queueRuntime?.blockedByNode ?? {})[nodeId] as any;
		expect(row?.reasonCode).toBe('WAITING_REQUIRED_INPUT');
		expect(row?.handle).toBe('in_cold');
		expect(Array.isArray(row?.missingEdgeIds)).toBe(true);
		expect(row?.missingEdgeIds?.[0]).toBe('e_cold');
		const logLines = ((next as any)?.logs ?? []).map((entry: any) => String(entry?.message ?? ''));
		expect(logLines.some((line: string) => line.includes('[monitor-blocker] action=set code=WAITING_REQUIRED_INPUT'))).toBe(true);
	});

	it('clears blockedByNode map on run_started for new run scope', () => {
		graphStore.hardResetGraph();
		const nodeId = graphStore.addNode('transform', { x: 0, y: 0 });
		const base = get(graphStore as any);
		let next = __applyRunEventForTest(
			base as any,
			{
				type: 'node_blocked',
				runId: 'run_blocked_2',
				at: '2026-03-29T03:05:00.000Z',
				nodeId,
				reasonCode: 'NO_READY_WORK'
			} as any,
			'run_blocked_2'
		);
		expect(Boolean(((next as any)?.queueRuntime?.blockedByNode ?? {})[nodeId])).toBe(true);
		next = __applyRunEventForTest(
			next as any,
			{
				type: 'run_started',
				runId: 'run_blocked_3',
				at: '2026-03-29T03:05:10.000Z',
				runFrom: null,
				runMode: 'from_start',
				plannedNodeIds: [nodeId]
			} as any,
			'run_blocked_3'
		);
		expect(Object.keys((next as any)?.queueRuntime?.blockedByNode ?? {})).toHaveLength(0);
	});

	it('emits blocker clear marker when node_started clears blocked entry', () => {
		graphStore.hardResetGraph();
		const nodeId = graphStore.addNode('transform', { x: 0, y: 0 });
		let next = get(graphStore as any);
		next = __applyRunEventForTest(
			next as any,
			{
				type: 'node_blocked',
				runId: 'run_blocked_4',
				at: '2026-03-29T03:10:00.000Z',
				nodeId,
				reasonCode: 'WAITING_REQUIRED_INPUT'
			} as any,
			'run_blocked_4'
		);
		next = __applyRunEventForTest(
			next as any,
			{
				type: 'node_started',
				runId: 'run_blocked_4',
				at: '2026-03-29T03:10:01.000Z',
				nodeId
			} as any,
			'run_blocked_4'
		);
		const logLines = ((next as any)?.logs ?? []).map((entry: any) => String(entry?.message ?? ''));
		expect(logLines.some((line: string) => line.includes('[monitor-blocker] action=cleared'))).toBe(true);
	});
});
