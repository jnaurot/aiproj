import { describe, expect, it } from 'vitest';

import { __applyRunEventForTest, graphStore } from './graphStore';
import { get } from 'svelte/store';

describe('graphStore failure debug item mapping', () => {
	it('tags MODEL_EXECUTION_FAILED logs with failure sequence and latest item index', () => {
		graphStore.hardResetGraph();
		const nodeId = graphStore.addNode('model', { x: 0, y: 0 });
		const base = get(graphStore as any);
		const withItemInput = __applyRunEventForTest(
			base as any,
			{
				type: 'log',
				runId: 'run_debug_item',
				nodeId,
				level: 'info',
				at: '2026-05-01T12:00:00.000Z',
				message: 'LLM work-item input: mode=table_rows index=5 artifact=abc123'
			} as any,
			'run_debug_item'
		);
		const withFailure = __applyRunEventForTest(
			withItemInput as any,
			{
				type: 'log',
				runId: 'run_debug_item',
				nodeId,
				level: 'error',
				at: '2026-05-01T12:00:10.000Z',
				message: 'MODEL_EXECUTION_FAILED: ollama request failed:'
			} as any,
			'run_debug_item'
		);
		const logs = ((withFailure as any)?.logs ?? []) as Array<{ message: string }>;
		const last = String(logs[logs.length - 1]?.message ?? '');
		expect(last).toContain('MODEL_EXECUTION_FAILED');
		expect(last).toContain('[debug] failure_seq=1 item_index=5');
	});

	it('increments failure sequence and falls back to unknown item index when missing', () => {
		graphStore.hardResetGraph();
		const nodeId = graphStore.addNode('model', { x: 0, y: 0 });
		const base = get(graphStore as any);
		const first = __applyRunEventForTest(
			base as any,
			{
				type: 'log',
				runId: 'run_debug_item_missing',
				nodeId,
				level: 'error',
				at: '2026-05-01T12:01:00.000Z',
				message: 'MODEL_EXECUTION_FAILED: ollama request failed:'
			} as any,
			'run_debug_item_missing'
		);
		const second = __applyRunEventForTest(
			first as any,
			{
				type: 'log',
				runId: 'run_debug_item_missing',
				nodeId,
				level: 'error',
				at: '2026-05-01T12:01:10.000Z',
				message: 'MODEL_EXECUTION_FAILED: ollama request failed:'
			} as any,
			'run_debug_item_missing'
		);
		const logs = ((second as any)?.logs ?? []) as Array<{ message: string }>;
		const firstFailure = String(logs[logs.length - 2]?.message ?? '');
		const secondFailure = String(logs[logs.length - 1]?.message ?? '');
		expect(firstFailure).toContain('[debug] failure_seq=1 item_index=unknown');
		expect(secondFailure).toContain('[debug] failure_seq=2 item_index=unknown');
	});
});

