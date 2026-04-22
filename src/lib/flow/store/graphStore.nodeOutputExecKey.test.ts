import { describe, expect, it } from 'vitest';

import { __applyRunEventForTest, __hardResetGraphForTest, __normalizeBindingForTest } from './graphStore';
import type { KnownRunEvent } from '$lib/flow/types/run';

describe('graphStore node_output execKey contract', () => {
	it('uses emitted node_output.execKey as lineage authority', () => {
		const runId = 'run-node-output-exec-key';
		const base = __hardResetGraphForTest({} as any, 'graph-node-output-exec-key');
		const prevBinding = __normalizeBindingForTest(
			{
				status: 'running',
				currentRunId: runId,
				current: { execKey: 'old-exec-key', artifactId: 'old-artifact-id' },
				last: { execKey: 'old-exec-key', artifactId: 'old-artifact-id' }
			},
			'n1'
		);
		const state = {
			...base,
			runStatus: 'running' as const,
			activeRunId: runId,
			nodes: [{ id: 'n1', data: { kind: 'source', params: {} } }] as any,
			nodeBindings: { n1: prevBinding }
		};
		const evt = {
			type: 'node_output',
			runId,
			at: '2026-04-22T13:00:00.000Z',
			nodeId: 'n1',
			artifactId: 'new-artifact-id',
			execKey: 'new-exec-key'
		} as KnownRunEvent;
		const next = __applyRunEventForTest(state as any, evt, runId);
		const binding = (next.nodeBindings as any).n1;
		expect(binding.current.execKey).toBe('new-exec-key');
		expect(binding.current.artifactId).toBe('new-artifact-id');
		expect(binding.last.execKey).toBe('new-exec-key');
		expect(binding.last.artifactId).toBe('new-artifact-id');
	});

	it('legacy fallback keeps pair invariants when node_output.execKey is omitted', () => {
		const runId = 'run-node-output-exec-key-legacy';
		const base = __hardResetGraphForTest({} as any, 'graph-node-output-exec-key-legacy');
		const state = {
			...base,
			runStatus: 'running' as const,
			activeRunId: runId,
			nodes: [{ id: 'n1', data: { kind: 'source', params: {} } }] as any,
			nodeBindings: {
				n1: __normalizeBindingForTest(
					{
						status: 'running',
						currentRunId: runId
					},
					'n1'
				)
			}
		};
		const evt = {
			type: 'node_output',
			runId,
			at: '2026-04-22T13:00:00.000Z',
			nodeId: 'n1',
			artifactId: 'legacy-artifact-id'
		} as KnownRunEvent;
		const next = __applyRunEventForTest(state as any, evt, runId);
		const binding = (next.nodeBindings as any).n1;
		expect(binding.current.execKey).toBe('legacy-artifact-id');
		expect(binding.current.artifactId).toBe('legacy-artifact-id');
		expect(binding.last.execKey).toBe('legacy-artifact-id');
		expect(binding.last.artifactId).toBe('legacy-artifact-id');
	});
});
