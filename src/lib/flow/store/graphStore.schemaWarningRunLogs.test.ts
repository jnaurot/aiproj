import { get } from 'svelte/store';
import { describe, expect, it } from 'vitest';

import { __applyRunEventForTest, graphStore, type GraphState } from './graphStore';

describe('graphStore schema warning run logs', () => {
	it('emits tagged schema warning logs for edge contract drift on run_started', () => {
		graphStore.hardResetGraph();
		const baseline = get(graphStore) as GraphState;
		const state = {
			...baseline,
			nodes: [
				{
					id: 'src',
					type: 'default',
					position: { x: 0, y: 0 },
					data: {
						kind: 'source',
						label: 'Source',
						sourceKind: 'file',
						params: { output: { mode: 'text' } },
						status: 'idle'
					}
				},
				{
					id: 'dst',
					type: 'default',
					position: { x: 120, y: 0 },
					data: {
						kind: 'model',
						label: 'Model',
						params: {},
						schema: {
							expectedInputSchemas: {
								in: { typedSchema: { type: 'text', fields: [] } }
							}
						},
						status: 'idle'
					}
				}
			] as any,
			edges: [
				{
					id: 'e_warn',
					source: 'src',
					target: 'dst',
					sourceHandle: 'out',
					targetHandle: 'in',
					data: {
						exec: 'idle',
						mode: 'work',
						contract: {
							snapshot: {
								sourceSchemaFingerprint: '{"type":"json"}',
								targetSchemaFingerprint: '{"type":"json"}',
								compatible: true,
								decision: 'native'
							}
						}
					}
				}
			] as any
		} as GraphState;
		const next = __applyRunEventForTest(
			state,
			{
				type: 'run_started',
				runId: 'run_schema_warn_drift',
				at: '2026-04-22T23:55:00Z',
				runMode: 'from_start',
				plannedNodeIds: ['src', 'dst']
			} as any,
			'run_schema_warn_drift'
		);
		const messages = (next.logs ?? []).map((entry) => String(entry?.message ?? ''));
		expect(messages.some((line) => line.includes('[SCHEMA_WARN_RAISED]') && line.includes('edge=e_warn'))).toBe(true);
		expect(
			messages.some(
				(line) =>
					line.includes('[SCHEMA_WARN]') &&
					line.includes('edge=e_warn') &&
					line.includes('from=src:out') &&
					line.includes('detail="')
			)
		).toBe(true);
	});

	it('delays opaque schema warning logs until source output is observed', () => {
		graphStore.hardResetGraph();
		const source = graphStore.addNode('model', { x: 0, y: 0 });
		const target = graphStore.addNode('transform', { x: 160, y: 0 });
		graphStore.addEdge({
			id: 'e_opaque',
			source,
			target,
			targetHandle: 'in',
			data: { exec: 'idle', mode: 'work' }
		} as any);
		const state = get(graphStore) as GraphState;
		const afterRunStarted = __applyRunEventForTest(
			state,
			{
				type: 'run_started',
				runId: 'run_schema_warn_opaque',
				at: '2026-04-22T23:56:00Z',
				runMode: 'from_start',
				plannedNodeIds: [source, target]
			} as any,
			'run_schema_warn_opaque'
		);
		const startedMessages = (afterRunStarted.logs ?? []).map((entry) => String(entry?.message ?? ''));
		expect(startedMessages.some((line) => line.includes('[SCHEMA_WARN]') && line.includes('OPAQUE_DEPENDENCY'))).toBe(
			false
		);
		const afterOutput = __applyRunEventForTest(
			afterRunStarted,
			{
				type: 'node_output',
				runId: 'run_schema_warn_opaque',
				at: '2026-04-22T23:56:02Z',
				nodeId: source,
				artifactId: 'artifact_opaque_source',
				payloadType: 'text',
				mimeType: 'text/plain',
				preview: 'opaque output'
			} as any,
			'run_schema_warn_opaque'
		);
		const messages = (afterOutput.logs ?? []).map((entry) => String(entry?.message ?? ''));
		expect(
			messages.some(
				(line) =>
					line.includes('[SCHEMA_INFO_RAISED]') &&
					line.includes('edge=e_opaque') &&
					line.includes('OPAQUE_DEPENDENCY')
			)
		).toBe(true);
		expect(messages.some((line) => line.includes('[SCHEMA_WARN]') && line.includes('edge=e_opaque'))).toBe(false);
	});

	it('emits raised once and cleared once for contract warnings across run starts', () => {
		graphStore.hardResetGraph();
		const baseline = get(graphStore) as GraphState;
		const warningState = {
			...baseline,
			nodes: [
				{
					id: 'src',
					type: 'default',
					position: { x: 0, y: 0 },
					data: { kind: 'source', sourceKind: 'file', label: 'Source', params: { output: { mode: 'text' } }, status: 'idle' }
				},
				{
					id: 'dst',
					type: 'default',
					position: { x: 120, y: 0 },
					data: { kind: 'model', label: 'Model', params: {}, status: 'idle' }
				}
			] as any,
			edges: [
				{
					id: 'e_warn_cycle',
					source: 'src',
					target: 'dst',
					sourceHandle: 'out',
					targetHandle: 'in',
					data: {
						exec: 'idle',
						mode: 'work',
						contract: {
							snapshot: {
								sourceSchemaFingerprint: '{"type":"json"}',
								targetSchemaFingerprint: '{"type":"json"}',
								compatible: true,
								decision: 'native'
							}
						}
					}
				}
			] as any
		} as GraphState;
		const afterFirstStart = __applyRunEventForTest(
			warningState,
			{
				type: 'run_started',
				runId: 'run_warn_cycle_1',
				at: '2026-04-23T00:10:00Z',
				runMode: 'from_start',
				plannedNodeIds: ['src', 'dst']
			} as any,
			'run_warn_cycle_1'
		);
		const firstMessages = (afterFirstStart.logs ?? []).map((entry) => String(entry?.message ?? ''));
		expect(firstMessages.filter((line) => line.includes('[SCHEMA_WARN_RAISED]')).length).toBe(1);
		const cleanedState = {
			...afterFirstStart,
			edges: (afterFirstStart.edges ?? []).map((edge: any) =>
				String(edge?.id ?? '') === 'e_warn_cycle'
					? { ...edge, data: { ...(edge?.data ?? {}), contract: undefined } }
					: edge
			)
		} as GraphState;
		const afterSecondStart = __applyRunEventForTest(
			cleanedState,
			{
				type: 'run_started',
				runId: 'run_warn_cycle_2',
				at: '2026-04-23T00:10:30Z',
				runMode: 'from_start',
				plannedNodeIds: ['src', 'dst']
			} as any,
			'run_warn_cycle_2'
		);
		const secondMessages = (afterSecondStart.logs ?? []).map((entry) => String(entry?.message ?? ''));
		expect(secondMessages.some((line) => line.includes('[SCHEMA_WARN_CLEARED]') && line.includes('edge=e_warn_cycle'))).toBe(
			true
		);
	});
});
