import { describe, expect, it } from 'vitest';

import {
	__collectPinnedArtifactsByNodeForTest,
	__normalizeBindingForTest,
	__resetRunUiStateForTest,
	type GraphState
} from './graphStore';

describe('graphStore resetRunUiState', () => {
	it('resets UI state to idle while preserving lineage for reuse', () => {
		const state = {
			graphId: 'g-reset',
			nodes: [
				{
					id: 'n_source',
					type: 'source',
					position: { x: 0, y: 0 },
					data: {
						kind: 'source',
						label: 'Source',
						params: {},
						meta: { freeze: { enabled: true, mode: 'sticky' } }
					}
				},
				{
					id: 'n_xform',
					type: 'transform',
					position: { x: 200, y: 0 },
					data: {
						kind: 'transform',
						label: 'Transform',
						params: {}
					}
				}
			] as any,
			edges: [{ id: 'e1', source: 'n_source', target: 'n_xform', data: { exec: 'active', mode: 'work' } }] as any,
			selectedNodeId: null,
			inspector: { nodeId: null, draftParams: {}, dirty: false },
			logs: [{ ts: 'now', level: 'info', msg: 'something' }] as any,
			runStatus: 'running',
			lastRunStatus: 'succeeded',
			freshness: 'up_to_date',
			staleNodeCount: 0,
			activeRunMode: 'from_start',
			activeRunFrom: null,
			activeRunNodeSet: new Set<string>(['n_source', 'n_xform']),
			nodeOutputs: {},
			nodeBindings: {
				n_source: __normalizeBindingForTest(
					{
						status: 'succeeded_up_to_date',
						isUpToDate: true,
						cacheValid: true,
						current: { execKey: 'exec-source', artifactId: 'art-source' },
						last: { execKey: 'exec-source', artifactId: 'art-source' },
						currentRunId: 'run-1',
						lastRunId: 'run-1'
					},
					'n_source'
				),
				n_xform: __normalizeBindingForTest(
					{
						status: 'failed',
						isUpToDate: false,
						cacheValid: false,
						current: { execKey: 'exec-xform', artifactId: 'art-xform' },
						last: { execKey: 'exec-xform', artifactId: 'art-xform' },
						currentRunId: 'run-1',
						lastRunId: 'run-1',
						staleReason: 'UPSTREAM_CHANGED'
					},
					'n_xform'
				)
			},
			activeRunId: 'run-1'
		} as unknown as GraphState;

		const next = __resetRunUiStateForTest(state);
		expect(next.runStatus).toBe('idle');
		expect(next.activeRunId).toBeNull();
		expect(next.logs).toEqual([]);
		for (const nodeId of ['n_source', 'n_xform']) {
			const b = next.nodeBindings[nodeId];
			expect(String(b.status ?? '')).toBe('idle');
			expect(Boolean(b.isUpToDate)).toBe(false);
			expect(Boolean(b.cacheValid)).toBe(false);
			expect(b.current?.execKey ?? null).toBeNull();
			expect(b.current?.artifactId ?? null).toBeNull();
			expect(b.last?.execKey ?? null).toBe(nodeId === 'n_source' ? 'exec-source' : 'exec-xform');
			expect(b.last?.artifactId ?? null).toBe(nodeId === 'n_source' ? 'art-source' : 'art-xform');
			expect((b as any).currentExecKey ?? null).toBeNull();
			expect((b as any).currentArtifactId ?? null).toBeNull();
			expect((b as any).lastExecKey ?? null).toBe(nodeId === 'n_source' ? 'exec-source' : 'exec-xform');
			expect((b as any).lastArtifactId ?? null).toBe(nodeId === 'n_source' ? 'art-source' : 'art-xform');
		}
	});

	it('keeps pinned lineage reusable after reset while status stays idle', () => {
		const state = {
			graphId: 'g-reset-pinned',
			nodes: [
				{
					id: 'n1',
					type: 'component',
					position: { x: 0, y: 0 },
					data: {
						kind: 'component',
						label: 'Pinned',
						params: {
							componentRef: {
								componentId: 'cmp_test',
								revisionId: 'crev_test',
								apiVersion: 'v1'
							},
							api: {
								outputs: [
									{ name: 'out_a', required: true },
									{ name: 'out_b', required: true }
								]
							}
						},
						meta: { freeze: { enabled: true, mode: 'sticky' } }
					}
				}
			] as any,
			edges: [] as any,
			selectedNodeId: null,
			inspector: { nodeId: null, draftParams: {}, dirty: false },
			logs: [{ ts: 'now', level: 'info', msg: 'something' }] as any,
			runStatus: 'running',
			lastRunStatus: 'succeeded',
			freshness: 'up_to_date',
			staleNodeCount: 0,
			activeRunMode: 'from_start',
			activeRunFrom: null,
			activeRunNodeSet: new Set<string>(),
			nodeOutputs: {
				n1: { preview: 'artifact still stored' }
			},
			nodeBindings: {
				n1: __normalizeBindingForTest(
					{
						status: 'succeeded_up_to_date',
						isUpToDate: true,
						cacheValid: true,
						current: { execKey: 'exec-1', artifactId: 'art-1' },
						last: { execKey: 'exec-1', artifactId: 'art-1' },
						outputLineage: {
							out_a: { execKey: 'exec-a', artifactId: 'art-a' },
							out_b: { execKey: 'exec-b', artifactId: 'art-b' }
						},
						currentRunId: 'run-1',
						lastRunId: 'run-1'
					},
					'n1'
				)
			},
			activeRunId: 'run-1'
		} as unknown as GraphState;

		const next = __resetRunUiStateForTest(state);
		expect(next.nodeBindings.n1?.status).toBe('idle');
		expect(next.nodeBindings.n1?.current?.execKey ?? null).toBeNull();
		expect(next.nodeBindings.n1?.current?.artifactId ?? null).toBeNull();
		expect(next.nodeBindings.n1?.last?.execKey ?? null).toBe('exec-1');
		expect(next.nodeBindings.n1?.last?.artifactId ?? null).toBe('art-1');
		expect(next.nodeOutputs.n1).toBeTruthy();

		const pinnedArtifacts = __collectPinnedArtifactsByNodeForTest(next.nodes as any, next.nodeBindings as any);
		expect(pinnedArtifacts).toEqual({
			n1: {
				artifactId: 'art-1',
				execKey: 'exec-1',
				outputs: {
					out_a: { artifactId: 'art-a', execKey: 'exec-a' },
					out_b: { artifactId: 'art-b', execKey: 'exec-b' }
				}
			}
		});
	});

	it('falls back to current lineage when last lineage is empty', () => {
		const state = {
			graphId: 'g-reset-lineage-fallback',
			nodes: [
				{
					id: 'n1',
					type: 'transform',
					position: { x: 0, y: 0 },
					data: { kind: 'transform', label: 'Node', params: {} }
				}
			] as any,
			edges: [] as any,
			selectedNodeId: null,
			inspector: { nodeId: null, draftParams: {}, dirty: false },
			logs: [] as any,
			runStatus: 'running',
			lastRunStatus: 'succeeded',
			freshness: 'up_to_date',
			staleNodeCount: 0,
			activeRunMode: 'from_start',
			activeRunFrom: null,
			activeRunNodeSet: new Set<string>(),
			nodeOutputs: {},
			nodeBindings: {
				n1: __normalizeBindingForTest(
					{
						status: 'succeeded_up_to_date',
						isUpToDate: true,
						cacheValid: true,
						current: { execKey: 'exec-current', artifactId: 'art-current' },
						last: { execKey: null, artifactId: null }
					},
					'n1'
				)
			},
			activeRunId: 'run-1'
		} as unknown as GraphState;

		const next = __resetRunUiStateForTest(state);
		expect(next.nodeBindings.n1?.status).toBe('idle');
		expect(next.nodeBindings.n1?.current?.execKey ?? null).toBeNull();
		expect(next.nodeBindings.n1?.current?.artifactId ?? null).toBeNull();
		expect(next.nodeBindings.n1?.last?.execKey ?? null).toBe('exec-current');
		expect(next.nodeBindings.n1?.last?.artifactId ?? null).toBe('art-current');
	});

	it('collects pinned artifacts from freezeLineage when nodeBindings are empty', () => {
		const nodes = [
			{
				id: 'n_pinned',
				type: 'transform',
				position: { x: 0, y: 0 },
				data: {
					kind: 'transform',
					label: 'Pinned internal',
					params: {},
					meta: {
						freeze: { enabled: true, mode: 'sticky' },
						freezeLineage: { artifactId: 'art-from-meta', execKey: 'exec-from-meta' }
					}
				}
			}
		] as any;
		const pinnedArtifacts = __collectPinnedArtifactsByNodeForTest(nodes, {} as any);
		expect(pinnedArtifacts).toEqual({
			n_pinned: { artifactId: 'art-from-meta', execKey: 'exec-from-meta' }
		});
	});

	it('collects component pinned output hints from freezeLineage outputs fallback', () => {
		const nodes = [
			{
				id: 'n_component',
				type: 'component',
				position: { x: 0, y: 0 },
				data: {
					kind: 'component',
					label: 'Pinned component',
					params: {},
					meta: {
						freeze: { enabled: true, mode: 'sticky' },
						freezeLineage: {
							artifactId: 'art-component',
							execKey: 'exec-component',
							outputs: {
								summary: { artifactId: 'art-summary', execKey: 'exec-summary' },
								source: { artifactId: 'art-source', execKey: 'exec-source' }
							}
						}
					}
				}
			}
		] as any;
		const pinnedArtifacts = __collectPinnedArtifactsByNodeForTest(nodes, {} as any);
		expect(pinnedArtifacts).toEqual({
			n_component: {
				artifactId: 'art-component',
				execKey: 'exec-component',
				outputs: {
					summary: { artifactId: 'art-summary', execKey: 'exec-summary' },
					source: { artifactId: 'art-source', execKey: 'exec-source' }
				}
			}
		});
	});

	it('clears transient run visuals and runtime fields while keeping graph shape', () => {
		const state = {
			graphId: 'g-reset-transient',
			nodes: [
				{
					id: 'n1',
					type: 'model',
					position: { x: 0, y: 0 },
					data: { kind: 'model', label: 'Model', params: {}, meta: { llmAllocated: true } }
				},
				{
					id: 'n2',
					type: 'transform',
					position: { x: 120, y: 0 },
					data: { kind: 'transform', label: 'Transform', params: {} }
				}
			] as any,
			edges: [
				{ id: 'e_work', source: 'n1', target: 'n2', data: { exec: 'active', mode: 'work' } },
				{ id: 'e_param', source: 'n1', target: 'n2', data: { exec: 'active', mode: 'param' } },
				{ id: 'e_control', source: 'n1', target: 'n2', data: { exec: 'active', mode: 'control' } }
			] as any,
			selectedNodeId: 'n2',
			inspector: { nodeId: 'n2', draftParams: { x: 1 }, dirty: true },
			logs: [{ ts: 'now', level: 'info', msg: 'something' }] as any,
			runStatus: 'pausing',
			lastRunStatus: 'succeeded',
			freshness: 'up_to_date',
			staleNodeCount: 0,
			activeRunMode: 'from_selected_onward',
			activeRunFrom: 'n2',
			activeRunNodeSet: new Set<string>(['n2']),
			nodeOutputs: { n1: { preview: 'kept' } },
			nodeBindings: {
				n1: __normalizeBindingForTest(
					{
						status: 'running',
						current: { execKey: 'e1', artifactId: 'a1' },
						last: { execKey: 'e1', artifactId: 'a1' }
					},
					'n1'
				),
				n2: __normalizeBindingForTest(
					{
						status: 'stale',
						current: { execKey: null, artifactId: null },
						last: { execKey: 'e2', artifactId: 'a2' }
					},
					'n2'
				)
			},
			activeRunId: 'run-1'
		} as unknown as GraphState;

		const next = __resetRunUiStateForTest(state);
		expect(next.runStatus).toBe('idle');
		expect(next.activeRunId).toBeNull();
		expect(next.activeRunMode).toBe('from_start');
		expect(next.activeRunFrom).toBeNull();
		expect(Array.from(next.activeRunNodeSet ?? [])).toEqual([]);
		expect(next.logs).toEqual([]);
		for (const edge of next.edges as any[]) {
			expect(String(edge?.data?.exec ?? 'idle')).toBe('idle');
		}
		const node1 = (next.nodes as any[]).find((n) => String(n?.id) === 'n1');
		expect(Boolean(node1?.data?.meta?.llmAllocated)).toBe(false);
		expect(next.selectedNodeId).toBe('n2');
		expect(next.nodeOutputs.n1).toBeTruthy();
	});
});

