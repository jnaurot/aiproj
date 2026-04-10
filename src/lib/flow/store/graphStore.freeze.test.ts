import { describe, expect, it } from 'vitest';
import { get } from 'svelte/store';

import {
	__markStaleFromNodeForTest,
	__normalizeBindingForTest,
	__validatePinEligibilityForTest,
	graphStore,
	type GraphState
} from './graphStore';

function makeBinding(nodeId: string, binding: Record<string, unknown>) {
	return __normalizeBindingForTest(binding as any, nodeId);
}

describe('graphStore freeze/pin rules', () => {
	it('pin eligibility rejects non-succeeded bindings', () => {
		const result = __validatePinEligibilityForTest(
			{ id: 'n1', data: { kind: 'llm', params: {} } } as any,
			makeBinding('n1', {
				status: 'stale',
				isUpToDate: false,
				current: { execKey: null, artifactId: null },
				last: { execKey: 'k1', artifactId: 'a1' }
			})
		);
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error).toContain('succeeded');
		}
	});

	it('pin eligibility accepts succeeded bindings with current artifact pair', () => {
		const result = __validatePinEligibilityForTest(
			{ id: 'n1', data: { kind: 'llm', params: {} } } as any,
			makeBinding('n1', {
				status: 'succeeded_up_to_date',
				isUpToDate: true,
				current: { execKey: 'k1', artifactId: 'a1' },
				last: { execKey: 'k1', artifactId: 'a1' }
			})
		);
		expect(result).toEqual({ ok: true });
	});

	it('stale propagation stops at pinned node boundary', () => {
		const state: GraphState = {
			graphId: 'graph-freeze',
			nodes: [
				{ id: 'src', data: { kind: 'source', params: {} } },
				{ id: 'mid', data: { kind: 'transform', params: {}, meta: { freeze: { enabled: true, mode: 'sticky' } } } },
				{ id: 'dst', data: { kind: 'llm', params: {} } }
			] as any,
			edges: [
				{ id: 'e1', source: 'src', target: 'mid', data: { exec: 'idle' } },
				{ id: 'e2', source: 'mid', target: 'dst', data: { exec: 'idle' } }
			] as any,
			selectedNodeId: null,
			inspector: { nodeId: null, draftParams: {}, dirty: false },
			logs: [],
			runStatus: 'idle',
			lastRunStatus: 'never_run',
			freshness: 'up_to_date',
			staleNodeCount: 0,
			activeRunMode: null,
			activeRunFrom: null,
			activeRunNodeSet: null,
			nodeOutputs: {},
			nodeBindings: {
				src: makeBinding('src', {
					status: 'succeeded_up_to_date',
					isUpToDate: true,
					current: { execKey: 'ks', artifactId: 'as' },
					last: { execKey: 'ks', artifactId: 'as' }
				}),
				mid: makeBinding('mid', {
					status: 'succeeded_up_to_date',
					isUpToDate: true,
					current: { execKey: 'km', artifactId: 'am' },
					last: { execKey: 'km', artifactId: 'am' }
				}),
				dst: makeBinding('dst', {
					status: 'succeeded_up_to_date',
					isUpToDate: true,
					current: { execKey: 'kd', artifactId: 'ad' },
					last: { execKey: 'kd', artifactId: 'ad' }
				})
			},
			activeRunId: null
		} as any;

		const next = __markStaleFromNodeForTest(state, 'src');
		expect(next.nodeBindings.src.status).toBe('stale');
		expect(next.nodeBindings.mid.status).toBe('succeeded_up_to_date');
		expect(next.nodeBindings.dst.status).toBe('succeeded_up_to_date');
	});

	it('setSelectedNodeFreezeMode rejects pin when node is not succeeded', () => {
		graphStore.hardResetGraph();
		const nodeId = graphStore.addNode('llm', { x: 40, y: 40 });
		graphStore.selectNode(nodeId);
		const result = graphStore.setSelectedNodeFreezeMode('per_run');
		expect(result.ok).toBe(false);
		expect(String(result.error ?? '').toLowerCase()).toContain('succeeded');
	});

	it('legacy pin metadata is migrated away on load before edits', () => {
		graphStore.hardResetGraph();
		graphStore.loadGraphDocument({
			nodes: [
				{
					id: 'n1',
					type: 'node',
					position: { x: 20, y: 20 },
					data: {
						kind: 'llm',
						label: 'Model',
						params: {
							model: 'glm-4.7-flash:latest',
							temperature: 0
						},
						meta: {
							freeze: { enabled: true, mode: 'sticky' }
						}
					}
				}
			],
			edges: []
		});
		graphStore.selectNode('n1');
		const before = get(graphStore);
		expect(((before.nodes.find((n) => n.id === 'n1')?.data as any)?.meta?.freeze?.enabled) ?? false).toBe(false);
		const updated = graphStore.updateNodeConfig('n1', {
			params: { model: 'glm-4.7-flash:latest', temperature: 0.2 }
		});
		expect(updated.ok).toBe(true);
		const after = get(graphStore);
		const freeze = ((after.nodes.find((n) => n.id === 'n1')?.data as any)?.meta?.freeze ?? null) as any;
		expect(freeze).toBeNull();
		expect(String((after.inspector as any)?.systemNotice ?? '')).not.toContain('[Pin cleared]');
	});

});
