import { describe, expect, it } from 'vitest';

import {
	buildRunCreateRequest,
	computeConnectedComponentNodeSets,
	computeGraphFreshness,
	computePlannedNodeSet,
	planRunConnectedComponents,
	computeSelectedConnectedComponentNodeSet,
	getStaleFlipNodeIds,
	isBindingStale,
	mergeBindingsSticky,
	statusProjectionFromBinding,
	shouldUpdateBinding
} from './runScope';

describe('runScope partial-run binding behavior', () => {
	it('keeps unrelated path bindings unchanged during from_selected_onward updates', () => {
		const nodes: any[] = [{ id: 'a1' }, { id: 'a2' }, { id: 'b1' }, { id: 'b2' }];
		const edges: any[] = [
			{ source: 'a1', target: 'a2' },
			{ source: 'b1', target: 'b2' }
		];
		const planned = computePlannedNodeSet(nodes, edges, 'a1', 'from_selected_onward');
		expect([...planned].sort()).toEqual(['a1', 'a2']);

		const previous = {
			a1: { status: 'succeeded_up_to_date', isUpToDate: true, lastArtifactId: 'art-a1' },
			a2: { status: 'succeeded_up_to_date', isUpToDate: true, lastArtifactId: 'art-a2' },
			b1: { status: 'succeeded_up_to_date', isUpToDate: true, lastArtifactId: 'art-b1' },
			b2: { status: 'succeeded_up_to_date', isUpToDate: true, lastArtifactId: 'art-b2' }
		};
		const patchForRunA = {
			a1: { status: 'running', isUpToDate: false },
			a2: { status: 'succeeded_up_to_date', isUpToDate: true, lastArtifactId: 'art-a2-new' }
		};

		const merged = mergeBindingsSticky(previous, patchForRunA);
		expect(merged.b1).toEqual(previous.b1);
		expect(merged.b2).toEqual(previous.b2);
		expect(merged.b1.isUpToDate).toBe(true);
		expect(merged.b2.isUpToDate).toBe(true);

		const afterRunA = {
			...merged,
			a1: { ...merged.a1, status: 'succeeded_up_to_date', isUpToDate: true, lastArtifactId: 'art-a1-new' }
		};
		const fresh = computeGraphFreshness(afterRunA);
		expect(fresh.freshness).toBe('up_to_date');
		expect(fresh.staleNodeCount).toBe(0);

		const afterAcceptParams = {
			...afterRunA,
			a1: { ...afterRunA.a1, isUpToDate: false },
			a2: { ...afterRunA.a2, isUpToDate: false }
		};
		const stale = computeGraphFreshness(afterAcceptParams);
		expect(stale.freshness).toBe('stale');
		expect(stale.staleNodeCount).toBe(2);
	});

	it('omits runMode and runFrom for full run requests', () => {
		const graph = { version: 1, nodes: [], edges: [] };
		const full = buildRunCreateRequest(graph, 'graph-test', null, 'from_start');
		expect(full).toEqual({ graphId: 'graph-test', graph });

		const partial = buildRunCreateRequest(graph, 'graph-test', 'a1', 'from_selected_onward');
		expect(partial).toEqual({
			graphId: 'graph-test',
			graph,
			runFrom: 'a1',
			runMode: 'from_selected_onward'
		});
	});

	it('includes adaptive override mode when provided', () => {
		const graph = { version: 1, nodes: [], edges: [] };
		const req = buildRunCreateRequest(graph, 'graph-test', null, 'from_start', [], 'default_on', 'observe');
		expect((req as any).adaptive).toEqual({ mode: 'observe' });
	});

	it('includes dirty execution hints when provided', () => {
		const graph = { version: 1, nodes: [], edges: [] };
		const req = buildRunCreateRequest(graph, 'graph-test', null, 'from_start', ['n1', 'n1']);
		expect(req.graph.__executionHints).toEqual({
			dirtyNodeIds: ['n1']
		});
	});

	it('preserves per-output checkpoint lineage in execution hints', () => {
		const graph = { version: 1, nodes: [], edges: [] };
		const req = buildRunCreateRequest(
			graph,
			'graph-test',
			null,
			'from_start',
			undefined,
			undefined,
			undefined,
			{
				component_1: {
					id: '00000000-0000-4000-8000-000000000001',
					name: 'ck',
					nodeId: 'component_1',
					graphId: 'graph-test',
					runId: 'run-1',
					artifactId: 'component-root',
					execKey: 'component-exec',
					fingerprintAtCreation: 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
					createdAt: '2026-04-10T00:00:00.000Z',
					staleness: 'valid',
					outputs: {
						summary: { artifactId: 'summary-art', execKey: 'summary-exec' },
						full: { artifactId: 'full-art', execKey: 'full-exec' },
						' ': { artifactId: 'ignored' } as any
					},
				}
			} as any
		);
		expect(req.graph.__executionHints).toEqual({
			checkpoints: {
				component_1: {
					artifactId: 'component-root',
					execKey: 'component-exec',
					fingerprintAtCreation: 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
					outputs: {
						summary: { artifactId: 'summary-art', execKey: 'summary-exec' },
						full: { artifactId: 'full-art', execKey: 'full-exec' }
					}
				}
			}
		});
	});

	it('drops invalid component output checkpoint lineage entries during sanitization', () => {
		const graph = { version: 1, nodes: [], edges: [] };
		const req = buildRunCreateRequest(
			graph,
			'graph-test',
			null,
			'from_start',
			undefined,
			undefined,
			undefined,
			{
				component_1: {
					id: '00000000-0000-4000-8000-000000000001',
					name: 'ck',
					nodeId: 'component_1',
					graphId: 'graph-test',
					runId: 'run-1',
					artifactId: 'component-root',
					execKey: 'component-exec',
					fingerprintAtCreation: 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
					createdAt: '2026-04-10T00:00:00.000Z',
					staleness: 'valid',
					outputs: {
						valid: { artifactId: 'valid-art', execKey: 'valid-exec' },
						missingArtifact: { artifactId: '' as any, execKey: 'x' },
						' ': { artifactId: 'ignored' } as any
					}
				}
			} as any
		);
		expect(req.graph.__executionHints).toEqual({
			checkpoints: {
				component_1: {
					artifactId: 'component-root',
					execKey: 'component-exec',
					fingerprintAtCreation: 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
					outputs: {
						valid: { artifactId: 'valid-art', execKey: 'valid-exec' }
					}
				}
			}
		});
	});

	it('includes checkpoint execution hints from registry when valid', () => {
		const graph = { version: 1, nodes: [], edges: [] };
		const req = buildRunCreateRequest(
			graph,
			'graph-test',
			null,
			'from_start',
			undefined,
			undefined,
			undefined,
			{
				n1: {
					id: '00000000-0000-4000-8000-000000000001',
					name: 'ck-1',
					nodeId: 'n1',
					graphId: 'graph-test',
					runId: 'run-1',
					artifactId: 'art-1',
					execKey: 'exec-1',
					fingerprintAtCreation: 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
					createdAt: '2026-04-10T00:00:00.000Z',
					staleness: 'valid'
				}
			} as any
		);
		expect((req.graph.__executionHints as any)?.checkpoints).toEqual({
			n1: {
				artifactId: 'art-1',
				execKey: 'exec-1',
				fingerprintAtCreation: 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
			}
		});
	});

	it('omits checkpoint hints with invalid fingerprint', () => {
		const graph = { version: 1, nodes: [], edges: [] };
		const req = buildRunCreateRequest(
			graph,
			'graph-test',
			null,
			'from_start',
			undefined,
			undefined,
			undefined,
			{
				n1: {
					id: '00000000-0000-4000-8000-000000000001',
					name: 'ck-1',
					nodeId: 'n1',
					graphId: 'graph-test',
					runId: 'run-1',
					artifactId: 'art-1',
					execKey: 'exec-1',
					fingerprintAtCreation: 'bad',
					createdAt: '2026-04-10T00:00:00.000Z',
					staleness: 'valid'
				}
			} as any
		);
		expect((req.graph.__executionHints as any)?.checkpoints).toBeUndefined();
	});

	it('stales only affected nodes for accept-params style updates', () => {
		const previous = {
			a1: { status: 'succeeded_up_to_date', isUpToDate: true, lastArtifactId: 'art-a1' },
			a2: { status: 'succeeded_up_to_date', isUpToDate: true, lastArtifactId: 'art-a2' },
			b1: { status: 'succeeded_up_to_date', isUpToDate: true, lastArtifactId: 'art-b1' },
			b2: { status: 'succeeded_up_to_date', isUpToDate: true, lastArtifactId: 'art-b2' }
		};
		const affected = ['a1', 'a2'];
		const patch: Record<string, any> = {};
		for (const id of affected) {
			patch[id] = {
				status: 'stale',
				isUpToDate: false,
				currentArtifactId: null,
				currentRunId: null,
				currentExecKey: null
			};
		}
		const merged = mergeBindingsSticky(previous, patch);
		expect(merged.a1.isUpToDate).toBe(false);
		expect(merged.a2.isUpToDate).toBe(false);
		expect(merged.b1.isUpToDate).toBe(true);
		expect(merged.b2.isUpToDate).toBe(true);
		expect(merged.b1.lastArtifactId).toBe('art-b1');
		expect(merged.b2.lastArtifactId).toBe('art-b2');
	});

	it('treats missing bindings as not stale', () => {
		const partial = {
			a1: { status: 'succeeded_up_to_date', isUpToDate: true, lastArtifactId: 'art-a1' }
		};
		const out = computeGraphFreshness(partial);
		expect(out.freshness).toBe('up_to_date');
		expect(out.staleNodeCount).toBe(0);
	});

	it('updates bindings only for nodes in active run scope', () => {
		const setA = new Set(['a1', 'a2']);
		expect(shouldUpdateBinding('run-1', setA, 'a1')).toBe(true);
		expect(shouldUpdateBinding('run-1', setA, 'b1')).toBe(false);
		expect(shouldUpdateBinding(null, setA, 'b1')).toBe(true);
		expect(shouldUpdateBinding('run-1', new Set(), 'b1')).toBe(true);
	});

	it('maps stale strictly (undefined is not stale)', () => {
		expect(isBindingStale(undefined)).toBe(false);
		expect(isBindingStale({})).toBe(false);
		expect(isBindingStale({ isUpToDate: undefined })).toBe(false);
		expect(isBindingStale({ isUpToDate: true })).toBe(false);
		expect(isBindingStale({ status: 'succeeded' })).toBe(false);
		expect(isBindingStale({ isUpToDate: false })).toBe(true);
		expect(isBindingStale({ status: 'stale' })).toBe(true);
	});

	it('exposes canonical status projection from bindings', () => {
		const projection = statusProjectionFromBinding({
			status: 'succeeded_up_to_date',
			isUpToDate: false,
			current: { execKey: 'next', artifactId: 'a2' },
			last: { execKey: 'prev', artifactId: 'a1' }
		});
		expect(projection.lifecycle).toBe('completed');
		expect(projection.execution).toBe('finished');
		expect(projection.freshness).toBe('stale');
		expect(projection.display).toBe('stale');
	});

	it('run-start metadata updates do not flip stale flags', () => {
		const prev = {
			a1: { isUpToDate: true, status: 'succeeded' },
			b1: { isUpToDate: true, status: 'succeeded' }
		};
		const next = {
			...prev
		};
		expect(getStaleFlipNodeIds(prev, next)).toEqual([]);
	});

	it('empty snapshot bindings merge is a no-op', () => {
		const existing = {
			a1: { status: 'succeeded_up_to_date', isUpToDate: true, lastArtifactId: 'art-a1' },
			b1: { status: 'succeeded_up_to_date', isUpToDate: true, lastArtifactId: 'art-b1' }
		};
		const merged = mergeBindingsSticky(existing, {});
		expect(merged).toEqual(existing);
		expect(merged.a1.isUpToDate).toBe(true);
		expect(merged.b1.isUpToDate).toBe(true);
	});

	it('mergeBindingsSticky keeps unpatched keys and does not overwrite with undefined', () => {
		const prev = {
			a: { status: 'running', isUpToDate: true, lastArtifactId: 'art-a' },
			b: { status: 'succeeded_up_to_date', isUpToDate: true, lastArtifactId: 'art-b' },
			c: { status: 'succeeded_up_to_date', isUpToDate: true, lastArtifactId: 'art-c' }
		};
		const patch = {
			a: { status: undefined as any, currentExecKey: 'a-next' }
		};
		const merged = mergeBindingsSticky(prev as any, patch as any);
		expect(merged.b).toEqual(prev.b);
		expect(merged.c).toEqual(prev.c);
		expect(merged.a.status).toBe('running');
		expect((merged.a as any).currentExecKey).toBe('a-next');
	});

	it('graph can be stale while unrelated node remains succeeded', () => {
		const bindings = {
			a1: { status: 'stale', isUpToDate: false, lastArtifactId: 'art-a1' },
			b1: { status: 'succeeded_up_to_date', isUpToDate: true, lastArtifactId: 'art-b1' }
		};
		const fresh = computeGraphFreshness(bindings);
		expect(fresh.freshness).toBe('stale');
		expect(isBindingStale(bindings.b1)).toBe(false);
	});

	it('sibling fan-out partial run keeps unaffected branch unchanged', () => {
		const activeRunNodeSet = new Set(['src', 'xfm', 'llm_b']);
		expect(shouldUpdateBinding('run-1', activeRunNodeSet, 'llm_a')).toBe(false);
		expect(shouldUpdateBinding('run-1', activeRunNodeSet, 'llm_b')).toBe(true);

		const previous = {
			src: { status: 'succeeded_up_to_date', isUpToDate: true, lastArtifactId: 'art-src' },
			xfm: { status: 'succeeded_up_to_date', isUpToDate: true, lastArtifactId: 'art-xfm' },
			llm_a: { status: 'succeeded_up_to_date', isUpToDate: true, lastArtifactId: 'art-a' },
			llm_b: { status: 'succeeded_up_to_date', isUpToDate: true, lastArtifactId: 'art-b' }
		};
		const patchForScope = {
			src: { status: 'running' },
			xfm: { status: 'succeeded_up_to_date', isUpToDate: true, lastArtifactId: 'art-xfm-2' },
			llm_b: { status: 'succeeded_up_to_date', isUpToDate: true, lastArtifactId: 'art-b-2' }
		};
		const merged = mergeBindingsSticky(previous, patchForScope);

		expect(merged.llm_a).toEqual(previous.llm_a);
		expect(merged.llm_a.status).toBe('succeeded_up_to_date');
		expect(merged.llm_a.isUpToDate).toBe(true);
		expect(merged.llm_a.lastArtifactId).toBe('art-a');
	});

	it('from_selected_onward excludes sibling branch but includes ancestors', () => {
		const nodes: any[] = [{ id: 'src' }, { id: 'xfm' }, { id: 'a' }, { id: 'b' }];
		const edges: any[] = [
			{ source: 'src', target: 'xfm' },
			{ source: 'xfm', target: 'a' },
			{ source: 'xfm', target: 'b' }
		];
		const planned = computePlannedNodeSet(nodes, edges, 'b', 'from_selected_onward');
		expect([...planned].sort()).toEqual(['b', 'src', 'xfm']);
		expect(planned.has('a')).toBe(false);
	});

	it('from_selected_onward stops upstream traversal at checkpoint boundary', () => {
		const nodes: any[] = [{ id: 'src' }, { id: 'xfm' }, { id: 'a' }];
		const edges: any[] = [
			{ source: 'src', target: 'xfm' },
			{ source: 'xfm', target: 'a' }
		];
		const planned = computePlannedNodeSet(
			nodes,
			edges,
			'a',
			'from_selected_onward',
			new Set(['xfm'])
		);
		expect([...planned].sort()).toEqual(['a', 'xfm']);
		expect(planned.has('src')).toBe(false);
	});

	it('from_selected_onward still includes sibling upstream when downstream depends on it', () => {
		const nodes: any[] = [{ id: 'src' }, { id: 'xfm' }, { id: 'sib' }, { id: 'a' }];
		const edges: any[] = [
			{ source: 'src', target: 'xfm' },
			{ source: 'xfm', target: 'a' },
			{ source: 'src', target: 'sib' },
			{ source: 'sib', target: 'a' }
		];
		const planned = computePlannedNodeSet(
			nodes,
			edges,
			'a',
			'from_selected_onward',
			new Set(['xfm'])
		);
		expect([...planned].sort()).toEqual(['a', 'sib', 'src', 'xfm']);
	});

	it('selected_only excludes sibling branch but includes ancestors', () => {
		const nodes: any[] = [{ id: 'src' }, { id: 'xfm' }, { id: 'a' }, { id: 'b' }];
		const edges: any[] = [
			{ source: 'src', target: 'xfm' },
			{ source: 'xfm', target: 'a' },
			{ source: 'xfm', target: 'b' }
		];
		const planned = computePlannedNodeSet(nodes, edges, 'b', 'selected_only');
		expect([...planned].sort()).toEqual(['b', 'src', 'xfm']);
		expect(planned.has('a')).toBe(false);
	});

	it('connected components split disconnected chains and isolated nodes', () => {
		const nodes: any[] = [{ id: 'a1' }, { id: 'a2' }, { id: 'b1' }, { id: 'iso' }];
		const edges: any[] = [
			{ source: 'a1', target: 'a2' },
			{ source: 'b1', target: 'b1' }
		];
		const components = computeConnectedComponentNodeSets(nodes, edges).map((set) => [...set].sort());
		expect(components).toEqual([['a1', 'a2'], ['b1'], ['iso']]);
	});

	it('selected component resolves exactly the selected node component', () => {
		const nodes: any[] = [{ id: 'left1' }, { id: 'left2' }, { id: 'right1' }, { id: 'right2' }];
		const edges: any[] = [
			{ source: 'left1', target: 'left2' },
			{ source: 'right1', target: 'right2' }
		];
		const selected = computeSelectedConnectedComponentNodeSet(nodes, edges, 'right2');
		expect([...selected].sort()).toEqual(['right1', 'right2']);
	});

	it('selected component returns empty for missing selected node', () => {
		const nodes: any[] = [{ id: 'n1' }];
		const edges: any[] = [];
		const selected = computeSelectedConnectedComponentNodeSet(nodes, edges, 'unknown');
		expect([...selected]).toEqual([]);
	});

	it('run planner returns all components for from_start', () => {
		const nodes: any[] = [{ id: 'a' }, { id: 'b' }, { id: 'c' }];
		const edges: any[] = [{ source: 'a', target: 'b' }];
		const planned = planRunConnectedComponents(nodes, edges, null, 'from_start').map((set) => [...set].sort());
		expect(planned).toEqual([['a', 'b'], ['c']]);
	});

	it('run planner returns selected component only for from_selected_onward', () => {
		const nodes: any[] = [{ id: 'l1' }, { id: 'l2' }, { id: 'r1' }];
		const edges: any[] = [{ source: 'l1', target: 'l2' }];
		const planned = planRunConnectedComponents(nodes, edges, 'l2', 'from_selected_onward').map((set) =>
			[...set].sort()
		);
		expect(planned).toEqual([['l1', 'l2']]);
	});
});
