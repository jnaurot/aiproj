import { describe, expect, it } from 'vitest';

import {
	buildRunCreateRequest,
	computeGraphFreshness,
	computePlannedNodeSet,
	getStaleFlipNodeIds,
	isBindingStale,
	mergeBindingsSticky,
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
		const full = buildRunCreateRequest(graph, null, 'from_start');
		expect(full).toEqual({ graph });

		const partial = buildRunCreateRequest(graph, 'a1', 'from_selected_onward');
		expect(partial).toEqual({
			graph,
			runFrom: 'a1',
			runMode: 'from_selected_onward'
		});
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
});
