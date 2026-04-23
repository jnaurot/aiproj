import { get } from 'svelte/store';
import { describe, expect, it } from 'vitest';

import {
	__applyRunEventForTest,
	getEdgeDiagnosticSnapshotFromState,
	graphStore,
	type GraphState
} from './graphStore';

describe('graphStore diagnostic transport events', () => {
	it('processes diagnostic transport events idempotently and supports out-of-order clear', () => {
		graphStore.hardResetGraph();
		const base = get(graphStore) as GraphState;
		const afterEarlyClear = __applyRunEventForTest(
			base,
			{
				type: 'diagnostic_cleared',
				runId: 'run_diag_transport',
				at: '2026-04-23T01:12:00Z',
				key: 'diag:e1:TYPE_MISMATCH',
				edgeId: 'e1'
			} as any,
			'run_diag_transport'
		);
		expect(afterEarlyClear).toEqual(base);
		const afterRaised = __applyRunEventForTest(
			afterEarlyClear,
			{
				type: 'diagnostic_raised',
				runId: 'run_diag_transport',
				at: '2026-04-23T01:12:01Z',
				key: 'diag:e1:TYPE_MISMATCH',
				edgeId: 'e1',
				source: 'schema_plane',
				severity: 'warning',
				code: 'TYPE_MISMATCH',
				message: 'Transport warning'
			} as any,
			'run_diag_transport'
		);
		expect(Object.keys((afterRaised.queueRuntime?.schemaDiagnosticSignals ?? {}) as Record<string, unknown>).length).toBe(1);
		const afterRaisedDuplicate = __applyRunEventForTest(
			afterRaised,
			{
				type: 'diagnostic_raised',
				runId: 'run_diag_transport',
				at: '2026-04-23T01:12:01Z',
				key: 'diag:e1:TYPE_MISMATCH',
				edgeId: 'e1',
				source: 'schema_plane',
				severity: 'warning',
				code: 'TYPE_MISMATCH',
				message: 'Transport warning'
			} as any,
			'run_diag_transport'
		);
		expect(afterRaisedDuplicate).toEqual(afterRaised);
		const afterCleared = __applyRunEventForTest(
			afterRaisedDuplicate,
			{
				type: 'diagnostic_cleared',
				runId: 'run_diag_transport',
				at: '2026-04-23T01:12:02Z',
				key: 'diag:e1:TYPE_MISMATCH',
				edgeId: 'e1'
			} as any,
			'run_diag_transport'
		);
		expect(Object.keys((afterCleared.queueRuntime?.schemaDiagnosticSignals ?? {}) as Record<string, unknown>).length).toBe(0);
	});

	it('transport warnings do not override canonical edge diagnostic severity', () => {
		graphStore.hardResetGraph();
		const source = graphStore.addNode('model', { x: 0, y: 0 });
		const target = graphStore.addNode('transform', { x: 120, y: 0 });
		graphStore.addEdge({
			id: 'e_canonical',
			source,
			target,
			targetHandle: 'in',
			data: { exec: 'idle', mode: 'work' }
		} as any);
		const base = get(graphStore) as GraphState;
		const before = getEdgeDiagnosticSnapshotFromState(base, 'e_canonical');
		expect(before?.effectiveSeverity).toBe('clean');
		const withTransport = __applyRunEventForTest(
			base,
			{
				type: 'diagnostic_raised',
				runId: 'run_diag_transport_2',
				at: '2026-04-23T01:13:00Z',
				key: 'diag:e_canonical:OPAQUE_DEPENDENCY',
				edgeId: 'e_canonical',
				source: 'schema_plane',
				severity: 'warning',
				code: 'OPAQUE_DEPENDENCY',
				message: 'Transport-only opaque warning'
			} as any,
			'run_diag_transport_2'
		);
		const after = getEdgeDiagnosticSnapshotFromState(withTransport, 'e_canonical');
		expect(after?.effectiveSeverity).toBe('clean');
		const reloaded = {
			...withTransport,
			queueRuntime: {
				...(withTransport.queueRuntime ?? {}),
				schemaDiagnosticSignals: {}
			}
		} as GraphState;
		const reloadedSnap = getEdgeDiagnosticSnapshotFromState(reloaded, 'e_canonical');
		expect(reloadedSnap?.effectiveSeverity).toBe('clean');
	});
});

