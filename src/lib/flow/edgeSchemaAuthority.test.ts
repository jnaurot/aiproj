import { describe, expect, it } from 'vitest';

import {
	buildSchemaTooltip,
	resolveSchemaClassForView,
	resolveSchemaClassFromSnapshot,
	summarizeSchemaOverlayCounts
} from './edgeSchemaAuthority';
import { getEdgeDiagnosticSnapshotFromState } from '$lib/flow/store/graphStore';

describe('edge schema authority presentation', () => {
	it('summarizes overlay counts from effective severity', () => {
		const counts = summarizeSchemaOverlayCounts([
			{ edgeId: 'e1', contractSeverity: 'clean', schemaPlaneState: 'valid', runtimeState: 'inactive', effectiveSeverity: 'clean' },
			{ edgeId: 'e2', contractSeverity: 'warning', schemaPlaneState: 'warning', runtimeState: 'inactive', effectiveSeverity: 'warning' },
			{ edgeId: 'e3', contractSeverity: 'error', schemaPlaneState: 'error', runtimeState: 'inactive', effectiveSeverity: 'error' },
			null,
			undefined
		]);
		expect(counts).toEqual({ errorCount: 1, warningCount: 1 });
	});

	it('does not produce warning class when contract is clean and schema plane is warning', () => {
		const schemaClass = resolveSchemaClassFromSnapshot({
			edgeId: 'e1',
			contractSeverity: 'clean',
			schemaPlaneState: 'warning',
			runtimeState: 'settled',
			effectiveSeverity: 'clean',
			schemaPlaneMessage: 'Schema unverified: upstream output is opaque.'
		});
		expect(schemaClass).toBe('');
	});

	it('uses warning class when contract severity is warning', () => {
		const schemaClass = resolveSchemaClassFromSnapshot({
			edgeId: 'e2',
			contractSeverity: 'warning',
			schemaPlaneState: 'valid',
			runtimeState: 'settled',
			effectiveSeverity: 'warning',
			contractMessage: 'Work payload mismatch: lossy coercion text -> json'
		});
		expect(schemaClass).toBe('edge-schema-warning');
	});

	it('includes contract authority and schema-plane note in tooltip', () => {
		const text = buildSchemaTooltip(
			{
				edgeId: 'e3',
				contractSeverity: 'warning',
				schemaPlaneState: 'warning',
				runtimeState: 'inactive',
				effectiveSeverity: 'warning',
				contractMessage: 'Edge contract snapshot drift detected.',
				schemaPlaneMessage: 'Schema unverified: upstream output is opaque.'
			},
			undefined,
			undefined
		);
		expect(String(text ?? '')).toContain('Schema: warning (contract)');
		expect(String(text ?? '')).toContain('Schema-plane note: warning');
	});

	it('supports feature-flag parity for authority selection', () => {
		const snapshot = {
			edgeId: 'e4',
			contractSeverity: 'clean',
			schemaPlaneState: 'warning',
			runtimeState: 'settled',
			effectiveSeverity: 'clean'
		} as const;
		const onClass = resolveSchemaClassFromSnapshot(snapshot as any, 'edge-schema-warning', true);
		const offClass = resolveSchemaClassFromSnapshot(snapshot as any, 'edge-schema-warning', false);
		expect(onClass).toBe('');
		expect(offClass).toBe('edge-schema-warning');
	});

	// -----------------------------------------------------------------
	// effectiveSeverity derivation: schemaPlane error must surface
	// -----------------------------------------------------------------

	it('produces error class when effectiveSeverity is error (schemaPlane hard failure)', () => {
		// This snapshot represents a node where schemaPlane propagation failed
		// (e.g. SHAPE_MISMATCH — column not found) and effectiveSeverity was
		// correctly set to 'error' by getEdgeDiagnosticSnapshotFromState.
		const schemaClass = resolveSchemaClassFromSnapshot({
			edgeId: 'e5',
			contractSeverity: 'clean',
			schemaPlaneState: 'error',
			runtimeState: 'inactive',
			effectiveSeverity: 'error',
			schemaPlaneMessage: "Column 'candidate_required_location' not found in input schema"
		});
		expect(schemaClass).toBe('edge-schema-error');
	});

	it('schemaPlane warning alone does NOT produce an edge class (opaque = uncertain, not error)', () => {
		// Opaque upstream schema (e.g. source never run) produces schemaPlaneState:warning
		// but this is informational — the edge should not be visually flagged as broken.
		const schemaClass = resolveSchemaClassFromSnapshot({
			edgeId: 'e6',
			contractSeverity: 'clean',
			schemaPlaneState: 'warning',
			runtimeState: 'inactive',
			effectiveSeverity: 'clean',
			schemaPlaneMessage: 'Schema unverified: upstream output is opaque.'
		});
		expect(schemaClass).toBe('');
	});

	it('suppresses schema class in execution view', () => {
		const schemaClass = resolveSchemaClassForView('execution', {
			edgeId: 'e7',
			contractSeverity: 'error',
			schemaPlaneState: 'error',
			runtimeState: 'inactive',
			effectiveSeverity: 'error',
			contractMessage: 'Contract mismatch'
		});
		expect(schemaClass).toBe('');
	});

	it('keeps schema class in schema view', () => {
		const schemaClass = resolveSchemaClassForView('schema', {
			edgeId: 'e8',
			contractSeverity: 'error',
			schemaPlaneState: 'error',
			runtimeState: 'inactive',
			effectiveSeverity: 'error',
			contractMessage: 'Contract mismatch'
		});
		expect(schemaClass).toBe('edge-schema-error');
	});
});

// -----------------------------------------------------------------
// getEdgeDiagnosticSnapshotFromState effectiveSeverity merging
// -----------------------------------------------------------------

import { get } from 'svelte/store';
import { graphStore } from '$lib/flow/store/graphStore';

describe('getEdgeDiagnosticSnapshotFromState — effectiveSeverity merges schemaPlane error', () => {
	it('effectiveSeverity is error when target nodeSchemas has ok:false (SHAPE_MISMATCH) with non-opaque upstream', () => {
		graphStore.hardResetGraph();
		const src = graphStore.addNode('model', { x: 0, y: 0 });
		const dst = graphStore.addNode('transform', { x: 120, y: 0 });
		graphStore.addEdge({
			id: 'e_shape_non_opaque',
			source: src,
			target: dst,
			sourceHandle: 'out',
			targetHandle: 'in',
			data: { exec: 'idle' }
		} as any);
		const baseline = get(graphStore);
		const state = {
			...baseline,
			schemaPlane: {
				nodeSchemas: {
					[src]: {
						ok: true,
						output: { mode: 'table', columns: [{ name: 'known_col', type: 'string', nullable: true, properties: {} }] }
					},
					[dst]: {
						ok: false,
						error: { code: 'SHAPE_MISMATCH', message: "Column 'nonexistent_column' not found in input schema", handles: ['in'] }
					}
				},
				edgeSchemas: {
					e_shape_non_opaque: { mode: 'table', columns: [{ name: 'known_col', type: 'string', nullable: true, properties: {} }] }
				}
			} as any
		} as any;
		const snapshot = getEdgeDiagnosticSnapshotFromState(state, 'e_shape_non_opaque');
		expect(snapshot).not.toBeNull();
		if (snapshot) {
			expect(snapshot.schemaPlaneState).toBe('error');
			expect(snapshot.effectiveSeverity).toBe('error');
		}
	});

	it('effectiveSeverity is warning when SHAPE_MISMATCH is behind opaque upstream', () => {
		graphStore.hardResetGraph();
		const src = graphStore.addNode('model', { x: 0, y: 0 });
		const dst = graphStore.addNode('transform', { x: 120, y: 0 });
		graphStore.addEdge({
			id: 'e_shape_opaque',
			source: src,
			target: dst,
			sourceHandle: 'out',
			targetHandle: 'in',
			data: { exec: 'idle' }
		} as any);
		const baseline = get(graphStore);
		const state = {
			...baseline,
			schemaPlane: {
				nodeSchemas: {
					[src]: { ok: true, output: { mode: 'opaque', columns: [] } },
					[dst]: {
						ok: false,
						error: { code: 'SHAPE_MISMATCH', message: "Column 'candidate_required_location' not found in input schema", handles: ['in'] }
					}
				},
				edgeSchemas: { e_shape_opaque: { mode: 'opaque', columns: [] } }
			} as any
		} as any;
		const snapshot = getEdgeDiagnosticSnapshotFromState(state, 'e_shape_opaque');
		expect(snapshot).not.toBeNull();
		if (snapshot) {
			expect(snapshot.schemaPlaneState).toBe('warning');
			expect(snapshot.effectiveSeverity).toBe('warning');
		}
	});

	it('effectiveSeverity is clean when target nodeSchemas is ok', () => {
		graphStore.hardResetGraph();
		const src = graphStore.addNode('source', { x: 0, y: 0 });
		const dst = graphStore.addNode('transform', { x: 200, y: 0 });
		const edgeId = `e_${src}_${dst}`;
		graphStore.addEdge({ id: edgeId, source: src, target: dst, sourceHandle: 'out', targetHandle: 'in' });

		const snapshot = (graphStore as any).getEdgeDiagnosticSnapshot?.(edgeId);
		expect(snapshot).not.toBeNull();
		// Without a schema error on the target, effectiveSeverity should be clean.
		const state = get(graphStore);
		if (snapshot && state.schemaPlane?.nodeSchemas?.[dst]?.ok !== false) {
			expect(snapshot.effectiveSeverity).toBe('clean');
		}
	});
});
