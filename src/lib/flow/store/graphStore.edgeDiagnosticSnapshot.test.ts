import { get } from 'svelte/store';
import { describe, expect, it } from 'vitest';

import { getEdgeDiagnosticSnapshotFromState, graphStore, type GraphState } from './graphStore';

describe('graphStore edge diagnostic snapshot authority', () => {
	it('uses contract clean as effective severity even when schema plane reports warning', () => {
		graphStore.hardResetGraph();
		const source = graphStore.addNode('model', { x: 0, y: 0 });
		const target = graphStore.addNode('transform', { x: 120, y: 0 });
		graphStore.addEdge({
			id: 'e_opaque_clean',
			source,
			target,
			targetHandle: 'in',
			data: { exec: 'idle', mode: 'work' }
		} as any);
		const state = get(graphStore) as GraphState;
		const snap = getEdgeDiagnosticSnapshotFromState(state, 'e_opaque_clean');
		expect(snap).toBeTruthy();
		expect(snap?.contractSeverity).toBe('clean');
		expect(snap?.schemaPlaneState).toBe('warning');
		expect(snap?.effectiveSeverity).toBe('clean');
	});

	it('uses contract warning as effective severity when schema plane is valid', () => {
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
						schema: {
							expectedSchema: {
								source: 'declared',
								typedSchema: { type: 'text', fields: [] }
							}
						},
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
					id: 'e_contract_warn',
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
			] as any,
			schemaPlane: {
				nodeSchemas: {
					src: { ok: true, output: { mode: 'text', columns: [] } },
					dst: { ok: true, output: { mode: 'text', columns: [] } }
				},
				edgeSchemas: {
					e_contract_warn: { mode: 'text', columns: [] }
				}
			} as any
		} as GraphState;
		const snap = getEdgeDiagnosticSnapshotFromState(state, 'e_contract_warn');
		expect(snap).toBeTruthy();
		expect(snap?.contractSeverity).toBe('warning');
		expect(snap?.schemaPlaneState).toBe('valid');
		expect(snap?.effectiveSeverity).toBe('warning');
	});
});
