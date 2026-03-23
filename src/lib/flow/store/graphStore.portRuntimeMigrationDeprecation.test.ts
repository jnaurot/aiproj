import { describe, expect, it } from 'vitest';
import { get } from 'svelte/store';

import { graphStore } from './graphStore';

describe('graphStore PORT-RUNTIME-010 migration/deprecation diagnostics', () => {
	it('emits save preflight warnings for legacy fields and preview queue policy', () => {
		graphStore.hardResetGraph();
		const state = get(graphStore as any);
		const preflight = graphStore.getSavePreflight({
			...(state as any),
			nodes: [
				{
					id: 'src',
					type: 'source',
					position: { x: 0, y: 0 },
					data: {
						kind: 'source',
						label: 'Source',
						params: { sourceKind: 'api', output: { mode: 'json' } },
						portContracts: { out: { out: { affinity: 'work' } } }
					}
				},
				{
					id: 'dst',
					type: 'model',
					position: { x: 300, y: 0 },
					data: {
						kind: 'model',
						label: 'Model',
						params: { model: 'stub' },
						portContracts: { in: { in: { affinity: 'work' } } },
						schema: {
							expectedInputSchema: {
								typedSchema: { type: 'json', fields: [] },
								source: 'declared',
								state: 'fresh'
							}
						}
					}
				}
			],
			edges: [
				{
					id: 'e1',
					source: 'src',
					target: 'dst',
					targetHandle: 'in',
					data: {
						exec: 'idle',
						mode: 'work',
						queue: { policy: 'round_robin', max: 1000, overflow: 'block' },
						contract: {
							out: 'json',
							in: 'json',
							payload: { source: { type: 'json' }, target: { type: 'json' } }
						}
					}
				}
			]
		} as any);
		expect(preflight.ok).toBe(true);
		const warningCodes = new Set(
			(preflight.diagnostics ?? [])
				.filter((d) => String(d?.severity ?? '').toLowerCase() === 'warning')
				.map((d) => String(d?.code ?? ''))
		);
		expect(warningCodes.has('LEGACY_EXPECTED_INPUT_SCHEMA_DEPRECATED')).toBe(true);
		expect(warningCodes.has('LEGACY_PORT_CONTRACTS_DEPRECATED')).toBe(true);
		expect(warningCodes.has('EDGE_QUEUE_POLICY_PREVIEW')).toBe(true);
	});

	it('migrates legacy expectedInputSchema on load and removes singleton field', () => {
		graphStore.hardResetGraph();
		const loaded = graphStore.loadGraphDocument({
			nodes: [
				{
					id: 'n_model',
					type: 'model',
					position: { x: 0, y: 0 },
					data: {
						kind: 'model',
						label: 'Model',
						params: { model: 'stub' },
						schema: {
							expectedInputSchema: {
								typedSchema: { type: 'json', fields: [] },
								source: 'declared',
								state: 'fresh'
							}
						}
					}
				}
			],
			edges: []
		});
		expect(loaded.ok).toBe(true);
		const st = get(graphStore as any);
		const node = st.nodes.find((n: any) => n.id === 'n_model');
		expect((node?.data as any)?.schema?.expectedInputSchemas?.in?.typedSchema?.type).toBe('json');
		expect((node?.data as any)?.schema?.expectedInputSchema).toBeUndefined();
	});
});
