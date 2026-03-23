import { describe, expect, it } from 'vitest';
import { get } from 'svelte/store';

import { graphStore } from './graphStore';

describe('graphStore legacy repair migration', () => {
	it('repairs malformed expectedInputSchemas and stale edge snapshots on load', () => {
		graphStore.hardResetGraph();
		const loaded = graphStore.loadGraphDocument({
			nodes: [
				{
					id: 'n_src',
					type: 'source',
					position: { x: 0, y: 0 },
					data: {
						kind: 'source',
						label: 'Source',
						params: { sourceKind: 'api', output: { mode: 'text' } }
					}
				},
				{
					id: 'n_dst',
					type: 'model',
					position: { x: 320, y: 0 },
					data: {
						kind: 'model',
						label: 'Model',
						params: { model: 'stub' },
						schema: {
							expectedInputSchemas: {
								'': { typedSchema: { type: 'json' } },
								in: { typedSchema: { type: 'text', fields: [] } },
								param_filters: { typedSchema: 'bad' }
							}
						}
					}
				}
			],
			edges: [
				{
					id: 'e_legacy',
					source: 'n_src',
					target: 'n_dst',
					data: {
						exec: 'idle',
						mode: 'work',
						contract: {
							payload: { source: { type: 'text' }, target: { type: 'text' } },
							snapshot: { decision: 'unknown' }
						}
					}
				}
			]
		});
		expect(loaded.ok).toBe(true);
		const state = get(graphStore as any);
		const dst = state.nodes.find((node: any) => node.id === 'n_dst');
		const expectedInputs = (dst?.data?.schema?.expectedInputSchemas ?? {}) as Record<string, any>;
		expect(Object.prototype.hasOwnProperty.call(expectedInputs, '')).toBe(false);
		expect(expectedInputs.in?.typedSchema?.type).toBe('text');
		expect(expectedInputs.param_filters?.typedSchema?.type).toBe('text');
		const edge = state.edges.find((row: any) => row.id === 'e_legacy');
		const snapshot = edge?.data?.contract?.snapshot ?? {};
		expect(String(snapshot.sourceSchemaFingerprint ?? '')).not.toBe('');
		expect(String(snapshot.targetSchemaFingerprint ?? '')).not.toBe('');
	});
});
