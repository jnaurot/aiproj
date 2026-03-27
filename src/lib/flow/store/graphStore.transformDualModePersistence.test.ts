import { describe, expect, it } from 'vitest';
import { get } from 'svelte/store';

import { __stripToDTOForTest, graphStore } from './graphStore';

describe('graphStore transform dual-mode persistence', () => {
	it('persists filter and derive dual-mode payloads across save/load', () => {
		graphStore.hardResetGraph();
		const filterId = graphStore.addNode('transform', { x: 0, y: 0 });
		const deriveId = graphStore.addNode('transform', { x: 260, y: 0 });
		const jsonFilterId = graphStore.addNode('transform', { x: 520, y: 0 });

		expect(
			graphStore.updateNodeConfig(filterId, {
				transformKind: 'filter',
				params: {
					op: 'filter',
					mode: 'rules',
					expr: '',
					rules: {
						kind: 'group',
						op: 'all',
						conditions: [
							{
								kind: 'condition',
								column: 'job_type',
								op: 'eq',
								value: { valueFrom: { handle: 'param_config', path: 'preferences.type' } }
							}
						]
					}
				}
			} as any)
		).toMatchObject({ ok: true });

		expect(
			graphStore.updateNodeConfig(deriveId, {
				transformKind: 'derive',
				params: {
					op: 'derive',
					mode: 'rules',
					columns: [],
					rules: [
						{
							name: 'value_plus_bonus',
							formula: {
								op: 'add',
								args: [{ column: 'value' }, { valueFrom: { handle: 'param_config', path: 'prefs.bonus' } }]
							}
						}
					]
				}
			} as any)
		).toMatchObject({ ok: true });

		expect(
			graphStore.updateNodeConfig(jsonFilterId, {
				transformKind: 'json_filter',
				params: {
					op: 'json_filter',
					json_filter: {
						mode: 'rules',
						rules: {
							kind: 'group',
							op: 'all',
							conditions: [
								{ kind: 'condition', path: 'pass', op: 'eq', value: true },
								{ kind: 'condition', path: 'score', op: 'gte', value: 70 }
							]
						},
						route_reject: true,
						include_reject_meta: true
					}
				}
			} as any)
		).toMatchObject({ ok: true });

		const state = get(graphStore);
		const dto = __stripToDTOForTest(state.nodes as any, state.edges as any, 'graph_transform_dual_mode');
		const persistedFilter = (dto.nodes as any[]).find((node) => String(node?.id ?? '') === filterId) as any;
		const persistedDerive = (dto.nodes as any[]).find((node) => String(node?.id ?? '') === deriveId) as any;
		const persistedJsonFilter = (dto.nodes as any[]).find((node) => String(node?.id ?? '') === jsonFilterId) as any;

		expect(persistedFilter?.data?.params?.filter?.mode).toBe('rules');
		expect(persistedFilter?.data?.params?.filter?.rules?.conditions?.length).toBe(1);
		expect(persistedDerive?.data?.params?.derive?.mode).toBe('rules');
		expect(persistedDerive?.data?.params?.derive?.rules?.[0]?.formula?.op).toBe('add');
		expect(persistedJsonFilter?.data?.params?.json_filter?.mode).toBe('rules');
		expect(persistedJsonFilter?.data?.params?.json_filter?.rules?.conditions?.[0]?.path).toBe('pass');
		expect(persistedJsonFilter?.data?.params?.json_filter?.rules?.conditions?.[1]?.path).toBe('score');

		const loaded = graphStore.loadGraphDocument(
			{
				nodes: dto.nodes as any,
				edges: dto.edges as any
			},
			'graph_transform_dual_mode_reloaded'
		);
		expect(loaded.ok).toBe(true);

		const reloaded = get(graphStore);
		const reloadedFilter = reloaded.nodes.find((node) => node.id === filterId) as any;
		const reloadedDerive = reloaded.nodes.find((node) => node.id === deriveId) as any;
		const reloadedJsonFilter = reloaded.nodes.find((node) => node.id === jsonFilterId) as any;

		expect(reloadedFilter?.data?.params?.filter?.mode).toBe('rules');
		expect(reloadedFilter?.data?.params?.filter?.rules?.conditions?.length).toBe(1);
		expect(reloadedDerive?.data?.params?.derive?.mode).toBe('rules');
		expect(reloadedDerive?.data?.params?.derive?.rules?.[0]?.formula?.op).toBe('add');
		expect(reloadedJsonFilter?.data?.params?.json_filter?.mode).toBe('rules');
		expect(reloadedJsonFilter?.data?.params?.json_filter?.rules?.conditions?.[0]?.path).toBe('pass');
		expect(reloadedJsonFilter?.data?.params?.json_filter?.rules?.conditions?.[1]?.path).toBe('score');
	});
});

