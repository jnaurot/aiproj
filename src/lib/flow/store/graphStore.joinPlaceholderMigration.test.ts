import { get } from 'svelte/store';
import { describe, expect, it } from 'vitest';
import { graphStore } from './graphStore';

describe('graphStore join placeholder migration', () => {
	it('migrates upstream_left/right placeholders to connected source node ids on load', () => {
		graphStore.hardResetGraph();
		const doc: any = {
			nodes: [
				{
					id: 'n_left',
					type: 'source',
					position: { x: 0, y: 0 },
					data: { kind: 'source', label: 'Left', params: {}, status: 'idle' }
				},
				{
					id: 'n_right',
					type: 'source',
					position: { x: 240, y: 0 },
					data: { kind: 'source', label: 'Right', params: {}, status: 'idle' }
				},
				{
					id: 'n_join',
					type: 'transform',
					position: { x: 480, y: 0 },
					data: {
						kind: 'transform',
						label: 'Join',
						status: 'idle',
						params: {
							op: 'join',
							join: {
								clauses: [
									{
										leftNodeId: 'upstream_left',
										leftCol: 'id',
										rightNodeId: 'upstream_right',
										rightCol: 'id',
										how: 'inner'
									}
								]
							}
						}
					}
				}
			],
			edges: [
				{
					id: 'e_left',
					source: 'n_left',
					sourceHandle: 'out',
					target: 'n_join',
					targetHandle: 'in',
					data: { mode: 'work' }
				},
				{
					id: 'e_right',
					source: 'n_right',
					sourceHandle: 'out',
					target: 'n_join',
					targetHandle: 'in',
					data: { mode: 'work' }
				}
			]
		};
		const loaded = graphStore.loadGraphDocument(doc);
		expect(loaded.ok).toBe(true);
		const state = get(graphStore);
		const joinNode = state.nodes.find((node: any) => node.id === 'n_join') as any;
		const clause = joinNode?.data?.params?.join?.clauses?.[0];
		expect(clause?.leftNodeId).toBe('n_left');
		expect(clause?.rightNodeId).toBe('n_right');
	});

	it('resolves duplicate-side placeholder to distinct right-side source', () => {
		graphStore.hardResetGraph();
		const doc: any = {
			nodes: [
				{
					id: 'n_a',
					type: 'source',
					position: { x: 0, y: 0 },
					data: { kind: 'source', label: 'A', params: {}, status: 'idle' }
				},
				{
					id: 'n_b',
					type: 'source',
					position: { x: 240, y: 0 },
					data: { kind: 'source', label: 'B', params: {}, status: 'idle' }
				},
				{
					id: 'n_join',
					type: 'transform',
					position: { x: 480, y: 0 },
					data: {
						kind: 'transform',
						label: 'Join',
						status: 'idle',
						params: {
							op: 'join',
							join: {
								clauses: [
									{
										leftNodeId: 'upstream_left',
										leftCol: 'id',
										rightNodeId: 'upstream_left',
										rightCol: 'id',
										how: 'inner'
									}
								]
							}
						}
					}
				}
			],
			edges: [
				{
					id: 'e_a',
					source: 'n_a',
					sourceHandle: 'out',
					target: 'n_join',
					targetHandle: 'in',
					data: { mode: 'work' }
				},
				{
					id: 'e_b',
					source: 'n_b',
					sourceHandle: 'out',
					target: 'n_join',
					targetHandle: 'in',
					data: { mode: 'work' }
				}
			]
		};
		const loaded = graphStore.loadGraphDocument(doc);
		expect(loaded.ok).toBe(true);
		const state = get(graphStore);
		const joinNode = state.nodes.find((node: any) => node.id === 'n_join') as any;
		const clause = joinNode?.data?.params?.join?.clauses?.[0];
		expect(clause?.leftNodeId).toBe('n_a');
		expect(clause?.rightNodeId).toBe('n_b');
	});

	it('keeps explicit node-id join clauses unchanged', () => {
		graphStore.hardResetGraph();
		const doc: any = {
			nodes: [
				{
					id: 'n_left',
					type: 'source',
					position: { x: 0, y: 0 },
					data: { kind: 'source', label: 'Left', params: {}, status: 'idle' }
				},
				{
					id: 'n_right',
					type: 'source',
					position: { x: 240, y: 0 },
					data: { kind: 'source', label: 'Right', params: {}, status: 'idle' }
				},
				{
					id: 'n_join',
					type: 'transform',
					position: { x: 480, y: 0 },
					data: {
						kind: 'transform',
						label: 'Join',
						status: 'idle',
						params: {
							op: 'join',
							join: {
								clauses: [
									{
										leftNodeId: 'n_left',
										leftCol: 'id',
										rightNodeId: 'n_right',
										rightCol: 'id',
										how: 'inner'
									}
								]
							}
						}
					}
				}
			],
			edges: [
				{
					id: 'e_left',
					source: 'n_left',
					sourceHandle: 'out',
					target: 'n_join',
					targetHandle: 'in',
					data: { mode: 'work' }
				},
				{
					id: 'e_right',
					source: 'n_right',
					sourceHandle: 'out',
					target: 'n_join',
					targetHandle: 'in',
					data: { mode: 'work' }
				}
			]
		};
		const loaded = graphStore.loadGraphDocument(doc);
		expect(loaded.ok).toBe(true);
		const state = get(graphStore);
		const joinNode = state.nodes.find((node: any) => node.id === 'n_join') as any;
		const clause = joinNode?.data?.params?.join?.clauses?.[0];
		expect(clause?.leftNodeId).toBe('n_left');
		expect(clause?.rightNodeId).toBe('n_right');
	});
});

