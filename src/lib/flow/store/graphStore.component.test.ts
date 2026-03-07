import { describe, expect, it } from 'vitest';
import { get } from 'svelte/store';

import { graphStore } from './graphStore';

describe('graphStore component integration', () => {
	it('applies component revision to node and derives immutable ports from API contract', async () => {
		graphStore.hardResetGraph();
		const nodeId = graphStore.addNode('component', { x: 20, y: 20 });
		graphStore.selectNode(nodeId);

		const originalFetch = globalThis.fetch;
		(globalThis as any).fetch = async (input: RequestInfo | URL) => {
			const url = String(input);
			if (url.includes('/api/components/cmp_test/revisions/crev_1')) {
				return new Response(
					JSON.stringify({
						schemaVersion: 1,
						componentId: 'cmp_test',
						revisionId: 'crev_1',
						parentRevisionId: null,
						createdAt: '2026-03-06T00:00:00Z',
						message: 'seed',
						revisionSchemaVersion: 1,
						checksum: 'abc',
						definition: {
							graph: { nodes: [], edges: [] },
							api: {
								inputs: [
									{
										name: 'in_data',
										portType: 'table',
										required: true,
										typedSchema: { type: 'table', fields: [] },
									},
								],
								outputs: [
									{
										name: 'out_data',
										portType: 'text',
										required: true,
										typedSchema: { type: 'text', fields: [] },
									},
								],
							},
							configSchema: {},
						},
					}),
					{ status: 200 }
				);
			}
			if (url.includes('/api/components/cmp_test/revisions?')) {
				return new Response(
					JSON.stringify({
						schemaVersion: 1,
						componentId: 'cmp_test',
						revisions: [
							{
								revisionId: 'crev_1',
								componentId: 'cmp_test',
								parentRevisionId: null,
								createdAt: '2026-03-06T00:00:00Z',
								message: 'seed',
								schemaVersion: 1,
								checksum: 'abc',
							},
						],
					}),
					{ status: 200 }
				);
			}
			return new Response('{}', { status: 200 });
		};

		try {
			const res = await graphStore.applyComponentRevisionToNode(nodeId, 'cmp_test', 'crev_1');
			expect((res as any)?.ok).toBe(true);

			const state = get(graphStore);
			const node = state.nodes.find((n) => n.id === nodeId);
			expect(node).toBeTruthy();
			expect(node?.data?.ports?.in).toBe('table');
			expect(node?.data?.ports?.out).toBe('json');
			expect((node?.data?.params as any)?.componentRef?.componentId).toBe('cmp_test');
			expect((node?.data?.params as any)?.componentRef?.revisionId).toBe('crev_1');
		} finally {
			(globalThis as any).fetch = originalFetch;
		}
	});

	it('renaming a component updates existing component node references', async () => {
		graphStore.hardResetGraph();
		const nodeId = graphStore.addNode('component', { x: 40, y: 40 });
		graphStore.selectNode(nodeId);

		const originalFetch = globalThis.fetch;
		(globalThis as any).fetch = async (input: RequestInfo | URL, init?: RequestInit) => {
			const url = String(input);
			if (url.includes('/api/components/component_example') && String(init?.method ?? 'GET') === 'PATCH') {
				return new Response(
					JSON.stringify({
						schemaVersion: 1,
						componentId: 'Reader_Describer',
						renamedFrom: 'component_example',
					}),
					{ status: 200 }
				);
			}
			return new Response('{}', { status: 200 });
		};

		try {
			const res = await graphStore.renameComponent('component_example', 'Reader_Describer');
			expect((res as any)?.ok).toBe(true);

			const state = get(graphStore);
			const node = state.nodes.find((n) => n.id === nodeId);
			expect(node).toBeTruthy();
			expect((node?.data?.params as any)?.componentRef?.componentId).toBe('Reader_Describer');
		} finally {
			(globalThis as any).fetch = originalFetch;
		}
	});

	it('forks a component revision and rebinds the selected node to the new component', async () => {
		graphStore.hardResetGraph();
		const nodeId = graphStore.addNode('component', { x: 40, y: 40 });
		graphStore.selectNode(nodeId);

		const originalFetch = globalThis.fetch;
		(globalThis as any).fetch = async (input: RequestInfo | URL, init?: RequestInit) => {
			const url = String(input);
			const method = String(init?.method ?? 'GET').toUpperCase();

			if (url.includes('/api/components/cmp_src/revisions/crev_src') && method === 'GET') {
				return new Response(
					JSON.stringify({
						schemaVersion: 1,
						componentId: 'cmp_src',
						revisionId: 'crev_src',
						parentRevisionId: null,
						createdAt: '2026-03-07T00:00:00Z',
						message: 'seed',
						revisionSchemaVersion: 1,
						checksum: 'seed',
						definition: {
							graph: { nodes: [], edges: [] },
							api: {
								inputs: [{ name: 'in_data', portType: 'table', required: true, typedSchema: { type: 'table', fields: [] } }],
								outputs: [{ name: 'out_data', portType: 'text', required: true, typedSchema: { type: 'text', fields: [] } }],
							},
							configSchema: {},
						},
					}),
					{ status: 200 }
				);
			}
			if (url.endsWith('/api/components') && method === 'POST') {
				return new Response(
					JSON.stringify({
						schemaVersion: 1,
						componentId: 'cmp_dst',
						revisionId: 'crev_dst',
						parentRevisionId: null,
						createdAt: '2026-03-07T00:01:00Z',
						message: 'fork',
						checksum: 'forked',
					}),
					{ status: 200 }
				);
			}
			if (url.includes('/api/components/cmp_dst/revisions/crev_dst') && method === 'GET') {
				return new Response(
					JSON.stringify({
						schemaVersion: 1,
						componentId: 'cmp_dst',
						revisionId: 'crev_dst',
						parentRevisionId: null,
						createdAt: '2026-03-07T00:01:00Z',
						message: 'fork',
						revisionSchemaVersion: 1,
						checksum: 'forked',
						definition: {
							graph: { nodes: [], edges: [] },
							api: {
								inputs: [{ name: 'in_data', portType: 'table', required: true, typedSchema: { type: 'table', fields: [] } }],
								outputs: [{ name: 'out_data', portType: 'text', required: true, typedSchema: { type: 'text', fields: [] } }],
							},
							configSchema: {},
						},
					}),
					{ status: 200 }
				);
			}
			if (url.includes('/api/components/cmp_dst/revisions?') && method === 'GET') {
				return new Response(
					JSON.stringify({
						schemaVersion: 1,
						componentId: 'cmp_dst',
						revisions: [
							{
								revisionId: 'crev_dst',
								componentId: 'cmp_dst',
								parentRevisionId: null,
								createdAt: '2026-03-07T00:01:00Z',
								message: 'fork',
								schemaVersion: 1,
								checksum: 'forked',
							},
						],
					}),
					{ status: 200 }
				);
			}
			return new Response('{}', { status: 200 });
		};

		try {
			const res = await graphStore.forkComponentRevisionToNode(
				nodeId,
				'cmp_src',
				'crev_src',
				'cmp_dst',
				{ message: 'fork' }
			);
			expect((res as any)?.ok).toBe(true);

			const state = get(graphStore);
			const node = state.nodes.find((n) => n.id === nodeId);
			expect(node).toBeTruthy();
			expect((node?.data?.params as any)?.componentRef?.componentId).toBe('cmp_dst');
			expect((node?.data?.params as any)?.componentRef?.revisionId).toBe('crev_dst');
			expect(node?.data?.ports?.in).toBe('table');
			expect(node?.data?.ports?.out).toBe('json');
		} finally {
			(globalThis as any).fetch = originalFetch;
		}
	});

	it('loads component internals into canvas for editing', async () => {
		graphStore.hardResetGraph();
		graphStore.addNode('source', { x: 10, y: 10 });

		const current = get(graphStore);
		const sampleNode = { ...(current.nodes[0] as any), id: 'n_cmp_internal_1' };
		const originalFetch = globalThis.fetch;
		(globalThis as any).fetch = async (input: RequestInfo | URL, init?: RequestInit) => {
			const url = String(input);
			const method = String(init?.method ?? 'GET').toUpperCase();
			if (url.includes('/api/components/cmp_edit/revisions/crev_edit') && method === 'GET') {
				return new Response(
					JSON.stringify({
						schemaVersion: 1,
						componentId: 'cmp_edit',
						revisionId: 'crev_edit',
						parentRevisionId: null,
						createdAt: '2026-03-07T00:00:00Z',
						message: 'edit',
						revisionSchemaVersion: 1,
						checksum: 'abc',
						definition: {
							graph: { nodes: [sampleNode], edges: [] },
							api: { inputs: [], outputs: [] },
							configSchema: {},
						},
					}),
					{ status: 200 }
				);
			}
			return new Response('{}', { status: 200 });
		};

		try {
			const res = await graphStore.openComponentRevisionForEditing('cmp_edit', 'crev_edit');
			expect((res as any)?.ok).toBe(true);
			const after = get(graphStore);
			expect(after.nodes.length).toBe(1);
			expect(after.nodes[0]?.id).toBe('n_cmp_internal_1');
			expect(after.selectedNodeId).toBeNull();
			expect(after.lastRunStatus).toBe('never_run');
		} finally {
			(globalThis as any).fetch = originalFetch;
		}
	});
});
