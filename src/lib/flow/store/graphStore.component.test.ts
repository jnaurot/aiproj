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

	it('defaults output bindings to first internal node when no leaf exists', async () => {
		graphStore.hardResetGraph();
		const nodeId = graphStore.addNode('component', { x: 20, y: 20 });
		graphStore.selectNode(nodeId);

		const originalFetch = globalThis.fetch;
		(globalThis as any).fetch = async (input: RequestInfo | URL) => {
			const url = String(input);
			if (url.includes('/api/components/cmp_cycle/revisions/crev_1')) {
				return new Response(
					JSON.stringify({
						schemaVersion: 1,
						componentId: 'cmp_cycle',
						revisionId: 'crev_1',
						parentRevisionId: null,
						createdAt: '2026-03-08T00:00:00Z',
						message: 'cycle',
						revisionSchemaVersion: 1,
						checksum: 'abc',
						definition: {
							graph: {
								nodes: [{ id: 'n1' }, { id: 'n2' }],
								edges: [{ source: 'n1', target: 'n2' }, { source: 'n2', target: 'n1' }]
							},
							api: {
								inputs: [],
								outputs: [{ name: 'out_data', portType: 'json', required: true, typedSchema: { type: 'json', fields: [] } }]
							},
							configSchema: {}
						}
					}),
					{ status: 200 }
				);
			}
			if (url.includes('/api/components/cmp_cycle/revisions?')) {
				return new Response(
					JSON.stringify({
						schemaVersion: 1,
						componentId: 'cmp_cycle',
						revisions: [{ revisionId: 'crev_1', componentId: 'cmp_cycle', createdAt: '2026-03-08T00:00:00Z', schemaVersion: 1, checksum: 'abc' }]
					}),
					{ status: 200 }
				);
			}
			return new Response('{}', { status: 200 });
		};

		try {
			const res = await graphStore.applyComponentRevisionToNode(nodeId, 'cmp_cycle', 'crev_1');
			expect((res as any)?.ok).toBe(true);
			const state = get(graphStore);
			const node = state.nodes.find((n) => n.id === nodeId);
			const outBinding = (node?.data?.params as any)?.bindings?.outputs?.out_data;
			expect(String(outBinding?.nodeId ?? '')).toBe('n1');
			expect(String(outBinding?.artifact ?? '')).toBe('current');
		} finally {
			(globalThis as any).fetch = originalFetch;
		}
	});

	it('applies multi-output component API and keeps bindings synchronized', async () => {
		graphStore.hardResetGraph();
		const nodeId = graphStore.addNode('component', { x: 20, y: 20 });
		graphStore.selectNode(nodeId);

		const originalFetch = globalThis.fetch;
		(globalThis as any).fetch = async (input: RequestInfo | URL) => {
			const url = String(input);
			if (url.includes('/api/components/cmp_multi/revisions/crev_1')) {
				return new Response(
					JSON.stringify({
						schemaVersion: 1,
						componentId: 'cmp_multi',
						revisionId: 'crev_1',
						parentRevisionId: null,
						createdAt: '2026-03-08T00:00:00Z',
						message: 'multi',
						revisionSchemaVersion: 1,
						checksum: 'abc',
						definition: {
							graph: {
								nodes: [{ id: 'inner_text' }, { id: 'inner_json' }],
								edges: [],
							},
							api: {
								inputs: [],
								outputs: [
									{ name: 'out_text', portType: 'text', required: true, typedSchema: { type: 'text', fields: [] } },
									{
										name: 'out_json',
										portType: 'json',
										required: true,
										typedSchema: { type: 'json', fields: [{ name: 'ok', type: 'text', nullable: false }] },
									},
								],
							},
							configSchema: {},
						},
					}),
					{ status: 200 }
				);
			}
			if (url.includes('/api/components/cmp_multi/revisions?')) {
				return new Response(
					JSON.stringify({
						schemaVersion: 1,
						componentId: 'cmp_multi',
						revisions: [{ revisionId: 'crev_1', componentId: 'cmp_multi', createdAt: '2026-03-08T00:00:00Z', schemaVersion: 1, checksum: 'abc' }],
					}),
					{ status: 200 }
				);
			}
			return new Response('{}', { status: 200 });
		};

		try {
			const res = await graphStore.applyComponentRevisionToNode(nodeId, 'cmp_multi', 'crev_1');
			expect((res as any)?.ok).toBe(true);
			const state = get(graphStore);
			const node = state.nodes.find((n) => n.id === nodeId);
			const outputs = ((node?.data?.params as any)?.api?.outputs ?? []) as Array<{ name: string }>;
			const bindings = ((node?.data?.params as any)?.bindings?.outputs ?? {}) as Record<
				string,
				{ nodeId?: string; artifact?: 'current' | 'last' }
			>;
			expect(outputs.map((o) => o.name)).toEqual(['out_text', 'out_json']);
			expect(String(bindings.out_text?.nodeId ?? '')).toBe('inner_text');
			expect(String(bindings.out_json?.nodeId ?? '')).toBe('inner_text');
			expect(String(bindings.out_text?.artifact ?? '')).toBe('current');
			expect(String(bindings.out_json?.artifact ?? '')).toBe('current');
		} finally {
			(globalThis as any).fetch = originalFetch;
		}
	});

	it('rejects ambiguous multi-output component edges without a source handle', () => {
		graphStore.hardResetGraph();
		const componentId = graphStore.addNode('component', { x: 10, y: 10 });
		const llmId = graphStore.addNode('llm', { x: 280, y: 20 });
		const configRes = graphStore.updateNodeConfig(componentId, {
			params: {
				componentRef: { componentId: 'cmp_local', revisionId: 'crev_local', apiVersion: 'v1' },
				api: {
					inputs: [],
					outputs: [
						{ name: 'out_a', portType: 'text', required: true, typedSchema: { type: 'text', fields: [] } },
						{ name: 'out_b', portType: 'text', required: true, typedSchema: { type: 'text', fields: [] } }
					]
				},
				bindings: {
					inputs: {},
					config: {},
					outputs: {
						out_a: { nodeId: 'n1', artifact: 'current' },
						out_b: { nodeId: 'n2', artifact: 'current' }
					}
				},
				config: {}
			},
			ports: { in: null, out: 'json' }
		});
		expect(configRes.ok).toBe(true);

		const addRes = graphStore.addEdge({
			id: 'e_ambiguous',
			source: componentId,
			target: llmId,
			targetHandle: 'in',
			data: { exec: 'idle' }
		} as any);

		expect(addRes.ok).toBe(false);
		expect(String(addRes.error ?? '')).toContain('Component output handle');
	});

	it('uses named component sourceHandle to compute edge payload contract', () => {
		graphStore.hardResetGraph();
		const componentId = graphStore.addNode('component', { x: 10, y: 10 });
		const llmId = graphStore.addNode('llm', { x: 280, y: 20 });
		const configRes = graphStore.updateNodeConfig(componentId, {
			params: {
				componentRef: { componentId: 'cmp_local', revisionId: 'crev_local', apiVersion: 'v1' },
				api: {
					inputs: [],
					outputs: [
						{ name: 'out_text', portType: 'text', required: true, typedSchema: { type: 'text', fields: [] } },
						{ name: 'out_json', portType: 'json', required: true, typedSchema: { type: 'json', fields: [{ name: 'value', type: 'text', nullable: false }] } }
					]
				},
				bindings: {
					inputs: {},
					config: {},
					outputs: {
						out_text: { nodeId: 'n1', artifact: 'current' },
						out_json: { nodeId: 'n2', artifact: 'current' }
					}
				},
				config: {}
			},
			ports: { in: null, out: 'json' }
		});
		expect(configRes.ok).toBe(true);

		const addRes = graphStore.addEdge({
			id: 'e_named',
			source: componentId,
			sourceHandle: 'out_text',
			target: llmId,
			targetHandle: 'in',
			data: { exec: 'idle' }
		} as any);
		expect(addRes.ok).toBe(true);

		const state = get(graphStore);
		const edge = state.edges.find((e) => e.id === 'e_named');
		expect(edge).toBeTruthy();
		expect((edge as any)?.data?.contract?.out).toBe('text');
		expect((edge as any)?.data?.contract?.payload?.source?.type).toBe('string');
	});

	it('derives component edge source contract from API output even when ports.out conflicts', () => {
		graphStore.hardResetGraph();
		const componentId = graphStore.addNode('component', { x: 10, y: 10 });
		const llmId = graphStore.addNode('llm', { x: 280, y: 20 });
		const configRes = graphStore.updateNodeConfig(componentId, {
			params: {
				componentRef: { componentId: 'cmp_local', revisionId: 'crev_local', apiVersion: 'v1' },
				api: {
					inputs: [],
					outputs: [
						{
							name: 'only_out',
							portType: 'text',
							required: true,
							typedSchema: { type: 'text', fields: [] }
						}
					]
				},
				bindings: {
					inputs: {},
					config: {},
					outputs: { only_out: { nodeId: 'n1', artifact: 'current' } }
				},
				config: {}
			},
			// Intentionally conflicting with API output portType.
			ports: { in: null, out: 'json' }
		});
		expect(configRes.ok).toBe(true);

		const addRes = graphStore.addEdge({
			id: 'e_conflicting_ports_out',
			source: componentId,
			sourceHandle: 'out',
			target: llmId,
			targetHandle: 'in',
			data: { exec: 'idle' }
		} as any);
		expect(addRes.ok).toBe(true);

		const state = get(graphStore);
		const edge = state.edges.find((e) => e.id === 'e_conflicting_ports_out');
		expect(edge).toBeTruthy();
		expect((edge as any)?.sourceHandle).toBe('only_out');
		expect((edge as any)?.data?.contract?.out).toBe('text');
		expect((edge as any)?.data?.contract?.payload?.source?.type).toBe('string');
	});

	it('prunes dangling component output bindings on Accept', async () => {
		graphStore.hardResetGraph();
		const componentId = graphStore.addNode('component', { x: 20, y: 20 });
		graphStore.selectNode(componentId);
		graphStore.patchInspectorDraft({
			api: {
				inputs: [],
				outputs: [
					{ name: 'summary', portType: 'text', required: true, typedSchema: { type: 'text', fields: [] } },
					{ name: 'source', portType: 'text', required: true, typedSchema: { type: 'text', fields: [] } }
				]
			},
			bindings: {
				inputs: {},
				config: {},
				outputs: {
					out_data: { nodeId: 'n_old', artifact: 'current' },
					summary: { nodeId: 'n_sum', artifact: 'current' },
					source: { nodeId: 'n_src', artifact: 'current' }
				}
			}
		});

		const result = await graphStore.applyInspectorDraft();
		expect((result as any)?.ok).toBe(true);

		const state = get(graphStore);
		const node = state.nodes.find((n) => n.id === componentId);
		const outputBindings = (((node?.data?.params as any)?.bindings ?? {}).outputs ?? {}) as Record<string, unknown>;
		expect(Object.keys(outputBindings).sort()).toEqual(['source', 'summary']);
		expect(outputBindings.out_data).toBeUndefined();
	});

	it('blocks Accept when a declared component output is missing binding nodeId', async () => {
		graphStore.hardResetGraph();
		const componentId = graphStore.addNode('component', { x: 20, y: 20 });
		graphStore.selectNode(componentId);
		graphStore.patchInspectorDraft({
			api: {
				inputs: [],
				outputs: [
					{
						name: 'summary',
						portType: 'text',
						required: true,
						typedSchema: { type: 'text', fields: [] }
					}
				]
			},
			bindings: {
				inputs: {},
				config: {},
				outputs: {
					summary: { nodeId: '', artifact: 'current' }
				}
			}
		});

		const result = await graphStore.applyInspectorDraft();
		expect((result as any)?.ok).toBe(false);
		expect(String((result as any)?.reason ?? '')).toBe('component_accept_blocked');
		expect(String((result as any)?.error ?? '')).toContain('requires a bound internal node');
	});

	it('blocks Accept when typedSchema.type is not aligned with component output portType', async () => {
		graphStore.hardResetGraph();
		const componentId = graphStore.addNode('component', { x: 20, y: 20 });
		graphStore.selectNode(componentId);
		graphStore.patchInspectorDraft({
			api: {
				inputs: [],
				outputs: [
					{
						name: 'summary',
						portType: 'text',
						required: true,
						typedSchema: { type: 'json', fields: [] }
					}
				]
			},
			bindings: {
				inputs: {},
				config: {},
				outputs: {
					summary: { nodeId: 'n_any', artifact: 'current' }
				}
			}
		});

		const result = await graphStore.applyInspectorDraft();
		expect((result as any)?.ok).toBe(false);
		expect(String((result as any)?.reason ?? '')).toBe('component_accept_blocked');
		expect(String((result as any)?.error ?? '')).toContain('typedSchema.type aligned with portType');
	});

	it('recomputes component edge contract payload source from sourceHandle on load', () => {
		graphStore.hardResetGraph();
		const componentId = graphStore.addNode('component', { x: 10, y: 10 });
		const llmId = graphStore.addNode('llm', { x: 280, y: 20 });
		const configRes = graphStore.updateNodeConfig(componentId, {
			params: {
				componentRef: { componentId: 'cmp_local', revisionId: 'crev_local', apiVersion: 'v1' },
				api: {
					inputs: [],
					outputs: [
						{ name: 'out_text', portType: 'text', required: true, typedSchema: { type: 'text', fields: [] } },
						{ name: 'out_json', portType: 'json', required: true, typedSchema: { type: 'json', fields: [] } }
					]
				},
				bindings: {
					inputs: {},
					config: {},
					outputs: {
						out_text: { nodeId: 'n1', artifact: 'current' },
						out_json: { nodeId: 'n2', artifact: 'current' }
					}
				},
				config: {}
			},
			ports: { in: null, out: 'json' }
		});
		expect(configRes.ok).toBe(true);

		const state = get(graphStore);
		const staleEdge = {
			id: 'e_loaded_stale',
			source: componentId,
			sourceHandle: 'out_text',
			target: llmId,
			targetHandle: 'in',
			data: {
				exec: 'idle',
				contract: {
					out: 'text',
					in: 'text',
					payload: {
						source: { type: 'json' },
						target: { type: 'string' }
					}
				}
			}
		} as any;

		const loaded = graphStore.loadGraphDocument(
			{ nodes: state.nodes as any, edges: [staleEdge] as any },
			null
		);
		expect((loaded as any)?.ok).toBe(true);

		const after = get(graphStore);
		const edge = after.edges.find((e) => e.id === 'e_loaded_stale');
		expect(edge).toBeTruthy();
		expect((edge as any)?.sourceHandle).toBe('out_text');
		expect((edge as any)?.data?.contract?.payload?.source?.type).toBe('string');
		expect((edge as any)?.data?.contract?.out).toBe('text');
	});

	it('recomputes component edge contract payload source from sourceHandle on save', async () => {
		graphStore.hardResetGraph();
		const componentId = graphStore.addNode('component', { x: 10, y: 10 });
		const llmId = graphStore.addNode('llm', { x: 280, y: 20 });
		const configRes = graphStore.updateNodeConfig(componentId, {
			params: {
				componentRef: { componentId: 'cmp_local', revisionId: 'crev_local', apiVersion: 'v1' },
				api: {
					inputs: [],
					outputs: [
						{ name: 'out_text', portType: 'text', required: true, typedSchema: { type: 'text', fields: [] } },
						{ name: 'out_json', portType: 'json', required: true, typedSchema: { type: 'json', fields: [] } }
					]
				},
				bindings: {
					inputs: {},
					config: {},
					outputs: {
						out_text: { nodeId: 'n1', artifact: 'current' },
						out_json: { nodeId: 'n2', artifact: 'current' }
					}
				},
				config: {}
			},
			ports: { in: null, out: 'json' }
		});
		expect(configRes.ok).toBe(true);

		const addRes = graphStore.addEdge({
			id: 'e_to_save',
			source: componentId,
			sourceHandle: 'out_text',
			target: llmId,
			targetHandle: 'in',
			data: {
				exec: 'idle',
				contract: {
					out: 'text',
					in: 'text',
					payload: {
						source: { type: 'json' },
						target: { type: 'string' }
					}
				}
			}
		} as any);
		expect(addRes.ok).toBe(true);

		let postedGraph: any = null;
		const originalFetch = globalThis.fetch;
		(globalThis as any).fetch = async (input: RequestInfo | URL, init?: RequestInit) => {
			const url = String(input);
			const method = String(init?.method ?? 'GET').toUpperCase();
			if (url.includes('/api/graphs') && method === 'POST') {
				postedGraph = JSON.parse(String(init?.body ?? '{}'))?.graph ?? null;
				return new Response(
					JSON.stringify({
						schemaVersion: 1,
						graphId: 'graph_saved',
						revisionId: 'rev_saved',
						graphName: null,
						versionName: null,
						createdAt: '2026-03-08T00:00:00Z'
					}),
					{ status: 200 }
				);
			}
			return new Response('{}', { status: 200 });
		};

		try {
			const result = await graphStore.saveGraph('save');
			expect((result as any)?.ok).toBe(true);
			expect(postedGraph).toBeTruthy();
			const savedEdge = (postedGraph?.edges ?? []).find((e: any) => String(e?.id ?? '') === 'e_to_save');
			expect(savedEdge).toBeTruthy();
			expect(String(savedEdge?.sourceHandle ?? '')).toBe('out_text');
			expect(savedEdge?.data?.contract?.payload?.source?.type).toBe('string');
			expect(savedEdge?.data?.contract?.out).toBe('text');
		} finally {
			(globalThis as any).fetch = originalFetch;
		}
	});

	it('rejects loading graph with ambiguous sourceHandle for multi-output component edge', () => {
		graphStore.hardResetGraph();
		const componentId = graphStore.addNode('component', { x: 10, y: 10 });
		const llmId = graphStore.addNode('llm', { x: 280, y: 20 });
		const configRes = graphStore.updateNodeConfig(componentId, {
			params: {
				componentRef: { componentId: 'cmp_local', revisionId: 'crev_local', apiVersion: 'v1' },
				api: {
					inputs: [],
					outputs: [
						{ name: 'out_text', portType: 'text', required: true, typedSchema: { type: 'text', fields: [] } },
						{ name: 'out_json', portType: 'json', required: true, typedSchema: { type: 'json', fields: [] } }
					]
				},
				bindings: {
					inputs: {},
					config: {},
					outputs: {
						out_text: { nodeId: 'n1', artifact: 'current' },
						out_json: { nodeId: 'n2', artifact: 'current' }
					}
				},
				config: {}
			},
			ports: { in: null, out: 'json' }
		});
		expect(configRes.ok).toBe(true);
		const state = get(graphStore);
		const loaded = graphStore.loadGraphDocument(
			{
				nodes: state.nodes as any,
				edges: [
					{
						id: 'e_ambiguous_on_load',
						source: componentId,
						sourceHandle: 'out',
						target: llmId,
						targetHandle: 'in',
						data: { exec: 'idle' }
					}
				] as any
			},
			null
		);
		expect((loaded as any)?.ok).toBe(false);
	});

	it('blocks save when graph contains ambiguous sourceHandle for multi-output component edge', async () => {
		graphStore.hardResetGraph();
		const componentId = graphStore.addNode('component', { x: 10, y: 10 });
		const llmId = graphStore.addNode('llm', { x: 280, y: 20 });
		const configRes = graphStore.updateNodeConfig(componentId, {
			params: {
				componentRef: { componentId: 'cmp_local', revisionId: 'crev_local', apiVersion: 'v1' },
				api: {
					inputs: [],
					outputs: [
						{ name: 'out_text', portType: 'text', required: true, typedSchema: { type: 'text', fields: [] } },
						{ name: 'out_json', portType: 'json', required: true, typedSchema: { type: 'json', fields: [] } }
					]
				},
				bindings: {
					inputs: {},
					config: {},
					outputs: {
						out_text: { nodeId: 'n1', artifact: 'current' },
						out_json: { nodeId: 'n2', artifact: 'current' }
					}
				},
				config: {}
			},
			ports: { in: null, out: 'json' }
		});
		expect(configRes.ok).toBe(true);

		graphStore.syncFromCanvas(get(graphStore).nodes as any, [
			{
				id: 'e_invalid_save',
				source: componentId,
				sourceHandle: 'out',
				target: llmId,
				targetHandle: 'in',
				data: { exec: 'idle' }
			}
		] as any);

		const originalFetch = globalThis.fetch;
		let postCalled = false;
		(globalThis as any).fetch = async (input: RequestInfo | URL) => {
			const url = String(input);
			if (url.includes('/api/graphs')) postCalled = true;
			return new Response('{}', { status: 200 });
		};

		try {
			const result = await graphStore.saveGraph('save');
			expect((result as any)?.ok).toBe(false);
			expect(String((result as any)?.reason ?? '')).toBe('invalid_graph');
			expect(postCalled).toBe(false);
		} finally {
			(globalThis as any).fetch = originalFetch;
		}
	});
});
