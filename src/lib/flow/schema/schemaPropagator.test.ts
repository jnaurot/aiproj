import { describe, expect, it, beforeEach } from 'vitest';
import type { Edge, Node } from '@xyflow/svelte';
import type { PipelineEdgeData, PipelineNodeData } from '$lib/flow/types';
import { propagateSchemas } from './schemaPropagator';
import {
	OPAQUE_SCHEMA,
	UNKNOWN_SCHEMA,
	clearSchemaFunctionRegistryForTest,
	registerSchemaFunction
} from './schemaRegistry';

type PNode = Node<PipelineNodeData & Record<string, unknown>>;
type PEdge = Edge<PipelineEdgeData & Record<string, unknown>>;

function node(id: string, kind: string, params: Record<string, unknown> = {}): PNode {
	return {
		id,
		position: { x: 0, y: 0 },
		data: { kind, label: id, status: 'idle', params } as any
	} as PNode;
}

function edge(id: string, source: string, target: string, targetHandle = 'in'): PEdge {
	return { id, source, target, targetHandle, data: { exec: 'idle' } as any } as PEdge;
}

describe('schemaPropagator', () => {
	beforeEach(() => {
		clearSchemaFunctionRegistryForTest();
	});

	it('returns empty state for empty graph', () => {
		expect(propagateSchemas([], [])).toEqual({ nodeSchemas: {}, edgeSchemas: {} });
	});

	it('single source with registered function uses its output', () => {
		registerSchemaFunction('source', () => ({ ok: true, output: { mode: 'table', columns: [] } }));
		const state = propagateSchemas([node('a', 'source')], []);
		expect(state.nodeSchemas.a?.ok).toBe(true);
		if (state.nodeSchemas.a?.ok) expect(state.nodeSchemas.a.output.mode).toBe('table');
	});

	it('single source with no registered function defaults to opaque', () => {
		const state = propagateSchemas([node('a', 'missing')], []);
		expect(state.nodeSchemas.a?.ok).toBe(true);
		if (state.nodeSchemas.a?.ok) expect(state.nodeSchemas.a.output).toEqual(OPAQUE_SCHEMA);
	});

	it('A -> B passes upstream schema to B', () => {
		registerSchemaFunction('source', () => ({ ok: true, output: { mode: 'table', columns: [{ name: 'x', type: 'number', nullable: false, properties: {} }] } }));
		registerSchemaFunction('transform', (inputs) => {
			expect(inputs[0]?.columns?.[0]?.name).toBe('x');
			return { ok: true, output: inputs[0] ?? UNKNOWN_SCHEMA };
		});
		const state = propagateSchemas([node('a', 'source'), node('b', 'transform')], [edge('e1', 'a', 'b')]);
		expect(state.edgeSchemas.e1.columns[0]?.name).toBe('x');
	});

	it('A -> B -> C passes derived schema through chain', () => {
		registerSchemaFunction('source', () => ({ ok: true, output: { mode: 'table', columns: [{ name: 'x', type: 'number', nullable: false, properties: {} }] } }));
		registerSchemaFunction('b', () => ({ ok: true, output: { mode: 'table', columns: [{ name: 'y', type: 'number', nullable: false, properties: {} }] } }));
		registerSchemaFunction('c', (inputs) => ({ ok: true, output: inputs[0] ?? OPAQUE_SCHEMA }));
		const state = propagateSchemas(
			[node('a', 'source'), node('b', 'b'), node('c', 'c')],
			[edge('e1', 'a', 'b'), edge('e2', 'b', 'c')]
		);
		if (state.nodeSchemas.c?.ok) expect(state.nodeSchemas.c.output.columns[0]?.name).toBe('y');
	});

	it('cycle nodes get CYCLE_DETECTED and downstream gets opaque', () => {
		registerSchemaFunction('pass', (inputs) => ({ ok: true, output: inputs[0] ?? OPAQUE_SCHEMA }));
		const state = propagateSchemas(
			[node('a', 'pass'), node('b', 'pass'), node('c', 'pass')],
			[edge('e1', 'a', 'b'), edge('e2', 'b', 'a'), edge('e3', 'b', 'c')]
		);
		expect(state.nodeSchemas.a?.ok).toBe(false);
		expect(state.nodeSchemas.b?.ok).toBe(false);
		if (!state.nodeSchemas.a?.ok) expect(state.nodeSchemas.a.error.code).toBe('CYCLE_DETECTED');
		if (state.nodeSchemas.c?.ok) expect(state.nodeSchemas.c.output.mode).toBe('opaque');
	});

	it('diamond graph provides both inputs to downstream', () => {
		registerSchemaFunction('srcA', () => ({ ok: true, output: { mode: 'table', columns: [{ name: 'a', type: 'number', nullable: false, properties: {} }] } }));
		registerSchemaFunction('srcB', () => ({ ok: true, output: { mode: 'table', columns: [{ name: 'b', type: 'number', nullable: false, properties: {} }] } }));
		registerSchemaFunction('d', (inputs) => ({
			ok: true,
			output: {
				mode: 'table',
				columns: [...(inputs[0]?.columns ?? []), ...(inputs[1]?.columns ?? [])]
			}
		}));
		const state = propagateSchemas(
			[node('a', 'srcA'), node('b', 'srcB'), node('d', 'd')],
			[edge('e1', 'a', 'd', 'left'), edge('e2', 'b', 'd', 'right')]
		);
		if (state.nodeSchemas.d?.ok) expect(state.nodeSchemas.d.output.columns.map((c) => c.name)).toEqual(['a', 'b']);
	});

	it('join node receives all multi-in edges on same handle with source refs', () => {
		registerSchemaFunction('srcA', () => ({
			ok: true,
			output: { mode: 'table', columns: [{ name: 'id', type: 'number', nullable: false, properties: {} }] }
		}));
		registerSchemaFunction('srcB', () => ({
			ok: true,
			output: { mode: 'table', columns: [{ name: 'id', type: 'number', nullable: false, properties: {} }] }
		}));
		registerSchemaFunction('transform', (inputs, params) => {
			expect(inputs).toHaveLength(2);
			const refs = Array.isArray((params as any)?.__schemaInputRefs) ? ((params as any).__schemaInputRefs as any[]) : [];
			expect(refs.map((item) => String(item?.sourceNodeId ?? ''))).toEqual(['a', 'b']);
			return { ok: true, output: { mode: 'table', columns: [] } };
		});
		const join = node('jn', 'transform', {
			op: 'join',
			join: { clauses: [{ leftNodeId: 'a', leftCol: 'id', rightNodeId: 'b', rightCol: 'id', how: 'inner' }] }
		});
		const state = propagateSchemas(
			[node('a', 'srcA'), node('b', 'srcB'), join],
			[edge('e1', 'a', 'jn', 'in'), edge('e2', 'b', 'jn', 'in')]
		);
		expect(state.nodeSchemas.jn?.ok).toBe(true);
	});

	it('sibling branch unaffected by schema error on other branch', () => {
		registerSchemaFunction('src', () => ({ ok: true, output: { mode: 'table', columns: [] } }));
		registerSchemaFunction('bad', () => ({
			ok: false,
			error: { code: 'TYPE_MISMATCH', message: 'bad', handles: ['in'] },
			output: OPAQUE_SCHEMA
		}));
		registerSchemaFunction('ok', () => ({ ok: true, output: { mode: 'text', columns: [] } }));
		const state = propagateSchemas(
			[node('s', 'src'), node('b', 'bad'), node('o', 'ok')],
			[edge('e1', 's', 'b'), edge('e2', 's', 'o')]
		);
		expect(state.nodeSchemas.b?.ok).toBe(false);
		expect(state.nodeSchemas.o?.ok).toBe(true);
	});

	it('edge from error node carries opaque schema', () => {
		registerSchemaFunction('src', () => ({ ok: false, error: { code: 'TYPE_MISMATCH', message: 'x', handles: [] }, output: OPAQUE_SCHEMA }));
		const state = propagateSchemas([node('a', 'src'), node('b', 'missing')], [edge('e1', 'a', 'b')]);
		expect(state.edgeSchemas.e1.mode).toBe('opaque');
	});

	it('disconnected declared input handle receives UNKNOWN_SCHEMA', () => {
		registerSchemaFunction('needTwo', (inputs) => {
			expect(inputs[0]).toEqual(UNKNOWN_SCHEMA);
			return { ok: true, output: OPAQUE_SCHEMA };
		});
		const n = node('a', 'needTwo');
		(n.data as any).portDeclarations = { in: { in: {}, aux: {} } };
		propagateSchemas([n], []);
	});

	it('component node derives output from internal terminal node when resolver is provided', () => {
		registerSchemaFunction('source', () => ({
			ok: true,
			output: { mode: 'table', columns: [{ name: 'text', type: 'string', nullable: false, properties: {} }] }
		}));
		registerSchemaFunction('transform', (inputs) => ({
			ok: true,
			output: {
				mode: 'table',
				columns: [...(inputs[0]?.columns ?? []), { name: 'score', type: 'number', nullable: false, properties: {} }]
			}
		}));
		const state = propagateSchemas([node('cmp1', 'component')], [], {
			resolveComponentGraph: () => ({
				nodes: [node('inner_source', 'source'), node('inner_transform', 'transform')],
				edges: [edge('inner_e1', 'inner_source', 'inner_transform')]
			})
		});
		expect(state.nodeSchemas.cmp1?.ok).toBe(true);
		if (state.nodeSchemas.cmp1?.ok) {
			expect(state.nodeSchemas.cmp1.output.columns.map((column) => column.name)).toEqual(['text', 'score']);
		}
	});

	// ── Declared output override tests ──────────────────────────────────────

	it('declared output override is used instead of schema function result', () => {
		registerSchemaFunction('transform', () => ({ ok: true, output: OPAQUE_SCHEMA }));
		const n = node('a', 'transform');
		(n.data as any).schema = {
			expectedSchema: {
				source: 'declared',
				state: 'fresh',
				typedSchema: {
					type: 'table',
					fields: [{ name: 'score', type: 'number', nullable: false }]
				}
			}
		};
		const state = propagateSchemas([n], []);
		expect(state.nodeSchemas.a?.ok).toBe(true);
		if (state.nodeSchemas.a?.ok) {
			expect(state.nodeSchemas.a.output.mode).toBe('table');
			expect(state.nodeSchemas.a.output.columns[0]?.name).toBe('score');
			expect(state.nodeSchemas.a.output.columns[0]?.type).toBe('number');
		}
	});

	it('declared override unblocks downstream when upstream is opaque', () => {
		// Mirrors the exact user scenario: source → Transform_Select (opaque input,
		// declared output) → Model (should see declared columns, not opaque).
		registerSchemaFunction('source', () => ({ ok: true, output: OPAQUE_SCHEMA }));
		registerSchemaFunction('transform', (inputs) => {
			// Schema function would normally return opaque because inputs[0] is opaque.
			if (inputs[0]?.mode === 'opaque') return { ok: true, output: OPAQUE_SCHEMA };
			return { ok: true, output: inputs[0] ?? OPAQUE_SCHEMA };
		});
		registerSchemaFunction('model', (inputs) => ({ ok: true, output: inputs[0] ?? OPAQUE_SCHEMA }));

		const transformNode = node('t', 'transform');
		(transformNode.data as any).schema = {
			expectedSchema: {
				source: 'declared',
				state: 'fresh',
				typedSchema: {
					type: 'table',
					fields: [{ name: 'candidate_required_location', type: 'text', nullable: true }]
				}
			}
		};
		const state = propagateSchemas(
			[node('src', 'source'), transformNode, node('m', 'model')],
			[edge('e1', 'src', 't'), edge('e2', 't', 'm')]
		);
		// The edge from transform carries the declared columns, not opaque.
		expect(state.edgeSchemas.e2?.mode).toBe('table');
		expect(state.edgeSchemas.e2?.columns[0]?.name).toBe('candidate_required_location');
		expect(state.edgeSchemas.e2?.columns[0]?.type).toBe('string');
		// Downstream model node receives declared schema.
		if (state.nodeSchemas.m?.ok) {
			expect(state.nodeSchemas.m.output.mode).toBe('table');
			expect(state.nodeSchemas.m.output.columns[0]?.name).toBe('candidate_required_location');
		}
	});

	it('non-declared source on expectedSchema does not override schema function', () => {
		registerSchemaFunction('transform', () => ({ ok: true, output: { mode: 'text', columns: [] } }));
		const n = node('a', 'transform');
		(n.data as any).schema = {
			expectedSchema: {
				source: 'sample',  // not 'declared' — should be ignored
				state: 'fresh',
				typedSchema: { type: 'table', fields: [{ name: 'x', type: 'number', nullable: false }] }
			}
		};
		const state = propagateSchemas([n], []);
		if (state.nodeSchemas.a?.ok) {
			// Schema function result (text) wins over the non-declared observation.
			expect(state.nodeSchemas.a.output.mode).toBe('text');
		}
	});

	it('declared override with text mode produces text output', () => {
		registerSchemaFunction('transform', () => ({ ok: true, output: OPAQUE_SCHEMA }));
		const n = node('a', 'transform');
		(n.data as any).schema = {
			expectedSchema: {
				source: 'declared',
				state: 'fresh',
				typedSchema: { type: 'text', fields: [] }
			}
		};
		const state = propagateSchemas([n], []);
		if (state.nodeSchemas.a?.ok) {
			expect(state.nodeSchemas.a.output.mode).toBe('text');
		}
	});

	it('declared override nullable mapping is correct', () => {
		registerSchemaFunction('transform', () => ({ ok: true, output: OPAQUE_SCHEMA }));
		const n = node('a', 'transform');
		(n.data as any).schema = {
			expectedSchema: {
				source: 'declared',
				state: 'fresh',
				typedSchema: {
					type: 'table',
					fields: [
						{ name: 'required_col', type: 'text', nullable: false },
						{ name: 'optional_col', type: 'number', nullable: true }
					]
				}
			}
		};
		const state = propagateSchemas([n], []);
		if (state.nodeSchemas.a?.ok) {
			const cols = state.nodeSchemas.a.output.columns;
			expect(cols.find((c) => c.name === 'required_col')?.nullable).toBe(false);
			expect(cols.find((c) => c.name === 'optional_col')?.nullable).toBe(true);
		}
	});

	it('declared override on source node with no inputs works', () => {
		// Source nodes have no schema function registration in this test.
		const n = node('src', 'unregistered_source');
		(n.data as any).schema = {
			expectedSchema: {
				source: 'declared',
				state: 'fresh',
				typedSchema: { type: 'table', fields: [{ name: 'id', type: 'number', nullable: false }] }
			}
		};
		const state = propagateSchemas([n], []);
		if (state.nodeSchemas.src?.ok) {
			expect(state.nodeSchemas.src.output.mode).toBe('table');
			expect(state.nodeSchemas.src.output.columns[0]?.name).toBe('id');
		}
	});

	it('component node declared override is ignored (component path is separate)', () => {
		// Component nodes go through computeComponentNodeResult, not the declared
		// override path. This test asserts the component path is unchanged.
		registerSchemaFunction('inner', () => ({ ok: true, output: { mode: 'text', columns: [] } }));
		const cmp = node('cmp', 'component');
		(cmp.data as any).schema = {
			expectedSchema: {
				source: 'declared',
				state: 'fresh',
				typedSchema: { type: 'table', fields: [{ name: 'x', type: 'number', nullable: false }] }
			}
		};
		const state = propagateSchemas([cmp], [], {
			resolveComponentGraph: () => ({
				nodes: [node('i', 'inner')],
				edges: []
			})
		});
		// Component goes through its own resolver, not declared override.
		// Result comes from internal terminal node (text), not declared (table).
		if (state.nodeSchemas.cmp?.ok) {
			expect(state.nodeSchemas.cmp.output.mode).toBe('text');
		}
	});

	it('component node carries warning metadata when internal propagation has errors', () => {
		registerSchemaFunction('bad', () => ({
			ok: false,
			error: { code: 'TYPE_MISMATCH', message: 'bad internal contract', handles: ['in'] },
			output: OPAQUE_SCHEMA
		}));
		const state = propagateSchemas([node('cmp_err', 'component')], [], {
			resolveComponentGraph: () => ({
				nodes: [node('inner_bad', 'bad')],
				edges: []
			})
		});
		expect(state.nodeSchemas.cmp_err?.ok).toBe(true);
		if (state.nodeSchemas.cmp_err?.ok) {
			expect((state.nodeSchemas.cmp_err.output.properties as any)?.component_internal_errors).toBe(1);
			expect(String(state.nodeSchemas.cmp_err.output.note ?? '')).toContain('internal schema error');
		}
	});
});
