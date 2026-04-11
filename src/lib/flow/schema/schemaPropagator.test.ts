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
