import { describe, expect, it } from 'vitest';

import { applyDerivedOutputPortTypes, deriveOutputPortType, syncOutputBindings, validateComponentOutputs } from './validation';

describe('ComponentEditor validation', () => {
	it('flags duplicate/reserved output names and missing required bindings', () => {
		const outputs = [
			{
				name: 'out_data',
				portType: 'json',
				required: true,
				typedSchema: { type: 'json', fields: [{ name: 'text', type: 'text' }] }
			},
			{
				name: 'out_data',
				portType: 'json',
				required: true,
				typedSchema: { type: 'json', fields: [{ name: 'text', type: 'text' }] }
			},
			{
				name: 'outputs',
				portType: 'text',
				required: true,
				typedSchema: { type: 'text', fields: [] }
			}
		];
		const result = validateComponentOutputs(outputs as any, {});
		expect(result.hasErrors).toBe(true);
		expect(result.outputErrors[1]?.join(' ')).toContain('duplicates');
		expect(result.outputErrors[2]?.join(' ')).toContain('reserved');
		expect(result.bindingErrors.out_data?.[0]).toContain('nodeId is required');
	});

	it('allows structured outputs without explicit fields', () => {
		const outputs = [
			{
				name: 'summary',
				portType: 'json',
				required: true,
				typedSchema: { type: 'json', fields: [] }
			}
		];
		const result = validateComponentOutputs(outputs as any, {
			summary: { nodeId: 'n1', artifact: 'current' }
		});
		expect(result.hasErrors).toBe(false);
	});

	it('enforces output name pattern and typedSchema.type alignment', () => {
		const outputs = [
			{
				name: 'out-data',
				portType: 'json',
				required: true,
				typedSchema: { type: 'text', fields: [] }
			}
		];
		const result = validateComponentOutputs(outputs as any, {
			'out-data': { nodeId: 'n1', artifact: 'current' }
		});
		expect(result.hasErrors).toBe(true);
		expect(result.outputErrors[0]?.join(' ')).toContain('must match [A-Za-z_][A-Za-z0-9_]*');
		expect(result.outputErrors[0]?.join(' ')).toContain('typedSchema.type must match portType');
	});

	it('requires bindings for declared outputs even when not required', () => {
		const outputs = [
			{
				name: 'optional_out',
				portType: 'text',
				required: false,
				typedSchema: { type: 'text', fields: [] }
			}
		];
		const result = validateComponentOutputs(outputs as any, {});
		expect(result.hasErrors).toBe(true);
		expect(result.bindingErrors.optional_out?.[0]).toContain('nodeId is required');
	});

	it('flags output bindings that target a missing internal node id', () => {
		const outputs = [
			{
				name: 'summary',
				portType: 'text',
				required: true,
				typedSchema: { type: 'text', fields: [] }
			}
		];
		const result = validateComponentOutputs(
			outputs as any,
			{
				summary: { nodeId: 'n_missing', artifact: 'current' }
			},
			{ internalNodeIds: ['n_present'] }
		);
		expect(result.hasErrors).toBe(true);
		expect(result.bindingErrors.summary?.join(' ')).toContain('must reference an internal node');
	});

	it('syncs outputs and auto-fills defaults', () => {
		const outputs = [
			{ name: 'out_data', portType: 'text', required: true, typedSchema: { type: 'text', fields: [] } },
			{ name: 'out_2', portType: 'json', required: true, typedSchema: { type: 'json', fields: [{ name: 'x', type: 'text' }] } }
		];
		const current = {
			out_data: { nodeId: 'n_old', artifact: 'last' as const },
			unused: { nodeId: 'n_unused', artifact: 'current' as const }
		};
		const synced = syncOutputBindings(outputs as any, current, ['n_default']);
		expect(synced.changed).toBe(true);
		expect(Object.keys(synced.next)).toEqual(['out_data', 'out_2']);
		expect(synced.next.out_data).toEqual({ nodeId: 'n_old', artifact: 'last' });
		expect(synced.next.out_2).toEqual({ nodeId: 'n_default', artifact: 'current' });
	});

	it('reuses an existing nodeId fallback when internal node list is empty', () => {
		const outputs = [
			{ name: 'out_data', portType: 'text', required: true, typedSchema: { type: 'text', fields: [] } },
			{ name: 'out_2', portType: 'json', required: true, typedSchema: { type: 'json', fields: [{ name: 'x', type: 'text' }] } }
		];
		const current = {
			out_data: { nodeId: 'n_kept', artifact: 'current' as const },
			out_2: { artifact: 'current' as const }
		};
		const synced = syncOutputBindings(outputs as any, current, []);
		expect(synced.next.out_data).toEqual({ nodeId: 'n_kept', artifact: 'current' });
		expect(synced.next.out_2).toEqual({ nodeId: 'n_kept', artifact: 'current' });
	});

	it('derives output port type from bound internal node metadata', () => {
		const output = {
			name: 'out_data',
			portType: 'json',
			required: true,
			typedSchema: { type: 'json', fields: [] }
		};
		const bindings = {
			out_data: { nodeId: 'n_llm', artifact: 'current' as const }
		};
		const meta = {
			n_llm: { outPortType: 'text' as const }
		};
		expect(deriveOutputPortType(output as any, bindings, meta)).toBe('text');
	});

	it('applies derived output port types and keeps typedSchema.type aligned', () => {
		const outputs = [
			{
				name: 'out_1',
				portType: 'json',
				required: true,
				typedSchema: { type: 'json', fields: [] }
			},
			{
				name: 'out_2',
				portType: 'text',
				required: true,
				typedSchema: { type: 'text', fields: [] }
			}
		];
		const bindings = {
			out_1: { nodeId: 'n_source', artifact: 'current' as const },
			out_2: { nodeId: 'n_llm', artifact: 'last' as const }
		};
		const meta = {
			n_source: { outPortType: 'table' as const },
			n_llm: { outPortType: 'text' as const }
		};
		const next = applyDerivedOutputPortTypes(outputs as any, bindings, meta);
		expect(next[0].portType).toBe('table');
		expect(next[0].typedSchema?.type).toBe('table');
		expect(next[1].portType).toBe('text');
		expect(next[1].typedSchema?.type).toBe('text');
	});
});
