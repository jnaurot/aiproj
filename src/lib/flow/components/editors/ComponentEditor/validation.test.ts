import { describe, expect, it } from 'vitest';

import { syncOutputBindings, validateComponentOutputs } from './validation';

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

	it('enforces typed schema completeness for structured outputs', () => {
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
		expect(result.hasErrors).toBe(true);
		expect(result.outputErrors[0]?.join(' ')).toContain('at least one field');
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
});
