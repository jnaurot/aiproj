import { describe, expect, it } from 'vitest';
import { NodeDocOverrideSchema, NodeDocV1Schema } from './nodeDocs';

describe('node docs schema', () => {
	it('accepts valid node doc payload', () => {
		const parsed = NodeDocV1Schema.parse({
			schema_version: 1,
			node_kind: 'model',
			subtype: 'ollama',
			title: 'Model node',
			summary: 'Executes model inference.',
			planes: {
				data: { title: 'Data', summary: 'Consumes and emits work-plane payloads.' },
				control: { title: 'Control', summary: 'Reflects scheduler state and blocked reasons.' },
				param: { title: 'Param', summary: 'Uses params for prompts and provider settings.' }
			}
		});
		expect(parsed.node_kind).toBe('model');
		expect(parsed.planes.data.summary.length).toBeGreaterThan(0);
	});

	it('rejects missing required plane sections', () => {
		expect(() =>
			NodeDocV1Schema.parse({
				schema_version: 1,
				node_kind: 'source',
				title: 'Source node',
				summary: 'Loads external data.',
				planes: {
					data: { title: 'Data', summary: 'Reads source payload.' },
					control: { title: 'Control', summary: 'Single run mode.' }
				}
			})
		).toThrow();
	});

	it('validates override schema', () => {
		const parsed = NodeDocOverrideSchema.parse({
			summary: 'Custom summary',
			notes: ['Use this for review only.'],
			disabled: false
		});
		expect(parsed.disabled).toBe(false);
	});
});

