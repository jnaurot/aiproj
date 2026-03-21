import { describe, expect, it } from 'vitest';
import { buildModelPreviewDiff } from './modelPreview';

describe('modelPreview', () => {
	it('builds table->json preview diff with schema-derived output columns', () => {
		const diff = buildModelPreviewDiff({
			params: {
				output: {
					mode: 'json',
					jsonSchema: {
						type: 'object',
						properties: {
							label: { type: 'string' },
							score: { type: 'number' }
						}
					}
				}
			},
			inputSchemas: [
				{
					artifactId: 'a1',
					label: 'input',
					inputHandle: 'in',
					sourceNodeId: 'n1',
					schema: {
						kind: 'table',
						required_fields: [{ name: 'text', type: 'text' }]
					}
				} as any
			],
			sampleRows: [{ text: 'hello world' }]
		});
		expect(diff.inputType).toBe('table');
		expect(diff.outputType).toBe('json');
		expect(diff.inputColumns).toEqual(['text']);
		expect(diff.outputColumns).toEqual(['label', 'score']);
		expect(diff.sampleOutput).toEqual({ label: '<label>', score: '<score>' });
	});

	it('falls back gracefully when sample input is missing', () => {
		const diff = buildModelPreviewDiff({
			params: { output: { mode: 'text' } },
			inputSchemas: [],
			sampleRows: []
		});
		expect(diff.inputType).toBe('unknown');
		expect(diff.outputType).toBe('text');
		expect(diff.sampleInput).toBeNull();
		expect(diff.sampleOutput).toBe('text response');
		expect(diff.notes.length).toBeGreaterThan(0);
	});
});
