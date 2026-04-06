import { describe, expect, it } from 'vitest';
import {
	NodeDocExplanationModeSchema,
	NodeDocGeneratedExplanationSchema,
	NodeDocOverrideSchema,
	NodeDocV1Schema,
	parseNodeDocGeneratedExplanation,
	sanitizeNodeDocGeneratedExplanation
} from './nodeDocs';

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

	it('accepts valid generated explanation payload', () => {
		const parsed = NodeDocGeneratedExplanationSchema.parse({
			summary: 'This node explains transformed output.',
			settings_explained: ['temperature=0', 'strict json on'],
			context_notes: ['runtime blocked=none'],
			generated_at: '2026-04-05T21:00:00.000Z',
			signature_key: 'node_1::abc123',
			provider_meta: { provider: 'ollama', model: 'glm-4.7-flash:latest' }
		});
		expect(parsed.settings_explained.length).toBe(2);
	});

	it('rejects unknown explanation mode values', () => {
		expect(NodeDocExplanationModeSchema.safeParse('default').success).toBe(true);
		expect(NodeDocExplanationModeSchema.safeParse('llm').success).toBe(true);
		expect(NodeDocExplanationModeSchema.safeParse('hybrid').success).toBe(false);
	});

	it('rejects generated explanation payload with disallowed mutation fields', () => {
		const parsed = parseNodeDocGeneratedExplanation({
			summary: 'x',
			settings_explained: [],
			context_notes: [],
			generated_at: '2026-04-05T21:00:00.000Z',
			signature_key: 'sig',
			runtime: { runStatus: 'running' }
		});
		expect(parsed.value).toBeNull();
		expect(parsed.reason).toBe('disallowed_fields');
	});

	it('sanitizes malformed generated explanation payload to null', () => {
		const sanitized = sanitizeNodeDocGeneratedExplanation({
			summary: '',
			settings_explained: [],
			context_notes: [],
			generated_at: '2026-04-05T21:00:00.000Z',
			signature_key: 'sig'
		});
		expect(sanitized).toBeNull();
	});
});
