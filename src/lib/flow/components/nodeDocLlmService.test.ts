import { afterEach, describe, expect, it, vi } from 'vitest';
import { generateNodeDocLlmExplanation } from './nodeDocLlmService';
import type { NodeDocLlmContext } from './nodeDocLlmContext';

const context: NodeDocLlmContext = {
	node_id: 'n_1',
	node_label: 'Source_Jobs',
	node_kind: 'source',
	node_subtype: 'file',
	settings: { file_name: 'jobs.json' },
	planes: {
		data_inputs: [],
		data_outputs: ['out'],
		param_inputs: [],
		control_inputs: []
	},
	runtime: {
		pending_input_count: 0,
		inflight: 0,
		ready_work: false,
		blocked_reason_code: ''
	}
};

describe('nodeDocLlmService', () => {
	afterEach(() => {
		vi.restoreAllMocks();
	});

	it('returns validated explanation on success', async () => {
		vi.stubGlobal(
			'fetch',
			vi.fn(async () => ({
				ok: true,
				json: async () => ({
					summary: 'Generated summary',
					settings_explained: ['file_name=jobs.json'],
					context_notes: ['pending_input_count=0'],
					generated_at: '2026-04-05T23:00:00.000Z',
					signature_key: 'sig-1',
					provider_meta: { provider: 'ollama', model: 'glm-4.7-flash:latest' }
				})
			}))
		);
		const result = await generateNodeDocLlmExplanation(context, 'sig-1');
		expect(result.explanation?.summary).toContain('Generated');
		expect(result.telemetry.status).toBe('success');
	});

	it('falls back safely on malformed payload', async () => {
		vi.stubGlobal(
			'fetch',
			vi.fn(async () => ({
				ok: true,
				json: async () => ({ summary: '' })
			}))
		);
		const result = await generateNodeDocLlmExplanation(context, 'sig-2', { retries: 0 });
		expect(result.explanation).toBeNull();
		expect(result.telemetry.status).toBe('failed');
	});

	it('handles network failure with fallback reason', async () => {
		vi.stubGlobal(
			'fetch',
			vi.fn(async () => {
				throw new Error('network down');
			})
		);
		const result = await generateNodeDocLlmExplanation(context, 'sig-3', { retries: 0 });
		expect(result.explanation).toBeNull();
		expect(result.telemetry.fallbackReason).toBe('network');
	});
});

