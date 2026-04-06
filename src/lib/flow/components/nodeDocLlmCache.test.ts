import { afterEach, describe, expect, it, vi } from 'vitest';
import {
	clearNodeDocLlmCache,
	getNodeDocLlmCacheEntry,
	getOrGenerateNodeDocLlmExplanation
} from './nodeDocLlmCache';
import type { NodeDocLlmContext } from './nodeDocLlmContext';
import * as service from './nodeDocLlmService';

const context: NodeDocLlmContext = {
	node_id: 'n_1',
	node_label: 'Model_ScoreJob',
	node_kind: 'model',
	node_subtype: 'ollama',
	settings: { model: 'glm-4.7-flash:latest' },
	planes: { data_inputs: ['in'], data_outputs: ['out'], param_inputs: [], control_inputs: [] },
	runtime: { pending_input_count: 0, inflight: 0, ready_work: false, blocked_reason_code: '' }
};

describe('nodeDocLlmCache', () => {
	afterEach(() => {
		clearNodeDocLlmCache();
		vi.restoreAllMocks();
	});

	it('returns cache hit on repeated signature', async () => {
		const spy = vi.spyOn(service, 'generateNodeDocLlmExplanation').mockResolvedValue({
			explanation: {
				summary: 'AI explanation',
				settings_explained: ['model=glm-4.7-flash:latest'],
				context_notes: [],
				generated_at: '2026-04-05T23:10:00.000Z',
				signature_key: 'sig-cache'
			},
			telemetry: { status: 'success', latencyMs: 21 }
		});
		const first = await getOrGenerateNodeDocLlmExplanation('llm', 'n_1', context, 'sig-cache');
		const second = await getOrGenerateNodeDocLlmExplanation('llm', 'n_1', context, 'sig-cache');
		expect(first.explanation?.summary).toContain('AI');
		expect(second.telemetry.cacheHit).toBe(true);
		expect(spy).toHaveBeenCalledTimes(1);
	});

	it('invalidates on signature change', async () => {
		const spy = vi.spyOn(service, 'generateNodeDocLlmExplanation').mockResolvedValue({
			explanation: {
				summary: 'AI explanation',
				settings_explained: [],
				context_notes: [],
				generated_at: '2026-04-05T23:10:00.000Z',
				signature_key: 'sig-a'
			},
			telemetry: { status: 'success', latencyMs: 11 }
		});
		await getOrGenerateNodeDocLlmExplanation('llm', 'n_1', context, 'sig-a');
		await getOrGenerateNodeDocLlmExplanation('llm', 'n_1', context, 'sig-b');
		expect(spy).toHaveBeenCalledTimes(2);
		expect(getNodeDocLlmCacheEntry('llm', 'n_1', 'sig-a')).not.toBeNull();
		expect(getNodeDocLlmCacheEntry('llm', 'n_1', 'sig-b')).not.toBeNull();
	});
});

