import { describe, expect, it } from 'vitest';
import { schemaFn_audio_augment, schemaFn_audio_source, schemaFn_spectrogram } from './audio';

describe('schemaFn_audio_source', () => {
	it('emits tensor schema with sample_rate property', () => {
		const result = schemaFn_audio_source([], { sample_rate: 22050 });
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.output.mode).toBe('tensor');
			expect(result.output.shape).toEqual(['B', 'samples']);
			expect(result.output.properties?.sample_rate).toBe(22050);
		}
	});
});

describe('schemaFn_spectrogram', () => {
	it('derives spectrogram shape from n_mels', () => {
		const result = schemaFn_spectrogram(
			[
				{
					mode: 'tensor',
					columns: [],
					shape: ['B', 'samples'],
					dtype: 'float32',
					properties: { sample_rate: 44100 }
				}
			],
			{ n_mels: 96 }
		);
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.output.shape).toEqual(['B', 'T', 96]);
			expect(result.output.properties?.non_negative).toBe(true);
		}
	});

	it('returns TYPE_MISMATCH for non-tensor input', () => {
		const result = schemaFn_spectrogram(
			[
				{
					mode: 'table',
					columns: [{ name: 'text', type: 'string', nullable: true, properties: {} }]
				}
			],
			{}
		);
		expect(result.ok).toBe(false);
		if (!result.ok) expect(result.error.code).toBe('TYPE_MISMATCH');
	});
});

describe('schemaFn_audio_augment', () => {
	it('passes tensor schema through unchanged', () => {
		const input = {
			mode: 'tensor' as const,
			columns: [],
			shape: ['B', 'T', 128] as Array<number | string>,
			dtype: 'float32' as const,
			properties: { sample_rate: 44100 }
		};
		const result = schemaFn_audio_augment([input], {});
		expect(result.ok).toBe(true);
		if (result.ok) expect(result.output).toEqual(input);
	});
});

