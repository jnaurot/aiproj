import type { SchemaFunction } from '$lib/flow/types/schemaPlane';
import { OPAQUE_SCHEMA } from '$lib/flow/schema/schemaRegistry';

export const schemaFn_audio_source: SchemaFunction = (_inputs, params) => {
	const sampleRate = Number((params as any)?.sample_rate ?? (params as any)?.sampleRate ?? 44100);
	return {
		ok: true,
		output: {
			mode: 'tensor',
			columns: [],
			shape: ['B', 'samples'],
			dtype: 'float32',
			properties: {
				sample_rate: Number.isFinite(sampleRate) ? sampleRate : 44100,
				cardinality: 'many'
			}
		}
	};
};

export const schemaFn_spectrogram: SchemaFunction = (inputs, params) => {
	const input = inputs[0];
	if (!input) {
		return {
			ok: false,
			error: {
				code: 'MISSING_REQUIRED_INPUT',
				message: 'Spectrogram requires tensor input',
				handles: ['in']
			}
		};
	}
	if (input.mode !== 'tensor') {
		return {
			ok: false,
			error: {
				code: 'TYPE_MISMATCH',
				message: 'Spectrogram input must be tensor',
				handles: ['in']
			}
		};
	}
	const nMels = Number((params as any)?.n_mels ?? (params as any)?.nMels ?? 128);
	return {
		ok: true,
		output: {
			mode: 'tensor',
			columns: [],
			shape: ['B', 'T', Number.isFinite(nMels) && nMels > 0 ? nMels : 128],
			dtype: 'float32',
			properties: {
				non_negative: true,
				sample_rate: input.properties?.sample_rate
			}
		}
	};
};

export const schemaFn_audio_augment: SchemaFunction = (inputs) => {
	const input = inputs[0];
	if (!input) {
		return {
			ok: false,
			error: { code: 'MISSING_REQUIRED_INPUT', message: 'Audio augment requires input', handles: ['in'] }
		};
	}
	if (input.mode !== 'tensor') {
		return {
			ok: false,
			error: { code: 'TYPE_MISMATCH', message: 'Audio augment input must be tensor', handles: ['in'] }
		};
	}
	return { ok: true, output: input };
};

export const schemaFn_audio_fallback: SchemaFunction = () => ({ ok: true, output: OPAQUE_SCHEMA });

