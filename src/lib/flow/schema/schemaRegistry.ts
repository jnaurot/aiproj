import type { SchemaFunction, SchemaPlaneOutput } from '$lib/flow/types/schemaPlane';
import { schemaFn_source } from './schemaFunctions/source';
import { schemaFn_transform } from './schemaFunctions/transform';
import { schemaFn_audio_augment, schemaFn_audio_fallback, schemaFn_audio_source, schemaFn_spectrogram } from './schemaFunctions/audio';
import { schemaFn_evaluation, schemaFn_ml_fallback, schemaFn_training_job } from './schemaFunctions/ml';

const registry = new Map<string, SchemaFunction>();

export const OPAQUE_SCHEMA: SchemaPlaneOutput = {
	mode: 'opaque',
	columns: []
};

export const UNKNOWN_SCHEMA: SchemaPlaneOutput = {
	mode: 'opaque',
	columns: []
};

export function registerSchemaFunction(kind: string, fn: SchemaFunction): void {
	registry.set(String(kind), fn);
}

export function getSchemaFunction(kind: string): SchemaFunction | undefined {
	return registry.get(String(kind));
}

export function clearSchemaFunctionRegistryForTest(): void {
	registry.clear();
}

export function registerAllBuiltinSchemaFunctions(): void {
	// Registration is idempotent.
	if (registry.size > 0) return;
	registerSchemaFunction('source', schemaFn_source);
	registerSchemaFunction('transform', schemaFn_transform);
	registerSchemaFunction('tool', () => ({ ok: true, output: OPAQUE_SCHEMA }));
	registerSchemaFunction('component', () => ({ ok: true, output: OPAQUE_SCHEMA }));
	registerSchemaFunction('llm', () => ({ ok: true, output: OPAQUE_SCHEMA }));
	registerSchemaFunction('model', schemaFn_ml_fallback);
	registerSchemaFunction('audio_source', schemaFn_audio_source);
	registerSchemaFunction('spectrogram', schemaFn_spectrogram);
	registerSchemaFunction('audio_augment', schemaFn_audio_augment);
	registerSchemaFunction('audio', schemaFn_audio_fallback);
	registerSchemaFunction('training_job', schemaFn_training_job);
	registerSchemaFunction('evaluation', schemaFn_evaluation);
}
