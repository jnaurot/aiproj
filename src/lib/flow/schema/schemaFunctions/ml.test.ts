import { describe, expect, it } from 'vitest';
import { schemaFn_evaluation, schemaFn_training_job } from './ml';

describe('schemaFn_training_job', () => {
	it('returns model_artifact output for valid train/validation inputs', () => {
		const result = schemaFn_training_job(
			[
				{
					mode: 'tensor',
					columns: [],
					shape: ['B', 'T', 128],
					dtype: 'float32',
					properties: {}
				},
				{
					mode: 'tensor',
					columns: [],
					shape: ['B', 'T', 128],
					dtype: 'float32',
					properties: {}
				}
			],
			{ input_dim: 128, num_classes: 3 }
		);
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.output.mode).toBe('model_artifact');
			expect(result.output.properties?.class_set).toEqual(['class_0', 'class_1', 'class_2']);
		}
	});

	it('returns SHAPE_MISMATCH when train shape conflicts with input_dim', () => {
		const result = schemaFn_training_job(
			[
				{
					mode: 'tensor',
					columns: [],
					shape: ['B', 'T', 64],
					dtype: 'float32',
					properties: {}
				},
				{
					mode: 'tensor',
					columns: [],
					shape: ['B', 'T', 64],
					dtype: 'float32',
					properties: {}
				}
			],
			{ input_dim: 128 }
		);
		expect(result.ok).toBe(false);
		if (!result.ok) expect(result.error.code).toBe('SHAPE_MISMATCH');
	});
});

describe('schemaFn_evaluation', () => {
	it('returns metric columns from metrics_config', () => {
		const result = schemaFn_evaluation(
			[
				{
					mode: 'model_artifact',
					columns: [],
					properties: { class_set: ['a', 'b'] }
				}
			],
			{ metrics_config: [{ name: 'f1' }, { name: 'accuracy' }], num_classes: 2 }
		);
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.output.mode).toBe('table');
			expect(result.output.columns.map((column) => column.name)).toEqual(['f1', 'accuracy']);
		}
	});

	it('returns PROPERTY_VIOLATION when class_set conflicts with expected count', () => {
		const result = schemaFn_evaluation(
			[
				{
					mode: 'model_artifact',
					columns: [],
					properties: { class_set: ['a', 'b', 'c'] }
				}
			],
			{ num_classes: 2 }
		);
		expect(result.ok).toBe(false);
		if (!result.ok) expect(result.error.code).toBe('PROPERTY_VIOLATION');
	});
});

