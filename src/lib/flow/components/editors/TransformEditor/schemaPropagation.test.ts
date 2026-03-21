import { describe, expect, it } from 'vitest';
import type { TransformKind } from '$lib/flow/schema/transform';
import type { InputSchemaView } from './inputSchema';
import {
	buildTransformSchemaProps,
	toTransformInputColumns,
	toTransformInputSchemaColumns
} from './schemaPropagation';

const inputSchemas: InputSchemaView[] = [
	{
		artifactId: 'a1',
		label: 'SourceA.in',
		sourceNodeId: 'n_source_a',
		inputHandle: 'in',
		columns: [
			{ name: 'id', type: 'int' },
			{ name: 'name', type: 'string' },
			{ name: 'score', type: 'unknown' }
		],
		rowCount: 10,
		provenance: null,
		coercion: null
	},
	{
		artifactId: 'a2',
		label: 'SourceB.in',
		sourceNodeId: 'n_source_b',
		inputHandle: 'in',
		columns: [
			{ name: 'score', type: 'float' },
			{ name: 'flag', type: 'bool' },
			{ name: 'name', type: 'unknown' }
		],
		rowCount: 8,
		provenance: null,
		coercion: null
	}
];

describe('schemaPropagation', () => {
	it('builds unique inputColumns from all input schemas', () => {
		expect(toTransformInputColumns(inputSchemas)).toEqual(['id', 'name', 'score', 'flag']);
	});

	it('builds typed inputSchemaColumns and prefers non-unknown duplicates', () => {
		expect(toTransformInputSchemaColumns(inputSchemas)).toEqual([
			{ name: 'flag', type: 'bool' },
			{ name: 'id', type: 'int' },
			{ name: 'name', type: 'string' },
			{ name: 'score', type: 'float' }
		]);
	});

	it('provides schema props for every transform kind', () => {
		const kinds: TransformKind[] = [
			'filter',
			'select',
			'rename',
			'derive',
			'aggregate',
			'join',
			'sort',
			'limit',
			'dedupe',
			'null_policy',
			'outlier_policy',
			'text_clean',
			'nlp_normalize',
			'tokenize_chunk',
			'dataset_split',
			'class_imbalance',
			'categorical_encode',
			'numeric_scale',
			'embedding',
			'feature_selection',
			'leakage_detect',
			'quality_profile',
			'drift_compare',
			'determinism_profile',
			'fit_state_registry',
			'pii_guard',
			'inference_parity',
			'split',
			'quality_gate',
			'ml_contract',
			'sql',
			'json_to_table',
			'text_to_table',
			'table_to_json'
		];
		for (const kind of kinds) {
			const props = buildTransformSchemaProps(kind, inputSchemas);
			expect(props.inputSchemas).toBe(inputSchemas);
			expect(props.inputColumns).toEqual(['id', 'name', 'score', 'flag']);
			expect(props.inputSchemaColumns).toEqual([
				{ name: 'flag', type: 'bool' },
				{ name: 'id', type: 'int' },
				{ name: 'name', type: 'string' },
				{ name: 'score', type: 'float' }
			]);
		}
	});
});
