import { describe, expect, it } from 'vitest';

import { SourceParamsSchemaByKind } from './source';
import { LlmParamsSchemaByKind, ModelNodeDataSchema } from './llm';
import { TransformParamsSchemaByKind, TransformParamsSchema } from './transform';
import { ToolParamsSchema } from './tool';
import { ComponentParamsSchema } from './component';

describe('node subtype contract schemas', () => {
	it('accepts valid source subtype payloads', () => {
		expect(
			SourceParamsSchemaByKind.file.safeParse({
				snapshotId: 'a'.repeat(64),
				file_format: 'txt',
				output: { mode: 'text' },
			}).success
		).toBe(true);

		expect(
			SourceParamsSchemaByKind.database.safeParse({
				connection_ref: 'warehouse',
				table_name: 'events',
				output: { mode: 'table' },
			}).success
		).toBe(true);

		expect(
			SourceParamsSchemaByKind.api.safeParse({
				url: 'https://example.com/api',
				method: 'GET',
				auth_type: 'none',
				output: { mode: 'json' },
			}).success
		).toBe(true);
	});

	it('accepts valid llm subtype payloads', () => {
		expect(
			LlmParamsSchemaByKind.ollama.safeParse({
				baseUrl: 'http://localhost:11434',
				model: 'gpt-oss:20b',
				user_prompt: 'Describe input.',
				output: { mode: 'text' },
			}).success
		).toBe(true);

		expect(
			LlmParamsSchemaByKind.openai_compat.safeParse({
				connectionRef: 'openai_prod',
				model: 'gpt-4.1-mini',
				user_prompt: 'Return json',
				output: { mode: 'json', jsonSchema: { type: 'object' } },
			}).success
		).toBe(true);

		expect(
			ModelNodeDataSchema.safeParse({
				kind: 'model',
				modelKind: 'vision',
				llmKind: 'openai_compat',
				label: 'Model',
				status: 'idle',
				params: {
					baseUrl: 'https://api.openai.com',
					model: 'gpt-4.1-mini',
					user_prompt: 'Describe the image',
					output: { mode: 'text' }
				}
			}).success
		).toBe(true);

		expect(
			ModelNodeDataSchema.safeParse({
				kind: 'model',
				modelKind: 'embedding',
				taskKind: 'generate',
				llmKind: 'openai_compat',
				label: 'Model',
				status: 'idle',
				params: {
					baseUrl: 'https://api.openai.com',
					model: 'text-embedding-3-small',
					user_prompt: 'Generate embeddings',
					output: { mode: 'embeddings', embedding: { dims: 1536 } }
				}
			}).success
		).toBe(false);
	});

	it('accepts valid transform subtype payloads', () => {
		expect(
			TransformParamsSchema.safeParse({
				op: 'select',
				select: { mode: 'include', columns: ['id'], strict: true },
			}).success
		).toBe(true);

		expect(
			TransformParamsSchemaByKind.dedupe.safeParse({
				allColumns: false,
				by: ['sku'],
			}).success
		).toBe(true);

		expect(
			TransformParamsSchemaByKind.quality_gate.safeParse({
				stopOnFail: true,
				checks: [
					{ kind: 'null_pct', column: 'text', maxNullPct: 0.1, severity: 'fail' },
					{ kind: 'uniqueness', column: 'id', minUniqueRatio: 0.99, severity: 'warn' },
				],
			}).success
		).toBe(true);

		expect(
			TransformParamsSchemaByKind.ml_contract.safeParse({
				taskType: 'classification',
				labelColumn: 'label',
				featureColumns: ['text'],
				idColumn: 'id',
				timestampColumn: '',
				allowExtraFeatures: true,
				requireNonNullLabel: true
			}).success
		).toBe(true);

		expect(
			TransformParamsSchemaByKind.null_policy.safeParse({
				mode: 'fill_stat',
				columns: ['text'],
				stat: 'mean',
				rules: []
			}).success
		).toBe(true);

		expect(
			TransformParamsSchemaByKind.outlier_policy.safeParse({
				mode: 'clip',
				method: 'iqr',
				columns: ['score'],
				iqrMultiplier: 1.5,
				zscoreThreshold: 3,
				lowerQuantile: 0.01,
				upperQuantile: 0.99
			}).success
		).toBe(true);

		expect(
			TransformParamsSchemaByKind.text_clean.safeParse({
				columns: ['text'],
				lowercase: true,
				unicodeNormalize: 'nfkc',
				removePunctuation: false,
				removeUrls: true,
				removeEmails: true,
				removeEmoji: false,
				normalizeWhitespace: true
			}).success
		).toBe(true);

		expect(
			TransformParamsSchemaByKind.nlp_normalize.safeParse({
				columns: ['text'],
				language: 'en',
				removeStopwords: true,
				stemmer: 'none',
				lemmatizer: 'none',
				tokenPattern: '\\w+'
			}).success
		).toBe(true);

		expect(
			TransformParamsSchemaByKind.tokenize_chunk.safeParse({
				columns: ['text'],
				tokenizer: 'whitespace',
				tokenPattern: '\\w+',
				maxTokens: 128,
				overlap: 16,
				sentenceAware: true,
				outColumn: 'chunk'
			}).success
		).toBe(true);

		expect(
			TransformParamsSchemaByKind.dataset_split.safeParse({
				strategy: 'random',
				trainRatio: 0.8,
				valRatio: 0.1,
				testRatio: 0.1,
				seed: 42,
				shuffle: true,
				stratifyColumn: '',
				groupColumn: '',
				timeColumn: '',
				leakageGuard: true
			}).success
		).toBe(true);

		expect(
			TransformParamsSchemaByKind.class_imbalance.safeParse({
				strategy: 'report',
				labelColumn: 'label',
				targetRatio: 1,
				seed: 42
			}).success
		).toBe(true);

		expect(
			TransformParamsSchemaByKind.categorical_encode.safeParse({
				columns: ['category'],
				encoding: 'one_hot',
				unknownPolicy: 'ignore',
				rareThreshold: 0.01,
				dropFirst: false
			}).success
		).toBe(true);

		expect(
			TransformParamsSchemaByKind.numeric_scale.safeParse({
				columns: ['x'],
				method: 'standard',
				withCenter: true,
				withScale: true,
				clip: false
			}).success
		).toBe(true);

		expect(
			TransformParamsSchemaByKind.embedding.safeParse({
				columns: ['text'],
				provider: 'local_hash',
				model: 'text-embedding-3-small',
				dimensions: 16,
				batchSize: 64,
				cacheEmbeddings: true,
				outputColumn: 'embedding'
			}).success
		).toBe(true);

		expect(
			TransformParamsSchemaByKind.feature_selection.safeParse({
				method: 'variance',
				columns: ['x', 'y'],
				topK: 2,
				varianceThreshold: 0,
				targetColumn: 'label',
				selectedColumns: []
			}).success
		).toBe(true);

		expect(
			TransformParamsSchemaByKind.leakage_detect.safeParse({
				splitColumn: 'split',
				keyColumns: ['id'],
				labelColumn: 'label',
				maxAllowedOverlap: 0
			}).success
		).toBe(true);

		expect(
			TransformParamsSchemaByKind.quality_profile.safeParse({
				columns: ['text'],
				includeHistograms: true,
				includeSamples: true
			}).success
		).toBe(true);

		expect(
			TransformParamsSchemaByKind.drift_compare.safeParse({
				baselineRef: '',
				compareColumns: ['x'],
				metric: 'psi',
				threshold: 0.2,
				failOnDrift: false
			}).success
		).toBe(true);

		expect(
			TransformParamsSchemaByKind.determinism_profile.safeParse({
				strict: true,
				seed: 42,
				stableSort: true,
				stableCoercion: true
			}).success
		).toBe(true);

		expect(
			TransformParamsSchemaByKind.fit_state_registry.safeParse({
				mode: 'fit',
				stateKey: 'default',
				includeColumns: ['x']
			}).success
		).toBe(true);

		expect(
			TransformParamsSchemaByKind.pii_guard.safeParse({
				columns: ['text'],
				action: 'report',
				failOnDetect: false
			}).success
		).toBe(true);

		expect(
			TransformParamsSchemaByKind.inference_parity.safeParse({
				trainSignature: 'a',
				inferenceSignature: 'a',
				failOnMismatch: true
			}).success
		).toBe(true);

		expect(
			TransformParamsSchemaByKind.json_to_table.safeParse({
				orient: 'records',
				rowsKey: 'rows',
			}).success
		).toBe(true);

		expect(
			TransformParamsSchemaByKind.text_to_table.safeParse({
				mode: 'csv',
				column: 'text',
				delimiter: ',',
				hasHeader: true,
			}).success
		).toBe(true);

		expect(
			TransformParamsSchemaByKind.table_to_json.safeParse({
				orient: 'records',
				pretty: false,
			}).success
		).toBe(true);
	});

	it('accepts valid tool providers and component params', () => {
		expect(
			ToolParamsSchema.safeParse({
				provider: 'http',
				name: 'HTTP Tool',
				http: { url: 'https://example.com', method: 'GET' },
			}).success
		).toBe(true);

		expect(
			ToolParamsSchema.safeParse({
				provider: 'db',
				name: 'DB Tool',
				db: { connectionRef: 'analytics', sql: 'select 1 as ok' },
			}).success
		).toBe(true);

		expect(
			ToolParamsSchema.safeParse({
				provider: 'python',
				name: 'Python Tool',
				python: { code: 'print(1)' },
				builtin: { profileId: 'data' }
			}).success
		).toBe(true);

		expect(
			ToolParamsSchema.safeParse({
				provider: 'builtin',
				name: 'Builtin Tool',
				builtin: {
					toolId: 'noop',
					profileId: 'llm_finetune',
					customPackages: ['transformers', 'peft'],
					locked: 'sha256:abc123'
				}
			}).success
		).toBe(true);

		const builtinDefaulted = ToolParamsSchema.safeParse({
			provider: 'builtin',
			name: 'Builtin Tool',
			builtin: {
				toolId: 'noop'
			}
		});
		expect(builtinDefaulted.success).toBe(true);
		if (builtinDefaulted.success) {
			expect((builtinDefaulted.data as any).builtin.profileId).toBe('core');
			expect((builtinDefaulted.data as any).builtin.customPackages).toEqual([]);
		}

		expect(
			ToolParamsSchema.safeParse({
				provider: 'builtin',
				name: 'Builtin Tool',
				builtin: {
					toolId: 'noop',
					profileId: 'not_a_profile'
				}
			}).success
		).toBe(false);

		expect(
			ComponentParamsSchema.safeParse({
				componentRef: { componentId: 'cmp_reader', revisionId: 'crev_1', apiVersion: 'v1' },
				bindings: {
					inputs: {},
					config: {},
					outputs: { out_data: { nodeId: 'inner_1', artifact: 'current' } },
				},
				api: {
					inputs: [],
					outputs: [
						{
							name: 'out_data',
							payloadType: 'json',
							required: true,
							typedSchema: {
								type: 'json',
								fields: [{ name: 'text', type: 'text', nullable: false }],
							},
						},
					],
				},
				config: {},
			}).success
		).toBe(true);
	});
});

