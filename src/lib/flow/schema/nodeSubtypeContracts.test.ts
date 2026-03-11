import { describe, expect, it } from 'vitest';

import { SourceParamsSchemaByKind } from './source';
import { LlmParamsSchemaByKind } from './llm';
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

