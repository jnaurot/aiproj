// src/lib/flow/schema/llm.ts
import { z } from 'zod';
import { BaseNodeDataSchema } from './base';

export const LlmOutputModeSchema = z.enum(['text', 'json', 'embeddings']);
export const LlmKindSchema = z.enum(['ollama', 'openai_compat']);
export const ModelKindSchema = z.enum(['llm', 'vision', 'audio', 'embedding', 'reranker', 'multimodal']);
export const ModelTaskKindSchema = z.enum([
	'generate',
	'classify',
	'extract',
	'embed',
	'rerank',
	'transcribe',
	'caption'
]);

const LlmInputEnvelopePartSchema = z
	.discriminatedUnion('type', [
		z.object({ type: z.literal('text'), text: z.string() }).strip(),
		z
			.object({
				type: z.literal('image'),
				dataUrl: z.string().min(1),
				mimeType: z.string().min(1).optional()
			})
			.strip(),
		z
			.object({
				type: z.literal('audio'),
				dataUrl: z.string().min(1),
				mimeType: z.string().min(1).optional()
			})
			.strip()
	]);

const ModelTaskKindsByModelKind: Record<z.infer<typeof ModelKindSchema>, ReadonlySet<string>> = {
	llm: new Set(['generate', 'classify', 'extract']),
	vision: new Set(['caption', 'classify', 'extract', 'generate']),
	audio: new Set(['transcribe', 'extract', 'classify']),
	embedding: new Set(['embed']),
	reranker: new Set(['rerank']),
	multimodal: new Set(['generate', 'classify', 'extract', 'caption', 'transcribe'])
};

export const LlmParamsSchema = z
	.object({
		// Either reference a stored connection OR inline a baseUrl.
		// Start with baseUrl for speed, add connectionRef when you build secrets mgmt.
		baseUrl: z.string().url().optional(),
		connectionRef: z.string().min(1).optional(),

		model: z.string().min(1),

		system_prompt: z.string().optional(),
		user_prompt: z.string().min(1),

		temperature: z.number().min(0).max(2).optional(),
		top_p: z.number().min(0).max(1).optional(),
		max_tokens: z.number().int().positive().optional(),
		seed: z.number().int().optional(),
		stop: z.array(z.string().min(1)).optional(),
		presence_penalty: z.number().min(-2).max(2).optional(),
		frequency_penalty: z.number().min(-2).max(2).optional(),
		repeat_penalty: z.number().min(0.5).max(2).optional(),
		thinking: z
			.object({
				enabled: z.boolean().optional(),
				mode: z.enum(['none', 'hidden', 'visible']).optional(),
				budget_tokens: z.number().int().positive().optional()
			})
			.strip()
			.optional(),
		inputEncoding: z.enum(['text', 'json_canonical', 'table_canonical']).optional(),
		inputEnvelope: z.array(LlmInputEnvelopePartSchema).optional(),
		requestPolicy: z
			.object({
				retries: z.number().int().min(0).max(20).optional(),
				timeout_seconds: z.number().min(1).max(3600).optional(),
				backoff: z
					.object({
						base_seconds: z.number().min(0).max(60).optional(),
						max_seconds: z.number().min(0).max(300).optional(),
						jitter_seconds: z.number().min(0).max(60).optional()
					})
					.strip()
					.optional(),
				circuit_breaker: z
					.object({
						enabled: z.boolean().optional(),
						fail_threshold: z.number().int().min(1).max(100).optional(),
						reset_seconds: z.number().min(1).max(3600).optional()
					})
					.strip()
					.optional(),
				fallback_chain: z
					.array(
						z
							.object({
								llmKind: LlmKindSchema.optional(),
								connectionRef: z.string().min(1).optional(),
								baseUrl: z.string().url().optional(),
								model: z.string().min(1).optional(),
								apiKeyRef: z.string().min(1).optional()
							})
							.strip()
					)
					.optional()
			})
			.strip()
			.optional(),

		output: z
			.object({
				mode: LlmOutputModeSchema,
				strict: z.boolean().optional().default(true),
				jsonSchema: z.unknown().optional(),
				embedding: z
					.object({
						dims: z.number().int().positive(),
						dtype: z.enum(['float32', 'float16', 'float64']).optional().default('float32'),
						layout: z.enum(['1d', '2d']).optional().default('1d')
					})
					.strip()
					.optional()
			})
			.strip()
	})
	.superRefine((v, ctx) => {
		if (!v.baseUrl && !v.connectionRef) {
			ctx.addIssue({ code: 'custom', message: 'Either baseUrl or connectionRef required' });
		}
		if (v.output.mode === 'json' && v.output.jsonSchema === undefined) {
			ctx.addIssue({
				code: 'custom',
				message: "output.jsonSchema required when output.mode='json'"
			});
		}
		if (v.output.mode !== 'json' && v.output.jsonSchema !== undefined) {
			ctx.addIssue({
				code: 'custom',
				message: "output.jsonSchema is only allowed when output.mode='json'"
			});
		}
		if (v.output.mode === 'embeddings' && v.output.embedding === undefined) {
			ctx.addIssue({
				code: 'custom',
				message: "output.embedding required when output.mode='embeddings'"
			});
		}
	})
	.strip();

export type LlmParams = z.infer<typeof LlmParamsSchema>;
export type LlmOutputMode = z.infer<typeof LlmOutputModeSchema>;

export const LlmOllamaParamsSchema = LlmParamsSchema;

export const LlmOpenAI_compatParamsSchema = LlmParamsSchema;

// Node-level discriminator: llmKind (Source-style)
export const LlmNodeDataSchema = BaseNodeDataSchema('llm', LlmParamsSchema).extend({
	llmKind: LlmKindSchema,
	modelKind: ModelKindSchema.optional().default('llm'),
	taskKind: ModelTaskKindSchema.optional().default('generate')
});
export const ModelNodeDataSchema = BaseNodeDataSchema('model', LlmParamsSchema).extend({
	llmKind: LlmKindSchema,
	modelKind: ModelKindSchema.optional().default('llm'),
	taskKind: ModelTaskKindSchema.optional().default('generate')
})
	.superRefine((v, ctx) => {
		const modelKind = v.modelKind ?? 'llm';
		const taskKind = v.taskKind ?? 'generate';
		const allowed = ModelTaskKindsByModelKind[modelKind];
		if (!allowed?.has(taskKind)) {
			ctx.addIssue({
				code: 'custom',
				message: `taskKind='${taskKind}' is not valid for modelKind='${modelKind}'`
			});
		}
	});

export type LlmNodeData = z.infer<typeof LlmNodeDataSchema>;
export type ModelNodeData = z.infer<typeof ModelNodeDataSchema>;
export type LlmKind = z.infer<typeof LlmKindSchema>;
export type ModelKind = z.infer<typeof ModelKindSchema>;
export type ModelTaskKind = z.infer<typeof ModelTaskKindSchema>;

export const LlmParamsSchemaByKind = {
	ollama: LlmOllamaParamsSchema,
	openai_compat: LlmOpenAI_compatParamsSchema
} as const;
