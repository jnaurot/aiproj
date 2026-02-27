// src/lib/flow/schema/llm.ts
import { z } from 'zod';
import { BaseNodeDataSchema } from './base';

export const LlmOutputModeSchema = z.enum(['text', 'json', 'embeddings']);
export const LlmKindSchema = z.enum(['ollama', 'openai_compat']);

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
	llmKind: LlmKindSchema
});

export type LlmNodeData = z.infer<typeof LlmNodeDataSchema>;
export type LlmKind = z.infer<typeof LlmKindSchema>;

export const LlmParamsSchemaByKind = {
	ollama: LlmOllamaParamsSchema,
	openai_compat: LlmOpenAI_compatParamsSchema
} as const;
