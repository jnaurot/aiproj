// src/lib/flow/schema/llm.ts
import { z } from "zod";
import { BaseNodeDataSchema } from "./base";

export const LlmOutputModeSchema = z.enum(["text", "markdown", "json"]);
export const LlmKindSchema = z.enum(["ollama", "openai_compat"]);

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

    output: z.object({
      mode: LlmOutputModeSchema,
      jsonSchema: z.unknown().optional(),
    }),
  })
  .superRefine((v, ctx) => {
    if (!v.baseUrl && !v.connectionRef) {
      ctx.addIssue({ code: "custom", message: "Either baseUrl or connectionRef required" });
    }
  })
  .strip();

export type LlmParams = z.infer<typeof LlmParamsSchema>;

export const LlmOllamaParamsSchema = LlmParamsSchema

export const LlmOpenAI_compatParamsSchema = LlmParamsSchema

// Node-level discriminator: llmKind (Source-style)
export const LlmNodeDataSchema = BaseNodeDataSchema("llm", LlmParamsSchema).extend({
  llmKind: LlmKindSchema,
});

export type LlmNodeData = z.infer<typeof LlmNodeDataSchema>;
export type LlmKind = z.infer<typeof LlmKindSchema>;

export const LlmParamsSchemaByKind  = {
    ollama: LlmOllamaParamsSchema,
    openai_compat: LlmOpenAI_compatParamsSchema,
} as const;
