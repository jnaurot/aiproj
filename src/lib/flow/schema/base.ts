import { z } from "zod";
import { PAYLOAD_TYPES } from "../types";

export const NodeTypesSchema = z.enum(["source", "transform", "model", "llm", "tool", "component"]);

export const NodeStatusSchema = z.enum([
  "idle",
  "stale",
  "running",
  "busy",
  "succeeded",
  "failed",
  "skipped",
  "canceled"
]);

export const PayloadTypeSchema = z.enum(PAYLOAD_TYPES);



export const NodeMetaSchema = z.object({
  createdAt: z.string().optional(),
  updatedAt: z.string().optional(),
  description: z.string().optional(),
  tags: z.array(z.string()).optional(),
  nodeDoc: z
    .object({
      summary: z.string().optional(),
      notes: z.array(z.string().min(1)).optional(),
      disabled: z.boolean().optional(),
      generated: z
        .object({
          summary: z.string().min(1),
          settings_explained: z.array(z.string().min(1)).default([]),
          context_notes: z.array(z.string().min(1)).default([]),
          generated_at: z.string().min(1),
          signature_key: z.string().min(1),
          provider_meta: z
            .object({
              provider: z.string().min(1).optional(),
              model: z.string().min(1).optional()
            })
            .strip()
            .optional()
        })
        .strip()
        .optional()
    })
    .strip()
    .optional(),
  freeze: z
    .object({
      enabled: z.boolean().optional(),
      mode: z.enum(["per_run", "sticky"]).optional()
    })
    .strip()
    .optional(),
  llmAllocated: z.boolean().optional()
}).strip();

export const BaseNodeDataSchema = <K extends string, P extends z.ZodTypeAny>(
  kind: K,
  paramsSchema: P
) =>
  z.object({
    kind: z.literal(kind),
    label: z.string().min(1),
    params: paramsSchema,
    status: NodeStatusSchema,

    lastRunId: z.string().optional(),
    lastStartedAt: z.string().optional(),
    lastEndedAt: z.string().optional(),
    error: z
      .object({
        message: z.string(),
        code: z.string().optional(),
        details: z.unknown().optional()
      })
      .optional(),
      
    meta: NodeMetaSchema.optional()
  });
