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
