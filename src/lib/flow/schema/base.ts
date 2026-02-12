import { z } from "zod";
import { PORT_TYPES } from "../types";

export const NodeTypesSchema = z.enum(["source", "transform", "llm", "tool"]);

export const NodeStatusSchema = z.enum([
  "idle",
  "stale",
  "running",
  "succeeded",
  "failed",
  "skipped",
  "canceled"
]);

export const PortTypeSchema = z.enum(PORT_TYPES);
export const PortsSchema = z.object({
  in: PortTypeSchema.nullable(),  // source nodes can be null/undefined
  out: PortTypeSchema.nullable() // require out for everyone (Phase 1)
});



export const NodeMetaSchema = z.object({
  createdAt: z.string().optional(),
  updatedAt: z.string().optional(),
  description: z.string().optional(),
  tags: z.array(z.string()).optional()
});

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
      
    ports: PortsSchema,

    meta: NodeMetaSchema.optional()
  });
