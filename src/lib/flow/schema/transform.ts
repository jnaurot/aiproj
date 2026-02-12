import { z } from "zod";
import { BaseNodeDataSchema } from "./base";

export const TransformOpSchema = z.enum([
  "filter",
  "select",
  "rename",
  "derive",
  "aggregate",
  "join",
  "sort",
  "limit",
  "dedupe",
  "sql",
  "python",
  "js"
]);

export const TransformParamsSchema = z.object({
  op: TransformOpSchema,
  enabled: z.boolean().optional(),
  notes: z.string().optional(),

  filter: z.object({ expr: z.string().min(1) }).optional(),
  select: z.object({ columns: z.array(z.string().min(1)) }).optional(),
  rename: z.object({ map: z.record(z.string(), z.string().min(1)) }).optional(),
  derive: z.object({ columns: z.array(z.object({ name: z.string().min(1), expr: z.string().min(1) })) }).optional(),
  aggregate: z
    .object({
      groupBy: z.array(z.string().min(1)),
      metrics: z.array(z.object({ as: z.string().min(1), expr: z.string().min(1) }))
    })
    .optional(),
  join: z
    .object({
      withNodeId: z.string().min(1),
      how: z.enum(["inner", "left", "right", "full"]),
      on: z.array(z.object({ left: z.string().min(1), right: z.string().min(1) }))
    })
    .optional(),
  sort: z.object({ by: z.array(z.object({ col: z.string().min(1), dir: z.enum(["asc", "desc"]) })) }).optional(),
  limit: z.object({ n: z.number().int().nonnegative() }).optional(),
  dedupe: z.object({ by: z.array(z.string().min(1)).optional() }).optional(),

  sql: z.object({ dialect: z.enum(["duckdb", "postgres", "sqlite"]).optional(), query: z.string().min(1) }).optional(),
  code: z.object({ language: z.enum(["python", "js"]), source: z.string().min(1) }).optional(),

  cache: z.object({ enabled: z.boolean(), key: z.string().optional() }).optional()
});

export const TransformNodeDataSchema = BaseNodeDataSchema("transform", TransformParamsSchema);


