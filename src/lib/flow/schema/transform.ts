//src/lib/flow/schema/transform.ts
import { z } from "zod";

// ---- shared enums ----
const TransformKindSchema = z.enum(["filter",
  "select",
  "rename",
  "derive",
  "aggregate",
  "join",
  "sort",
  "limit",
  "dedupe",
  "sql",
  "python"]);

// ─────────────────────────────────────────────
// Per-operation parameter schemas
// Each is standalone, strict, and uses .strip() to reject unknown keys
// ─────────────────────────────────────────────

export const TransformFilterParamsSchema = z.object({
  expr: z.string().min(1, "Filter expression cannot be empty"),
}).strip();

export const TransformSelectParamsSchema = z.object({
  columns: z.array(z.string().min(1)).min(1, "Select must specify at least one column"),
}).strip();

export const TransformRenameParamsSchema = z.object({
  map: z.record(
    z.string().min(1),
    z.string().min(1, "New column name cannot be empty")
  ).refine(
    (map) => Object.keys(map).length > 0,
    { message: "Rename map cannot be empty" }
  ),
}).strip();

export const TransformDeriveParamsSchema = z.object({
  columns: z.array(
    z.object({
      name: z.string().min(1, "Derived column name cannot be empty"),
      expr: z.string().min(1, "Derived expression cannot be empty"),
    })
  ).min(1, "Derive must specify at least one new column"),
}).strip();

export const TransformAggregateParamsSchema = z.object({
  groupBy: z.array(z.string().min(1)).optional().default([]),
  metrics: z.array(
    z.object({
      as: z.string().min(1, "Aggregate alias cannot be empty"),
      expr: z.string().min(1, "Aggregate expression cannot be empty"),
    })
  ).min(1, "Aggregate must specify at least one metric"),
}).strip();

export const TransformJoinParamsSchema = z.object({
  withNodeId: z.string().min(1, "Join target node ID is required"),

  how: z.enum(["inner", "left", "right", "full"]),

  on: z
    .array(
      z.object({
        left: z.string().min(1, "Left join key cannot be empty"),
        right: z.string().min(1, "Right join key cannot be empty"),
      })
    )
    .min(1, "Join must specify at least one ON condition"),
}).strip();

export const TransformSortParamsSchema = z.object({
  by: z
    .array(
      z.object({
        col: z.string().min(1, "Sort column cannot be empty"),
        dir: z.enum(["asc", "desc"]),
      })
    )
    .min(1, "Sort must specify at least one column"),
}).strip();

export const TransformLimitParamsSchema = z.object({
  n: z.number()
    .int("Limit must be an integer")
    .nonnegative("Limit cannot be negative")
    .min(1, "Limit must be at least 1"),
}).strip();

export const TransformDedupeParamsSchema = z.object({
  by: z.array(z.string().min(1)).optional().default([]),
  // Optional: could add .refine() to check for empty array meaning "dedupe all columns"
}).strip();

export const TransformSqlParamsSchema = z.object({
  dialect: z.enum(["duckdb", "postgres", "sqlite"]).optional().default("duckdb"),
  query: z.string().min(1, "SQL query cannot be empty"),
}).strip();

export const TransformPythonParamsSchema = z.object({
  source: z.string().min(1, "Python code cannot be empty"),
  // If you want to enforce language (though it's always python here)
  language: z.literal("python").optional().default("python"),
}).strip();

export const TransformParamsSchemaByKind = {
  filter: TransformFilterParamsSchema,
  select: TransformSelectParamsSchema,
  rename: TransformRenameParamsSchema,
  derive: TransformDeriveParamsSchema,
  aggregate: TransformAggregateParamsSchema,
  join: TransformJoinParamsSchema,
  sort: TransformSortParamsSchema,
  limit: TransformLimitParamsSchema,
  dedupe: TransformDedupeParamsSchema,
  sql: TransformSqlParamsSchema,
  python: TransformPythonParamsSchema
} as const

// ---- inferred types (single source of truth) ----
export type TransformFilterParams = z.infer<typeof TransformFilterParamsSchema>;
export type TransformSelectParams  = z.infer<typeof   TransformSelectParamsSchema>;
export type TransformRenameParams  = z.infer<typeof   TransformRenameParamsSchema>;
export type TransformDeriveParams  = z.infer<typeof   TransformDeriveParamsSchema>;
export type TransformAggregateParams  = z.infer<typeof   TransformAggregateParamsSchema>;
export type TransformJoinParams  = z.infer<typeof   TransformJoinParamsSchema>;
export type TransformSortParams  = z.infer<typeof   TransformSortParamsSchema>;
export type TransformLimitParams  = z.infer<typeof   TransformLimitParamsSchema>;
export type  TransformDedupeParams = z.infer<typeof   TransformDedupeParamsSchema>;
export type  TransformSqlParams = z.infer<typeof   TransformSqlParamsSchema>;
export type  TransformPythonParams = z.infer<typeof   TransformPythonParamsSchema>;


// ---- common params (shared across all ops) ----
const TransformCacheSchema = z
  .object({
    enabled: z.boolean().default(false),
    key: z.string().optional()
  })
  .strip();

const TransformCommonSchema = z
  .object({
    enabled: z.boolean().default(true),
    notes: z.string().optional().default(""),
    cache: TransformCacheSchema.optional().default({ enabled: false })
  })
  .strip();

// ---- node-level params schema (discriminated union) ----
export const TransformParamsSchema = z.discriminatedUnion("op", [
  TransformCommonSchema.extend({
    op: z.literal("filter"),
    filter: TransformFilterParamsSchema
  }).strip(),

  TransformCommonSchema.extend({
    op: z.literal("select"),
    select: TransformSelectParamsSchema
  }).strip(),

  TransformCommonSchema.extend({
    op: z.literal("rename"),
    rename: TransformRenameParamsSchema
  }).strip(),

  TransformCommonSchema.extend({
    op: z.literal("derive"),
    derive: TransformDeriveParamsSchema
  }).strip(),

  TransformCommonSchema.extend({
    op: z.literal("aggregate"),
    aggregate: TransformAggregateParamsSchema
  }).strip(),

  TransformCommonSchema.extend({
    op: z.literal("join"),
    join: TransformJoinParamsSchema
  }).strip(),

  TransformCommonSchema.extend({
    op: z.literal("sort"),
    sort: TransformSortParamsSchema
  }).strip(),

  TransformCommonSchema.extend({
    op: z.literal("limit"),
    limit: TransformLimitParamsSchema
  }).strip(),

  TransformCommonSchema.extend({
    op: z.literal("dedupe"),
    dedupe: TransformDedupeParamsSchema
  }).strip(),

  TransformCommonSchema.extend({
    op: z.literal("sql"),
    sql: TransformSqlParamsSchema
  }).strip(),

  TransformCommonSchema.extend({
    op: z.literal("python"),
    code: TransformPythonParamsSchema
  }).strip()
]);

export type TransformParams = z.infer<typeof TransformParamsSchema>;
export type TransformKind = z.infer<typeof TransformKindSchema>;
