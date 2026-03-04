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
  "split",
  "sql"]);

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
  clauses: z
    .array(
      z.object({
        leftNodeId: z.string().min(1, "Left join node id cannot be empty"),
        leftCol: z.string().min(1, "Left join key cannot be empty"),
        rightNodeId: z.string().min(1, "Right join node id cannot be empty"),
        rightCol: z.string().min(1, "Right join key cannot be empty"),
        how: z.enum(["inner", "left", "right", "full"]).default("inner"),
      })
    )
    .min(1, "Join must specify at least one clause"),
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

// export const TransformDedupeParamsSchema = z.object({
//   allColumns: z.boolean().optional().default(false),
//   by: z.array(z.string().min(1)).optional().default([]),
// }).strip();

export const TransformDedupeParamsSchema = z.object({
  allColumns: z.boolean().default(false),
  by: z.array(z.string().min(1)).default([]),
}).superRefine((val, ctx) => {
  if (val.allColumns && val.by.length > 0) {
    ctx.addIssue({
      code: "custom",
      path: ["by"],
      message: "by must be empty when allColumns is true",
    });
  }
});

export const TransformSqlParamsSchema = z.object({
  dialect: z.enum(["duckdb", "postgres", "sqlite"]).optional().default("duckdb"),
  query: z.string().min(1, "SQL query cannot be empty"),
}).strip();

export const TransformSplitParamsSchema = z
	.object({
		sourceColumn: z.string().min(1, 'Source column is required').default('text'),
		outColumn: z.string().min(1, 'Output column is required').default('part'),
		mode: z.enum(['sentences', 'lines', 'regex', 'delimiter']).default('sentences'),
		pattern: z.string().optional(),
		delimiter: z.string().optional(),
		flags: z
			.string()
			.default('')
			.refine((v) => /^[ims]*$/.test(v), 'Flags must contain only i, m, s'),
		trim: z.boolean().default(true),
		dropEmpty: z.boolean().default(true),
		emitIndex: z.boolean().default(true),
		emitSourceRow: z.boolean().default(true),
		maxParts: z.number().int().min(1).max(100000).default(5000)
	})
	.superRefine((v, ctx) => {
		if (v.mode === 'regex' && !String(v.pattern ?? '').trim()) {
			ctx.addIssue({
				code: 'custom',
				path: ['pattern'],
				message: 'Pattern is required when mode=regex'
			});
		}
		if (v.mode === 'delimiter' && !String(v.delimiter ?? '').length) {
			ctx.addIssue({
				code: 'custom',
				path: ['delimiter'],
				message: 'Delimiter is required when mode=delimiter'
			});
		}
	})
	.strip();

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
  split: TransformSplitParamsSchema,
  sql: TransformSqlParamsSchema
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
export type TransformSplitParams = z.infer<typeof TransformSplitParamsSchema>;


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
    op: z.literal("split"),
    split: TransformSplitParamsSchema
  }).strip(),

  TransformCommonSchema.extend({
    op: z.literal("sql"),
    sql: TransformSqlParamsSchema
  }).strip()
]);

export type TransformParams = z.infer<typeof TransformParamsSchema>;
export type TransformKind = z.infer<typeof TransformKindSchema>;
