// src/lib/flow/schema/source.ts
import { z } from "zod";

// ---- shared enums ----
const SourceKindSchema = z.enum(["file", "database", "api"]);

// ---- schemas ----
export const SourceFileParamsSchema = z.object({
  file_path: z.string().min(1),
  file_format: z.enum(["csv", "parquet", "json", "excel", "txt"]).default("csv"),
  delimiter: z.string().optional(),
  sheet_name: z.string().optional(),
  sample_size: z.number().int().positive().optional(),
  encoding: z.string().default("utf-8"),
  cache_enabled: z.boolean().default(true)
}).strip();

export const SourceDatabaseParamsSchema = z.object({
  connection_string: z.string().optional(),
  connection_ref: z.string().optional(),
  query: z.string().optional(),
  table_name: z.string().optional(),
  limit: z.number().int().positive().optional()
}).superRefine((v, ctx) => {
  if (!v.connection_string && !v.connection_ref) {
    ctx.addIssue({ code: "custom", message: "Either connection_string or connection_ref required" });
  }
  if (!v.query && !v.table_name) {
    ctx.addIssue({ code: "custom", message: "Either query or table_name required" });
  }
}).strip();

export const SourceAPIParamsSchema = z.object({
  url: z.string().url(),
  method: z.enum(["GET", "POST", "PUT", "DELETE"]).default("GET"),
  headers: z.record(z.string(), z.string()).default({}),
  body: z.record(z.string(), z.any()).optional(),
  auth_type: z.enum(["none", "bearer", "basic", "api_key"]).default("none"),
  auth_token_ref: z.string().optional(),
  timeout_seconds: z.number().int().positive().default(30)
}).superRefine((v, ctx) => {
  if (v.auth_type !== "none" && !v.auth_token_ref) {
    ctx.addIssue({ code: "custom", message: "auth_token_ref required when using authentication" });
  }
}).strip();

export const SourceParamsSchemaByKind  = {
    file: SourceFileParamsSchema,
    database: SourceDatabaseParamsSchema,
    api: SourceAPIParamsSchema
} as const;



// ---- inferred types (single source of truth) ----
export type SourceFileParams = z.infer<typeof SourceFileParamsSchema>;
export type SourceDatabaseParams = z.infer<typeof SourceDatabaseParamsSchema>;
export type SourceAPIParams = z.infer<typeof SourceAPIParamsSchema>;