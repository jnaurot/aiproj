import { z } from "zod";
import { BaseNodeDataSchema } from "./base";
import { TOOL_BUILTIN_PROFILE_IDS } from "./toolBuiltinProfiles";

const BuiltinEnvironmentSchema = z.object({
  profileId: z.enum(TOOL_BUILTIN_PROFILE_IDS).optional().default("core"),
  customPackages: z.array(z.string().min(1)).optional().default([]),
  locked: z.string().min(1).optional()
}).strip();

const ToolCommonSchema = z.object({
  name: z.string().min(1),
  toolVersion: z.string().optional().default("v1"),
  side_effect_mode: z.enum(["pure", "idempotent", "effectful"]).default("pure"),
  cache_enabled: z.boolean().optional().default(true),
  armed: z.boolean().optional().default(false),
  timeoutMs: z.number().int().positive().optional(),
  retry: z
    .object({
      max_attempts: z.number().int().positive().default(1),
      backoff_ms: z.number().int().nonnegative().optional().default(0),
      on: z.array(z.enum(["timeout", "429", "5xx"])).optional().default(["timeout", "429", "5xx"])
    })
    .strip()
    .optional(),
  permissions: z
    .object({
      net: z.boolean().optional().default(false),
      fs: z.boolean().optional().default(false),
      env: z.boolean().optional().default(false),
      subprocess: z.boolean().optional().default(false),
    })
    .strip()
    .optional(),

  input: z.object({
    schema: z.unknown().optional(),
    mapping: z.record(z.string(), z.string()).optional()
  }).strip().optional(),
  output: z.object({ schema: z.unknown().optional() }).strip().optional(),
  builtin: BuiltinEnvironmentSchema.optional()
}).strip();

const McpSchema = ToolCommonSchema.extend({
  provider: z.literal("mcp"),
  mcp: z.object({
    serverId: z.string().min(1),
    toolName: z.string().min(1),
    args: z.record(z.string(), z.unknown()).optional()

  }).strip()
}).strip();

const HttpSchema = ToolCommonSchema.extend({
  provider: z.literal("http"),
  http: z.object({
    url: z.string().url(),
    method: z.enum(["GET", "POST", "PUT", "PATCH", "DELETE"]),
    headers: z.record(z.string(), z.string()).optional(),
    query: z.record(z.string(), z.union([z.string(), z.number(), z.boolean()])).optional(),
    body: z.unknown().optional()
  }).strip()
}).strip();

const FunctionSchema = ToolCommonSchema.extend({
  provider: z.literal("function"),
  function: z.object({
    module: z.string().min(1),
    export: z.string().min(1),
    args: z.record(z.string(), z.unknown()).optional(),
    capture_output: z.boolean().optional().default(true)

  }).strip()
}).strip();

const PythonSchema = ToolCommonSchema.extend({
  provider: z.literal("python"),
  python: z.object({
    code: z.string().min(1),
    args: z.record(z.string(), z.unknown()).optional(),
    capture_output: z.boolean().optional().default(true)
  }).strip()
}).strip();

const JsSchema = ToolCommonSchema.extend({
  provider: z.literal("js"),
  js: z.object({
    code: z.string().min(1),
    args: z.record(z.string(), z.unknown()).optional(),
    capture_output: z.boolean().optional().default(true)
  }).strip()
}).strip();

const ShellSchema = ToolCommonSchema.extend({
  provider: z.literal("shell"),
  shell: z.object({
    command: z.string().min(1),
    cwd: z.string().optional(),
    env: z.record(z.string(), z.string()).optional(),
    fail_on_nonzero: z.boolean().optional().default(true)
  }).strip()
}).strip();

const DbSchema = ToolCommonSchema.extend({
  provider: z.literal("db"),
  db: z.object({
    connectionRef: z.string().min(1),
    sql: z.string().min(1),
    params: z.record(z.string(), z.unknown()).optional(),
    capture_output: z.boolean().optional().default(true)

  }).strip()
}).strip();

const BuiltinSchema = ToolCommonSchema.extend({
  provider: z.literal("builtin"),
  builtin: BuiltinEnvironmentSchema.extend({
    toolId: z.string().min(1),
    args: z.record(z.string(), z.unknown()).optional()
  }).strip()
}).strip();

export const ToolParamsSchema = z.discriminatedUnion("provider", [
  McpSchema,
  HttpSchema,
  FunctionSchema,
  PythonSchema,
  JsSchema,
  ShellSchema,
  DbSchema,
  BuiltinSchema
]);

export const ToolNodeDataSchema = BaseNodeDataSchema("tool", ToolParamsSchema);

export type ToolParams = z.infer<typeof ToolParamsSchema>;
export type ToolProvider = ToolParams["provider"];
export type ToolNodeData = z.infer<typeof ToolNodeDataSchema>;

export const defaultToolParams = {
  provider: "mcp" as const,
  name: "MCP Tool",
  mcp: { serverId: "local", toolName: "search_docs", args: {} }
};
