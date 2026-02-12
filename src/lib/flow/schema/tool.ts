import { z } from "zod";
import { BaseNodeDataSchema } from "./base";

const ToolCommonSchema = z.object({
  name: z.string().min(1),
  timeoutMs: z.number().int().positive().optional(),
  retries: z.object({ max: z.number().int().nonnegative(), backoffMs: z.number().int().nonnegative().optional() }).optional(),

  input: z.object({
    schema: z.unknown().optional(),
    mapping: z.record(z.string(), z.string()).optional()
  }).optional(),
  output: z.object({ schema: z.unknown().optional(), mode: z.enum(["json", "text", "binary"]).optional() }).optional()
});

const McpSchema = ToolCommonSchema.extend({
  provider: z.literal("mcp"),
  mcp: z.object({
    serverId: z.string().min(1),
    toolName: z.string().min(1),
    args: z.record(z.string(), z.unknown()).optional()

  })
});

const HttpSchema = ToolCommonSchema.extend({
  provider: z.literal("http"),
  http: z.object({
    url: z.string().url(),
    method: z.enum(["GET", "POST", "PUT", "PATCH", "DELETE"]),
    headers: z.record(z.string(), z.string()).optional(),
    query: z.record(z.string(), z.union([z.string(), z.number(), z.boolean()])).optional(),
    body: z.unknown().optional()
  })
});

const FunctionSchema = ToolCommonSchema.extend({
  provider: z.literal("function"),
  function: z.object({
    module: z.string().min(1),
    export: z.string().min(1),
    args: z.record(z.string(), z.unknown()).optional()

  })
});

const PythonSchema = ToolCommonSchema.extend({
  provider: z.literal("python"),
  python: z.object({ code: z.string().min(1) })
});

const ShellSchema = ToolCommonSchema.extend({
  provider: z.literal("shell"),
  shell: z.object({ command: z.string().min(1) })
});

const DbSchema = ToolCommonSchema.extend({
  provider: z.literal("db"),
  db: z.object({
    connectionRef: z.string().min(1),
    sql: z.string().min(1),
    params: z.record(z.string(), z.unknown()).optional()

  })
});

const BuiltinSchema = ToolCommonSchema.extend({
  provider: z.literal("builtin"),
  builtin: z.object({
    toolId: z.string().min(1),
    args: z.record(z.string(), z.unknown()).optional()

  })
});

export const ToolParamsSchema = z.discriminatedUnion("provider", [
  McpSchema,
  HttpSchema,
  FunctionSchema,
  PythonSchema,
  ShellSchema,
  DbSchema,
  BuiltinSchema
]);

export const ToolNodeDataSchema = BaseNodeDataSchema("tool", ToolParamsSchema);

export const defaultToolParams = {
  provider: "mcp" as const,
  name: "MCP Tool",
  mcp: { serverId: "local", toolName: "search_docs", args: {} }
};
