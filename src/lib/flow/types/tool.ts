// src/lib/flow/types/tool.ts
import type { BaseNodeData } from "./base";

export type ToolProvider = "mcp" | "http" | "function" | "python" | "shell" | "db" | "builtin";

type ToolCommon = {
  name: string;
  timeoutMs?: number;
  retries?: { max: number; backoffMs?: number };

  input?: {
    schema?: unknown;
    mapping?: Record<string, string>;
  };
  output?: {
    schema?: unknown;
    mode?: "json" | "text" | "binary";
  };
};

export type McpToolParams = ToolCommon & {
  provider: "mcp";
  mcp: {
    serverId: string;
    toolName: string;
    args?: Record<string, unknown>;
  };
};

export type HttpToolParams = ToolCommon & {
  provider: "http";
  http: {
    url: string;
    method: "GET" | "POST" | "PUT" | "PATCH" | "DELETE";
    headers?: Record<string, string>;
    query?: Record<string, string | number | boolean>;
    body?: unknown;
  };
};

export type FunctionToolParams = ToolCommon & {
  provider: "function";
  function: {
    module: string;
    export: string;
    args?: Record<string, unknown>;
  };
};

export type PythonToolParams = ToolCommon & {
  provider: "python";
  python: { code: string };
};

export type ShellToolParams = ToolCommon & {
  provider: "shell";
  shell: { command: string };
};

export type DbToolParams = ToolCommon & {
  provider: "db";
  db: {
    connectionRef: string;
    sql: string;
    params?: Record<string, unknown>;
  };
};

export type BuiltinToolParams = ToolCommon & {
  provider: "builtin";
  builtin: {
    name: string;                 // e.g. "parse_csv" / "vector_search"
    args?: Record<string, unknown>;
  };
};

export type ToolParams =
  | McpToolParams
  | HttpToolParams
  | FunctionToolParams
  | PythonToolParams
  | ShellToolParams
  | DbToolParams
  | BuiltinToolParams;

export type ToolNodeData = BaseNodeData<"tool", ToolParams>;
