// src/lib/flow/schema/toolDefaults.ts
import type { z } from "zod";
import { ToolParamsSchema } from "./tool";

/** Infer the full union type */
export type ToolParams = z.infer<typeof ToolParamsSchema>;
export type ToolProvider = ToolParams["provider"];

/* -----------------------------
 * Provider-specific defaults
 * ----------------------------- */

export const defaultMcpToolParams = {
    provider: "mcp" as const,
    name: "MCP Tool",
    toolVersion: "v1",
    side_effect_mode: "pure" as const,
    armed: false,
    permissions: { net: true, fs: false, env: false, subprocess: false },
    mcp: {
        serverId: "local",
        toolName: "tool_name",
        args: {}
    }
};

export const defaultHttpToolParams = {
    provider: "http" as const,
    name: "HTTP Tool",
    toolVersion: "v1",
    side_effect_mode: "idempotent" as const,
    armed: false,
    permissions: { net: true, fs: false, env: false, subprocess: false },
    http: {
        url: "https://example.com",
        method: "GET",
        headers: {}
    }
};

export const defaultPythonToolParams = {
    provider: "python" as const,
    name: "Python Tool",
    toolVersion: "v1",
    side_effect_mode: "pure" as const,
    armed: false,
    permissions: { net: false, fs: false, env: false, subprocess: false },
    python: {
        code: "# write python here"
    }
};

export const defaultJsToolParams = {
    provider: "js" as const,
    name: "JavaScript Tool",
    toolVersion: "v1",
    side_effect_mode: "pure" as const,
    armed: false,
    permissions: { net: false, fs: false, env: false, subprocess: true },
    js: {
        code: "result = { ok: true };"
    }
};

export const defaultShellToolParams = {
    provider: "shell" as const,
    name: "Shell Tool",
    toolVersion: "v1",
    side_effect_mode: "effectful" as const,
    armed: false,
    permissions: { net: false, fs: true, env: true, subprocess: true },
    shell: {
        command: "echo hello"
    }
};

export const defaultFunctionToolParams = {
    provider: "function" as const,
    name: "Function Tool",
    toolVersion: "v1",
    side_effect_mode: "pure" as const,
    armed: false,
    permissions: { net: false, fs: false, env: false, subprocess: false },
    function: {
        module: "tools.module",
        export: "run",
        args: {}
    }
};

export const defaultDbToolParams = {
    provider: "db" as const,
    name: "DB Tool",
    toolVersion: "v1",
    side_effect_mode: "idempotent" as const,
    armed: false,
    permissions: { net: true, fs: false, env: false, subprocess: false },
    db: {
        connectionRef: "conn:default",
        sql: "select 1",
        params: {}
    }
};

export const defaultBuiltinToolParams = {
    provider: "builtin" as const,
    name: "Builtin Tool",
    toolVersion: "v1",
    side_effect_mode: "pure" as const,
    armed: false,
    permissions: { net: false, fs: false, env: false, subprocess: false },
    builtin: {
        toolId: "noop",
        args: {}
    }
};

/* -----------------------------
 * Canonical map (THIS is key)
 * ----------------------------- */

export const defaultToolParamsByProvider = {
    mcp: defaultMcpToolParams,
    http: defaultHttpToolParams,
    python: defaultPythonToolParams,
    js: defaultJsToolParams,
    shell: defaultShellToolParams,
    function: defaultFunctionToolParams,
    db: defaultDbToolParams,
    builtin: defaultBuiltinToolParams
} as const;

/* -----------------------------
 * Canonical default
 * ----------------------------- */

export const defaultToolParams = defaultMcpToolParams;

export const defaultToolNodeData = {
    kind: "tool" as const,
    label: "Tool",
    params: defaultToolParams,
    status: "idle" as const,
    ports: { in: "json" as const, out: "json" as const },
};
