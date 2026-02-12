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
    mcp: {
        serverId: "local",
        toolName: "",
        args: {}
    }
};

export const defaultHttpToolParams = {
    provider: "http" as const,
    name: "HTTP Tool",
    http: {
        url: "https://",
        method: "GET",
        headers: {}
    }
};

export const defaultPythonToolParams = {
    provider: "python" as const,
    name: "Python Tool",
    python: {
        code: "# write python here"
    }
};

export const defaultShellToolParams = {
    provider: "shell" as const,
    name: "Shell Tool",
    shell: {
        command: ""
    }
};

export const defaultFunctionToolParams = {
    provider: "function" as const,
    name: "Function Tool",
    function: {
        module: "",
        export: "",
        args: {}
    }
};

export const defaultDbToolParams = {
    provider: "db" as const,
    name: "DB Tool",
    db: {
        connectionRef: "",
        sql: "",
        params: {}
    }
};

export const defaultBuiltinToolParams = {
    provider: "builtin" as const,
    name: "Builtin Tool",
    builtin: {
        toolId: "",
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
