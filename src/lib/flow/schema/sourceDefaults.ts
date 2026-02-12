//lib/flow/schema/sourceDefaults.ts
import type { SourceFileParams, SourceDatabaseParams, SourceAPIParams } from "$lib/flow/schema/source";

export const defaultSourceFileParams: SourceFileParams = {
    file_path: "file:///data.csv",
    file_format: "csv",
    delimiter: ",",
    sheet_name: "sheet1",
    sample_size: 1000,
    encoding: "utf-8",
    cache_enabled: true
};

export const defaultSourceDatabaseParams: SourceDatabaseParams = {
    connection_string: "connection_string",
    connection_ref: "connection_ref",
    query: "",
    table_name: "table_name",
    limit: 1000,
};

export const defaultSourceAPIParams: SourceAPIParams = {
    url: "",
    method: "GET",
    headers: {},
    body: undefined,
    auth_type: "none",
    auth_token_ref: undefined,
    timeout_seconds: 30
};

export const defaultSourceParamsByKind = {
    file: defaultSourceFileParams,
    database: defaultSourceDatabaseParams,
    api: defaultSourceAPIParams,
} as const;

// Optional: keep your original name as "file default"
export const defaultSourceParams = defaultSourceFileParams;

export const defaultSourceNodeData = {
    kind: "source" as const,
    sourceKind: "file" as const,
    label: "Source",
    params: defaultSourceParams,
    status: "idle" as const,
    ports: { in: null, out: "table" as const },
};