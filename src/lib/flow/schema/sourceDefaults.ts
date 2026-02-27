//lib/flow/schema/sourceDefaults.ts
import type { SourceFileParams, SourceDatabaseParams, SourceAPIParams } from "$lib/flow/schema/source";

export const defaultSourceFileParams: SourceFileParams = {
    snapshotId: undefined,
    recentSnapshotIds: [],
    recentSnapshots: [],
    snapshotMetadata: undefined,
    rel_path: ".",
    filename: "data.csv",
    file_format: "csv",
    delimiter: ",",
    sheet_name: "Sheet1",
    sample_size: 1000,
    encoding: "utf-8",
    cache_enabled: true,
    output: { mode: "table" }
};

export const defaultSourceDatabaseParams: SourceDatabaseParams = {
    connection_ref: "conn:default",
    table_name: "my_table",
    limit: 1000,
    output: { mode: "table" },
};

export const defaultSourceAPIParams: SourceAPIParams = {
    url: "https://example.com/api",
    method: "GET",
    headers: {},
    body: undefined,
    auth_type: "none",
    auth_token_ref: undefined,
    timeout_seconds: 30,
    cache_policy: { mode: "default" },
    output: { mode: "json" }
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
