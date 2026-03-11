//lib/flow/schema/sourceDefaults.ts
import type { SourceFileParams, SourceDatabaseParams, SourceAPIParams } from "$lib/flow/schema/source";

export const defaultSourceFileParams: SourceFileParams = {
    snapshotId: undefined,
    recentSnapshotIds: [],
    recentSnapshots: [],
    snapshotMetadata: undefined,
    rel_path: ".",
    filename: "data.txt",
    file_format: "txt",
    delimiter: ",",
    sheet_name: "Sheet1",
    encoding: "utf-8",
    cache_enabled: true,
    output: { mode: "text" }
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
	query: {},
	contentType: undefined,
	bodyMode: "none",
	bodyJson: undefined,
	bodyForm: undefined,
	bodyRaw: undefined,
	__managedHeaders: { contentType: true },
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
};
