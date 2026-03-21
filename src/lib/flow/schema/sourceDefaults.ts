//lib/flow/schema/sourceDefaults.ts
import type {
	SourceFileParams,
	SourceDatabaseParams,
	SourceAPIParams,
	SourceObjectStoreParams,
	SourceWarehouseParams
} from "$lib/flow/schema/source";

export const defaultSourceFileParams: SourceFileParams = {
    snapshotId: undefined,
    recentSnapshotIds: [],
    recentSnapshots: [],
    snapshotMetadata: undefined,
    rel_path: ".",
    filename: "data.txt",
    file_format: "txt",
    delimiter: ",",
    quote_char: "\"",
    escape_char: "\\",
    malformed_row_policy: "fail",
    decimal_separator: ".",
    thousands_separator: undefined,
    date_columns: [],
    date_format: undefined,
    json_mode: "auto",
    json_streaming_enabled: false,
    json_stream_chunk_lines: 1000,
    json_stream_max_records: undefined,
    json_flatten_strategy: "none",
    json_flatten_separator: ".",
    excel_import_strategy: "single",
    excel_sheets: [],
    txt_record_mode: "raw",
    txt_chunk_size: 1000,
    pdf_extraction_mode: "text",
    image_extract_metadata: true,
    image_svg_policy: "sanitize",
    image_tiff_pages_mode: "first",
    parquet_columns: [],
    parquet_row_groups: [],
    parquet_max_rows: undefined,
    sheet_name: "Sheet1",
    encoding: "utf-8",
    cache_enabled: true,
    priming: { enabled: false, mode: "advisory", drift_policy: "soft", sample_rows: 50, sample_bytes: 65536, timeout_ms: 1500 },
    output: { mode: "text" }
};

export const defaultSourceDatabaseParams: SourceDatabaseParams = {
    connection_ref: "conn:default",
    table_name: "my_table",
    limit: 1000,
    incremental: {
		enabled: false,
		cursor_type: "auto"
	},
	partition: {
		enabled: false,
		kind: "static_list",
		on_error: "fail_fast",
		bind_key: "partition",
		parallelism_cap: 2,
		static_values: [],
		numeric_step: 1,
		date_every_days: 1
	},
	priming: { enabled: false, mode: "advisory", drift_policy: "soft", sample_rows: 50, sample_bytes: 65536, timeout_ms: 1500 },
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
	incremental: {
		enabled: false,
		cursor_type: "auto"
	},
	partition: {
		enabled: false,
		kind: "static_list",
		on_error: "fail_fast",
		bind_key: "partition",
		parallelism_cap: 2,
		static_values: [],
		numeric_step: 1,
		date_every_days: 1
	},
	retry: {
		max_attempts: 1,
		backoff_seconds: 0.25,
		jitter_seconds: 0.05,
		retry_on_status: [429, 500, 502, 503, 504]
	},
	rate_limit: {
		burst: 1
	},
	cache_policy: { mode: "default" },
	priming: { enabled: false, mode: "advisory", drift_policy: "soft", sample_rows: 50, sample_bytes: 65536, timeout_ms: 1500 },
	output: { mode: "json" }
};

export const defaultSourceObjectStoreParams: SourceObjectStoreParams = {
	provider: "s3",
	connection_ref: "conn:object_store_default",
	bucket: "my-bucket",
	key: "data.txt",
	file_format: "txt",
	encoding: "utf-8",
	priming: { enabled: false, mode: "advisory", drift_policy: "soft", sample_rows: 50, sample_bytes: 65536, timeout_ms: 1500 },
	output: { mode: "text" }
};

export const defaultSourceWarehouseParams: SourceWarehouseParams = {
	provider: "snowflake",
	connection_ref: "conn:warehouse_default",
	query: "select * from my_table",
	limit: 1000,
	priming: { enabled: false, mode: "advisory", drift_policy: "soft", sample_rows: 50, sample_bytes: 65536, timeout_ms: 1500 },
	output: { mode: "table" }
};

export const defaultSourceParamsByKind = {
	file: defaultSourceFileParams,
	database: defaultSourceDatabaseParams,
	api: defaultSourceAPIParams,
	object_store: defaultSourceObjectStoreParams,
	warehouse: defaultSourceWarehouseParams
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
