import { z } from "zod";
import { BaseNodeDataSchema } from "./base";

export const SourceKindSchema = z.enum(["file", "database", "api", "object_store", "warehouse"]);
export const SourceOutputModeSchema = z.enum(["table", "text", "json", "binary"]);

export const SourceOutputSchema = z
	.object({
		mode: SourceOutputModeSchema,
		schema: z.unknown().optional()
	})
	.strip();

const FILE_TO_DEFAULT_OUTPUT_MODE: Record<string, z.infer<typeof SourceOutputModeSchema>> = {
	csv: "table",
	tsv: "table",
	parquet: "table",
	excel: "table",
	json: "json",
	txt: "text",
	pdf: "text",
	jpg: "binary",
	jpeg: "binary",
	png: "binary",
	webp: "binary",
	gif: "binary",
	svg: "binary",
	tif: "binary",
	tiff: "binary",
	mp3: "binary",
	wav: "binary",
	flac: "binary",
	ogg: "binary",
	m4a: "binary",
	aac: "binary",
	mp4: "binary",
	mov: "binary",
	webm: "binary"
};

export const SourceIncrementalSchema = z
	.object({
		enabled: z.boolean().default(false),
		state_key: z.string().min(1).optional(),
		cursor_column: z.string().min(1).optional(),
		cursor_type: z.enum(["auto", "int", "float", "datetime", "string"]).default("auto"),
		window_start: z.string().optional(),
		window_end: z.string().optional()
	})
	.strip();

export const SourcePartitionSchema = z
	.object({
		enabled: z.boolean().default(false),
		kind: z.enum(["static_list", "numeric_shards", "date_range"]).default("static_list"),
		on_error: z.enum(["fail_fast", "skip_failed"]).default("fail_fast"),
		static_values: z.array(z.union([z.string(), z.number()])).default([]),
		numeric_start: z.number().optional(),
		numeric_end: z.number().optional(),
		numeric_step: z.number().positive().default(1),
		date_start: z.string().optional(),
		date_end: z.string().optional(),
		date_every_days: z.number().int().positive().default(1),
		bind_key: z.string().min(1).default("partition"),
		parallelism_cap: z.number().int().positive().default(2)
	})
	.strip();

export const SourceFileParamsSchema = z
	.object({
		snapshotId: z.string().regex(/^[a-f0-9]{64}$/).optional(),
		recentSnapshotIds: z.array(z.string().regex(/^[a-f0-9]{64}$/)).optional(),
		recentSnapshots: z
			.array(
				z
					.object({
						id: z.string().regex(/^[a-f0-9]{64}$/),
						filename: z.string().optional(),
						importedAt: z.string().optional(),
						size: z.number().int().nonnegative().optional(),
						mimeType: z.string().optional()
					})
					.strip()
			)
			.optional(),
		snapshotMetadata: z
			.object({
				snapshotId: z.string().regex(/^[a-f0-9]{64}$/),
				originalFilename: z.string().optional(),
				byteSize: z.number().int().nonnegative().optional(),
				mimeType: z.string().optional(),
				importedAt: z.string().optional(),
				graphId: z.string().optional()
			})
			.strip()
			.optional(),
		rel_path: z.string().min(1).optional(),
		filename: z.string().min(1).optional(),
		file_size: z.number().int().nonnegative().optional(),
		file_mime: z.string().optional(),
		file_format: z
			.enum([
				"csv",
				"tsv",
				"parquet",
				"json",
				"excel",
				"txt",
				"pdf",
				"jpg",
				"jpeg",
				"png",
				"webp",
				"gif",
				"svg",
				"tif",
				"tiff",
				"mp3",
				"wav",
				"flac",
				"ogg",
				"m4a",
				"aac",
				"mp4",
				"mov",
				"webm"
			])
			.default("txt"),
		delimiter: z.string().optional(),
		sheet_name: z.string().optional(),
		encoding: z.string().default("utf-8"),
		cache_enabled: z.boolean().default(true),
		output: SourceOutputSchema.optional()
	})
	.strip()
	.transform((v) => {
		const defaultMode = FILE_TO_DEFAULT_OUTPUT_MODE[v.file_format] ?? "binary";
		return {
			...v,
			output: v.output ?? { mode: defaultMode }
		};
	});

export const SourceDatabaseParamsSchema = z
	.object({
		connection_string: z.string().optional(),
		connection_ref: z.string().optional(),
		query: z.string().optional(),
		table_name: z.string().optional(),
		limit: z.number().int().positive().optional(),
		incremental: SourceIncrementalSchema.default({ enabled: false, cursor_type: "auto" }),
		partition: SourcePartitionSchema.default({
			enabled: false,
			kind: "static_list",
			on_error: "fail_fast",
			bind_key: "partition",
			parallelism_cap: 2
		}),
		output: SourceOutputSchema.default({ mode: "table" })
	})
	.superRefine((v, ctx) => {
		if (!v.connection_string && !v.connection_ref) {
			ctx.addIssue({ code: "custom", message: "Either connection_string or connection_ref required" });
		}
		if (!v.query && !v.table_name) {
			ctx.addIssue({ code: "custom", message: "Either query or table_name required" });
		}
	})
	.strip();

export const SourceCachePolicySchema = z
	.object({
		mode: z.enum(["default", "never", "ttl"]).default("default"),
		ttl_seconds: z.number().int().positive().optional()
	})
	.strip();

export const SourceApiContentTypeSchema = z.enum([
	"application/json",
	"application/x-www-form-urlencoded",
	"multipart/form-data",
	"text/plain",
	"application/xml"
]);

export const SourceApiBodyModeSchema = z.enum(["none", "json", "form", "raw", "multipart"]);

export const SourceApiRetryPolicySchema = z
	.object({
		max_attempts: z.number().int().positive().default(1),
		backoff_seconds: z.number().nonnegative().default(0.25),
		jitter_seconds: z.number().nonnegative().default(0.05),
		retry_on_status: z.array(z.number().int().min(100).max(599)).default([429, 500, 502, 503, 504])
	})
	.strip();

export const SourceApiRateLimitSchema = z
	.object({
		rps: z.number().positive().optional(),
		burst: z.number().int().positive().default(1)
	})
	.strip();

export const SourceAPIParamsSchema = z
	.object({
		url: z.string().url(),
		method: z.enum(["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD"]).default("GET"),
		headers: z.record(z.string(), z.string()).default({}),
		query: z.record(z.string(), z.string()).default({}),
		contentType: SourceApiContentTypeSchema.optional(),
		bodyMode: SourceApiBodyModeSchema.default("none"),
		bodyJson: z.record(z.string(), z.unknown()).optional(),
		bodyForm: z.record(z.string(), z.string()).optional(),
		bodyRaw: z.string().optional(),
		// legacy compatibility for one migration cycle
		body: z.union([z.record(z.string(), z.unknown()), z.string()]).optional(),
		__managedHeaders: z
			.object({
				contentType: z.boolean().optional()
			})
			.strip()
			.optional(),
		auth_type: z.enum(["none", "bearer", "basic", "api_key"]).default("none"),
		auth_token_ref: z.string().optional(),
		timeout_seconds: z.number().int().positive().default(30),
		incremental: SourceIncrementalSchema.default({ enabled: false, cursor_type: "auto" }),
		partition: SourcePartitionSchema.default({
			enabled: false,
			kind: "static_list",
			on_error: "fail_fast",
			bind_key: "partition",
			parallelism_cap: 2
		}),
		retry: SourceApiRetryPolicySchema.default({
			max_attempts: 1,
			backoff_seconds: 0.25,
			jitter_seconds: 0.05,
			retry_on_status: [429, 500, 502, 503, 504]
		}),
		rate_limit: SourceApiRateLimitSchema.default({ burst: 1 }),
		cache_policy: SourceCachePolicySchema.default({ mode: "default" }),
		output: SourceOutputSchema.default({ mode: "json" })
	})
	.strip()
	.transform((v) => {
		let bodyMode = v.bodyMode;
		let bodyJson = v.bodyJson;
		let bodyForm = v.bodyForm;
		let bodyRaw = v.bodyRaw;
		let contentType = v.contentType;

		if (!bodyJson && !bodyForm && bodyRaw === undefined && v.body !== undefined) {
			if (typeof v.body === "string") {
				bodyMode = "raw";
				bodyRaw = v.body;
			} else {
				bodyMode = "json";
				bodyJson = v.body as Record<string, unknown>;
			}
		}

		if (!contentType && bodyMode === "json") {
			contentType = "application/json";
		}

		if (bodyMode === "none") {
			bodyJson = undefined;
			bodyForm = undefined;
			bodyRaw = undefined;
		} else if (bodyMode === "json") {
			bodyForm = undefined;
			bodyRaw = undefined;
		} else if (bodyMode === "form" || bodyMode === "multipart") {
			bodyJson = undefined;
			bodyRaw = undefined;
		} else if (bodyMode === "raw") {
			bodyJson = undefined;
			bodyForm = undefined;
		}

		return {
			...v,
			contentType,
			bodyMode,
			bodyJson,
			bodyForm,
			bodyRaw
		};
	})
	.superRefine((v, ctx) => {
		if (v.auth_type !== "none" && !v.auth_token_ref) {
			ctx.addIssue({ code: "custom", message: "auth_token_ref required when using authentication" });
		}
	});

export const SourceObjectStoreParamsSchema = z
	.object({
		provider: z.enum(["s3", "azure_blob", "gcs"]).default("s3"),
		connection_ref: z.string().optional(),
		bucket: z.string().min(1).optional(),
		key: z.string().min(1).optional(),
		file_format: z
			.enum([
				"csv",
				"tsv",
				"parquet",
				"json",
				"excel",
				"txt",
				"pdf",
				"jpg",
				"jpeg",
				"png",
				"webp",
				"gif",
				"svg",
				"tif",
				"tiff",
				"mp3",
				"wav",
				"flac",
				"ogg",
				"m4a",
				"aac",
				"mp4",
				"mov",
				"webm"
			])
			.default("txt"),
		encoding: z.string().default("utf-8"),
		output: SourceOutputSchema.optional()
	})
	.strip()
	.transform((v) => {
		const defaultMode = FILE_TO_DEFAULT_OUTPUT_MODE[v.file_format] ?? "binary";
		return {
			...v,
			output: v.output ?? { mode: defaultMode }
		};
	})
	.superRefine((v, ctx) => {
		if (!v.key) {
			ctx.addIssue({ code: "custom", message: "key is required" });
		}
		if (!v.bucket) {
			ctx.addIssue({ code: "custom", message: "bucket is required" });
		}
	});

export const SourceWarehouseParamsSchema = z
	.object({
		provider: z.enum(["snowflake", "bigquery", "databricks_sql"]).default("snowflake"),
		connection_string: z.string().optional(),
		connection_ref: z.string().optional(),
		query: z.string().min(1),
		limit: z.number().int().positive().optional(),
		output: SourceOutputSchema.default({ mode: "table" })
	})
	.strip()
	.superRefine((v, ctx) => {
		if (!v.connection_string && !v.connection_ref) {
			ctx.addIssue({ code: "custom", message: "Either connection_string or connection_ref required" });
		}
	});

export const SourceParamsSchemaByKind = {
	file: SourceFileParamsSchema,
	database: SourceDatabaseParamsSchema,
	api: SourceAPIParamsSchema,
	object_store: SourceObjectStoreParamsSchema,
	warehouse: SourceWarehouseParamsSchema
} as const;

export const SourceNodeDataSchema = BaseNodeDataSchema(
	"source",
	z.union([
		SourceFileParamsSchema,
		SourceDatabaseParamsSchema,
		SourceAPIParamsSchema,
		SourceObjectStoreParamsSchema,
		SourceWarehouseParamsSchema
	])
).extend({
	sourceKind: SourceKindSchema
});

export type SourceFileParams = z.infer<typeof SourceFileParamsSchema>;
export type SourceDatabaseParams = z.infer<typeof SourceDatabaseParamsSchema>;
export type SourceAPIParams = z.infer<typeof SourceAPIParamsSchema>;
export type SourceObjectStoreParams = z.infer<typeof SourceObjectStoreParamsSchema>;
export type SourceWarehouseParams = z.infer<typeof SourceWarehouseParamsSchema>;
export type SourceOutputMode = z.infer<typeof SourceOutputModeSchema>;
export type SourceKind = z.infer<typeof SourceKindSchema>;
export type SourceNodeData = z.infer<typeof SourceNodeDataSchema>;
