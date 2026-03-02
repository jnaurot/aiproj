import { z } from "zod";
import { BaseNodeDataSchema } from "./base";

export const SourceKindSchema = z.enum(["file", "database", "api"]);
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
	pdf: "text"
};

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
		file_format: z.enum(["csv", "tsv", "parquet", "json", "excel", "txt", "pdf"]).default("txt"),
		delimiter: z.string().optional(),
		sheet_name: z.string().optional(),
		sample_size: z.number().int().positive().optional(),
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

export const SourceParamsSchemaByKind = {
	file: SourceFileParamsSchema,
	database: SourceDatabaseParamsSchema,
	api: SourceAPIParamsSchema
} as const;

export const SourceNodeDataSchema = BaseNodeDataSchema(
	"source",
	z.union([SourceFileParamsSchema, SourceDatabaseParamsSchema, SourceAPIParamsSchema])
).extend({
	sourceKind: SourceKindSchema
});

export type SourceFileParams = z.infer<typeof SourceFileParamsSchema>;
export type SourceDatabaseParams = z.infer<typeof SourceDatabaseParamsSchema>;
export type SourceAPIParams = z.infer<typeof SourceAPIParamsSchema>;
export type SourceOutputMode = z.infer<typeof SourceOutputModeSchema>;
export type SourceKind = z.infer<typeof SourceKindSchema>;
export type SourceNodeData = z.infer<typeof SourceNodeDataSchema>;
