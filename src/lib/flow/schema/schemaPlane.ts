import { z } from 'zod';

export const SchemaPlaneColumnTypeSchema = z.enum([
	'string',
	'number',
	'boolean',
	'datetime',
	'binary',
	'tensor',
	'unknown'
]);

export const SchemaPlaneModeSchema = z.enum(['table', 'tensor', 'text', 'binary', 'model_artifact', 'opaque']);

export const AbstractPropertiesSchema = z
	.object({
		range: z.tuple([z.number(), z.number()]).nullable().optional(),
		normalized: z.boolean().optional(),
		device: z.enum(['cpu', 'gpu', 'any']).optional(),
		dtype: z.enum(['float32', 'float16', 'int32', 'int64', 'uint8']).optional(),
		non_negative: z.boolean().optional(),
		cardinality: z.enum(['one', 'many', 'stream']).optional(),
		class_set: z.array(z.string()).nullable().optional(),
		consume_once: z.boolean().optional(),
		sample_rate: z.number().optional(),
		architecture_signature: z.string().optional()
	})
	.passthrough();

export const SchemaPlaneColumnSchema = z
	.object({
		name: z.string().min(1),
		type: SchemaPlaneColumnTypeSchema,
		nullable: z.boolean(),
		properties: AbstractPropertiesSchema.default({})
	})
	.strip();

export const SchemaPlaneOutputSchema = z
	.object({
		mode: SchemaPlaneModeSchema,
		columns: z.array(SchemaPlaneColumnSchema).default([]),
		shape: z.array(z.union([z.number(), z.string()])).optional(),
		dtype: z.enum(['float32', 'float16', 'int32', 'int64', 'uint8']).optional(),
		properties: AbstractPropertiesSchema.optional(),
		note: z.string().optional()
	})
	.strip();

export const SchemaErrorCodeSchema = z.enum([
	'SHAPE_MISMATCH',
	'TYPE_MISMATCH',
	'MISSING_REQUIRED_INPUT',
	'CARDINALITY_CONFLICT',
	'PROPERTY_VIOLATION',
	'OPAQUE_DEPENDENCY',
	'CYCLE_DETECTED'
]);

export const SchemaErrorSchema = z
	.object({
		code: SchemaErrorCodeSchema,
		message: z.string().min(1),
		handles: z.array(z.string())
	})
	.strip();

export const SchemaPlaneResultSchema = z.discriminatedUnion('ok', [
	z
		.object({
			ok: z.literal(true),
			output: SchemaPlaneOutputSchema
		})
		.strip(),
	z
		.object({
			ok: z.literal(false),
			error: SchemaErrorSchema,
			output: SchemaPlaneOutputSchema.optional()
		})
		.strip()
]);

export const SchemaPlaneStateSchema = z
	.object({
		nodeSchemas: z.record(SchemaPlaneResultSchema),
		edgeSchemas: z.record(SchemaPlaneOutputSchema)
	})
	.strip();

export type SchemaPlaneOutputSchemaType = z.infer<typeof SchemaPlaneOutputSchema>;
export type SchemaErrorSchemaType = z.infer<typeof SchemaErrorSchema>;
export type SchemaPlaneResultSchemaType = z.infer<typeof SchemaPlaneResultSchema>;
export type SchemaPlaneStateSchemaType = z.infer<typeof SchemaPlaneStateSchema>;

