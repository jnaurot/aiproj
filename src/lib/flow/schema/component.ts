import { z } from "zod";
import { BaseNodeDataSchema } from "./base";
import { NodeDebugParamsSchema } from "./debug";

export const ComponentKindSchema = z.literal("graph_component");

const CanonicalComponentTypedPrimitiveSchema = z.enum([
	"table",
	"json",
	"text",
	"binary",
	"embeddings",
	"unknown"
]);

export const ComponentTypedPrimitiveSchema = z.preprocess((value) => {
	const normalized = String(value ?? "").trim().toLowerCase();
	if (normalized === "string") return "text";
	return value;
}, CanonicalComponentTypedPrimitiveSchema);

export const ComponentTypedFieldSchema = z
	.object({
		name: z.string().min(1),
		type: ComponentTypedPrimitiveSchema,
		nativeType: z.string().optional(),
		nullable: z.boolean().optional().default(false)
	})
	.strip();

export const ComponentTypedSchemaSchema = z
	.object({
		type: ComponentTypedPrimitiveSchema,
		fields: z.array(ComponentTypedFieldSchema).optional().default([])
	})
	.strip();

export const ComponentApiPortSchema = z
	.object({
		name: z.string().min(1),
		required: z.boolean().optional().default(true),
		typedSchema: ComponentTypedSchemaSchema
	})
	.strip();

export const ComponentApiContractSchema = z
	.preprocess((value) => {
		const raw = (value ?? {}) as Record<string, unknown>;
		const workInputs = Array.isArray(raw.workInputs)
			? raw.workInputs
			: Array.isArray(raw.inputs)
				? raw.inputs
				: [];
		return {
			...raw,
			inputs: workInputs,
			workInputs,
			paramInputs: Array.isArray(raw.paramInputs) ? raw.paramInputs : [],
			controlInputs: Array.isArray(raw.controlInputs) ? raw.controlInputs : []
		};
	}, z
		.object({
			inputs: z.array(ComponentApiPortSchema).default([]),
			workInputs: z.array(ComponentApiPortSchema).default([]),
			paramInputs: z.array(ComponentApiPortSchema).default([]),
			controlInputs: z.array(ComponentApiPortSchema).default([]),
			outputs: z.array(ComponentApiPortSchema).default([])
		})
		.strip());

export const ComponentExposureKindSchema = z.enum([
	'data_input',
	'data_output',
	'param_input',
	'control_input'
]);

export const ComponentExposureHandleSchema = z
	.object({
		handle_id: z.string().min(1),
		alias: z.string().min(1),
		internal_source_path: z.string().min(1),
		kind: ComponentExposureKindSchema,
		native_contract: ComponentTypedSchemaSchema,
		exposed: z.boolean().default(true),
		published: z.boolean().default(false),
		debug_visible: z.boolean().default(false)
	})
	.strip();

export const ComponentRefSchema = z
	.object({
		componentId: z.string().min(1),
		revisionId: z.string().min(1),
		apiVersion: z.string().min(1).optional().default("v1")
	})
	.strip();

export const ComponentBindingsSchema = z
	.object({
		inputs: z.record(z.string(), z.string()).default({}),
		config: z.record(z.string(), z.string()).optional().default({}),
		outputs: z
			.record(
				z.string(),
				z
					.object({
						outputRef: z.string().min(1).optional(),
						artifact: z.literal("current").optional().default("current")
					})
					.strip()
			)
			.optional()
			.default({})
	})
	.strip();

export const ComponentParamsSchema = z
	.object({
		componentRef: ComponentRefSchema,
		bindings: ComponentBindingsSchema.default({ inputs: {}, config: {}, outputs: {} }),
		config: z.record(z.string(), z.unknown()).optional().default({}),
		api: ComponentApiContractSchema.optional(),
		exposureRegistry: z.array(ComponentExposureHandleSchema).optional(),
		published_profile: z.array(ComponentExposureHandleSchema).optional(),
		debug_profile: z.array(ComponentExposureHandleSchema).optional(),
		debug: NodeDebugParamsSchema.optional()
	})
	.strip();

export const ComponentNodeDataSchema = BaseNodeDataSchema("component", ComponentParamsSchema)
	.extend({
		componentKind: ComponentKindSchema
	})
	.strip();

export type ComponentTypedPrimitive = z.infer<typeof ComponentTypedPrimitiveSchema>;
export type ComponentTypedField = z.infer<typeof ComponentTypedFieldSchema>;
export type ComponentTypedSchema = z.infer<typeof ComponentTypedSchemaSchema>;
export type ComponentApiPort = z.infer<typeof ComponentApiPortSchema>;
export type ComponentApiContract = z.infer<typeof ComponentApiContractSchema>;
export type ComponentExposureKind = z.infer<typeof ComponentExposureKindSchema>;
export type ComponentExposureHandle = z.infer<typeof ComponentExposureHandleSchema>;
export type ComponentRef = z.infer<typeof ComponentRefSchema>;
export type ComponentBindings = z.infer<typeof ComponentBindingsSchema>;
export type ComponentKind = z.infer<typeof ComponentKindSchema>;
export type ComponentParams = z.infer<typeof ComponentParamsSchema>;
export type ComponentNodeData = z.infer<typeof ComponentNodeDataSchema>;
