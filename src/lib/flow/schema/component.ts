import { z } from "zod";
import { BaseNodeDataSchema } from "./base";

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
	.object({
		inputs: z.array(ComponentApiPortSchema).default([]),
		outputs: z.array(ComponentApiPortSchema).default([])
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
						artifact: z.enum(["current", "last"]).optional().default("current")
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
		api: ComponentApiContractSchema.optional()
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
export type ComponentRef = z.infer<typeof ComponentRefSchema>;
export type ComponentBindings = z.infer<typeof ComponentBindingsSchema>;
export type ComponentKind = z.infer<typeof ComponentKindSchema>;
export type ComponentParams = z.infer<typeof ComponentParamsSchema>;
export type ComponentNodeData = z.infer<typeof ComponentNodeDataSchema>;
