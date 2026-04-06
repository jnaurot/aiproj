import { z } from 'zod';

export const NodeDocPlaneKindSchema = z.enum(['data', 'control', 'param']);

export const NodeDocPortRefSchema = z
	.object({
		handle: z.string().min(1),
		plane: NodeDocPlaneKindSchema,
		direction: z.enum(['in', 'out']),
		cardinality: z.enum(['one', 'many']).optional(),
		required: z.boolean().optional(),
		item_mode: z.enum(['artifact', 'json_items', 'table_rows']).optional()
	})
	.strip();

export const NodeDocPlaneSectionSchema = z
	.object({
		title: z.string().min(1),
		summary: z.string().min(1),
		ports: z.array(NodeDocPortRefSchema).optional().default([]),
		notes: z.array(z.string().min(1)).optional().default([])
	})
	.strip();

export const NodeDocExampleSchema = z
	.object({
		label: z.string().min(1),
		input: z.string().optional(),
		output: z.string().optional()
	})
	.strip();

export const NodeDocOverrideSchema = z
	.object({
		summary: z.string().optional(),
		notes: z.array(z.string().min(1)).optional(),
		disabled: z.boolean().optional()
	})
	.strip();

export const NodeDocV1Schema = z
	.object({
		schema_version: z.literal(1),
		node_kind: z.enum(['source', 'transform', 'model', 'tool', 'component']),
		subtype: z.string().optional(),
		title: z.string().min(1),
		summary: z.string().min(1),
		planes: z
			.object({
				data: NodeDocPlaneSectionSchema,
				control: NodeDocPlaneSectionSchema,
				param: NodeDocPlaneSectionSchema
			})
			.strip(),
		examples: z.array(NodeDocExampleSchema).optional().default([]),
		see_also: z.array(z.string().min(1)).optional().default([])
	})
	.strip();

export type NodeDocV1 = z.infer<typeof NodeDocV1Schema>;
export type NodeDocOverride = z.infer<typeof NodeDocOverrideSchema>;

