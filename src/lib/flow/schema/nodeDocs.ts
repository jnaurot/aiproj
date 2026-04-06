import { z } from 'zod';

const DISALLOWED_GENERATED_EXPLANATION_FIELDS = new Set([
	'params',
	'runtime',
	'runStatus',
	'nodeOutputs',
	'nodeBindings',
	'edges',
	'nodes',
	'contracts',
	'scheduler'
]);

export const NodeDocPlaneKindSchema = z.enum(['data', 'control', 'param']);
export const NodeDocExplanationModeSchema = z.enum(['default', 'llm']);

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

export const NodeDocGeneratedProviderMetaSchema = z
	.object({
		provider: z.string().min(1).optional(),
		model: z.string().min(1).optional()
	})
	.strip();

export const NodeDocGeneratedExplanationSchema = z
	.object({
		summary: z.string().min(1),
		settings_explained: z.array(z.string().min(1)).default([]),
		context_notes: z.array(z.string().min(1)).default([]),
		generated_at: z.string().min(1),
		signature_key: z.string().min(1),
		provider_meta: NodeDocGeneratedProviderMetaSchema.optional()
	})
	.strip();

export type NodeDocGeneratedExplanationParseResult = {
	value: NodeDocGeneratedExplanation | null;
	reason: 'ok' | 'invalid_shape' | 'disallowed_fields';
};

export function parseNodeDocGeneratedExplanation(
	input: unknown
): NodeDocGeneratedExplanationParseResult {
	if (!input || typeof input !== 'object') {
		return { value: null, reason: 'invalid_shape' };
	}
	for (const key of Object.keys(input as Record<string, unknown>)) {
		if (DISALLOWED_GENERATED_EXPLANATION_FIELDS.has(String(key ?? '').trim())) {
			return { value: null, reason: 'disallowed_fields' };
		}
	}
	const parsed = NodeDocGeneratedExplanationSchema.safeParse(input);
	if (!parsed.success) return { value: null, reason: 'invalid_shape' };
	return { value: parsed.data, reason: 'ok' };
}

export function sanitizeNodeDocGeneratedExplanation(input: unknown): NodeDocGeneratedExplanation | null {
	return parseNodeDocGeneratedExplanation(input).value;
}

export type NodeDocV1 = z.infer<typeof NodeDocV1Schema>;
export type NodeDocOverride = z.infer<typeof NodeDocOverrideSchema>;
export type NodeDocExplanationMode = z.infer<typeof NodeDocExplanationModeSchema>;
export type NodeDocGeneratedExplanation = z.infer<typeof NodeDocGeneratedExplanationSchema>;
