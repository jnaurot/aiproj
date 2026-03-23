import { z } from "zod";
import { ComponentTypedSchemaSchema } from "./component";

export const NodeSchemaStateSchema = z.enum(["fresh", "partial", "stale", "unknown"]);

export const NodeSchemaSourceSchema = z.enum([
	"sample",
	"artifact",
	"declared",
	"runtime",
	"component_contract",
	"unknown"
]);

export const NodeSchemaObservationSchema = z
	.object({
		typedSchema: ComponentTypedSchemaSchema.optional(),
		source: NodeSchemaSourceSchema.optional().default("unknown"),
		state: NodeSchemaStateSchema.optional().default("unknown"),
		schemaFingerprint: z.string().optional(),
		updatedAt: z.string().optional()
	})
	.strip();

export const NodeInputClassContractSchema = z
	.object({
		defaultSchema: NodeSchemaObservationSchema.optional(),
		handles: z.record(z.string(), NodeSchemaObservationSchema).optional().default({})
	})
	.strip();

export const NodeSchemaEnvelopeSchema = z
	.object({
		inferredSchema: NodeSchemaObservationSchema.optional(),
		expectedInputSchemas: z.record(z.string(), NodeSchemaObservationSchema).optional(),
		workInputs: NodeInputClassContractSchema.optional(),
		paramInputs: NodeInputClassContractSchema.optional(),
		controlInputs: NodeInputClassContractSchema.optional(),
		expectedSchema: NodeSchemaObservationSchema.optional(),
		observedSchema: NodeSchemaObservationSchema.optional()
	})
	.strip();

export const EdgeContractSnapshotSchema = z
	.object({
		sourceSchemaFingerprint: z.string().optional(),
		targetSchemaFingerprint: z.string().optional(),
		compatible: z.boolean().optional(),
		decision: z.enum(['native', 'coerced', 'adapter', 'incompatible']).optional(),
		coercion: z
			.object({
				allowed: z.boolean().optional(),
				lossy: z.boolean().optional(),
				mode: z.enum(['native', 'widened', 'coerced']).optional()
			})
			.strip()
			.optional(),
		updatedAt: z.string().optional()
	})
	.strip();

export type NodeSchemaState = z.infer<typeof NodeSchemaStateSchema>;
export type NodeSchemaSource = z.infer<typeof NodeSchemaSourceSchema>;
export type NodeSchemaObservation = z.infer<typeof NodeSchemaObservationSchema>;
export type NodeInputClassContract = z.infer<typeof NodeInputClassContractSchema>;
export type NodeSchemaEnvelope = z.infer<typeof NodeSchemaEnvelopeSchema>;
export type EdgeContractSnapshot = z.infer<typeof EdgeContractSnapshotSchema>;

