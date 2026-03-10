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

export const NodeSchemaEnvelopeSchema = z
	.object({
		inferredSchema: NodeSchemaObservationSchema.optional(),
		expectedSchema: NodeSchemaObservationSchema.optional(),
		observedSchema: NodeSchemaObservationSchema.optional()
	})
	.strip();

export type NodeSchemaState = z.infer<typeof NodeSchemaStateSchema>;
export type NodeSchemaSource = z.infer<typeof NodeSchemaSourceSchema>;
export type NodeSchemaObservation = z.infer<typeof NodeSchemaObservationSchema>;
export type NodeSchemaEnvelope = z.infer<typeof NodeSchemaEnvelopeSchema>;

