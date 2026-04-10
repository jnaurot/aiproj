import { z } from 'zod';

export const MemoKeySchema = z
	.string()
	.regex(/^[0-9a-f]{64}$/, 'MemoKey must be 64-char hex');

export const CheckpointStalenessSchema = z.enum([
	'valid',
	'stale',
	'artifact_missing',
	'unknown'
]);

const CheckpointOutputLineageSchema = z
	.object({
		artifactId: z.string().min(1),
		execKey: z.string().optional()
	})
	.strip();

export const CheckpointRecordSchema = z
	.object({
		id: z.string().uuid(),
		name: z.string().min(1),
		description: z.string().optional(),
		nodeId: z.string().min(1),
		graphId: z.string().min(1),
		runId: z.string().min(1),
		artifactId: z.string().min(1),
		execKey: z.string().min(1),
		fingerprintAtCreation: MemoKeySchema,
		createdAt: z.string().datetime(),
		staleness: CheckpointStalenessSchema,
		outputs: z.record(z.string(), CheckpointOutputLineageSchema).optional()
	})
	.strip();

export const CheckpointRegistrySchema = z.record(z.string(), CheckpointRecordSchema);

export const CheckpointExecutionHintsSchema = z
	.object({
		checkpoints: z.record(
			z.string(),
			z
				.object({
					artifactId: z.string().min(1),
					execKey: z.string().min(1),
					fingerprintAtCreation: MemoKeySchema,
					outputs: z.record(z.string(), CheckpointOutputLineageSchema).optional()
				})
				.strip()
		)
	})
	.strip();
