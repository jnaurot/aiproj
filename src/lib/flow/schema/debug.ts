import { z } from 'zod';

export const NodeDebugParamsSchema = z
	.object({
		enabled: z.boolean().optional().default(false),
		log_input_preview: z.boolean().optional().default(false),
		log_raw_output: z.boolean().optional().default(false)
	})
	.strip();

export type NodeDebugParams = z.infer<typeof NodeDebugParamsSchema>;
