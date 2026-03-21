import { describe, expect, it } from 'vitest';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';

import { ModelNodeDataSchema } from './llm';

describe('model node example templates', () => {
	it('validates shared example payloads against schema', () => {
		const filePath = resolve(process.cwd(), 'shared/examples/model_node_examples.json');
		const payload = JSON.parse(readFileSync(filePath, 'utf-8'));
		const nodes = Array.isArray(payload?.nodes) ? payload.nodes : [];
		expect(nodes.length).toBeGreaterThan(0);
		for (const node of nodes) {
			const parsed = ModelNodeDataSchema.safeParse(node);
			expect(parsed.success).toBe(true);
		}
	});
});
