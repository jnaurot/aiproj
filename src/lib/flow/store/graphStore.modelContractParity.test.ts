import { describe, expect, it } from 'vitest';
import fixture from '../../../../shared/test_fixtures/model_contract_parity.v1.json';
import { deriveNodeIoForData } from '$lib/flow/store/graphStore';
import { ModelNodeDataSchema } from '$lib/flow/schema/llm';

describe('model contract parity fixtures (frontend)', () => {
	it('matches schema validation and expected io for all fixture vectors', () => {
		const cases = Array.isArray((fixture as any)?.cases) ? (fixture as any).cases : [];
		expect(cases.length).toBeGreaterThan(0);
		for (const testCase of cases) {
			const nodeData = {
				label: 'Model',
				status: 'idle',
				...(testCase?.nodeData ?? {})
			} as any;
			const parsed = ModelNodeDataSchema.safeParse(nodeData);
			expect(parsed.success).toBe(true);
			const io = deriveNodeIoForData(nodeData);
			expect(io.in).toBe(testCase.expectedInType);
			expect(io.out).toBe(testCase.expectedOutType);
		}
	});
});