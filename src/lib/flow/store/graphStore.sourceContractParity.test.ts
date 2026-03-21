import { describe, expect, it } from 'vitest';
import fixture from '../../../../shared/test_fixtures/source_contract_parity.v1.json';
import { deriveNodeIoForData } from '$lib/flow/store/graphStore';

describe('source contract parity fixtures (frontend)', () => {
	it('matches expected source out types for all fixture vectors', () => {
		const cases = Array.isArray((fixture as any)?.cases) ? (fixture as any).cases : [];
		expect(cases.length).toBeGreaterThan(0);
		for (const testCase of cases) {
			const nodeData = {
				label: 'Source',
				status: 'idle',
				...(testCase?.nodeData ?? {})
			} as any;
			const io = deriveNodeIoForData(nodeData);
			expect(io.out).toBe(testCase.expectedOutType);
		}
	});
});
