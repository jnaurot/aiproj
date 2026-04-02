import { describe, expect, it } from 'vitest';

import {
	buildNodeExecutionBadge,
	normalizeConsumeMode,
	resolveNodeRuntimeCounts
} from './nodeExecutionBadge';

describe('nodeExecutionBadge', () => {
	it('normalizes consume mode safely', () => {
		expect(normalizeConsumeMode({ consume_mode: 'once' })).toBe('once');
		expect(normalizeConsumeMode({ consume_mode: 'single_item' })).toBe('single_item');
		expect(normalizeConsumeMode({ consume_mode: 'batch' })).toBe('batch');
		expect(normalizeConsumeMode({ consume_mode: 'weird' })).toBe('once');
	});

	it('resolves node counters from run-scoped metrics first', () => {
		const counts = resolveNodeRuntimeCounts(
			{
				runScoped: {
					runtimeItemMetrics: {
						nodeCounters: {
							n1: { accepted: 7, rejected: 2 }
						}
					}
				},
				runtimeItemMetrics: {
					nodeCounters: {
						n1: { accepted: 99, rejected: 99 }
					}
				}
			},
			'n1'
		);
		expect(counts).toEqual({ accepted: 7, rejected: 2, total: 9 });
	});

	it('builds once/single/batch badge details', () => {
		expect(buildNodeExecutionBadge('once', { accepted: 0, rejected: 0, total: 0 }, 1)).toEqual({
			mode: 'once',
			label: 'once',
			detail: '0/1'
		});
		expect(buildNodeExecutionBadge('once', { accepted: 1, rejected: 0, total: 1 }, 1)).toEqual({
			mode: 'once',
			label: 'once',
			detail: '1/1'
		});
		expect(buildNodeExecutionBadge('single_item', { accepted: 4, rejected: 1, total: 5 }, 1)).toEqual({
			mode: 'single_item',
			label: 'single',
			detail: '5'
		});
		expect(buildNodeExecutionBadge('batch', { accepted: 9, rejected: 1, total: 10 }, 4)).toEqual({
			mode: 'batch',
			label: 'batch',
			detail: '10/3'
		});
	});
});
