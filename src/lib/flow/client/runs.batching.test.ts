import { describe, expect, it, vi } from 'vitest';

import { createEventBatcher } from './runs';

describe('createEventBatcher', () => {
	it('flushes on max batch size', () => {
		const batches: number[][] = [];
		const batcher = createEventBatcher<number>((events) => batches.push(events), {
			maxBatchSize: 2,
			maxDelayMs: 100
		});
		batcher.push(1);
		batcher.push(2);
		expect(batches).toEqual([[1, 2]]);
	});

	it('flushes on timer', async () => {
		vi.useFakeTimers();
		try {
			const batches: number[][] = [];
			const batcher = createEventBatcher<number>((events) => batches.push(events), {
				maxBatchSize: 10,
				maxDelayMs: 20
			});
			batcher.push(1);
			expect(batches).toEqual([]);
			vi.advanceTimersByTime(20);
			await Promise.resolve();
			expect(batches).toEqual([[1]]);
		} finally {
			vi.useRealTimers();
		}
	});
});
