import { describe, expect, it } from 'vitest';
import { firstEnabledIndex, nextEnabledIndex } from './toolbarMenu';

describe('toolbarMenu helpers', () => {
	it('finds first enabled item', () => {
		expect(firstEnabledIndex([{ id: 'a', label: 'A', disabled: true }, { id: 'b', label: 'B' }])).toBe(1);
	});

	it('cycles to next enabled item with keyboard direction', () => {
		const items = [
			{ id: 'a', label: 'A' },
			{ id: 'b', label: 'B', disabled: true },
			{ id: 'c', label: 'C' }
		];
		expect(nextEnabledIndex(items, 0, 1)).toBe(2);
		expect(nextEnabledIndex(items, 2, 1)).toBe(0);
		expect(nextEnabledIndex(items, 0, -1)).toBe(2);
	});
});
