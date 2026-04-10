import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { describe, expect, it } from 'vitest';

describe('FlowCanvas pin controls removal', () => {
	it('does not render manual pin toggle controls in inspector header', () => {
		const file = resolve(process.cwd(), 'src/lib/flow/FlowCanvas.svelte');
		const text = readFileSync(file, 'utf8');
		expect(text.includes('cycleSelectedPinMode')).toBe(false);
		expect(text.includes('selectedPinPillClass')).toBe(false);
		expect(text.includes('setSelectedNodeFreezeMode(')).toBe(false);
		expect(text.includes('Cycle pin mode: unpinned')).toBe(false);
	});
});
