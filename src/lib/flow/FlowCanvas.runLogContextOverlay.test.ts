import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { describe, expect, it } from 'vitest';

describe('FlowCanvas run log context overlay wiring', () => {
	it('supports selection-to-filter and ctrl-click context interactions', () => {
		const file = resolve(process.cwd(), 'src/lib/flow/FlowCanvas.svelte');
		const text = readFileSync(file, 'utf8');
		expect(text.includes('function beginRunLogSelection')).toBe(true);
		expect(text.includes('function applyRunLogSelectionFilter')).toBe(true);
		expect(text.includes('use:runLogInteraction')).toBe(true);
		expect(text.includes('maybeOpenRunLogContext')).toBe(true);
		expect(text.includes('if (!event.ctrlKey && !event.metaKey) return;')).toBe(true);
	});

	it('renders a closable log context overlay dialog', () => {
		const file = resolve(process.cwd(), 'src/lib/flow/FlowCanvas.svelte');
		const text = readFileSync(file, 'utf8');
		expect(text.includes('runLogContextOverlayOpen')).toBe(true);
		expect(text.includes('role="dialog"')).toBe(true);
		expect(text.includes('Log Context')).toBe(true);
		expect(text.includes('closeRunLogContextOverlay')).toBe(true);
		expect(text.includes('RUN_LOG_CONTEXT_RADIUS = 50')).toBe(true);
		expect(text.includes("scrollIntoView({ block: 'center' })")).toBe(true);
	});
});
