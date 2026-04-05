import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { describe, expect, it } from 'vitest';

describe('NodeInspector component editor context wiring', () => {
	it('passes editingContext to ComponentEditor', () => {
		const file = resolve(process.cwd(), 'src/lib/flow/components/NodeInspector.svelte');
		const text = readFileSync(file, 'utf8');
		expect(text.includes('editingContext={$graphStore.editingContext}')).toBe(true);
	});
});

