import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { describe, expect, it } from 'vitest';

import ComponentEditor from './ComponentEditor.svelte';

describe('ComponentEditor', () => {
	it('loads component editor module', () => {
		expect(ComponentEditor).toBeTruthy();
	});

	it('describes config as param payload convention in UI copy', () => {
		const file = resolve(process.cwd(), 'src/lib/flow/components/editors/ComponentEditor/ComponentEditor.svelte');
		const text = readFileSync(file, 'utf8');
		expect(text.includes('Param Payload (config)')).toBe(true);
		expect(text.includes('Component config is a param-plane payload convention, not a separate plane.')).toBe(true);
	});
});
