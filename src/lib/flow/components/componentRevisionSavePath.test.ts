import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { describe, expect, it } from 'vitest';

describe('component revision save path', () => {
	it('uses component-edit session contract draft when validating and creating revision', () => {
		const file = resolve(process.cwd(), 'src/lib/flow/FlowCanvas.svelte');
		const text = readFileSync(file, 'utf8');
		expect(text.includes('const draftParams = ((session.contractDraftParams ?? {}) as Record<string, any>);')).toBe(true);
		expect(text.includes('const draftApi = draftParams?.api;')).toBe(true);
		expect(text.includes('exposureRegistry: draftExposureRegistry')).toBe(true);
	});

	it('tracks pin metadata in component dirty snapshot detection', () => {
		const file = resolve(process.cwd(), 'src/lib/flow/FlowCanvas.svelte');
		const text = readFileSync(file, 'utf8');
		expect(text.includes('function canonicalPersistentNodeMeta')).toBe(true);
		expect(text.includes('meta: canonicalPersistentNodeMeta(data)')).toBe(true);
	});
});
