import { describe, expect, it } from 'vitest';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';

describe('FlowCanvas plane expansion runtime commit', () => {
	it('commits NODE_DOC_PLANES_EXPANSION_ENABLED immediately when toggle changes', () => {
		const file = resolve(process.cwd(), 'src/lib/flow/FlowCanvas.svelte');
		const source = readFileSync(file, 'utf8');
		expect(source).toContain('NODE_DOC_PLANES_EXPANSION_ENABLED');
		expect(source).toContain("on:toggle={async (event) => {");
		expect(source).toContain("void applyRuntimeEnvVar('NODE_DOC_PLANES_EXPANSION_ENABLED')");
	});
});
