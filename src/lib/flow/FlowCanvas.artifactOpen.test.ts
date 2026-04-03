import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { describe, expect, it } from 'vitest';

describe('FlowCanvas artifact-open wiring', () => {
	it('passes runId into openArtifactFromRunLog from run-log rows', () => {
		const filePath = resolve(process.cwd(), 'src/lib/flow/FlowCanvas.svelte');
		const src = readFileSync(filePath, 'utf8');
		expect(src).toContain("void openArtifactFromRunLog(artifactId, String((l as any)?.runId ?? ''))");
	});

	it('includes run_log source and runId query params in artifact URL builder', () => {
		const filePath = resolve(process.cwd(), 'src/lib/flow/FlowCanvas.svelte');
		const src = readFileSync(filePath, 'utf8');
		expect(src).toContain("params.set('source', 'run_log')");
		expect(src).toContain("params.set('runId', resolvedRunId)");
		expect(src).toContain("const url = `/artifacts/${encodeURIComponent(aid)}?${params.toString()}`");
	});

	it('artifact page consumes runId and attempts graph resolution via getRun', () => {
		const filePath = resolve(process.cwd(), 'src/routes/artifacts/[id]/+page.svelte');
		const src = readFileSync(filePath, 'utf8');
		expect(src).toContain("import { getRun } from '$lib/flow/client/runs';");
		expect(src).toContain("$page.url.searchParams.get('runId')");
		expect(src).toContain('const run = await getRun(runId);');
		expect(src).toContain('resolvedGraphId = runGraphId;');
	});
});

