import fs from 'node:fs';
import path from 'node:path';

const repoRoot = process.cwd();

function read(relPath) {
	const abs = path.join(repoRoot, relPath);
	return fs.readFileSync(abs, 'utf8');
}

function assertNoMatch(relPath, patterns) {
	const text = read(relPath);
	const hits = patterns.filter((p) => p.test(text));
	if (hits.length === 0) return;
	const details = hits.map((h) => String(h)).join(', ');
	throw new Error(`CSS ownership violation in ${relPath}: matched ${details}`);
}

try {
	assertNoMatch('src/lib/flow/FlowCanvas.svelte', [
		/\.inspector\s*\.field/,
		/\.inspector\s*\.k/,
		/\.inspector\s*\.v/,
		/\.editorScroll\s*:global\(input\|/
	]);
	assertNoMatch('src/lib/flow/components/NodeInspector.svelte', [/\.sourceFileEditor/]);
	console.log('css ownership checks passed');
} catch (err) {
	console.error(String(err instanceof Error ? err.message : err));
	process.exit(1);
}
