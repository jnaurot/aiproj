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

function walkFiles(dir, predicate, out = []) {
	for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
		const abs = path.join(dir, entry.name);
		if (entry.isDirectory()) {
			walkFiles(abs, predicate, out);
			continue;
		}
		if (predicate(abs)) out.push(abs);
	}
	return out;
}

function rel(abs) {
	return path.relative(repoRoot, abs).replaceAll('\\', '/');
}

function assertNoFieldLayoutOutsideFieldComponent() {
	const root = path.join(repoRoot, 'src/lib/flow');
	const files = walkFiles(
		root,
		(abs) => abs.endsWith('.svelte') || abs.endsWith('.css')
	).map(rel);
	const allow = new Set([
		'src/lib/flow/components/ui/Field.svelte',
		'src/lib/flow/styles/inspectorForm.css'
	]);
	const selectorBlocks = [
		{ selector: /\.field\b[^{]*\{[^}]*\}/gms, props: /\b(display|grid-template-columns|grid-template-rows|flex|position|min-width|max-width|width|left|right|top|bottom)\s*:/m },
		{ selector: /\.k\b[^{]*\{[^}]*\}/gms, props: /\b(display|grid-template-columns|grid-template-rows|flex|position|min-width|max-width|width|left|right|top|bottom)\s*:/m },
		{ selector: /\.v\b[^{]*\{[^}]*\}/gms, props: /\b(display|grid-template-columns|grid-template-rows|flex|position|min-width|max-width|width|left|right|top|bottom)\s*:/m }
	];
	for (const file of files) {
		if (allow.has(file)) continue;
		const text = read(file);
		for (const rule of selectorBlocks) {
			const blocks = text.match(rule.selector) || [];
			for (const block of blocks) {
				if (rule.props.test(block)) {
					throw new Error(`CSS ownership violation in ${file}: .field/.k/.v layout authority is Field.svelte only`);
				}
			}
		}
	}
}

function assertInspectorGlobalScopeAllowlist() {
	const root = path.join(repoRoot, 'src/lib/flow');
	const files = walkFiles(root, (abs) => abs.endsWith('.svelte') || abs.endsWith('.css')).map(rel);
	const allow = new Set([
		'src/lib/flow/styles/inspectorForm.css',
		'src/lib/flow/FlowCanvas.svelte'
	]);
	for (const file of files) {
		if (allow.has(file)) continue;
		const text = read(file);
		if (/:global\(\.inspector\b/.test(text)) {
			throw new Error(`CSS ownership violation in ${file}: :global(.inspector...) only allowed in inspectorForm.css/FlowCanvas.svelte`);
		}
	}
}

function assertTokensHaveNoConcreteColors() {
	const text = read('src/lib/flow/styles/tokens.css');
	const bad = [/#(?:[0-9a-fA-F]{3,8})\b/, /\brgb(a)?\(/i, /\bhsl(a)?\(/i];
	const hit = bad.find((p) => p.test(text));
	if (hit) {
		throw new Error(`CSS ownership violation in src/lib/flow/styles/tokens.css: concrete color literal found (${hit})`);
	}
}

try {
	assertNoMatch('src/lib/flow/FlowCanvas.svelte', [
		/\.inspector\s*\.field/,
		/\.inspector\s*\.k/,
		/\.inspector\s*\.v/
	]);
	assertNoMatch('src/lib/flow/components/NodeInspector.svelte', [/\.sourceFileEditor/]);
	assertNoFieldLayoutOutsideFieldComponent();
	assertInspectorGlobalScopeAllowlist();
	assertTokensHaveNoConcreteColors();
	console.log('css ownership checks passed');
} catch (err) {
	console.error(String(err instanceof Error ? err.message : err));
	process.exit(1);
}
