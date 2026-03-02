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

function stripComments(text) {
	return text.replace(/\/\*[\s\S]*?\*\//g, '');
}

function hasUnallowedHexLiteral(text) {
	const cleaned = stripComments(text);
	const hex = /#[0-9a-fA-F]{3,8}\b/g;
	let match;
	while ((match = hex.exec(cleaned)) !== null) {
		const idx = match.index;
		const prev = idx > 0 ? cleaned[idx - 1] : '';
		const nextIdx = idx + match[0].length;
		const next = nextIdx < cleaned.length ? cleaned[nextIdx] : '';
		const validPrefix = idx === 0 || /[:\s(,=]/.test(prev);
		const validSuffix = next === '' || /[\s;),}]/.test(next);
		if (validPrefix && validSuffix) return true;
	}
	return false;
}

function hasConcreteColorFunction(text) {
	const cleaned = stripComments(text);
	return /\brgba?\(/i.test(cleaned) || /\bhsla?\(/i.test(cleaned);
}

const HEX_COLOR_MIGRATION_ALLOWLIST = {
	'src/lib/flow/FlowCanvas.svelte': {
		reason: 'Legacy palette literals pending token migration',
		removeBy: '2026-06-30'
	},
	'src/lib/flow/components/ArtifactViewer.svelte': {
		reason: 'Legacy palette literals pending token migration',
		removeBy: '2026-06-30'
	},
	'src/lib/flow/components/BoolSelect.svelte': {
		reason: 'Legacy palette literals pending token migration',
		removeBy: '2026-06-30'
	},
	'src/lib/flow/components/KeyValueEditor.svelte': {
		reason: 'Legacy fallback literals pending token migration',
		removeBy: '2026-06-30'
	},
	'src/lib/flow/components/JsonTreeNode.svelte': {
		reason: 'Legacy palette literals pending token migration',
		removeBy: '2026-06-30'
	},
	'src/lib/flow/components/NodeInspector.svelte': {
		reason: 'Legacy component-local theme vars pending central theme migration',
		removeBy: '2026-06-30'
	},
	'src/lib/flow/components/NumberInput.svelte': {
		reason: 'Legacy palette literals pending token migration',
		removeBy: '2026-06-30'
	},
	'src/lib/flow/components/OutputModal.svelte': {
		reason: 'Legacy palette literals pending token migration',
		removeBy: '2026-06-30'
	},
	'src/lib/flow/components/PortsEditor.svelte': {
		reason: 'Intentional local themed skin; migrate to shared theme later',
		removeBy: '2026-06-30'
	},
	'src/lib/flow/components/editors/SourceEditor/SourceAPIEditor.svelte': {
		reason: 'Legacy palette literals pending token migration',
		removeBy: '2026-06-30'
	},
	'src/lib/flow/components/editors/SourceEditor/SourceFileEditor.svelte': {
		reason: 'Legacy palette literals pending token migration',
		removeBy: '2026-06-30'
	},
	'src/lib/flow/components/editors/TransformEditor/TransformAggregateEditor.svelte': {
		reason: 'Legacy palette literals pending token migration',
		removeBy: '2026-06-30'
	},
	'src/lib/flow/components/editors/TransformEditor/TransformDeriveEditor.svelte': {
		reason: 'Legacy palette literals pending token migration',
		removeBy: '2026-06-30'
	},
	'src/lib/flow/components/editors/TransformEditor/TransformDedupeEditor.svelte': {
		reason: 'Legacy palette literals pending token migration',
		removeBy: '2026-06-30'
	},
	'src/lib/flow/components/editors/TransformEditor/TransformJoinEditor.svelte': {
		reason: 'Legacy palette literals pending token migration',
		removeBy: '2026-06-30'
	},
	'src/lib/flow/components/editors/TransformEditor/TransformLimitEditor.svelte': {
		reason: 'Legacy palette literals pending token migration',
		removeBy: '2026-06-30'
	},
	'src/lib/flow/components/editors/TransformEditor/TransformPythonEditor.svelte': {
		reason: 'Legacy palette literals pending token migration',
		removeBy: '2026-06-30'
	},
	'src/lib/flow/components/editors/TransformEditor/TransformRenameEditor.svelte': {
		reason: 'Legacy palette literals pending token migration',
		removeBy: '2026-06-30'
	},
	'src/lib/flow/components/editors/TransformEditor/TransformSelectEditor.svelte': {
		reason: 'Legacy palette literals pending token migration',
		removeBy: '2026-06-30'
	},
	'src/lib/flow/components/editors/TransformEditor/TransformSortEditor.svelte': {
		reason: 'Legacy palette literals pending token migration',
		removeBy: '2026-06-30'
	},
	'src/lib/flow/components/editors/TransformEditor/TransformSqlEditor.svelte': {
		reason: 'Legacy palette literals pending token migration',
		removeBy: '2026-06-30'
	},
	'src/lib/flow/components/ui/Disclosure.svelte': {
		reason: 'New component still using direct literals; migrate to tokens',
		removeBy: '2026-06-30'
	},
	'src/lib/flow/components/ui/Field.svelte': {
		reason: 'Legacy rgba usage pending token migration',
		removeBy: '2026-06-30'
	},
	'src/lib/flow/components/ui/Input.svelte': {
		reason: 'Legacy rgba usage pending token migration',
		removeBy: '2026-06-30'
	},
	'src/lib/flow/components/ui/Section.svelte': {
		reason: 'Legacy rgba usage pending token migration',
		removeBy: '2026-06-30'
	},
	'src/lib/flow/components/ui/theme.css': {
		reason: 'Legacy UI theme file pending deprecation/migration',
		removeBy: '2026-06-30'
	},
	'src/lib/flow/nodes/BaseNode.svelte': {
		reason: 'Legacy node chrome palette pending token migration',
		removeBy: '2026-06-30'
	},
	'src/lib/ui/SplitPane.svelte': {
		reason: 'Legacy split pane palette pending token migration',
		removeBy: '2026-06-30'
	},
	'src/routes/artifacts/[id]/+page.svelte': {
		reason: 'Route-local palette pending token migration',
		removeBy: '2026-06-30'
	}
};

const MAX_HEX_ALLOWLIST_COUNT = 29;

function assertAllowlistGovernance() {
	const entries = Object.entries(HEX_COLOR_MIGRATION_ALLOWLIST);
	if (entries.length > MAX_HEX_ALLOWLIST_COUNT) {
		throw new Error(
			`CSS ownership violation: hex allowlist grew to ${entries.length} (max ${MAX_HEX_ALLOWLIST_COUNT}). Migrate colors instead of adding entries.`
		);
	}
	for (const [file, meta] of entries) {
		if (!meta || typeof meta.reason !== 'string' || !meta.reason.trim()) {
			throw new Error(`CSS ownership violation: allowlist entry ${file} missing reason`);
		}
		if (!meta || typeof meta.removeBy !== 'string' || !meta.removeBy.trim()) {
			throw new Error(`CSS ownership violation: allowlist entry ${file} missing removeBy date`);
		}
	}
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
		{
			selector: /(?:^|[\s,>+~]|:global\()\.field\b[^{]*\{[^}]*\}/gms,
			props: /\b(display|grid-template-columns|grid-template-rows|grid-auto-flow|grid-auto-columns|grid-auto-rows|flex-direction|flex-wrap|position|min-width|max-width|width|left|right|top|bottom|align-items|justify-content|place-items|place-content|gap|row-gap|column-gap)\s*:/m
		},
		{
			selector: /(?:^|[\s,>+~]|:global\()\.k\b[^{]*\{[^}]*\}/gms,
			props: /\b(display|grid-template-columns|grid-template-rows|grid-auto-flow|grid-auto-columns|grid-auto-rows|flex|flex-grow|flex-shrink|flex-basis|align-self|justify-self|position|min-width|max-width|width|left|right|top|bottom)\s*:/m
		},
		{
			selector: /(?:^|[\s,>+~]|:global\()\.v\b[^{]*\{[^}]*\}/gms,
			props: /\b(display|grid-template-columns|grid-template-rows|grid-auto-flow|grid-auto-columns|grid-auto-rows|flex|flex-grow|flex-shrink|flex-basis|align-self|justify-self|position|min-width|max-width|width|left|right|top|bottom)\s*:/m
		}
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
	if (hasUnallowedHexLiteral(text) || hasConcreteColorFunction(text)) {
		throw new Error(
			'CSS ownership violation in src/lib/flow/styles/tokens.css: concrete color literal found (hex/rgb/hsl not allowed)'
		);
	}
}

function assertNoHexOutsideThemes() {
	const root = path.join(repoRoot, 'src');
	const files = walkFiles(root, (abs) => abs.endsWith('.css') || abs.endsWith('.svelte')).map(rel);
	const allow = new Set([
		'src/lib/flow/styles/themes.css',
		...Object.keys(HEX_COLOR_MIGRATION_ALLOWLIST)
	]);
	for (const file of files) {
		if (allow.has(file)) continue;
		const text = read(file);
		if (hasUnallowedHexLiteral(text)) {
			throw new Error(`CSS ownership violation in ${file}: hex color literal found outside themes.css`);
		}
	}
}

function assertNoColorFunctionsOutsideThemes() {
	const root = path.join(repoRoot, 'src');
	const files = walkFiles(root, (abs) => abs.endsWith('.css') || abs.endsWith('.svelte')).map(rel);
	const allow = new Set([
		'src/lib/flow/styles/themes.css',
		...Object.keys(HEX_COLOR_MIGRATION_ALLOWLIST)
	]);
	for (const file of files) {
		if (allow.has(file)) continue;
		const text = read(file);
		if (hasConcreteColorFunction(text)) {
			throw new Error(`CSS ownership violation in ${file}: rgb/hsl color function literal found outside themes.css`);
		}
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
	assertAllowlistGovernance();
	assertTokensHaveNoConcreteColors();
	assertNoHexOutsideThemes();
	assertNoColorFunctionsOutsideThemes();
	console.log('css ownership checks passed');
} catch (err) {
	console.error(String(err instanceof Error ? err.message : err));
	process.exit(1);
}
