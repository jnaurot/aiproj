import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { spawnSync } from 'node:child_process';

function fail(message) {
	console.error(`[status-contract] ${message}`);
	process.exit(1);
}

const fixturePath = resolve(process.cwd(), 'src/lib/flow/components/backend-node-states.fixture.json');
let frontendStates;
try {
	const raw = JSON.parse(readFileSync(fixturePath, 'utf8'));
	if (!Array.isArray(raw)) fail(`fixture is not an array: ${fixturePath}`);
	frontendStates = raw.map((value) => String(value ?? '').trim()).filter(Boolean).sort();
} catch (error) {
	fail(`unable to read frontend fixture: ${String(error)}`);
}

const pythonCode = [
	'import json',
	'from app.runner.execution_state import NODE_STATES',
	'print(json.dumps(sorted(list(NODE_STATES))))'
].join('; ');

const python = spawnSync('python', ['-c', pythonCode], {
	cwd: resolve(process.cwd(), 'backend'),
	encoding: 'utf8'
});
if (python.error || python.status !== 0) {
	const detail = python.error ? String(python.error) : String(python.stderr || '').trim();
	fail(
		`unable to extract backend NODE_STATES via python import. ` +
			`Ensure Python/backend env is available. Detail: ${detail}`
	);
}

let backendStates;
try {
	const parsed = JSON.parse(String(python.stdout ?? '[]'));
	if (!Array.isArray(parsed)) fail('backend NODE_STATES extraction did not return JSON array');
	backendStates = parsed.map((value) => String(value ?? '').trim()).filter(Boolean).sort();
} catch (error) {
	fail(`unable to parse backend NODE_STATES output: ${String(error)}`);
}

const backendSet = new Set(backendStates);
const frontendSet = new Set(frontendStates);

const missingOnFrontend = backendStates.filter((value) => !frontendSet.has(value));
const extraOnFrontend = frontendStates.filter((value) => !backendSet.has(value));

if (missingOnFrontend.length > 0 || extraOnFrontend.length > 0) {
	console.error('[status-contract] backend/frontend node status sets differ');
	console.error(`[status-contract] missing_on_frontend=${JSON.stringify(missingOnFrontend)}`);
	console.error(`[status-contract] extra_on_frontend=${JSON.stringify(extraOnFrontend)}`);
	process.exit(1);
}

console.log(`[status-contract] OK (${backendStates.length} states)`);
