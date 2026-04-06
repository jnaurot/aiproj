import { describe, expect, it } from 'vitest';
import type { NodeDocRegistry } from './nodeDocsRegistry';
import {
	hasNodeDocWildcardForKind,
	resolveNodeDocBase,
	supportedNodeDocKinds,
	validateNodeDocRegistry
} from './nodeDocsResolver';
import { NODE_DOCS_REGISTRY } from './nodeDocsRegistry';

function section(title: string) {
	return { title, summary: `${title} summary` };
}

const testRegistry: NodeDocRegistry = {
	source: {
		'*': {
			schema_version: 1,
			node_kind: 'source',
			subtype: '*',
			title: 'Source wildcard',
			summary: 'Source wildcard summary',
			planes: {
				data: section('Data'),
				control: section('Control'),
				param: section('Param')
			}
		},
		file: {
			schema_version: 1,
			node_kind: 'source',
			subtype: 'file',
			title: 'Source file',
			summary: 'Source file summary',
			planes: {
				data: section('Data'),
				control: section('Control'),
				param: section('Param')
			}
		}
	},
	transform: {
		'*': {
			schema_version: 1,
			node_kind: 'transform',
			subtype: '*',
			title: 'Transform wildcard',
			summary: 'Transform wildcard summary',
			planes: {
				data: section('Data'),
				control: section('Control'),
				param: section('Param')
			}
		}
	},
	model: {
		'*': {
			schema_version: 1,
			node_kind: 'model',
			subtype: '*',
			title: 'Model wildcard',
			summary: 'Model wildcard summary',
			planes: {
				data: section('Data'),
				control: section('Control'),
				param: section('Param')
			}
		}
	},
	tool: {
		'*': {
			schema_version: 1,
			node_kind: 'tool',
			subtype: '*',
			title: 'Tool wildcard',
			summary: 'Tool wildcard summary',
			planes: {
				data: section('Data'),
				control: section('Control'),
				param: section('Param')
			}
		}
	},
	component: {
		'*': {
			schema_version: 1,
			node_kind: 'component',
			subtype: '*',
			title: 'Component wildcard',
			summary: 'Component wildcard summary',
			planes: {
				data: section('Data'),
				control: section('Control'),
				param: section('Param')
			}
		}
	}
};

describe('node docs resolver', () => {
	it('prefers exact kind+subtype over wildcard', () => {
		const resolved = resolveNodeDocBase('source', 'file', testRegistry);
		expect(resolved?.title).toBe('Source file');
	});

	it('falls back to wildcard when subtype is missing', () => {
		const resolved = resolveNodeDocBase('source', 'api', testRegistry);
		expect(resolved?.title).toBe('Source wildcard');
	});

	it('returns null for unknown kinds safely', () => {
		const resolved = resolveNodeDocBase('unknown', 'x', testRegistry);
		expect(resolved).toBeNull();
	});

	it('maps llm kind to model wildcard docs', () => {
		const resolved = resolveNodeDocBase('llm', 'ollama', testRegistry);
		expect(resolved?.node_kind).toBe('model');
	});

	it('validates registry shape with actionable failure', () => {
		expect(() => validateNodeDocRegistry(testRegistry)).not.toThrow();
	});

	it('enforces wildcard coverage for all supported node kinds', () => {
		const kinds = supportedNodeDocKinds();
		for (const kind of kinds) {
			const result = hasNodeDocWildcardForKind(testRegistry, kind);
			expect(result.ok).toBe(true);
		}
	});

	it('ships wildcard base docs for all supported kinds in the real registry', () => {
		const kinds = supportedNodeDocKinds();
		for (const kind of kinds) {
			const result = hasNodeDocWildcardForKind(NODE_DOCS_REGISTRY, kind);
			expect(result.ok).toBe(true);
		}
	});

	it('returns null safely for malformed registry entries', () => {
		const malformedRegistry = structuredClone(testRegistry);
		(malformedRegistry as any).model['*'] = { invalid: true };
		const resolved = resolveNodeDocBase('model', 'ollama', malformedRegistry as any);
		expect(resolved).toBeNull();
	});
});
