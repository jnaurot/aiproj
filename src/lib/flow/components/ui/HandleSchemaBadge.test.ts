/**
 * Phase 2b — HandleSchemaBadge unit tests
 *
 * The component renders a coloured badge (⚠ / ✕) on a node input handle
 * when a schema warning or error exists in Schema View.
 *
 * Since the test environment is node-only (no DOM), we test the badge
 * logic as pure functions that mirror the template conditions.
 */

import { describe, expect, it } from 'vitest';

type Severity = 'error' | 'warning' | null;
type ViewMode = 'execution' | 'schema';

/** Mirrors: {#if viewMode === 'schema' && severity !== null} */
function badgeShouldRender(viewMode: ViewMode, severity: Severity): boolean {
	return viewMode === 'schema' && severity !== null;
}

/** Mirrors: severity === 'error' ? '✕' : '⚠' */
function badgeGlyph(severity: Severity): string | null {
	if (severity === 'error') return '✕';
	if (severity === 'warning') return '⚠';
	return null;
}

/** Mirrors: `handle-schema-badge--${severity}` class */
function badgeClass(severity: Severity): string | null {
	if (severity === 'error') return 'handle-schema-badge--error';
	if (severity === 'warning') return 'handle-schema-badge--warning';
	return null;
}

describe('HandleSchemaBadge — render conditions', () => {
	it('renders warning badge in schema view', () => {
		expect(badgeShouldRender('schema', 'warning')).toBe(true);
	});

	it('renders error badge in schema view', () => {
		expect(badgeShouldRender('schema', 'error')).toBe(true);
	});

	it('does NOT render in execution view regardless of severity', () => {
		expect(badgeShouldRender('execution', 'error')).toBe(false);
		expect(badgeShouldRender('execution', 'warning')).toBe(false);
		expect(badgeShouldRender('execution', null)).toBe(false);
	});

	it('does NOT render when severity is null in schema view', () => {
		expect(badgeShouldRender('schema', null)).toBe(false);
	});
});

describe('HandleSchemaBadge — glyph', () => {
	it('error → ✕', () => {
		expect(badgeGlyph('error')).toBe('✕');
	});

	it('warning → ⚠', () => {
		expect(badgeGlyph('warning')).toBe('⚠');
	});

	it('null → no glyph', () => {
		expect(badgeGlyph(null)).toBeNull();
	});
});

describe('HandleSchemaBadge — CSS class', () => {
	it('error → handle-schema-badge--error', () => {
		expect(badgeClass('error')).toBe('handle-schema-badge--error');
	});

	it('warning → handle-schema-badge--warning', () => {
		expect(badgeClass('warning')).toBe('handle-schema-badge--warning');
	});
});

// ---------------------------------------------------------------------------
// handleErrorsByNodeId derivation logic
// ---------------------------------------------------------------------------

type EdgeStub = {
	id: string;
	target: string;
	targetHandle: string;
	severity: 'error' | 'warning' | null;
};

/**
 * Mirror the reactive block in BaseNode.svelte:
 *   For each edge targeting nodeId, record the highest severity per handle.
 *   Error wins over warning.
 */
function deriveHandleSeverities(
	nodeId: string,
	edges: EdgeStub[]
): Map<string, 'error' | 'warning'> {
	const result = new Map<string, 'error' | 'warning'>();
	for (const edge of edges) {
		if (edge.target !== nodeId) continue;
		if (!edge.severity) continue;
		const handleId = edge.targetHandle || 'in';
		const current = result.get(handleId);
		if (!current || (current === 'warning' && edge.severity === 'error')) {
			result.set(handleId, edge.severity);
		}
	}
	return result;
}

describe('handleErrorsByNodeId derivation', () => {
	it('records error on target handle for error edge', () => {
		const edges: EdgeStub[] = [{ id: 'e1', target: 'n1', targetHandle: 'in', severity: 'error' }];
		const result = deriveHandleSeverities('n1', edges);
		expect(result.get('in')).toBe('error');
	});

	it('source node handle is NOT badged', () => {
		const edges: EdgeStub[] = [{ id: 'e1', target: 'n2', targetHandle: 'in', severity: 'error' }];
		// n1 is the source, not the target
		const result = deriveHandleSeverities('n1', edges);
		expect(result.size).toBe(0);
	});

	it('two edges into same handle: higher severity wins', () => {
		const edges: EdgeStub[] = [
			{ id: 'e1', target: 'n1', targetHandle: 'in', severity: 'warning' },
			{ id: 'e2', target: 'n1', targetHandle: 'in', severity: 'error' }
		];
		const result = deriveHandleSeverities('n1', edges);
		expect(result.get('in')).toBe('error');
	});

	it('two edges into same handle: warning does not overwrite existing error', () => {
		const edges: EdgeStub[] = [
			{ id: 'e1', target: 'n1', targetHandle: 'in', severity: 'error' },
			{ id: 'e2', target: 'n1', targetHandle: 'in', severity: 'warning' }
		];
		const result = deriveHandleSeverities('n1', edges);
		expect(result.get('in')).toBe('error');
	});

	it('edges with null severity are not recorded', () => {
		const edges: EdgeStub[] = [{ id: 'e1', target: 'n1', targetHandle: 'in', severity: null }];
		const result = deriveHandleSeverities('n1', edges);
		expect(result.size).toBe(0);
	});

	it('different handles on same node are tracked independently', () => {
		const edges: EdgeStub[] = [
			{ id: 'e1', target: 'n1', targetHandle: 'in', severity: 'error' },
			{ id: 'e2', target: 'n1', targetHandle: 'param_model', severity: 'warning' }
		];
		const result = deriveHandleSeverities('n1', edges);
		expect(result.get('in')).toBe('error');
		expect(result.get('param_model')).toBe('warning');
	});
});
