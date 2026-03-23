import { describe, expect, it } from 'vitest';

import { collectExpectedInputHandles } from '$lib/flow/components/nodeInspectorSchema';

describe('nodeInspector uniform handle policy coverage', () => {
	it('collects at least one editable input handle across all primary node kinds', () => {
		const cases = [
			{ id: 'n_source', data: { kind: 'source', portDeclarations: { in: { in: { plane: 'work' } } } } },
			{ id: 'n_transform', data: { kind: 'transform', portDeclarations: { in: { in: { plane: 'work' } } } } },
			{ id: 'n_model', data: { kind: 'model', portDeclarations: { in: { in: { plane: 'work' } } } } },
			{ id: 'n_tool', data: { kind: 'tool', portDeclarations: { in: { in: { plane: 'work' } } } } }
		] as any[];
		for (const node of cases) {
			const handles = collectExpectedInputHandles(node, [] as any);
			expect(handles.length).toBeGreaterThan(0);
			expect(handles[0]?.handle).toBe('in');
		}
	});
});

