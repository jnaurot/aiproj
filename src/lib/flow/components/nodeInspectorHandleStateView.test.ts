import { describe, expect, it } from 'vitest';

import { collectNodeHandleStates } from './nodeInspectorSchema';

describe('nodeInspector handle state view helpers', () => {
	it('filters and orders runtime handle states for selected node', () => {
		const states = collectNodeHandleStates('n_target', {
			'n_other:in': { state: 'busy', updatedAt: '2026-03-23T10:00:00Z' },
			'n_target:param_filters': { state: 'blocked', updatedAt: '2026-03-23T10:00:01Z' },
			'n_target:in': { state: 'ready', updatedAt: '2026-03-23T10:00:02Z' }
		});
		expect(states.map((row) => row.handle)).toEqual(['in', 'param_filters']);
		expect(states[0]?.state).toBe('ready');
		expect(states[1]?.state).toBe('blocked');
	});
});
