import { describe, expect, it } from 'vitest';

import { schemaEdgeContractBadges } from './nodeInspectorSchema';

describe('schemaEdgeContractBadges', () => {
	it('includes drift/coercion/adapter badges when applicable', () => {
		const badges = schemaEdgeContractBadges({
			edgeId: 'e1',
			mode: 'work',
			direction: 'incoming',
			sourceNodeId: 'a',
			targetNodeId: 'b',
			sourceHandle: 'out',
			targetHandle: 'in',
			providedSchema: { type: 'json' },
			requiredSchema: { type: 'text' },
			severity: 'warning',
			snapshotDrift: true,
			suggestions: ['accept coercion or insert adapter'],
			adapterKind: 'json_to_table' as any
		} as any);
		expect(badges).toContain('drift');
		expect(badges).toContain('coercion');
		expect(badges).toContain('adapter:json_to_table');
	});
});

