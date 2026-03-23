import { describe, expect, it } from 'vitest';

import { schemaEdgeContractBadges } from './nodeInspectorSchema';

describe('nodeInspector adapter guidance badges', () => {
	it('shows adapter badge for actionable mismatches', () => {
		const badges = schemaEdgeContractBadges({
			edgeId: 'e_adapter',
			direction: 'incoming',
			mode: 'work',
			sourceNodeId: 'n_src',
			targetNodeId: 'n_dst',
			sourceHandle: 'out',
			targetHandle: 'in',
			severity: 'error',
			suggestions: ['Insert an adapter node to convert text -> table before this target.'],
			adapterKind: 'text_to_table'
		} as any);
		expect(badges).toContain('adapter:text_to_table');
	});
});
