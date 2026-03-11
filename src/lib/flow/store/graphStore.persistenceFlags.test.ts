import { describe, expect, it } from 'vitest';

import { __stripToDTOForTest } from './graphStore';

describe('graphStore persisted graph payload', () => {
	it('preserves canonical node schema payload in persisted output', () => {
		const node = {
			id: 'n1',
			type: 'source',
			position: { x: 0, y: 0 },
			data: {
				kind: 'source',
				label: 'Source',
				sourceKind: 'file',
				params: { file_format: 'csv', output: { mode: 'table' } },
				status: 'idle'
			}
		} as any;
		const dto = __stripToDTOForTest([node], [], 'graph_persist_ports');
		const persistedNode = (dto.nodes ?? [])[0] as any;
		expect(node).toBeTruthy();
		expect(String(persistedNode?.data?.kind ?? '')).toBe('source');
	});
});
