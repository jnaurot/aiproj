import { describe, expect, it } from 'vitest';

import { __stripToDTOForTest } from './graphStore';

describe('graphStore persisted graph payload flags', () => {
	it('always omits node data.ports in persisted payload', () => {
		const node = {
			id: 'n1',
			type: 'source',
			position: { x: 0, y: 0 },
			data: {
				kind: 'source',
				label: 'Source',
				sourceKind: 'file',
				params: { file_format: 'csv', output: { mode: 'table' } },
				status: 'idle',
				ports: { in: null, out: 'table' }
			}
		} as any;
		const dto = __stripToDTOForTest([node], [], 'graph_persist_ports');
		const persistedNode = (dto.nodes ?? [])[0] as any;
		expect(node).toBeTruthy();
		expect(Object.prototype.hasOwnProperty.call(persistedNode?.data ?? {}, 'ports')).toBe(false);
		expect(Boolean((dto.meta as any)?.migrations?.OMIT_NODE_PORTS_V1)).toBe(true);
	});
});
