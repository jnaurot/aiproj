import { describe, expect, it } from 'vitest';

import { __setGraphPersistenceFeatureFlagsForTest } from '$lib/flow/portCapabilities';
import { __stripToDTOForTest } from './graphStore';

describe('graphStore persisted graph payload flags', () => {
	it('keeps node data.ports in persisted payload when omit flag is disabled', () => {
		__setGraphPersistenceFeatureFlagsForTest({ GRAPH_PERSIST_DERIVED_PORTS_OMITTED: false });
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
		expect(persistedNode?.data?.ports).toBeTruthy();
	});

	it('omits node data.ports in persisted payload and marks migration when flag is enabled', () => {
		__setGraphPersistenceFeatureFlagsForTest({ GRAPH_PERSIST_DERIVED_PORTS_OMITTED: true });
		const node = {
			id: 'n2',
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
		__setGraphPersistenceFeatureFlagsForTest({ GRAPH_PERSIST_DERIVED_PORTS_OMITTED: false });
	});
});
