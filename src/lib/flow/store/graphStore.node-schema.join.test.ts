import { describe, expect, it } from 'vitest';
import { sameHandleProvidedSchemaConflict } from './graphStore.node-schema';

function sourceNode(id: string, outputType: 'text' | 'table') {
	return {
		id,
		data: {
			kind: 'source',
			label: id,
			status: 'idle',
			params: {},
			schema:
				outputType === 'table'
					? {
							expectedSchema: {
								typedSchema: {
									type: 'table',
									fields: [{ name: 'id', type: 'number', nullable: false }]
								}
							}
						}
					: {
							expectedSchema: {
								typedSchema: { type: 'text', fields: [] }
							}
						}
		}
	} as any;
}

describe('sameHandleProvidedSchemaConflict (join exemption)', () => {
	it('allows heterogeneous same-handle work edges for join targets', () => {
		const nodes = [
			sourceNode('src_text', 'text'),
			sourceNode('src_table', 'table'),
			{
				id: 'join_node',
				data: { kind: 'transform', transformKind: 'join', label: 'Join', status: 'idle', params: { op: 'join' } }
			}
		] as any[];
		const existing = [
			{
				id: 'e1',
				source: 'src_text',
				target: 'join_node',
				sourceHandle: 'out',
				targetHandle: 'in',
				data: { exec: 'idle', mode: 'work' }
			},
			{
				id: 'e2',
				source: 'src_table',
				target: 'join_node',
				sourceHandle: 'out',
				targetHandle: 'in',
				data: { exec: 'idle', mode: 'work' }
			}
		] as any[];
		const candidate = {
			id: 'e2',
			source: 'src_table',
			target: 'join_node',
			sourceHandle: 'out',
			targetHandle: 'in',
			data: { exec: 'idle', mode: 'work' }
		} as any;
		const result = sameHandleProvidedSchemaConflict(nodes as any, existing as any, candidate);
		expect(result.conflict).toBe(false);
	});

	it('still detects heterogeneous same-handle work edges for non-join targets', () => {
		const nodes = [
			sourceNode('src_text', 'text'),
			sourceNode('src_table', 'table'),
			{
				id: 'select_node',
				data: { kind: 'transform', transformKind: 'select', label: 'Select', status: 'idle', params: { op: 'select' } }
			}
		] as any[];
		const existing = [
			{
				id: 'e1',
				source: 'src_text',
				target: 'select_node',
				sourceHandle: 'out',
				targetHandle: 'in',
				data: { exec: 'idle', mode: 'work' }
			}
		] as any[];
		const candidate = {
			id: 'e2',
			source: 'src_table',
			target: 'select_node',
			sourceHandle: 'out',
			targetHandle: 'in',
			data: { exec: 'idle', mode: 'work' }
		} as any;
		const result = sameHandleProvidedSchemaConflict(nodes as any, existing as any, candidate);
		expect(result.conflict).toBe(true);
	});
});
