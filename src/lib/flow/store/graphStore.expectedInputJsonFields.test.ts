import { describe, expect, it } from 'vitest';
import { get } from 'svelte/store';

import { graphStore } from './graphStore';

describe('graphStore expected input json fields', () => {
	it('preserves declared json fields for input handles', () => {
		graphStore.hardResetGraph();
		const nodeId = graphStore.addNode('transform', { x: 0, y: 0 });
		const updated = graphStore.setNodeExpectedInputSchemaForHandle(nodeId, 'in', {
			type: 'json',
			fields: ['thing1', 'thing2']
		});
		expect(updated.ok).toBe(true);
		const state = get(graphStore as any);
		const node = state.nodes.find((n: any) => n.id === nodeId);
		const fields = (node?.data?.schema?.expectedInputSchemas?.in?.typedSchema?.fields ?? []) as Array<{
			name?: string;
		}>;
		expect(fields.map((field) => String(field?.name ?? ''))).toEqual(['thing1', 'thing2']);
	});
});

