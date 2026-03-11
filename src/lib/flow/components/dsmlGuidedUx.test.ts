import { describe, expect, it } from 'vitest';

import type { Node } from '@xyflow/svelte';

import type { PipelineNodeData } from '$lib/flow/types';
import {
	DSML_STARTER_TEMPLATES,
	getOperationPresetsForKind,
	getStarterTemplateById,
	recommendNextStep
} from './dsmlGuidedUx';

function asNode(id: string, kind: PipelineNodeData['kind']): Node<PipelineNodeData> {
	return {
		id,
		type: kind,
		position: { x: 0, y: 0 },
		data: { kind, label: id, params: {} } as PipelineNodeData
	};
}

describe('dsmlGuidedUx', () => {
	it('exposes starter templates with connected nodes/edges', () => {
		expect(DSML_STARTER_TEMPLATES.length).toBeGreaterThan(0);
		for (const template of DSML_STARTER_TEMPLATES) {
			expect(template.nodes.length).toBeGreaterThan(1);
			expect(template.edges.length).toBeGreaterThan(0);
			const ids = new Set(template.nodes.map((n) => n.id));
			for (const edge of template.edges) {
				expect(ids.has(edge.source)).toBe(true);
				expect(ids.has(edge.target)).toBe(true);
			}
		}
	});

	it('resolves template ids and operation presets deterministically', () => {
		const first = DSML_STARTER_TEMPLATES[0];
		expect(getStarterTemplateById(first.id)?.name).toBe(first.name);
		expect(getStarterTemplateById('missing')).toBeNull();
		expect(getOperationPresetsForKind('tool').length).toBeGreaterThan(0);
		expect(getOperationPresetsForKind('source').length).toBe(0);
	});

	it('recommends next step by pipeline progression', () => {
		expect(recommendNextStep([], null).id).toBe('rec_add_source');
		expect(recommendNextStep([asNode('n1', 'source')], 'source').id).toBe('rec_add_transform');
		expect(recommendNextStep([asNode('n1', 'source'), asNode('n2', 'transform')], 'transform').id).toBe(
			'rec_add_tool'
		);
		expect(
			recommendNextStep([asNode('n1', 'source'), asNode('n2', 'transform'), asNode('n3', 'tool')], 'tool').id
		).toBe('rec_apply_tool_preset');
		expect(
			recommendNextStep([asNode('n1', 'source'), asNode('n2', 'transform'), asNode('n3', 'tool')], 'transform').id
		).toBe('rec_run_pipeline');
	});

	it('contains a starter smoke path from source to tool', () => {
		const classification = getStarterTemplateById('starter_classification_baseline');
		expect(classification).not.toBeNull();
		const nodes = classification?.nodes ?? [];
		const sourceNode = nodes.find((n) => n.kind === 'source');
		const toolNode = nodes.find((n) => n.kind === 'tool');
		expect(sourceNode).toBeTruthy();
		expect(toolNode).toBeTruthy();
		expect(
			(classification?.edges ?? []).some((edge) => edge.source === sourceNode?.id && edge.target !== sourceNode?.id)
		).toBe(true);
		expect(
			(classification?.edges ?? []).some((edge) => edge.target === toolNode?.id && edge.source !== toolNode?.id)
		).toBe(true);
	});

	it('returns recommendation copy suitable for accessible UI labels', () => {
		const recommendations = [
			recommendNextStep([], null),
			recommendNextStep([asNode('n1', 'source')], 'source'),
			recommendNextStep([asNode('n1', 'source'), asNode('n2', 'transform')], 'transform'),
			recommendNextStep([asNode('n1', 'source'), asNode('n2', 'transform'), asNode('n3', 'tool')], 'tool'),
			recommendNextStep([asNode('n1', 'source'), asNode('n2', 'transform'), asNode('n3', 'tool')], 'transform')
		];
		const ids = new Set<string>();
		for (const recommendation of recommendations) {
			expect(recommendation.id.trim().length).toBeGreaterThan(0);
			expect(ids.has(recommendation.id)).toBe(false);
			ids.add(recommendation.id);
			expect(recommendation.label.trim().length).toBeGreaterThan(0);
			expect(recommendation.description.trim().length).toBeGreaterThan(0);
		}
	});
});
