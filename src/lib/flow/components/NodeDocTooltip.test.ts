import { describe, expect, it } from 'vitest';
import { render } from 'svelte/server';
import NodeDocTooltip from './NodeDocTooltip.svelte';

const doc = {
	schema_version: 1 as const,
	node_kind: 'model' as const,
	subtype: '*',
	title: 'Model Node',
	summary: 'Runs model inference.',
	planes: {
		data: { title: 'Data', summary: 'Consumes and emits payloads.' },
		control: { title: 'Control', summary: 'Tracks scheduler state.' },
		param: { title: 'Param', summary: 'Uses model and prompt params.' }
	},
	examples: [],
	see_also: [],
	source: 'base' as const,
	disabled: false,
	overrideApplied: false,
	runtime: {
		pendingInputCount: 0,
		inflight: 0,
		readyWork: false
	}
};

describe('NodeDocTooltip', () => {
	it('renders safe fallback text for null docs', () => {
		const rendered = render(NodeDocTooltip as any, { props: { doc: null, open: true, expanded: false } });
		expect(rendered.body).toContain('No documentation is available');
	});

	it('renders full plane sections in expanded mode', () => {
		const rendered = render(NodeDocTooltip as any, { props: { doc, open: true, expanded: true } });
		expect(rendered.body).toContain('Data Plane');
		expect(rendered.body).toContain('Control Plane');
		expect(rendered.body).toContain('Param Plane');
	});

	it('renders default explanation source label in default mode', () => {
		const rendered = render(NodeDocTooltip as any, {
			props: { doc, open: true, expanded: true, mode: 'default' }
		});
		expect(rendered.body).toContain('Default explanation');
	});

	it('renders llm explanation source label and loading state in llm mode', () => {
		const rendered = render(NodeDocTooltip as any, {
			props: {
				doc,
				open: true,
				expanded: true,
				mode: 'llm',
				nodeId: 'n_1',
				llmContext: {
					node_id: 'n_1',
					node_label: 'Model_ScoreJob',
					node_kind: 'model',
					node_subtype: 'ollama',
					settings: { model: 'glm-4.7-flash:latest' },
					planes: { data_inputs: ['in'], data_outputs: ['out'], param_inputs: [], control_inputs: [] },
					runtime: { pending_input_count: 0, inflight: 0, ready_work: false, blocked_reason_code: '' }
				},
				llmSignature: 'sig-llm'
			}
		});
		expect(rendered.body).toContain('AI-generated explanation');
		expect(rendered.body).toContain('generating...');
	});
});
