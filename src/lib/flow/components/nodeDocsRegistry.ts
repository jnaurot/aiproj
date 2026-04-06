import type { NodeDocV1 } from '$lib/flow/schema/nodeDocs';

type NodeDocKind = NodeDocV1['node_kind'];

export type NodeDocRegistry = Record<NodeDocKind, Record<string, NodeDocV1>>;

const section = (title: string, summary: string) => ({ title, summary, ports: [], notes: [] as string[] });

export const NODE_DOCS_REGISTRY: NodeDocRegistry = {
	source: {
		'*': {
			schema_version: 1,
			node_kind: 'source',
			subtype: '*',
			title: 'Source Node',
			summary: 'Produces data-plane payloads from external files, APIs, or stores.',
			planes: {
				data: section('Data Plane', 'Outputs work-plane payloads for downstream nodes.'),
				control: section('Control Plane', 'Reports readiness and completion for scheduling.'),
				param: section('Param Plane', 'Uses source settings, format, and capability options.')
			}
		}
	},
	transform: {
		'*': {
			schema_version: 1,
			node_kind: 'transform',
			subtype: '*',
			title: 'Transform Node',
			summary: 'Transforms incoming data-plane payloads into a new shape.',
			planes: {
				data: section('Data Plane', 'Consumes input payloads and emits transformed output payloads.'),
				control: section('Control Plane', 'Participates in scheduler admission and blocked-state reporting.'),
				param: section('Param Plane', 'Uses transform-specific settings and schema constraints.')
			}
		}
	},
	model: {
		'*': {
			schema_version: 1,
			node_kind: 'model',
			subtype: '*',
			title: 'Model Node',
			summary: 'Runs model inference on incoming data-plane items.',
			planes: {
				data: section('Data Plane', 'Consumes model input items and emits inference output items.'),
				control: section('Control Plane', 'Tracks lease/running state, blocked reasons, and lifecycle.'),
				param: section('Param Plane', 'Uses model/provider params, prompts, and output mode settings.')
			}
		}
	},
	tool: {
		'*': {
			schema_version: 1,
			node_kind: 'tool',
			subtype: '*',
			title: 'Tool Node',
			summary: 'Executes tool providers and emits tool result payloads.',
			planes: {
				data: section('Data Plane', 'Consumes tool inputs and publishes tool outputs.'),
				control: section('Control Plane', 'Reflects queue admission and terminal outcomes.'),
				param: section('Param Plane', 'Uses provider config, auth, and execution options.')
			}
		}
	},
	component: {
		'*': {
			schema_version: 1,
			node_kind: 'component',
			subtype: '*',
			title: 'Component Node',
			summary: 'Executes a versioned internal graph through its published API contract.',
			planes: {
				data: section('Data Plane', 'Routes published component inputs/outputs through exposed handles.'),
				control: section('Control Plane', 'Tracks component node scheduling and completion state.'),
				param: section('Param Plane', 'Uses revision pinning and component config payload.')
			}
		}
	}
};

