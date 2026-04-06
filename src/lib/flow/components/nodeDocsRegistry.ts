import type { NodeDocV1 } from '$lib/flow/schema/nodeDocs';

type NodeDocKind = NodeDocV1['node_kind'];

export type NodeDocRegistry = Record<NodeDocKind, Record<string, NodeDocV1>>;

const section = (title: string, summary: string) => ({ title, summary, ports: [], notes: [] as string[] });

export const NODE_DOCS_REGISTRY: NodeDocRegistry = {
	source: {
		file: {
			schema_version: 1,
			node_kind: 'source',
			subtype: 'file',
			title: 'Source Node (File)',
			summary: 'Reads local/snapshot files and emits parsed records on the data plane.',
			planes: {
				data: section('Data Plane', 'Outputs records parsed from file snapshots (csv/json/text/binary).'),
				control: section('Control Plane', 'Signals upstream-open and completion for one-shot or queued work.'),
				param: section('Param Plane', 'Configured by file path/snapshot, format, encoding, and capability.')
			}
		},
		api: {
			schema_version: 1,
			node_kind: 'source',
			subtype: 'api',
			title: 'Source Node (API)',
			summary: 'Calls remote APIs and emits response payloads into the work plane.',
			planes: {
				data: section('Data Plane', 'Publishes decoded API response payloads.'),
				control: section('Control Plane', 'Reports API fetch scheduling and blocked/ready transitions.'),
				param: section('Param Plane', 'Configured by url/method/auth/query/header/body options.')
			}
		},
		database: {
			schema_version: 1,
			node_kind: 'source',
			subtype: 'database',
			title: 'Source Node (Database)',
			summary: 'Executes database reads and emits query/table results.',
			planes: {
				data: section('Data Plane', 'Emits rows returned by query/table scans.'),
				control: section('Control Plane', 'Tracks query admission, retries, and completion.'),
				param: section('Param Plane', 'Configured by connection/table/query and execution mode.')
			}
		},
		object_store: {
			schema_version: 1,
			node_kind: 'source',
			subtype: 'object_store',
			title: 'Source Node (Object Store)',
			summary: 'Reads objects from bucket/blob stores and emits records/content.',
			planes: {
				data: section('Data Plane', 'Publishes object payloads to downstream nodes.'),
				control: section('Control Plane', 'Signals object listing/download progress and completion.'),
				param: section('Param Plane', 'Configured by bucket/container/key/prefix credentials.')
			}
		},
		warehouse: {
			schema_version: 1,
			node_kind: 'source',
			subtype: 'warehouse',
			title: 'Source Node (Warehouse)',
			summary: 'Reads analytical warehouse results and emits tabular outputs.',
			planes: {
				data: section('Data Plane', 'Outputs warehouse query result sets.'),
				control: section('Control Plane', 'Tracks warehouse job scheduling and state.'),
				param: section('Param Plane', 'Configured by sql/query profile and warehouse connection.')
			}
		},
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
		json_filter: {
			schema_version: 1,
			node_kind: 'transform',
			subtype: 'json_filter',
			title: 'Transform Node (JSON Filter)',
			summary: 'Evaluates JSON rules and routes pass/reject outputs.',
			planes: {
				data: section('Data Plane', 'Consumes json and emits filtered json (+ optional reject output).'),
				control: section('Control Plane', 'Tracks blocked reasons and queued items.'),
				param: section('Param Plane', 'Configured by rule mode, conditions, and strictness.')
			}
		},
		select: {
			schema_version: 1,
			node_kind: 'transform',
			subtype: 'select',
			title: 'Transform Node (Select)',
			summary: 'Selects columns/fields from incoming tabular payloads.',
			planes: {
				data: section('Data Plane', 'Outputs projected records with selected fields.'),
				control: section('Control Plane', 'Reports readiness and drain behavior.'),
				param: section('Param Plane', 'Configured by selected field list and schema expectations.')
			}
		},
		derive: {
			schema_version: 1,
			node_kind: 'transform',
			subtype: 'derive',
			title: 'Transform Node (Derive)',
			summary: 'Adds derived columns from existing fields.',
			planes: {
				data: section('Data Plane', 'Emits rows with computed columns.'),
				control: section('Control Plane', 'Reflects transform execution lifecycle.'),
				param: section('Param Plane', 'Configured by derive expressions and type coercion.')
			}
		},
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
		ollama: {
			schema_version: 1,
			node_kind: 'model',
			subtype: 'ollama',
			title: 'Model Node (Ollama)',
			summary: 'Runs Ollama chat inference with optional JSON strict output.',
			planes: {
				data: section('Data Plane', 'Consumes items and emits model responses.'),
				control: section('Control Plane', 'Uses LLM lease arbitration and runtime blocked states.'),
				param: section('Param Plane', 'Configured by baseUrl/model/prompt/policy settings.')
			}
		},
		openai_compat: {
			schema_version: 1,
			node_kind: 'model',
			subtype: 'openai_compat',
			title: 'Model Node (OpenAI-compatible)',
			summary: 'Runs OpenAI-compatible chat/completions inference.',
			planes: {
				data: section('Data Plane', 'Consumes upstream inputs and emits model output payloads.'),
				control: section('Control Plane', 'Tracks lease/queue state and node lifecycle.'),
				param: section('Param Plane', 'Configured by API base URL, model id, and prompt controls.')
			}
		},
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
		builtin: {
			schema_version: 1,
			node_kind: 'tool',
			subtype: 'builtin',
			title: 'Tool Node (Built-in)',
			summary: 'Executes built-in tool capabilities.',
			planes: {
				data: section('Data Plane', 'Consumes tool input payloads and emits tool results.'),
				control: section('Control Plane', 'Tracks execution attempts, retries, and completion.'),
				param: section('Param Plane', 'Configured by provider-specific tool options.')
			}
		},
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
		graph_component: {
			schema_version: 1,
			node_kind: 'component',
			subtype: 'graph_component',
			title: 'Component Node (Graph Component)',
			summary: 'Runs a published component revision with stable exposed handles.',
			planes: {
				data: section('Data Plane', 'Maps exposed inputs/outputs to internal node handles.'),
				control: section('Control Plane', 'Tracks component runtime scheduling and completion.'),
				param: section('Param Plane', 'Configured by revision pinning and component config payload.')
			}
		},
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
