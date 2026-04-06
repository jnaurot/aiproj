export type NodeDocQuickFieldKey =
	| 'source_kind'
	| 'file_name'
	| 'file_format'
	| 'delimiter'
	| 'has_header'
	| 'encoding'
	| 'api_url'
	| 'api_method'
	| 'api_auth_mode'
	| 'database_table'
	| 'database_query'
	| 'connection_ref'
	| 'object_bucket'
	| 'object_prefix'
	| 'warehouse_source'
	| 'transform_kind'
	| 'operation'
	| 'selected_fields'
	| 'group_by'
	| 'metrics'
	| 'provider'
	| 'model'
	| 'user_prompt'
	| 'temperature'
	| 'max_tokens'
	| 'output_mode'
	| 'output_strict'
	| 'tool_id'
	| 'tool_args'
	| 'component_id'
	| 'revision_id'
	| 'required_outputs';

type QuickFieldSet = Record<string, Set<NodeDocQuickFieldKey>>;

const WILDCARD = '*';

const quickFieldMap: Record<string, QuickFieldSet> = {
	source: {
		file: new Set<NodeDocQuickFieldKey>([
			'source_kind',
			'file_name',
			'file_format',
			'delimiter',
			'has_header',
			'encoding'
		]),
		api: new Set<NodeDocQuickFieldKey>(['source_kind', 'api_url', 'api_method', 'api_auth_mode']),
		database: new Set<NodeDocQuickFieldKey>(['source_kind', 'database_table', 'database_query', 'connection_ref']),
		object_store: new Set<NodeDocQuickFieldKey>(['source_kind', 'object_bucket', 'object_prefix']),
		warehouse: new Set<NodeDocQuickFieldKey>(['source_kind', 'warehouse_source']),
		[WILDCARD]: new Set<NodeDocQuickFieldKey>(['source_kind'])
	},
	transform: {
		select: new Set<NodeDocQuickFieldKey>(['transform_kind', 'operation', 'selected_fields']),
		project: new Set<NodeDocQuickFieldKey>(['transform_kind', 'operation', 'selected_fields']),
		json_filter: new Set<NodeDocQuickFieldKey>(['transform_kind', 'operation']),
		dedupe: new Set<NodeDocQuickFieldKey>(['transform_kind', 'operation', 'selected_fields']),
		aggregate: new Set<NodeDocQuickFieldKey>(['transform_kind', 'operation', 'group_by', 'metrics']),
		derive: new Set<NodeDocQuickFieldKey>(['transform_kind', 'operation']),
		[WILDCARD]: new Set<NodeDocQuickFieldKey>(['transform_kind', 'operation'])
	},
	model: {
		ollama: new Set<NodeDocQuickFieldKey>([
			'provider',
			'model',
			'user_prompt',
			'temperature',
			'max_tokens',
			'output_mode',
			'output_strict'
		]),
		openai_compat: new Set<NodeDocQuickFieldKey>([
			'provider',
			'model',
			'user_prompt',
			'temperature',
			'max_tokens',
			'output_mode',
			'output_strict'
		]),
		[WILDCARD]: new Set<NodeDocQuickFieldKey>([
			'provider',
			'model',
			'user_prompt',
			'temperature',
			'max_tokens',
			'output_mode',
			'output_strict'
		])
	},
	tool: {
		builtin: new Set<NodeDocQuickFieldKey>(['provider', 'tool_id', 'tool_args']),
		[WILDCARD]: new Set<NodeDocQuickFieldKey>(['provider', 'tool_id', 'tool_args'])
	},
	component: {
		graph_component: new Set<NodeDocQuickFieldKey>([
			'component_id',
			'revision_id',
			'required_outputs'
		]),
		[WILDCARD]: new Set<NodeDocQuickFieldKey>(['component_id', 'revision_id'])
	}
};

export function resolveQuickFieldsForNode(kindRaw: string, subtypeRaw: string): Set<NodeDocQuickFieldKey> {
	const kind = String(kindRaw ?? '').trim().toLowerCase();
	const subtype = String(subtypeRaw ?? '').trim().toLowerCase();
	const byKind = quickFieldMap[kind];
	if (!byKind) return new Set<NodeDocQuickFieldKey>();
	return byKind[subtype] ?? byKind[WILDCARD] ?? new Set<NodeDocQuickFieldKey>();
}

export function pickQuickFields(
	kindRaw: string,
	subtypeRaw: string,
	settings: Record<string, string>
): Record<string, string> {
	const allow = resolveQuickFieldsForNode(kindRaw, subtypeRaw);
	if (allow.size === 0) return {};
	const next: Record<string, string> = {};
	for (const [key, value] of Object.entries(settings)) {
		if (!allow.has(key as NodeDocQuickFieldKey)) continue;
		const text = String(value ?? '').trim();
		if (!text) continue;
		next[key] = text;
	}
	return next;
}

