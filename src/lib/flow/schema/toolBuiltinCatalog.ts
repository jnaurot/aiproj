import type { ToolBuiltinProfileId } from './toolBuiltinProfiles';

export type ToolBuiltinOperation = {
	id: string;
	label: string;
	description: string;
	profiles: ToolBuiltinProfileId[];
	defaultArgs: Record<string, unknown>;
};

export const TOOL_BUILTIN_OPERATIONS: ToolBuiltinOperation[] = [
	{
		id: 'noop',
		label: 'No-op',
		description: 'Return { ok: true } with no side effects.',
		profiles: ['core', 'data', 'ml', 'llm_finetune', 'full', 'custom'],
		defaultArgs: {}
	},
	{
		id: 'echo',
		label: 'Echo Input',
		description: 'Return tool args and upstream input payload unchanged.',
		profiles: ['core', 'data', 'ml', 'llm_finetune', 'full', 'custom'],
		defaultArgs: {}
	},
	{
		id: 'validate_json',
		label: 'Validate JSON Type',
		description: 'Check whether payload is object/array and return { valid }.',
		profiles: ['core', 'data', 'ml', 'llm_finetune', 'full', 'custom'],
		defaultArgs: {}
	},
	{
		id: 'core.array.summary_stats',
		label: 'Array Summary Stats',
		description: 'Compute count/min/max/mean/std/p50/p95 from numeric array.',
		profiles: ['core', 'full', 'custom'],
		defaultArgs: { values: [1, 2, 3, 4] }
	},
	{
		id: 'core.array.normalize',
		label: 'Array Normalize',
		description: 'Normalize numeric array by zscore or minmax.',
		profiles: ['core', 'full', 'custom'],
		defaultArgs: { values: [1, 2, 3, 4], method: 'zscore' }
	},
	{
		id: 'core.json.validate_schema',
		label: 'JSON Validate Schema',
		description: 'Validate payload with dynamic Pydantic field schema.',
		profiles: ['core', 'full', 'custom'],
		defaultArgs: {
			payload: { name: 'alice', age: 42 },
			fields: {
				name: { type: 'string', required: true },
				age: { type: 'integer', required: true }
			}
		}
	},
	{
		id: 'core.datetime.parse',
		label: 'Datetime Parse',
		description: 'Parse date/time string into normalized fields.',
		profiles: ['core', 'full', 'custom'],
		defaultArgs: { value: '2026-03-09T18:00:00-05:00' }
	},
	{
		id: 'core.datetime.normalize_tz',
		label: 'Datetime Normalize TZ',
		description: 'Parse datetime and convert to target timezone.',
		profiles: ['core', 'full', 'custom'],
		defaultArgs: {
			value: '2026-03-09T18:00:00-05:00',
			target_tz: 'UTC'
		}
	},
	{
		id: 'core.http.request_json',
		label: 'HTTP Request JSON',
		description: 'Send HTTP request and parse JSON response. Requires perm.net=true.',
		profiles: ['core', 'full', 'custom'],
		defaultArgs: {
			url: 'https://example.com',
			method: 'GET',
			headers: {}
		}
	},
	{
		id: 'core.http.request_text',
		label: 'HTTP Request Text',
		description: 'Send HTTP request and return text response. Requires perm.net=true.',
		profiles: ['core', 'full', 'custom'],
		defaultArgs: {
			url: 'https://example.com',
			method: 'GET',
			headers: {}
		}
	},
	{
		id: 'data.pandas.profile',
		label: 'Pandas Profile',
		description: 'Summarize rows with columns, dtypes, null counts, and sample rows.',
		profiles: ['data', 'full', 'custom'],
		defaultArgs: {
			rows: [
				{ id: 1, city: 'Boston', score: 0.8 },
				{ id: 2, city: 'Austin', score: 0.6 }
			],
			sample_size: 5
		}
	},
	{
		id: 'data.pandas.select_columns',
		label: 'Pandas Select Columns',
		description: 'Project only selected columns from tabular rows.',
		profiles: ['data', 'full', 'custom'],
		defaultArgs: {
			rows: [
				{ id: 1, city: 'Boston', score: 0.8 },
				{ id: 2, city: 'Austin', score: 0.6 }
			],
			columns: ['id', 'score']
		}
	},
	{
		id: 'data.polars.profile',
		label: 'Polars Profile',
		description: 'Summarize rows using Polars typing/null-count semantics.',
		profiles: ['data', 'full', 'custom'],
		defaultArgs: {
			rows: [
				{ id: 1, city: 'Boston', score: 0.8 },
				{ id: 2, city: null, score: 0.6 }
			],
			sample_size: 5
		}
	},
	{
		id: 'data.pyarrow.schema',
		label: 'PyArrow Schema',
		description: 'Infer Arrow schema fields (name/type/nullable) from rows.',
		profiles: ['data', 'full', 'custom'],
		defaultArgs: {
			rows: [
				{ id: 1, city: 'Boston', score: 0.8 },
				{ id: 2, city: 'Austin', score: 0.6 }
			]
		}
	}
];

export function getBuiltinOperationById(toolId: string | null | undefined): ToolBuiltinOperation | null {
	const id = String(toolId ?? '').trim();
	if (!id) return null;
	return TOOL_BUILTIN_OPERATIONS.find((operation) => operation.id === id) ?? null;
}

export function getBuiltinOperationsForProfile(profileId: ToolBuiltinProfileId): ToolBuiltinOperation[] {
	return TOOL_BUILTIN_OPERATIONS.filter((operation) => operation.profiles.includes(profileId));
}
