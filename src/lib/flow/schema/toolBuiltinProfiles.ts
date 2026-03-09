export const TOOL_BUILTIN_PROFILE_IDS = [
	'core',
	'data',
	'ml',
	'llm_finetune',
	'full',
	'custom',
] as const;

export type ToolBuiltinProfileId = (typeof TOOL_BUILTIN_PROFILE_IDS)[number];

export type ToolBuiltinProfileDefinition = {
	id: ToolBuiltinProfileId;
	label: string;
	description: string;
	packages: string[];
};

export const TOOL_BUILTIN_PROFILES: ToolBuiltinProfileDefinition[] = [
	{
		id: 'core',
		label: 'Core',
		description: 'Safe default for lightweight scripts and utilities.',
		packages: ['numpy', 'requests', 'pydantic', 'python-dateutil']
	},
	{
		id: 'data',
		label: 'Data',
		description: 'Tabular and file-heavy data workflows.',
		packages: ['polars', 'pandas', 'pyarrow', 'openpyxl', 'duckdb']
	},
	{
		id: 'ml',
		label: 'ML',
		description: 'Classical ML and basic evaluation workflows.',
		packages: ['scikit-learn', 'scipy', 'xgboost', 'lightgbm', 'matplotlib', 'seaborn']
	},
	{
		id: 'llm_finetune',
		label: 'LLM Fine-tune',
		description: 'Model training, finetuning, and adapter workflows.',
		packages: [
			'torch',
			'transformers',
			'datasets',
			'accelerate',
			'peft',
			'trl',
			'bitsandbytes',
			'sentencepiece',
			'evaluate'
		]
	},
	{
		id: 'full',
		label: 'Full AI Stack',
		description: 'Combined data + ML + finetuning bundle.',
		packages: [
			'numpy',
			'requests',
			'pydantic',
			'python-dateutil',
			'polars',
			'pandas',
			'pyarrow',
			'openpyxl',
			'duckdb',
			'scikit-learn',
			'scipy',
			'xgboost',
			'lightgbm',
			'matplotlib',
			'seaborn',
			'torch',
			'transformers',
			'datasets',
			'accelerate',
			'peft',
			'trl',
			'bitsandbytes',
			'sentencepiece',
			'evaluate'
		]
	},
	{
		id: 'custom',
		label: 'Custom',
		description: 'Manually choose package list for this tool.',
		packages: []
	}
];

export function getBuiltinProfileById(profileId: string | null | undefined): ToolBuiltinProfileDefinition {
	return (
		TOOL_BUILTIN_PROFILES.find((profile) => profile.id === profileId) ??
		TOOL_BUILTIN_PROFILES[0]
	);
}

