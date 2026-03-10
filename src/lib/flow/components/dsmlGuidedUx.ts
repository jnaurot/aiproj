import type { Node } from '@xyflow/svelte';

import type { PipelineNodeData, NodeKind, PortType } from '$lib/flow/types';
import type { SourceKind, TransformKind, ToolProvider } from '$lib/flow/types/paramsMap';

export type GuidedStarterTemplateNode = {
	id: string;
	kind: NodeKind;
	label: string;
	position: { x: number; y: number };
	sourceKind?: SourceKind;
	transformKind?: TransformKind;
	toolProvider?: ToolProvider;
	params: Record<string, unknown>;
	ports: { in: PortType | null; out: PortType | null };
};

export type GuidedStarterTemplate = {
	id: string;
	name: string;
	description: string;
	nodes: GuidedStarterTemplateNode[];
	edges: Array<{ id: string; source: string; target: string }>;
};

export type GuidedOperationPreset = {
	id: string;
	name: string;
	description: string;
	kind: NodeKind;
	sourceKind?: SourceKind;
	transformKind?: TransformKind;
	toolProvider?: ToolProvider;
	params: Record<string, unknown>;
	ports: { in?: PortType | null; out?: PortType | null };
};

export type GuidedRecommendation = {
	id: string;
	label: string;
	description: string;
	action: 'add_node' | 'open_template' | 'apply_preset' | 'run';
	nodeKind?: NodeKind;
	presetId?: string;
};

export const DSML_STARTER_TEMPLATES: GuidedStarterTemplate[] = [
	{
		id: 'starter_classification_baseline',
		name: 'Classification Baseline',
		description: 'Source -> select features -> train classifier',
		nodes: [
			{
				id: 'src',
				kind: 'source',
				label: 'Training Data',
				position: { x: 0, y: 0 },
				sourceKind: 'file',
				params: {
					source_type: 'file',
					filename: 'train.csv',
					file_format: 'csv',
					output_mode: 'table',
					cache_enabled: true,
				},
				ports: { in: null, out: 'table' },
			},
			{
				id: 'select',
				kind: 'transform',
				label: 'Select Features',
				position: { x: 320, y: 0 },
				transformKind: 'select',
				params: {
					op: 'select',
					select: { mode: 'include', strict: true, columns: ['x1', 'x2', 'label'] },
				},
				ports: { in: 'table', out: 'table' },
			},
			{
				id: 'train',
				kind: 'tool',
				label: 'Train Classifier',
				position: { x: 640, y: 0 },
				toolProvider: 'builtin',
				params: {
					provider: 'builtin',
					builtin: {
						profileId: 'ml',
						toolId: 'ml.sklearn.train_classifier',
						args: {
							label_col: 'label',
							feature_cols: ['x1', 'x2'],
							max_iter: 200,
						},
					},
				},
				ports: { in: 'table', out: 'json' },
			},
		],
		edges: [
			{ id: 'e_src_select', source: 'src', target: 'select' },
			{ id: 'e_select_train', source: 'select', target: 'train' },
		],
	},
	{
		id: 'starter_regression_baseline',
		name: 'Regression Baseline',
		description: 'Source -> quality gate -> train regressor',
		nodes: [
			{
				id: 'src',
				kind: 'source',
				label: 'Training Data',
				position: { x: 0, y: 0 },
				sourceKind: 'file',
				params: {
					source_type: 'file',
					filename: 'train.csv',
					file_format: 'csv',
					output_mode: 'table',
					cache_enabled: true,
				},
				ports: { in: null, out: 'table' },
			},
			{
				id: 'quality',
				kind: 'transform',
				label: 'Quality Gate',
				position: { x: 320, y: 0 },
				transformKind: 'quality_gate',
				params: {
					op: 'quality_gate',
					quality_gate: {
						failOn: 'error',
						checks: [
							{ kind: 'null_pct', column: 'target', maxNullPct: 0.02, severity: 'error' },
						],
					},
				},
				ports: { in: 'table', out: 'table' },
			},
			{
				id: 'train',
				kind: 'tool',
				label: 'Train Regressor',
				position: { x: 640, y: 0 },
				toolProvider: 'builtin',
				params: {
					provider: 'builtin',
					builtin: {
						profileId: 'ml',
						toolId: 'ml.sklearn.train_regressor',
						args: {
							label_col: 'target',
							feature_cols: ['x1', 'x2'],
						},
					},
				},
				ports: { in: 'table', out: 'json' },
			},
		],
		edges: [
			{ id: 'e_src_quality', source: 'src', target: 'quality' },
			{ id: 'e_quality_train', source: 'quality', target: 'train' },
		],
	},
];

export const DSML_OPERATION_PRESETS: GuidedOperationPreset[] = [
	{
		id: 'preset_transform_quality_gate_basic',
		name: 'Quality Gate (Basic)',
		description: 'Fail on high null% in target column.',
		kind: 'transform',
		transformKind: 'quality_gate',
		params: {
			op: 'quality_gate',
			quality_gate: {
				failOn: 'error',
				checks: [{ kind: 'null_pct', column: 'target', maxNullPct: 0.02, severity: 'error' }],
			},
		},
		ports: { in: 'table', out: 'table' },
	},
	{
		id: 'preset_tool_ml_train_classifier',
		name: 'ML Train Classifier',
		description: 'Builtin sklearn LogisticRegression training preset.',
		kind: 'tool',
		toolProvider: 'builtin',
		params: {
			provider: 'builtin',
			builtin: {
				profileId: 'ml',
				toolId: 'ml.sklearn.train_classifier',
				args: { label_col: 'label', feature_cols: ['x1', 'x2'], max_iter: 200 },
			},
		},
		ports: { in: 'table', out: 'json' },
	},
	{
		id: 'preset_tool_ml_train_regressor',
		name: 'ML Train Regressor',
		description: 'Builtin sklearn LinearRegression training preset.',
		kind: 'tool',
		toolProvider: 'builtin',
		params: {
			provider: 'builtin',
			builtin: {
				profileId: 'ml',
				toolId: 'ml.sklearn.train_regressor',
				args: { label_col: 'target', feature_cols: ['x1', 'x2'] },
			},
		},
		ports: { in: 'table', out: 'json' },
	},
];

export function getStarterTemplateById(templateId: string): GuidedStarterTemplate | null {
	const id = String(templateId ?? '').trim();
	if (!id) return null;
	return DSML_STARTER_TEMPLATES.find((t) => t.id === id) ?? null;
}

export function getOperationPresetsForKind(kind: NodeKind | null | undefined): GuidedOperationPreset[] {
	const k = String(kind ?? '').trim();
	if (!k) return [];
	return DSML_OPERATION_PRESETS.filter((preset) => preset.kind === k);
}

export function recommendNextStep(
	nodes: Node<PipelineNodeData>[],
	selectedKind: NodeKind | null | undefined
): GuidedRecommendation {
	const kinds = new Set(nodes.map((n) => String(n.data?.kind ?? '')));
	if (!kinds.has('source')) {
		return {
			id: 'rec_add_source',
			label: 'Start with a Source node',
			description: 'Pipelines are easiest to reason about when data ingress is explicit.',
			action: 'add_node',
			nodeKind: 'source',
		};
	}
	if (!kinds.has('transform')) {
		return {
			id: 'rec_add_transform',
			label: 'Add a Transform step',
			description: 'Add cleaning/feature prep before training or evaluation.',
			action: 'add_node',
			nodeKind: 'transform',
		};
	}
	if (!kinds.has('tool')) {
		return {
			id: 'rec_add_tool',
			label: 'Add an ML Tool step',
			description: 'Use a builtin ML operation to train or evaluate.',
			action: 'add_node',
			nodeKind: 'tool',
		};
	}
	if (selectedKind === 'tool') {
		return {
			id: 'rec_apply_tool_preset',
			label: 'Apply an ML operation preset',
			description: 'Quickly configure the selected tool for train/eval defaults.',
			action: 'apply_preset',
		};
	}
	return {
		id: 'rec_run_pipeline',
		label: 'Run the pipeline',
		description: 'Execute now and inspect typed artifacts/results.',
		action: 'run',
	};
}
