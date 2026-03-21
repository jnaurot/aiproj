import type { ModelKind, ModelTaskKind } from '$lib/flow/schema/llm';

export type ModelGuidedControl = {
	id: string;
	label: string;
	description: string;
};

const TaskKindsByModelKind: Record<ModelKind, ModelTaskKind[]> = {
	llm: ['generate', 'classify', 'extract'],
	vision: ['caption', 'classify', 'extract', 'generate'],
	audio: ['transcribe', 'extract', 'classify'],
	embedding: ['embed'],
	reranker: ['rerank'],
	multimodal: ['generate', 'classify', 'extract', 'caption', 'transcribe']
};

export function taskKindsForModelKind(kind: ModelKind): ModelTaskKind[] {
	return TaskKindsByModelKind[kind] ?? TaskKindsByModelKind.llm;
}

export function guidedControlsForModelKind(kind: ModelKind): ModelGuidedControl[] {
	const common: ModelGuidedControl[] = [
		{ id: 'provider', label: 'Provider', description: 'Pick the runtime adapter/provider first.' },
		{ id: 'model', label: 'Model', description: 'Set model id/version used by this node.' },
		{ id: 'output', label: 'Output Mode', description: 'Choose text/json/embeddings contract.' }
	];
	const byKind: Record<ModelKind, ModelGuidedControl[]> = {
		llm: [
			{ id: 'task', label: 'Task', description: 'Choose generate/classify/extract behavior.' },
			{ id: 'prompt', label: 'Prompt', description: 'Set user prompt template and placeholders.' }
		],
		vision: [
			{ id: 'task', label: 'Task', description: 'Pick caption/classify/extract for image inputs.' },
			{ id: 'schema', label: 'JSON Schema', description: 'Define extract response shape for structured output.' }
		],
		audio: [
			{ id: 'task', label: 'Task', description: 'Pick transcribe/extract/classify for audio inputs.' },
			{ id: 'language', label: 'Prompt Hint', description: 'Provide transcript/extraction language hints.' }
		],
		embedding: [
			{ id: 'dims', label: 'Embedding Dims', description: 'Set dimensions expected downstream.' },
			{ id: 'dtype', label: 'Embedding Dtype', description: 'Select float precision for vector payloads.' }
		],
		reranker: [
			{ id: 'task', label: 'Task', description: 'Use rerank task with stable scoring prompt.' },
			{ id: 'strict', label: 'Strict Output', description: 'Keep stable score output formatting.' }
		],
		multimodal: [
			{ id: 'task', label: 'Task', description: 'Choose mixed-modality task (generate/extract/caption).' },
			{ id: 'schema', label: 'Output Schema', description: 'Pin structured output for robust chaining.' }
		]
	};
	return [...common, ...(byKind[kind] ?? byKind.llm)];
}
