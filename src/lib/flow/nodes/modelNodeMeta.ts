import type { LlmNodeData, ModelNodeData } from '$lib/flow/types';

type ModelLikeNodeData = LlmNodeData | ModelNodeData;

export type ModelNodeMeta = {
	model: string;
	modelKind: string;
	taskKind: string;
	provider: string;
	outputMode: string;
};

export function modelNodeMeta(data: ModelLikeNodeData | null | undefined): ModelNodeMeta {
	const params = (data?.params ?? {}) as Record<string, unknown>;
	const output = (params.output && typeof params.output === 'object' ? params.output : {}) as Record<
		string,
		unknown
	>;
	return {
		model: String(params.model ?? '—'),
		modelKind: String((data as any)?.modelKind ?? 'llm'),
		taskKind: String((data as any)?.taskKind ?? 'generate'),
		provider: String((data as any)?.llmKind ?? 'ollama'),
		outputMode: String(output.mode ?? 'text')
	};
}
