// src/lib/flow/schema/llmDefaults.ts
import type { LlmKind, LlmParams, ModelKind } from "$lib/flow/schema/llm";

/**
 * Canonical default params by llmKind.
 * Keep this file dumb: stable defaults only.
 */
export const defaultLlmOllamaParams: LlmParams = {
    baseUrl: "http://192.168.12.251:11434",
    model: "llama3.1:8b",
    system_prompt: "",
    user_prompt: "Summarize the input data.",
    temperature: 0.7,
    debug: {
        enabled: false,
        log_input_preview: false,
        log_raw_output: false
    },
    output: { mode: "text" },
};

export const defaultLlmOpenAICompatParams: LlmParams = {
    // Pick an OpenAI-compatible baseUrl you actually use in dev.
    // If you're talking to real OpenAI, baseUrl can be "https://api.openai.com".
    baseUrl: "https://api.openai.com",
    model: "gpt-4o-mini", // change to your preferred default
    system_prompt: "",
    user_prompt: "Summarize the input data.",
    temperature: 0.7,
    debug: {
        enabled: false,
        log_input_preview: false,
        log_raw_output: false
    },
    output: { mode: "text" },
};

export const defaultLlmParamsByKind: Record<LlmKind, LlmParams> = {
    ollama: defaultLlmOllamaParams,
    openai_compat: defaultLlmOpenAICompatParams,
};

export const defaultLlmParams: LlmParams = defaultLlmOllamaParams;

export const defaultModelParamsByKind: Record<ModelKind, LlmParams> = {
	llm: defaultLlmOllamaParams,
	vision: {
		...defaultLlmOpenAICompatParams,
		model: "gpt-4.1-mini",
		user_prompt: "Describe the visual input."
	},
	audio: {
		...defaultLlmOpenAICompatParams,
		model: "gpt-4o-mini-transcribe",
		user_prompt: "Transcribe and summarize the audio input."
	},
	embedding: {
		...defaultLlmOpenAICompatParams,
		model: "text-embedding-3-small",
		user_prompt: "Generate embeddings for the input.",
		output: { mode: "embeddings", embedding: { dims: 1536 } }
	},
	reranker: {
		...defaultLlmOpenAICompatParams,
		model: "gpt-4o-mini",
		user_prompt: "Rank candidates by relevance to the query."
	},
	multimodal: {
		...defaultLlmOpenAICompatParams,
		model: "gpt-4o-mini",
		user_prompt: "Analyze the multimodal input and return the result."
	}
};

/**
 * Canonical default node.data for kind="model"
 * (Used by defaultNodeData("model") / addNode)
 */
export const defaultModelNodeData = {
    kind: "model" as const,
	modelKind: "llm" as const,
	taskKind: "generate" as const,
    llmKind: "ollama" as const,
    label: "Model",
    params: defaultModelParamsByKind.llm,
    status: "idle" as const,
} as const;

// Legacy alias for imported graphs that still materialize kind="llm".
export const defaultLlmNodeData = {
    ...defaultModelNodeData,
    kind: "llm" as const,
	modelKind: "llm" as const,
	taskKind: "generate" as const,
    label: "LLM",
} as const;
