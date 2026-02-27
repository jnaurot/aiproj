// src/lib/flow/schema/llmDefaults.ts
import type { LlmKind, LlmParams } from "$lib/flow/schema/llm";

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
    output: { mode: "text" },
};

export const defaultLlmParamsByKind: Record<LlmKind, LlmParams> = {
    ollama: defaultLlmOllamaParams,
    openai_compat: defaultLlmOpenAICompatParams,
};

export const defaultLlmParams: LlmParams = defaultLlmOllamaParams;

/**
 * Canonical default node.data for kind="llm"
 * (Used by defaultNodeData("llm") / addNode)
 */
export const defaultLlmNodeData = {
    kind: "llm" as const,
    llmKind: "ollama" as const,
    label: "LLM",
    params: defaultLlmParams,
    status: "idle" as const,
    ports: { in: "text" as const, out: "text" as const },
} as const;
