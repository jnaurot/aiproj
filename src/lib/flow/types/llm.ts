// src/lib/flow/types/llm.ts
import type { BaseNodeData } from "./base";

export type LlmKind = "ollama" | "openai_compat";
export type LlmOutputMode = "text" | "json" | "markdown";

export type LlmOutput =
  | { mode: "text" | "markdown" }
  | { mode: "json"; jsonSchema: unknown };

export type LlmParams = {
  // endpoint selection (shared)
  baseUrl?: string;
  connection_ref?: string;

  model: string;

  system_prompt?: string;
  user_prompt: string;

  temperature?: number; // 0..2
  top_p?: number;       // 0..1
  max_tokens?: number;  // positive int
  seed?: number;        // int (provider-dependent)

  output: LlmOutput;
};

export type LlmNodeData = BaseNodeData<"llm", LlmParams> & {
  llmKind: LlmKind; // node-level discriminator (Source-style)
};
