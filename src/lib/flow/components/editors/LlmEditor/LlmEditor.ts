// src/lib/flow/components/editors/LlmEditor/LlmEditor.ts
import LlmOllamaEditor from './LlmOllamaEditor.svelte';
import LlmOpenAIEditor from './LlmOpenAIEditor.svelte';

export const LlmEditorByKind = {
    ollama: LlmOllamaEditor,
    openai_compat: LlmOpenAIEditor
} as const;
