import type { Node } from "@xyflow/svelte";
import type { PipelineNodeData } from "$lib/flow/types";
import { normalizeWithDefaults } from "$lib/flow/util/normalize";
import { defaultSourceParamsByKind } from "$lib/flow/schema/sourceDefaults";
import { SourceParamsSchemaByKind } from "$lib/flow/schema/source";
import { defaultLlmParamsByKind } from "$lib/flow/schema/llmDefaults";
import { LlmParamsSchemaByKind } from "$lib/flow/schema/llm";

// pick schema + defaults by kind
type Pick = {
  schema: unknown;
  defaults: unknown;
};

export function pickValidation(
  data: PipelineNodeData,
  patch: unknown,
  existing: unknown
): Pick {
  switch (data.kind) {
    case "source": {
      const sk = data.sourceKind ?? "file";
      return {
        schema: SourceParamsSchemaByKind[sk],
        defaults: defaultSourceParamsByKind[sk]
      };
    }

    // case "transform":
    //   return { schema: TransformParamsSchema, defaults: defaultTransformParamsByKind };

    case "llm": {
      const lk = data.llmKind ?? "ollama"
      return {
        schema: LlmParamsSchemaByKind[lk],
        defaults: defaultLlmParamsByKind[lk]
      };
    }

    // case "tool":
    //   return { schema: ToolParamsSchema, defaults: defaultToolParams };
    // default:
  }
}

export function updateNodeParamsValidated(
  nodes: Node<PipelineNodeData>[],
  nodeId: string,
  patch: unknown
): { nodes: Node<PipelineNodeData>[]; error?: string } {
  const node = nodes.find((n) => n.id === nodeId);
  if (!node) return { nodes, error: "Node not found" };

  const existing = node.data.params;
  const pick = pickValidation(node.data, patch, existing);

  const norm = normalizeWithDefaults(pick.schema as any, pick.defaults as any, existing, patch);

  if (norm.ok === false) {
    console.log("PARAMS NORMALIZATION ERROR:", norm.error);
    return { nodes, error: norm.error };
  }
  console.log("PARAMS NORMALIZED (stripped):", norm.value);

  const next = nodes.map((n) =>
    n.id === nodeId
      ? {
        ...n,
        data: {
          ...n.data,
          params: norm.value,
          meta: { ...n.data.meta, updatedAt: new Date().toISOString() }
        }
      }
      : n
  );

  return { nodes: next };
}
