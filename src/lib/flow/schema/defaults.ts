// src/lib/flow/schema/defaults.ts
import type { NodeKind } from "$lib/flow/types/base";
import type { PipelineNodeData } from "$lib/flow/types";

import { defaultSourceNodeData } from "$lib/flow/schema/sourceDefaults";
import { defaultTransformNodeData } from "$lib/flow/schema/transformDefaults";
import { defaultLlmNodeData } from "$lib/flow/schema/llmDefaults";
import { defaultToolNodeData } from "$lib/flow/schema/toolDefaults";

export function defaultNodeData(kind: NodeKind): PipelineNodeData {
  switch (kind) {
    case "source":
      return structuredClone(defaultSourceNodeData) as any;

    case "transform":
      return structuredClone(defaultTransformNodeData) as any;

    case "llm":
      return structuredClone(defaultLlmNodeData) as any;

    case "tool":
      return structuredClone(defaultToolNodeData) as any;

    default: {
      const _exhaustive: never = kind;
      return _exhaustive;
    }
  }
}

