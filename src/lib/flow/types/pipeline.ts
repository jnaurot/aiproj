// src/lib/flow/types/pipeline.ts

import type { SourceNodeData } from "./source";
import type { TransformNodeData } from "./transform";
import type { LlmNodeData } from "./llm";
import type { ToolNodeData } from "./tool";
import type { Node } from "@xyflow/svelte";

export type PipelineNodeData =
    | SourceNodeData
    | TransformNodeData
    | LlmNodeData
    | ToolNodeData;

export type PipelineNode = Node<PipelineNodeData>;