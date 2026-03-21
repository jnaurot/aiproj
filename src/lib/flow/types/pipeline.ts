// src/lib/flow/types/pipeline.ts

import type { SourceNodeData } from "./source";
import type { TransformNodeData } from "./transform";
import type { LlmNodeData, ModelNodeData } from "./llm";
import type { ToolNodeData } from "./tool";
import type { ComponentNodeData } from "./component";
import type { Node } from "@xyflow/svelte";

export type PipelineNodeData =
    | SourceNodeData
    | TransformNodeData
    | ModelNodeData
    | LlmNodeData
    | ToolNodeData
    | ComponentNodeData;

export type PipelineNode = Node<PipelineNodeData>;
