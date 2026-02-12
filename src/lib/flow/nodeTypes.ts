import SourceNode from "./nodes/SourceNode.svelte";
import TransformNode from "./nodes/TransformNode.svelte";
import LLMNode from "./nodes/LLMNode.svelte";
import ToolNode from "./nodes/ToolNode.svelte";

export const nodeTypes = {
  source: SourceNode,
  transform: TransformNode,
  llm: LLMNode,
  tool: ToolNode,
} as const;
